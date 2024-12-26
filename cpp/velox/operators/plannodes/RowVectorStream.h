/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#pragma once

#include "compute/ResultIterator.h"
#include "memory/VeloxColumnarBatch.h"
#include "velox/exec/Driver.h"
#include "velox/exec/Operator.h"
#include "velox/exec/Task.h"

namespace {

class SuspendedSection {
 public:
  explicit SuspendedSection(facebook::velox::exec::Driver* driver) : driver_(driver) {
    if (driver_->task()->enterSuspended(driver->state()) != facebook::velox::exec::StopReason::kNone) {
      VELOX_FAIL("Terminate detected when entering suspended section");
    }
  }

  virtual ~SuspendedSection() {
    if (driver_->task()->leaveSuspended(driver_->state()) != facebook::velox::exec::StopReason::kNone) {
      LOG(WARNING) << "Terminate detected when leaving suspended section for driver " << driver_->driverCtx()->driverId
                   << " from task " << driver_->task()->taskId();
    }
  }

 private:
  facebook::velox::exec::Driver* const driver_;
};

} // namespace

namespace gluten {

class RowVectorStream {
 public:
  explicit RowVectorStream(
      facebook::velox::exec::DriverCtx* driverCtx,
      facebook::velox::memory::MemoryPool* pool,
      ResultIterator* iterator,
      const facebook::velox::RowTypePtr& outputType)
      : driverCtx_(driverCtx), pool_(pool), outputType_(outputType), iterator_(iterator) {}

  bool hasNext() {
    if (finished_) {
      return false;
    }
    bool hasNext;
    {
      // We are leaving Velox task execution and are probably entering Spark code through JNI. Suspend the current
      // driver to make the current task open to spilling.
      //
      // When a task is getting spilled, it should have been suspended so has zero running threads, otherwise there's
      // possibility that this spill call hangs. See https://github.com/apache/incubator-gluten/issues/7243.
      // As of now, non-zero running threads usually happens when:
      // 1. Task A spills task B;
      // 2. Task A tries to grow buffers created by task B, during which spill is requested on task A again.
      SuspendedSection ss(driverCtx_->driver);
      hasNext = iterator_->hasNext();
    }
    if (!hasNext) {
      finished_ = true;
    }
    return hasNext;
  }

  // Convert arrow batch to row vector and use new output columns
  facebook::velox::RowVectorPtr next() {
    if (finished_) {
      return nullptr;
    }
    std::shared_ptr<ColumnarBatch> cb;
    {
      // We are leaving Velox task execution and are probably entering Spark code through JNI. Suspend the current
      // driver to make the current task open to spilling.
      SuspendedSection ss(driverCtx_->driver);
      cb = iterator_->next();
    }
    const std::shared_ptr<VeloxColumnarBatch>& vb = VeloxColumnarBatch::from(pool_, cb);
    auto vp = vb->getRowVector();
    VELOX_DCHECK(vp != nullptr);
    return std::make_shared<facebook::velox::RowVector>(
        vp->pool(), outputType_, facebook::velox::BufferPtr(0), vp->size(), vp->children());
  }

 private:
  facebook::velox::exec::DriverCtx* driverCtx_;
  facebook::velox::memory::MemoryPool* pool_;
  const facebook::velox::RowTypePtr outputType_;
  ResultIterator* iterator_;

  bool finished_{false};
};

class ValueStreamNode final : public facebook::velox::core::PlanNode {
 public:
  ValueStreamNode(
      const facebook::velox::core::PlanNodeId& id,
      const facebook::velox::RowTypePtr& outputType,
      std::shared_ptr<ResultIterator> iterator)
      : facebook::velox::core::PlanNode(id), outputType_(outputType), iterator_(std::move(iterator)) {}

  const facebook::velox::RowTypePtr& outputType() const override {
    return outputType_;
  }

  const std::vector<facebook::velox::core::PlanNodePtr>& sources() const override {
    return kEmptySources_;
  };

  ResultIterator* iterator() const {
    return iterator_.get();
  }

  std::string_view name() const override {
    return "ValueStream";
  }

  folly::dynamic serialize() const override {
    VELOX_UNSUPPORTED("ValueStream plan node is not serializable");
  }

 private:
  void addDetails(std::stringstream& stream) const override{};

  const facebook::velox::RowTypePtr outputType_;
  std::shared_ptr<ResultIterator> iterator_;
  const std::vector<facebook::velox::core::PlanNodePtr> kEmptySources_;
};

class ValueStream : public facebook::velox::exec::SourceOperator {
 public:
  ValueStream(
      int32_t operatorId,
      facebook::velox::exec::DriverCtx* driverCtx,
      std::shared_ptr<const ValueStreamNode> valueStreamNode)
      : facebook::velox::exec::SourceOperator(
            driverCtx,
            valueStreamNode->outputType(),
            operatorId,
            valueStreamNode->id(),
            valueStreamNode->name().data()) {
    ResultIterator* itr = valueStreamNode->iterator();
    VELOX_CHECK_NOT_NULL(itr);
    rvStream_ = std::make_unique<RowVectorStream>(driverCtx, pool(), itr, outputType_);
  }

  facebook::velox::RowVectorPtr getOutput() override {
    if (finished_) {
      return nullptr;
    }
    if (rvStream_->hasNext()) {
      return rvStream_->next();
    } else {
      finished_ = true;
      return nullptr;
    }
  }

  facebook::velox::exec::BlockingReason isBlocked(facebook::velox::ContinueFuture* /* unused */) override {
    return facebook::velox::exec::BlockingReason::kNotBlocked;
  }

  bool isFinished() override {
    return finished_;
  }

 private:
  bool finished_ = false;
  std::unique_ptr<RowVectorStream> rvStream_;
};

class RowVectorStreamOperatorTranslator : public facebook::velox::exec::Operator::PlanNodeTranslator {
  std::unique_ptr<facebook::velox::exec::Operator> toOperator(
      facebook::velox::exec::DriverCtx* ctx,
      int32_t id,
      const facebook::velox::core::PlanNodePtr& node) override {
    if (auto valueStreamNode = std::dynamic_pointer_cast<const ValueStreamNode>(node)) {
      return std::make_unique<ValueStream>(id, ctx, valueStreamNode);
    }
    return nullptr;
  }
};
} // namespace gluten
