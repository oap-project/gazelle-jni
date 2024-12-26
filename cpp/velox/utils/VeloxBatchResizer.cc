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

#include "VeloxBatchResizer.h"

namespace gluten {
namespace {

class SliceRowVector : public ColumnarBatchIterator {
 public:
  SliceRowVector(int32_t maxOutputBatchSize, facebook::velox::RowVectorPtr in)
      : maxOutputBatchSize_(maxOutputBatchSize), in_(in) {}

  std::shared_ptr<ColumnarBatch> next() override {
    int32_t remainingLength = in_->size() - cursor_;
    GLUTEN_CHECK(remainingLength >= 0, "Invalid state");
    if (remainingLength == 0) {
      return nullptr;
    }
    int32_t sliceLength = std::min(maxOutputBatchSize_, remainingLength);
    auto out = std::dynamic_pointer_cast<facebook::velox::RowVector>(in_->slice(cursor_, sliceLength));
    cursor_ += sliceLength;
    GLUTEN_CHECK(out != nullptr, "Invalid state");
    return std::make_shared<VeloxColumnarBatch>(out);
  }

 private:
  int32_t maxOutputBatchSize_;
  facebook::velox::RowVectorPtr in_;
  int32_t cursor_ = 0;
};
} // namespace

gluten::VeloxBatchResizer::VeloxBatchResizer(
    facebook::velox::memory::MemoryPool* pool,
    int32_t minOutputBatchSize,
    int32_t maxOutputBatchSize,
    std::unique_ptr<ColumnarBatchIterator> in)
    : pool_(pool),
      minOutputBatchSize_(minOutputBatchSize),
      maxOutputBatchSize_(maxOutputBatchSize),
      in_(std::move(in)) {
  GLUTEN_CHECK(
      minOutputBatchSize_ > 0 && maxOutputBatchSize_ > 0,
      "Either minOutputBatchSize or maxOutputBatchSize should be larger than 0");
}

std::shared_ptr<ColumnarBatch> VeloxBatchResizer::next() {
  if (next_) {
    auto next = next_->next();
    if (next != nullptr) {
      return next;
    }
    // Cached output was drained. Continue reading data from input iterator.
    next_ = nullptr;
  }

  auto cb = in_->next();
  if (cb == nullptr) {
    // Input iterator was drained.
    return nullptr;
  }

  if (cb->numRows() < minOutputBatchSize_) {
    auto vb = VeloxColumnarBatch::from(pool_, cb);
    auto rv = vb->getRowVector();
    auto buffer = facebook::velox::RowVector::createEmpty(rv->type(), pool_);
    buffer->append(rv.get());

    for (auto nextCb = in_->next(); nextCb != nullptr; nextCb = in_->next()) {
      auto nextVb = VeloxColumnarBatch::from(pool_, nextCb);
      auto nextRv = nextVb->getRowVector();
      if (buffer->size() + nextRv->size() > maxOutputBatchSize_) {
        GLUTEN_CHECK(next_ == nullptr, "Invalid state");
        next_ = std::make_unique<SliceRowVector>(maxOutputBatchSize_, nextRv);
        return std::make_shared<VeloxColumnarBatch>(buffer);
      }
      buffer->append(nextRv.get());
      if (buffer->size() >= minOutputBatchSize_) {
        // Buffer is full.
        break;
      }
    }
    return std::make_shared<VeloxColumnarBatch>(buffer);
  }

  if (cb->numRows() > maxOutputBatchSize_) {
    auto vb = VeloxColumnarBatch::from(pool_, cb);
    auto rv = vb->getRowVector();
    GLUTEN_CHECK(next_ == nullptr, "Invalid state");
    next_ = std::make_unique<SliceRowVector>(maxOutputBatchSize_, rv);
    auto next = next_->next();
    GLUTEN_CHECK(next != nullptr, "Invalid state");
    return next;
  }

  // Fast flush path.
  return cb;
}

int64_t VeloxBatchResizer::spillFixedSize(int64_t size) {
  return in_->spillFixedSize(size);
}

} // namespace gluten
