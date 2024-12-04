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

#include <glog/logging.h>

#include "compute/ProtobufUtils.h"
#include "compute/ResultIterator.h"
#include "memory/ArrowMemoryPool.h"
#include "memory/ColumnarBatch.h"
#include "memory/MemoryManager.h"
#include "operators/c2r/ColumnarToRow.h"
#include "operators/r2c/RowToColumnar.h"
#include "operators/serializer/ColumnarBatchSerializer.h"
#include "operators/writer/ArrowWriter.h"
#include "shuffle/ShuffleReader.h"
#include "shuffle/ShuffleWriter.h"
#include "substrait/plan.pb.h"
#include "utils/ObjectStore.h"

namespace gluten {

class ResultIterator;

struct SparkTaskInfo {
  int32_t stageId{0};
  int32_t partitionId{0};
  // Same as TID.
  int64_t taskId{0};

  std::string toString() const {
    return "[Stage: " + std::to_string(stageId) + " TID: " + std::to_string(taskId) + "]";
  }

  friend std::ostream& operator<<(std::ostream& os, const SparkTaskInfo& taskInfo) {
    os << "[Stage: " << taskInfo.stageId << " TID: " << taskInfo.taskId << "]";
    return os;
  }
};

class Runtime : public std::enable_shared_from_this<Runtime> {
 public:
  using Factory = std::function<Runtime*(
      const std::string& kind,
      MemoryManager* memoryManager,
      const std::unordered_map<std::string, std::string>& sessionConf)>;
  using Releaser = std::function<void(Runtime*)>;
  static void registerFactory(const std::string& kind, Factory factory, Releaser releaser);
  static Runtime* create(
      const std::string& kind,
      MemoryManager* memoryManager,
      const std::unordered_map<std::string, std::string>& sessionConf = {});
  static void release(Runtime*);
  static std::optional<std::string>* localWriteFilesTempPath();

  Runtime(
      const std::string& kind,
      MemoryManager* memoryManager,
      const std::unordered_map<std::string, std::string>& confMap)
      : kind_(kind), memoryManager_(memoryManager), confMap_(confMap) {}

  virtual ~Runtime() = default;

  virtual std::string kind() {
    return kind_;
  }

  virtual void parsePlan(const uint8_t* data, int32_t size, std::optional<std::string> dumpFile) {
    throw GlutenException("Not implemented");
}

  virtual void parseSplitInfo(const uint8_t* data, int32_t size, std::optional<std::string> dumpFile)   {
    throw GlutenException("Not implemented");
  }

  virtual std::string planString(bool details, const std::unordered_map<std::string, std::string>& sessionConf) {
    throw GlutenException("Not implemented");
  }

  // Just for benchmark
  ::substrait::Plan& getPlan() {
    return substraitPlan_;
  }

  virtual std::shared_ptr<ResultIterator> createResultIterator(
      const std::string& spillDir,
      const std::vector<std::shared_ptr<ResultIterator>>& inputs,
      const std::unordered_map<std::string, std::string>& sessionConf) {
    throw GlutenException("Not implemented");
  }

  virtual std::shared_ptr<ColumnarBatch> createOrGetEmptySchemaBatch(int32_t numRows) {
    throw GlutenException("Not implemented");
  }

  virtual std::shared_ptr<ColumnarBatch> select(std::shared_ptr<ColumnarBatch>, const std::vector<int32_t>&) {
    throw GlutenException("Not implemented");
  }

  virtual MemoryManager* memoryManager() {
    return memoryManager_;
  };

  /// This function is used to create certain converter from the format used by
  /// the backend to Spark unsafe row.
  virtual std::shared_ptr<ColumnarToRowConverter> createColumnar2RowConverter(int64_t column2RowMemThreshold) {
    throw GlutenException("Not implemented");
  }

  virtual std::shared_ptr<RowToColumnarConverter> createRow2ColumnarConverter(struct ArrowSchema* cSchema) {
    throw GlutenException("Not implemented");
  }

  virtual std::shared_ptr<ShuffleWriter> createShuffleWriter(
      int numPartitions,
      std::unique_ptr<PartitionWriter> partitionWriter,
      ShuffleWriterOptions options) {
    throw GlutenException("Not implemented");
  }

  virtual Metrics* getMetrics(ColumnarBatchIterator* rawIter, int64_t exportNanos) {
    throw GlutenException("Not implemented");
  }

  virtual std::shared_ptr<ShuffleReader> createShuffleReader(
      std::shared_ptr<arrow::Schema> schema,
      ShuffleReaderOptions options) {
    throw GlutenException("Not implemented");
  }

  virtual std::unique_ptr<ColumnarBatchSerializer> createColumnarBatchSerializer(struct ArrowSchema* cSchema) {
    throw GlutenException("Not implemented");
  }

  virtual void dumpConf(const std::string& path) {
    throw GlutenException("Not implemented");
  }

  virtual std::shared_ptr<ArrowWriter> createArrowWriter(const std::string& path) {
    throw GlutenException("Not implemented");
  };

  const std::unordered_map<std::string, std::string>& getConfMap() {
    return confMap_;
  }

  void setSparkTaskInfo(SparkTaskInfo taskInfo) {
    taskInfo_ = taskInfo;
  }

  ObjectHandle saveObject(std::shared_ptr<void> obj) {
    return objStore_->save(obj);
  }

 protected:
  std::string kind_;
  MemoryManager* memoryManager_;
  std::unique_ptr<ObjectStore> objStore_ = ObjectStore::create();
  std::unordered_map<std::string, std::string> confMap_; // Session conf map

  ::substrait::Plan substraitPlan_;
  std::vector<::substrait::ReadRel_LocalFiles> localFiles_;
  SparkTaskInfo taskInfo_;
};
} // namespace gluten
