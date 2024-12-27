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

#include "operators/serializer/VeloxColumnarBatchSerializer.h"
#include "shuffle/Payload.h"
#include "shuffle/ShuffleReader.h"
#include "shuffle/VeloxSortShuffleWriter.h"
#include "utils/Timer.h"
#include "velox/type/Type.h"
#include "velox/vector/ComplexVector.h"

#include <velox/serializers/PrestoSerializer.h>

namespace gluten {

class VeloxHashShuffleReaderDeserializer final : public ColumnarBatchIterator {
 public:
  VeloxHashShuffleReaderDeserializer(
      std::shared_ptr<arrow::io::InputStream> in,
      const std::shared_ptr<arrow::Schema>& schema,
      const std::shared_ptr<arrow::util::Codec>& codec,
      const facebook::velox::RowTypePtr& rowType,
      int32_t batchSize,
      int64_t bufferSize,
      arrow::MemoryPool* memoryPool,
      facebook::velox::memory::MemoryPool* veloxPool,
      std::vector<bool>* isValidityBuffer,
      bool hasComplexType,
      int64_t& deserializeTime,
      int64_t& decompressTime);

  std::shared_ptr<ColumnarBatch> next() override;

 private:
  std::shared_ptr<arrow::io::InputStream> in_;
  std::shared_ptr<arrow::Schema> schema_;
  std::shared_ptr<arrow::util::Codec> codec_;
  facebook::velox::RowTypePtr rowType_;
  int32_t batchSize_;
  arrow::MemoryPool* memoryPool_;
  facebook::velox::memory::MemoryPool* veloxPool_;
  std::vector<bool>* isValidityBuffer_;
  bool hasComplexType_;

  int64_t& deserializeTime_;
  int64_t& decompressTime_;

  std::unique_ptr<InMemoryPayload> merged_{nullptr};
  bool reachEos_{false};
};

class VeloxSortShuffleReaderDeserializer final : public ColumnarBatchIterator {
 public:
  using RowSizeType = VeloxSortShuffleWriter::RowSizeType;

  VeloxSortShuffleReaderDeserializer(
      std::shared_ptr<arrow::io::InputStream> in,
      const std::shared_ptr<arrow::Schema>& schema,
      const std::shared_ptr<arrow::util::Codec>& codec,
      const facebook::velox::RowTypePtr& rowType,
      int32_t batchSize,
      int64_t bufferSize,
      arrow::MemoryPool* memoryPool,
      facebook::velox::memory::MemoryPool* veloxPool,
      int64_t& deserializeTime,
      int64_t& decompressTime);

  std::shared_ptr<ColumnarBatch> next() override;

 private:
  std::shared_ptr<ColumnarBatch> deserializeToBatch();

  void readLargeRow(std::vector<std::shared_ptr<arrow::Buffer>>& arrowBuffers);

  std::shared_ptr<arrow::io::InputStream> in_;
  std::shared_ptr<arrow::Schema> schema_;
  std::shared_ptr<arrow::util::Codec> codec_;
  facebook::velox::RowTypePtr rowType_;
  uint32_t batchSize_;
  arrow::MemoryPool* arrowPool_;
  facebook::velox::memory::MemoryPool* veloxPool_;
  int64_t& deserializeTime_;
  int64_t& decompressTime_;

  std::list<std::pair<uint32_t, facebook::velox::BufferPtr>> cachedInputs_;
  uint32_t cachedRows_{0};
  bool reachedEos_{false};

  uint32_t rowOffset_{0};
  size_t byteOffset_{0};
};

class VeloxRssSortShuffleReaderDeserializer : public ColumnarBatchIterator {
 public:
  VeloxRssSortShuffleReaderDeserializer(
      const std::shared_ptr<facebook::velox::memory::MemoryPool>& veloxPool,
      const facebook::velox::RowTypePtr& rowType,
      int32_t batchSize,
      facebook::velox::common::CompressionKind veloxCompressionType,
      int64_t& deserializeTime,
      std::shared_ptr<arrow::io::InputStream> in);

  std::shared_ptr<ColumnarBatch> next();

 private:
  class VeloxInputStream;

  std::shared_ptr<facebook::velox::memory::MemoryPool> veloxPool_;
  facebook::velox::RowTypePtr rowType_;
  std::vector<facebook::velox::RowVectorPtr> batches_;
  bool reachEos_{false};
  int32_t rowCount_;
  int32_t batchSize_;
  facebook::velox::common::CompressionKind veloxCompressionType_;
  facebook::velox::VectorSerde* const serde_;
  facebook::velox::serializer::presto::PrestoVectorSerde::PrestoOptions serdeOptions_;
  int64_t& deserializeTime_;
  std::shared_ptr<VeloxInputStream> in_;
};

class VeloxColumnarBatchDeserializerFactory : public DeserializerFactory {
 public:
  VeloxColumnarBatchDeserializerFactory(
      const std::shared_ptr<arrow::Schema>& schema,
      const std::shared_ptr<arrow::util::Codec>& codec,
      const facebook::velox::common::CompressionKind veloxCompressionType,
      const facebook::velox::RowTypePtr& rowType,
      int32_t batchSize,
      int64_t bufferSize,
      arrow::MemoryPool* memoryPool,
      std::shared_ptr<facebook::velox::memory::MemoryPool> veloxPool,
      ShuffleWriterType shuffleWriterType);

  std::unique_ptr<ColumnarBatchIterator> createDeserializer(std::shared_ptr<arrow::io::InputStream> in) override;

  arrow::MemoryPool* getPool() override;

  int64_t getDecompressTime() override;

  int64_t getDeserializeTime() override;

  ShuffleWriterType getShuffleWriterType() override;

 private:
  void initFromSchema();

  std::shared_ptr<arrow::Schema> schema_;
  std::shared_ptr<arrow::util::Codec> codec_;
  facebook::velox::common::CompressionKind veloxCompressionType_;
  facebook::velox::RowTypePtr rowType_;
  int32_t batchSize_;
  int64_t bufferSize_;
  arrow::MemoryPool* memoryPool_;
  std::shared_ptr<facebook::velox::memory::MemoryPool> veloxPool_;

  std::vector<bool> isValidityBuffer_;
  bool hasComplexType_{false};

  ShuffleWriterType shuffleWriterType_;

  int64_t deserializeTime_{0};
  int64_t decompressTime_{0};
};

class VeloxShuffleReader final : public ShuffleReader {
 public:
  VeloxShuffleReader(std::unique_ptr<DeserializerFactory> factory);
};
} // namespace gluten
