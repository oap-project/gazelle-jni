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

#include "ShuffleMemoryPool.h"
#include "memory/Evictable.h"
#include "shuffle/Options.h"

namespace gluten {

class Payload {
 public:
  virtual ~Payload() = default;

  virtual arrow::Status serialize(arrow::io::OutputStream* outputStream) = 0;
};

class Evictor {
 public:
  enum Type { kCache, kFlush, kStop };

  Evictor(ShuffleWriterOptions* options) : options_(options) {}

  virtual ~Evictor() = default;

  virtual arrow::Status evict(uint32_t partitionId, std::unique_ptr<Payload> payload) = 0;

  virtual arrow::Status finish() = 0;

  int64_t getEvictTime() {
    return evictTime_;
  }

 protected:
  ShuffleWriterOptions* options_;

  int64_t evictTime_{0};
};

class PartitionWriter {
 public:
  PartitionWriter(uint32_t numPartitions, ShuffleWriterOptions* options)
      : numPartitions_(numPartitions), options_(options) {
    payloadPool_ = std::make_unique<ShuffleMemoryPool>(options_->memory_pool);
    codec_ = createArrowIpcCodec(options_->compression_type, options_->codec_backend);
  }

  virtual ~PartitionWriter() = default;

  virtual arrow::Status stop(ShuffleWriterMetrics* metrics) = 0;

  /// Evict buffers for `partitionId` partition.
  /// \param flush Whether to flush the evicted data immediately. If it's false,
  /// the data can be cached first.
  virtual arrow::Status evict(
      uint32_t partitionId,
      uint32_t numRows,
      std::vector<std::shared_ptr<arrow::Buffer>> buffers,
      bool reuseBuffers,
      Evictor::Type evictType) = 0;

  virtual arrow::Status finishEvict() = 0;

  uint64_t cachedPayloadSize() {
    return payloadPool_->bytes_allocated();
  }

 protected:
  uint32_t numPartitions_;
  ShuffleWriterOptions* options_;

  // Memory Pool used to track memory allocation of partition payloads.
  // The actual allocation is delegated to options_.memory_pool.
  std::unique_ptr<ShuffleMemoryPool> payloadPool_;

  std::unique_ptr<arrow::util::Codec> codec_;

  int64_t compressTime_{0};
  int64_t evictTime_{0};
  int64_t writeTime_{0};
};
} // namespace gluten
