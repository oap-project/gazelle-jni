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

#include <arrow/ipc/writer.h>
#include <numeric>
#include <utility>

#include "memory/ArrowMemoryPool.h"
#include "memory/ColumnarBatch.h"
#include "memory/Evictable.h"
#include "shuffle/Options.h"
#include "shuffle/Partitioner.h"
#include "shuffle/Partitioning.h"
#include "utils/Compression.h"

namespace gluten {

struct ShuffleWriterMetrics {
  int64_t totalBytesWritten{0};
  int64_t totalBytesEvicted{0};
  int64_t totalWriteTime{0};
  int64_t totalEvictTime{0};
  int64_t totalCompressTime{0};
  std::vector<int64_t> partitionLengths{};
  std::vector<int64_t> rawPartitionLengths{}; // Uncompressed size.
};

class ShuffleMemoryPool : public arrow::MemoryPool {
 public:
  ShuffleMemoryPool(arrow::MemoryPool* pool) : pool_(pool) {}

  arrow::MemoryPool* delegated() {
    return pool_;
  }

  arrow::Status Allocate(int64_t size, int64_t alignment, uint8_t** out) override {
    auto status = pool_->Allocate(size, alignment, out);
    if (status.ok()) {
      bytesAllocated_ += size;
    }
    return status;
  }

  arrow::Status Reallocate(int64_t old_size, int64_t new_size, int64_t alignment, uint8_t** ptr) override {
    auto status = pool_->Reallocate(old_size, new_size, alignment, ptr);
    if (status.ok()) {
      bytesAllocated_ += (new_size - old_size);
    }
    return status;
  }

  void Free(uint8_t* buffer, int64_t size, int64_t alignment) override {
    pool_->Free(buffer, size, alignment);
    bytesAllocated_ -= size;
  }

  int64_t bytes_allocated() const override {
    return bytesAllocated_;
  }

  int64_t max_memory() const override {
    return pool_->max_memory();
  }

  std::string backend_name() const override {
    return pool_->backend_name();
  }

  int64_t total_bytes_allocated() const override {
    return pool_->total_bytes_allocated();
  }

  int64_t num_allocations() const override {
    throw pool_->num_allocations();
  }

 private:
  arrow::MemoryPool* pool_;
  uint64_t bytesAllocated_ = 0;
};

class ShuffleWriter : public Evictable {
 public:
  static constexpr int64_t kMinMemLimit = 128LL * 1024 * 1024;

  virtual arrow::Status split(std::shared_ptr<ColumnarBatch> cb, int64_t memLimit) = 0;

  virtual arrow::Status evictPartitionBuffers(uint32_t partitionId, bool reuseBuffers) = 0;

  virtual arrow::Status stop() = 0;

  virtual std::shared_ptr<arrow::Schema>& schema() {
    return schema_;
  }

  int32_t numPartitions() const {
    return numPartitions_;
  }

  int64_t partitionBufferSize() const {
    return partitionBufferPool_->bytes_allocated();
  }

  int64_t totalBytesWritten() const {
    return metrics_.totalBytesWritten;
  }

  int64_t totalBytesEvicted() const {
    return metrics_.totalBytesEvicted;
  }

  int64_t totalWriteTime() const {
    return metrics_.totalWriteTime;
  }

  int64_t totalEvictTime() const {
    return metrics_.totalEvictTime;
  }

  int64_t totalCompressTime() const {
    return metrics_.totalCompressTime;
  }

  const std::vector<int64_t>& partitionLengths() const {
    return metrics_.partitionLengths;
  }

  const std::vector<int64_t>& rawPartitionLengths() const {
    return metrics_.rawPartitionLengths;
  }

  ShuffleWriterOptions& options() {
    return options_;
  }

  virtual const uint64_t cachedPayloadSize() const = 0;

  class PartitionWriter;

  class PartitionWriterCreator;

 protected:
  ShuffleWriter(
      int32_t numPartitions,
      std::shared_ptr<PartitionWriterCreator> partitionWriterCreator,
      const ShuffleWriterOptions& options)
      : numPartitions_(numPartitions),
        partitionWriterCreator_(std::move(partitionWriterCreator)),
        options_(std::move(options)),
        partitionBufferPool_(std::make_shared<ShuffleMemoryPool>(options_.memory_pool)) {
    options_.codec = createArrowIpcCodec(options_.compression_type, options_.codec_backend);
  }

  virtual ~ShuffleWriter() = default;

  int32_t numPartitions_;

  std::shared_ptr<PartitionWriterCreator> partitionWriterCreator_;

  ShuffleWriterOptions options_;
  // Memory Pool used to track memory usage of partition buffers.
  // The actual allocation is delegated to options_.memory_pool.
  std::shared_ptr<ShuffleMemoryPool> partitionBufferPool_;

  std::shared_ptr<arrow::Schema> schema_;

  // col partid
  std::vector<std::vector<std::vector<std::shared_ptr<arrow::ResizableBuffer>>>> partitionBuffers_;

  std::shared_ptr<Partitioner> partitioner_;

  ShuffleWriterMetrics metrics_{};
};

} // namespace gluten
