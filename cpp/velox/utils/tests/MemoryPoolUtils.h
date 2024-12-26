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

#include <arrow/memory_pool.h>
#include "memory/Reclaimable.h"
#include "utils/Exception.h"
#include "velox/common/base/Exceptions.h"

namespace gluten {

// arrow::MemoryPool instance with limited capacity, used by tests and benchmarks.
class LimitedMemoryPool final : public arrow::MemoryPool {
 public:
  explicit LimitedMemoryPool() : capacity_(std::numeric_limits<int64_t>::max()) {}
  explicit LimitedMemoryPool(int64_t capacity) : capacity_(capacity) {}

  arrow::Status Allocate(int64_t size, int64_t alignment, uint8_t** out) override;

  arrow::Status Reallocate(int64_t oldSize, int64_t newSize, int64_t alignment, uint8_t** ptr) override;

  void Free(uint8_t* buffer, int64_t size, int64_t alignment) override;

  int64_t bytes_allocated() const override;

  int64_t max_memory() const override;

  int64_t total_bytes_allocated() const override;

  int64_t num_allocations() const override;

  std::string backend_name() const override;

 private:
  arrow::MemoryPool* pool_ = arrow::default_memory_pool();
  int64_t capacity_;
  arrow::internal::MemoryPoolStats stats_;
};

// arrow::MemoryPool instance with limited capacity and can be evictable on OOM, used by tests and benchmarks.
class SelfEvictedMemoryPool : public arrow::MemoryPool {
 public:
  explicit SelfEvictedMemoryPool(arrow::MemoryPool* pool, bool failIfOOM = true) : pool_(pool), failIfOOM_(failIfOOM) {}

  bool checkEvict(int64_t newCapacity, std::function<void()> block);

  void setCapacity(int64_t capacity);

  int64_t capacity() const;

  void setEvictable(Reclaimable* evictable);

  arrow::Status Allocate(int64_t size, int64_t alignment, uint8_t** out) override;

  arrow::Status Reallocate(int64_t oldSize, int64_t newSize, int64_t alignment, uint8_t** ptr) override;

  void Free(uint8_t* buffer, int64_t size, int64_t alignment) override;

  int64_t bytes_allocated() const override;

  int64_t max_memory() const override;

  std::string backend_name() const override;

  int64_t total_bytes_allocated() const override;

  int64_t num_allocations() const override;

 private:
  arrow::Status ensureCapacity(int64_t size);

  arrow::MemoryPool* pool_;
  bool failIfOOM_;

  Reclaimable* evictable_;
  int64_t capacity_{std::numeric_limits<int64_t>::max()};

  int64_t bytesEvicted_{0};
};

} // namespace gluten
