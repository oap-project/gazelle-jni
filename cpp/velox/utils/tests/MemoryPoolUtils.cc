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

#include "utils/tests/MemoryPoolUtils.h"

namespace gluten {

arrow::Status LimitedMemoryPool::Allocate(int64_t size, int64_t alignment, uint8_t** out) {
  if (bytes_allocated() + size > capacity_) {
    return arrow::Status::OutOfMemory("malloc of size ", size, " failed");
  }
  RETURN_NOT_OK(pool_->Allocate(size, alignment, out));
  stats_.UpdateAllocatedBytes(size);
  return arrow::Status::OK();
}

arrow::Status LimitedMemoryPool::Reallocate(int64_t oldSize, int64_t newSize, int64_t alignment, uint8_t** ptr) {
  if (newSize > capacity_) {
    return arrow::Status::OutOfMemory("malloc of size ", newSize, " failed");
  }
  RETURN_NOT_OK(pool_->Reallocate(oldSize, newSize, alignment, ptr));
  stats_.UpdateAllocatedBytes(newSize - oldSize);
  return arrow::Status::OK();
}

void LimitedMemoryPool::Free(uint8_t* buffer, int64_t size, int64_t alignment) {
  pool_->Free(buffer, size, alignment);
  stats_.UpdateAllocatedBytes(-size);
}

int64_t LimitedMemoryPool::bytes_allocated() const {
  return stats_.bytes_allocated();
}

int64_t LimitedMemoryPool::max_memory() const {
  return pool_->max_memory();
}

int64_t LimitedMemoryPool::total_bytes_allocated() const {
  return pool_->total_bytes_allocated();
}

int64_t LimitedMemoryPool::num_allocations() const {
  throw pool_->num_allocations();
}

std::string LimitedMemoryPool::backend_name() const {
  return pool_->backend_name();
}

bool SelfEvictedMemoryPool::checkEvict(int64_t newCapacity, std::function<void()> block) {
  bytesEvicted_ = 0;
  auto capacity = capacity_;
  // Limit the capacity to trigger evict.
  setCapacity(newCapacity);

  block();

  capacity_ = capacity;
  return bytesEvicted_ > 0;
}

void SelfEvictedMemoryPool::setCapacity(int64_t capacity) {
  if (capacity < bytes_allocated()) {
    capacity_ = bytes_allocated();
  } else {
    capacity_ = capacity;
  }
}

int64_t SelfEvictedMemoryPool::capacity() const {
  return capacity_;
}

void SelfEvictedMemoryPool::setEvictable(Reclaimable* evictable) {
  evictable_ = evictable;
}

arrow::Status SelfEvictedMemoryPool::Allocate(int64_t size, int64_t alignment, uint8_t** out) {
  RETURN_NOT_OK(ensureCapacity(size));
  return pool_->Allocate(size, alignment, out);
}

arrow::Status SelfEvictedMemoryPool::Reallocate(int64_t oldSize, int64_t newSize, int64_t alignment, uint8_t** ptr) {
  if (newSize > oldSize) {
    RETURN_NOT_OK(ensureCapacity(newSize - oldSize));
  }
  return pool_->Reallocate(oldSize, newSize, alignment, ptr);
}

void SelfEvictedMemoryPool::Free(uint8_t* buffer, int64_t size, int64_t alignment) {
  return pool_->Free(buffer, size, alignment);
}

int64_t SelfEvictedMemoryPool::bytes_allocated() const {
  return pool_->bytes_allocated();
}

int64_t SelfEvictedMemoryPool::max_memory() const {
  return pool_->max_memory();
}

std::string SelfEvictedMemoryPool::backend_name() const {
  return pool_->backend_name();
}

int64_t SelfEvictedMemoryPool::total_bytes_allocated() const {
  return pool_->total_bytes_allocated();
}

int64_t SelfEvictedMemoryPool::num_allocations() const {
  throw pool_->num_allocations();
}

arrow::Status SelfEvictedMemoryPool::ensureCapacity(int64_t size) {
  VELOX_CHECK_NOT_NULL(evictable_);
  DLOG(INFO) << "Size: " << size << ", capacity_: " << capacity_ << ", bytes allocated: " << pool_->bytes_allocated();
  if (size > capacity_ - pool_->bytes_allocated()) {
    // Self evict.
    int64_t actual;
    RETURN_NOT_OK(evictable_->reclaimFixedSize(size, &actual));
    if (size > capacity_ - pool_->bytes_allocated()) {
      if (failIfOOM_) {
        return arrow::Status::OutOfMemory(
            "Failed to allocate after evict. Capacity: ",
            capacity_,
            ", Requested: ",
            size,
            ", Evicted: ",
            actual,
            ", Allocated: ",
            pool_->bytes_allocated());
      } else {
        capacity_ = size + pool_->bytes_allocated();
      }
    }
    bytesEvicted_ += actual;
  }
  return arrow::Status::OK();
}

} // namespace gluten
