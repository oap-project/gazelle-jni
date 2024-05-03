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

#include <algorithm>
#include <memory>
#include <mutex>

namespace gluten {

extern bool backtrace_allocation;

class AllocationListener {
 public:
  static std::unique_ptr<AllocationListener> noop();

  virtual ~AllocationListener() = default;

  // Value of diff can be either positive or negative
  virtual void allocationChanged(int64_t diff) = 0;

  virtual int64_t currentBytes() {
    return 0;
  }

  virtual int64_t peakBytes() {
    return 0;
  }

 protected:
  AllocationListener() = default;
};

/// Memory changes will be round to specified block size which aim to decrease delegated listener calls.
// The class must be thread safe
class BlockAllocationListener final : public AllocationListener {
 public:
  BlockAllocationListener(AllocationListener* delegated, uint64_t blockSize)
      : delegated_(delegated), blockSize_(blockSize) {}

  void allocationChanged(int64_t diff) override {
    if (diff == 0) {
      return;
    }
    std::unique_lock<std::mutex> guard{mutex_, std::defer_lock};
    if (diff > 0) {
      guard.lock();
      if (reservationBytes_ - usedBytes_ < diff) {
        auto roundSize = (diff + (blockSize_ - 1)) / blockSize_ * blockSize_;
        reservationBytes_ += roundSize;
        peakBytes_ = std::max(peakBytes_, reservationBytes_);
        guard.unlock();
        // unnecessary to lock the delegated listener, assume it's thread safe
        delegated_->allocationChanged(roundSize);
      }
      usedBytes_ += diff;
      guard.unlock();
    } else {
      guard.lock();
      usedBytes_ += diff;
      auto unreservedSize = (reservationBytes_ - usedBytes_) / blockSize_ * blockSize_;
      reservationBytes_ -= unreservedSize;
      guard.unlock();
      // unnecessary to lock the delegated listener
      delegated_->allocationChanged(-unreservedSize);
    }
  }

  int64_t currentBytes() override {
    return reservationBytes_;
  }

  int64_t peakBytes() override {
    return peakBytes_;
  }

 private:
  AllocationListener* const delegated_;
  const uint64_t blockSize_;
  uint64_t usedBytes_{0L};
  uint64_t peakBytes_{0L};
  uint64_t reservationBytes_{0L};

  mutable std::mutex mutex_;
};

} // namespace gluten
