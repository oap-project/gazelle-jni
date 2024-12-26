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

#include <chrono>

namespace gluten {
template <typename T = std::chrono::nanoseconds>
class Timer {
 public:
  using TimePoint = std::chrono::time_point<std::chrono::steady_clock>;
  explicit Timer() = default;

  void start() {
    running_ = true;
    startTime_ = std::chrono::steady_clock::now();
  }

  void stop() {
    if (!running_) {
      return;
    }
    running_ = false;
    realTimeUsed_ += std::chrono::duration_cast<T>(std::chrono::steady_clock::now() - startTime_).count();
  }

  void reset() {
    running_ = false;
    realTimeUsed_ = 0;
  }

  bool running() const {
    return running_;
  }

  // REQUIRES: timer is not running
  int64_t realTimeUsed() const {
    return realTimeUsed_;
  }

 private:
  bool running_ = false; // Is the timer running
  TimePoint startTime_{}; // If running_

  // Accumulated time so far (does not contain current slice if running_)
  int64_t realTimeUsed_ = 0;
};

template <typename T = std::chrono::nanoseconds>
class ScopedTimerImpl {
 public:
  explicit ScopedTimerImpl(int64_t* toAdd) : toAdd_(toAdd) {
    startInternal();
  }

  ~ScopedTimerImpl() {
    stopInternal();
  }

  void switchTo(int64_t* toAdd) {
    stopInternal();
    toAdd_ = toAdd;
    startInternal();
  }

 private:
  Timer<T> timer_{};
  int64_t* toAdd_;

  void stopInternal() {
    timer_.stop();
    *toAdd_ += timer_.realTimeUsed();
  }

  void startInternal() {
    timer_.reset();
    timer_.start();
  }
};

using ScopedTimer = ScopedTimerImpl<std::chrono::nanoseconds>;
using ScopedSecondsTimer = ScopedTimerImpl<std::chrono::seconds>;
using ScopedMillisecondsTimer = ScopedTimerImpl<std::chrono::milliseconds>;
using ScopedMicrosecondsTimer = ScopedTimerImpl<std::chrono::microseconds>;

} // namespace gluten
