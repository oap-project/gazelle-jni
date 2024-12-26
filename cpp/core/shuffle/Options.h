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

#include <arrow/ipc/options.h>
#include <arrow/util/compression.h>
#include "shuffle/Partitioning.h"
#include "utils/Compression.h"

namespace gluten {

static constexpr int16_t kDefaultBatchSize = 4096;
static constexpr int32_t kDefaultShuffleWriterBufferSize = 4096;
static constexpr int64_t kDefaultSortBufferThreshold = 64 << 20;
static constexpr int64_t kDefaultPushMemoryThreshold = 4096;
static constexpr int32_t kDefaultNumSubDirs = 64;
static constexpr int32_t kDefaultCompressionThreshold = 100;
static constexpr int32_t kDefaultSortEvictBufferSize = 32 * 1024;
static const std::string kDefaultCompressionTypeStr = "lz4";
static constexpr int32_t kDefaultBufferAlignment = 64;
static constexpr double kDefaultBufferReallocThreshold = 0.25;
static constexpr double kDefaultMergeBufferThreshold = 0.25;
static constexpr bool kEnableBufferedWrite = true;
static constexpr bool kDefaultUseRadixSort = true;
static constexpr int32_t kDefaultSortBufferSize = 4096;
static constexpr int64_t kDefaultReadBufferSize = 1 << 20;
static constexpr int64_t kDefaultShuffleFileBufferSize = 32 << 10;

enum ShuffleWriterType { kHashShuffle, kSortShuffle, kRssSortShuffle };
enum PartitionWriterType { kLocal, kRss };
enum SortAlgorithm { kRadixSort, kQuickSort };

struct ShuffleReaderOptions {
  arrow::Compression::type compressionType = arrow::Compression::type::LZ4_FRAME;
  std::string compressionTypeStr = "lz4";
  ShuffleWriterType shuffleWriterType = kHashShuffle;
  CodecBackend codecBackend = CodecBackend::NONE;
  int32_t batchSize = kDefaultBatchSize;
  int64_t bufferSize = kDefaultReadBufferSize;
};

struct ShuffleWriterOptions {
  int32_t bufferSize = kDefaultShuffleWriterBufferSize;
  double bufferReallocThreshold = kDefaultBufferReallocThreshold;
  int64_t pushMemoryThreshold = kDefaultPushMemoryThreshold;
  Partitioning partitioning = Partitioning::kRoundRobin;
  int64_t taskAttemptId = -1;
  int32_t startPartitionId = 0;
  int64_t threadId = -1;
  ShuffleWriterType shuffleWriterType = kHashShuffle;

  // Sort shuffle writer.
  int32_t sortBufferInitialSize = kDefaultSortBufferSize;
  int32_t sortEvictBufferSize = kDefaultSortEvictBufferSize;
  bool useRadixSort = kDefaultUseRadixSort;
};

struct PartitionWriterOptions {
  int32_t mergeBufferSize = kDefaultShuffleWriterBufferSize;
  double mergeThreshold = kDefaultMergeBufferThreshold;
  int32_t compressionThreshold = kDefaultCompressionThreshold;
  arrow::Compression::type compressionType = arrow::Compression::LZ4_FRAME;
  std::string compressionTypeStr = kDefaultCompressionTypeStr;
  CodecBackend codecBackend = CodecBackend::NONE;
  int32_t compressionLevel = arrow::util::kUseDefaultCompressionLevel;
  CompressionMode compressionMode = CompressionMode::BUFFER;

  bool bufferedWrite = kEnableBufferedWrite;

  int32_t numSubDirs = kDefaultNumSubDirs;

  int64_t pushBufferMaxSize = kDefaultPushMemoryThreshold;

  int64_t sortBufferMaxSize = kDefaultSortBufferThreshold;

  int64_t shuffleFileBufferSize = kDefaultShuffleFileBufferSize;
};

struct ShuffleWriterMetrics {
  int64_t totalBytesWritten{0};
  int64_t totalBytesEvicted{0};
  int64_t totalBytesToEvict{0};
  int64_t totalWriteTime{0};
  int64_t totalEvictTime{0};
  int64_t totalCompressTime{0};
  std::vector<int64_t> partitionLengths{};
  std::vector<int64_t> rawPartitionLengths{}; // Uncompressed size.
};
} // namespace gluten
