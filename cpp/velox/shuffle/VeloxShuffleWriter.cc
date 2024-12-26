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

#include "shuffle/VeloxShuffleWriter.h"
#include "shuffle/VeloxHashShuffleWriter.h"
#include "shuffle/VeloxRssSortShuffleWriter.h"
#include "shuffle/VeloxSortShuffleWriter.h"

namespace gluten {

arrow::Result<std::shared_ptr<VeloxShuffleWriter>> VeloxShuffleWriter::create(
    ShuffleWriterType type,
    uint32_t numPartitions,
    std::unique_ptr<PartitionWriter> partitionWriter,
    ShuffleWriterOptions options,
    std::shared_ptr<facebook::velox::memory::MemoryPool> veloxPool,
    arrow::MemoryPool* arrowPool) {
  std::shared_ptr<VeloxShuffleWriter> shuffleWriter;
  switch (type) {
    case ShuffleWriterType::kHashShuffle:
      return VeloxHashShuffleWriter::create(
          numPartitions, std::move(partitionWriter), std::move(options), veloxPool, arrowPool);
    case ShuffleWriterType::kSortShuffle:
      return VeloxSortShuffleWriter::create(
          numPartitions, std::move(partitionWriter), std::move(options), veloxPool, arrowPool);
    case ShuffleWriterType::kRssSortShuffle:
      return VeloxRssSortShuffleWriter::create(
          numPartitions, std::move(partitionWriter), std::move(options), veloxPool, arrowPool);
    default:
      return arrow::Status::Invalid("Unsupported shuffle writer type: ", std::to_string(type));
  }
}

} // namespace gluten
