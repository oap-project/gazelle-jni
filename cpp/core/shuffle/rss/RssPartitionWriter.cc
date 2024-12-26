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

#include <numeric>

#include "shuffle/Payload.h"
#include "shuffle/Utils.h"
#include "shuffle/rss/RssPartitionWriter.h"
#include "utils/Timer.h"

namespace gluten {

void RssPartitionWriter::init() {
  bytesEvicted_.resize(numPartitions_, 0);
  rawPartitionLengths_.resize(numPartitions_, 0);
}

arrow::Status RssPartitionWriter::stop(ShuffleWriterMetrics* metrics) {
  // Push data and collect metrics.
  auto totalBytesEvicted = std::accumulate(bytesEvicted_.begin(), bytesEvicted_.end(), 0LL);
  rssClient_->stop();
  // Populate metrics.
  metrics->totalCompressTime += compressTime_;
  metrics->totalEvictTime += spillTime_;
  metrics->totalWriteTime += writeTime_;
  metrics->totalBytesEvicted += totalBytesEvicted;
  metrics->totalBytesWritten += totalBytesEvicted;
  metrics->partitionLengths = std::move(bytesEvicted_);
  metrics->rawPartitionLengths = std::move(rawPartitionLengths_);
  return arrow::Status::OK();
}

arrow::Status RssPartitionWriter::reclaimFixedSize(int64_t size, int64_t* actual) {
  *actual = 0;
  return arrow::Status::OK();
}

arrow::Status RssPartitionWriter::hashEvict(
    uint32_t partitionId,
    std::unique_ptr<InMemoryPayload> inMemoryPayload,
    Evict::type evictType,
    bool reuseBuffers,
    bool hasComplexType) {
  return doEvict(partitionId, std::move(inMemoryPayload), nullptr);
}

arrow::Status RssPartitionWriter::sortEvict(
    uint32_t partitionId,
    std::unique_ptr<InMemoryPayload> inMemoryPayload,
    std::shared_ptr<arrow::Buffer> compressed,
    bool isFinal) {
  return doEvict(partitionId, std::move(inMemoryPayload), std::move(compressed));
}

arrow::Status RssPartitionWriter::evict(uint32_t partitionId, std::unique_ptr<BlockPayload> blockPayload, bool) {
  rawPartitionLengths_[partitionId] += blockPayload->rawSize();
  ScopedTimer timer(&spillTime_);
  ARROW_ASSIGN_OR_RAISE(auto buffer, blockPayload->readBufferAt(0));
  bytesEvicted_[partitionId] += rssClient_->pushPartitionData(partitionId, buffer->data_as<char>(), buffer->size());
  return arrow::Status::OK();
}

arrow::Status RssPartitionWriter::doEvict(
    uint32_t partitionId,
    std::unique_ptr<InMemoryPayload> inMemoryPayload,
    std::shared_ptr<arrow::Buffer> compressed) {
  rawPartitionLengths_[partitionId] += inMemoryPayload->rawSize();
  auto payloadType = codec_ ? Payload::Type::kCompressed : Payload::Type::kUncompressed;
  ARROW_ASSIGN_OR_RAISE(
      auto payload,
      inMemoryPayload->toBlockPayload(
          payloadType, payloadPool_.get(), codec_ ? codec_.get() : nullptr, std::move(compressed)));
  // Copy payload to arrow buffered os.
  ARROW_ASSIGN_OR_RAISE(auto rssBufferOs, arrow::io::BufferOutputStream::Create(options_.pushBufferMaxSize));
  RETURN_NOT_OK(payload->serialize(rssBufferOs.get()));
  payload = nullptr; // Invalidate payload immediately.

  // Push.
  ScopedTimer timer(&spillTime_);
  ARROW_ASSIGN_OR_RAISE(auto buffer, rssBufferOs->Finish());
  bytesEvicted_[partitionId] += rssClient_->pushPartitionData(
      partitionId, reinterpret_cast<char*>(const_cast<uint8_t*>(buffer->data())), buffer->size());
  return arrow::Status::OK();
}
} // namespace gluten
