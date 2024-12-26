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

#include "VeloxColumnarBatchSerializer.h"

#include <arrow/buffer.h>

#include "memory/ArrowMemory.h"
#include "memory/VeloxColumnarBatch.h"
#include "velox/common/memory/Memory.h"
#include "velox/vector/FlatVector.h"
#include "velox/vector/arrow/Bridge.h"

#include <iostream>

using namespace facebook::velox;

namespace gluten {
namespace {

std::unique_ptr<ByteInputStream> toByteStream(uint8_t* data, int32_t size) {
  std::vector<ByteRange> byteRanges;
  byteRanges.push_back(ByteRange{data, size, 0});
  auto byteStream = std::make_unique<BufferInputStream>(byteRanges);
  return byteStream;
}

} // namespace

VeloxColumnarBatchSerializer::VeloxColumnarBatchSerializer(
    arrow::MemoryPool* arrowPool,
    std::shared_ptr<memory::MemoryPool> veloxPool,
    struct ArrowSchema* cSchema)
    : ColumnarBatchSerializer(arrowPool), veloxPool_(std::move(veloxPool)) {
  // serializeColumnarBatches don't need rowType_
  if (cSchema != nullptr) {
    rowType_ = asRowType(importFromArrow(*cSchema));
    ArrowSchemaRelease(cSchema); // otherwise the c schema leaks memory
  }
  serde_ = std::make_unique<serializer::presto::PrestoVectorSerde>();
  options_.useLosslessTimestamp = true;
}

std::shared_ptr<arrow::Buffer> VeloxColumnarBatchSerializer::serializeColumnarBatches(
    const std::vector<std::shared_ptr<ColumnarBatch>>& batches) {
  VELOX_DCHECK(batches.size() != 0, "Should serialize at least 1 vector");
  const std::shared_ptr<VeloxColumnarBatch>& vb = VeloxColumnarBatch::from(veloxPool_.get(), batches[0]);
  auto firstRowVector = vb->getRowVector();
  auto numRows = firstRowVector->size();
  auto arena = std::make_unique<StreamArena>(veloxPool_.get());
  auto rowType = asRowType(firstRowVector->type());
  auto serializer = serde_->createIterativeSerializer(rowType, numRows, arena.get(), &options_);
  for (auto& batch : batches) {
    auto rowVector = VeloxColumnarBatch::from(veloxPool_.get(), batch)->getRowVector();
    numRows = rowVector->size();
    std::vector<IndexRange> rows(numRows);
    for (int i = 0; i < numRows; i++) {
      rows[i] = IndexRange{i, 1};
    }
    serializer->append(rowVector, folly::Range(rows.data(), numRows));
  }

  std::shared_ptr<arrow::Buffer> valueBuffer;
  GLUTEN_ASSIGN_OR_THROW(valueBuffer, arrow::AllocateResizableBuffer(serializer->maxSerializedSize(), arrowPool_));
  auto output = std::make_shared<arrow::io::FixedSizeBufferWriter>(valueBuffer);
  serializer::presto::PrestoOutputStreamListener listener;
  ArrowFixedSizeBufferOutputStream out(output, &listener);
  serializer->flush(&out);
  GLUTEN_THROW_NOT_OK(output->Close());
  return valueBuffer;
}

std::shared_ptr<ColumnarBatch> VeloxColumnarBatchSerializer::deserialize(uint8_t* data, int32_t size) {
  RowVectorPtr result;
  auto byteStream = toByteStream(data, size);
  serde_->deserialize(byteStream.get(), veloxPool_.get(), rowType_, &result, &options_);
  return std::make_shared<VeloxColumnarBatch>(result);
}

} // namespace gluten
