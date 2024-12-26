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

#include <gtest/gtest.h>

#include "memory/ArrowMemoryPool.h"
#include "memory/VeloxColumnarBatch.h"
#include "operators/serializer/VeloxColumnarBatchSerializer.h"
#include "utils/VeloxArrowUtils.h"
#include "velox/vector/arrow/Bridge.h"
#include "velox/vector/tests/utils/VectorTestBase.h"

#include <arrow/buffer.h>

using namespace facebook::velox;

namespace gluten {

class VeloxColumnarBatchSerializerTest : public ::testing::Test, public test::VectorTestBase {
 protected:
  static void SetUpTestCase() {
    memory::MemoryManager::testingSetInstance({});
  }

  std::shared_ptr<arrow::MemoryPool> arrowPool_ = defaultArrowMemoryPool();
};

TEST_F(VeloxColumnarBatchSerializerTest, serialize) {
  std::vector<VectorPtr> children = {
      makeNullableFlatVector<int8_t>({1, 2, 3, std::nullopt, 4}),
      makeNullableFlatVector<int8_t>({1, -1, std::nullopt, std::nullopt, -2}),
      makeNullableFlatVector<int32_t>({1, 2, 3, 4, std::nullopt}),
      makeNullableFlatVector<int64_t>({std::nullopt, std::nullopt, std::nullopt, std::nullopt, std::nullopt}),
      makeNullableFlatVector<float>({-0.1234567, std::nullopt, 0.1234567, std::nullopt, -0.142857}),
      makeNullableFlatVector<bool>({std::nullopt, true, false, std::nullopt, true}),
      makeFlatVector<StringView>({"alice0", "bob1", "alice2", "bob3", "Alice4uuudeuhdhfudhfudhfudhbvudubvudfvu"}),
      makeNullableFlatVector<StringView>({"alice", "bob", std::nullopt, std::nullopt, "Alice"}),
      makeNullableFlatVector<int64_t>({34567235, 4567, 222, 34567, 333}, DECIMAL(12, 4)),
      makeNullableFlatVector<int128_t>({34567235, 4567, 222, 34567, 333}, DECIMAL(20, 4)),
  };
  auto vector = makeRowVector(children);
  auto batch = std::make_shared<VeloxColumnarBatch>(vector);
  auto serializer = std::make_shared<VeloxColumnarBatchSerializer>(arrowPool_.get(), pool_, nullptr);
  auto buffer = serializer->serializeColumnarBatches({batch});

  ArrowSchema cSchema;
  exportToArrow(vector, cSchema, ArrowUtils::getBridgeOptions());
  auto deserializer = std::make_shared<VeloxColumnarBatchSerializer>(arrowPool_.get(), pool_, &cSchema);
  auto deserialized = deserializer->deserialize(const_cast<uint8_t*>(buffer->data()), buffer->size());
  auto deserializedVector = std::dynamic_pointer_cast<VeloxColumnarBatch>(deserialized)->getRowVector();
  test::assertEqualVectors(vector, deserializedVector);
}

} // namespace gluten
