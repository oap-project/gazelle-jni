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

#include "memory/BufferOutputStream.h"
#include "compute/VeloxBackend.h"
#include "memory/VeloxColumnarBatch.h"
#include "velox/common/memory/ByteStream.h"
#include "velox/vector/tests/utils/VectorTestBase.h"

using namespace facebook::velox;

namespace gluten {

class BufferOutputStreamTest : public ::testing::Test, public test::VectorTestBase {
 protected:
  // Velox requires the mem manager to be instanced.
  static void SetUpTestCase() {
    VeloxBackend::create({});
    memory::MemoryManager::testingSetInstance({});
  }

  std::shared_ptr<memory::MemoryPool> veloxPool_ = defaultLeafVeloxMemoryPool();
};

TEST_F(BufferOutputStreamTest, outputStream) {
  auto out = std::make_unique<BufferOutputStream>(veloxPool_.get(), 10000);
  std::stringstream referenceSStream;
  auto reference = std::make_unique<facebook::velox::OStreamOutputStream>(&referenceSStream);
  for (auto i = 0; i < 100; ++i) {
    std::string data;
    data.resize(10000);
    std::fill(data.begin(), data.end(), i);
    out->write(data.data(), data.size());
    reference->write(data.data(), data.size());
  }
  EXPECT_EQ(reference->tellp(), out->tellp());
  for (auto i = 0; i < 100; ++i) {
    std::string data;
    data.resize(6000);
    std::fill(data.begin(), data.end(), i + 10);
    out->seekp(i * 10000 + 5000);
    reference->seekp(i * 10000 + 5000);
    out->write(data.data(), data.size());
    reference->write(data.data(), data.size());
  }
  auto str = referenceSStream.str();
  auto numBytes = veloxPool_->usedBytes();
  EXPECT_LT(0, numBytes);
  {
    auto buffer = out->getBuffer();
    EXPECT_EQ(numBytes, veloxPool_->usedBytes());
    EXPECT_EQ(str, std::string(buffer->as<char>(), buffer->size()));
  }

  out.reset();
  // We expect dropping the stream frees the backing memory.
  EXPECT_EQ(0, veloxPool_->usedBytes());
}

} // namespace gluten
