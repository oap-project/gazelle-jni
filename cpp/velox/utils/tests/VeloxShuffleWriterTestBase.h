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

#include <arrow/record_batch.h>
#include <arrow/result.h>
#include <arrow/util/compression.h>
#include <gtest/gtest.h>
#include "LocalRssClient.h"
#include "memory/VeloxColumnarBatch.h"
#include "shuffle/PartitionWriter.h"
#include "shuffle/VeloxShuffleReader.h"
#include "utils/Compression.h"
#include "velox/type/Type.h"
#include "velox/vector/tests/VectorTestUtils.h"

namespace gluten {

namespace {
std::string makeString(uint32_t length) {
  static const std::string kLargeStringOf128Bytes =
      "thisisalaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
      "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaargestringlengthmorethan16bytes";
  std::string res{};
  auto repeats = length / kLargeStringOf128Bytes.length();
  while (repeats--) {
    res.append(kLargeStringOf128Bytes);
  }
  if (auto remains = length % kLargeStringOf128Bytes.length()) {
    res.append(kLargeStringOf128Bytes.substr(0, remains));
  }
  return res;
}

std::unique_ptr<PartitionWriter> createPartitionWriter(
    PartitionWriterType partitionWriterType,
    uint32_t numPartitions,
    const std::string& dataFile,
    const std::vector<std::string>& localDirs,
    const PartitionWriterOptions& options,
    arrow::MemoryPool* pool) {
  if (partitionWriterType == PartitionWriterType::kRss) {
    auto rssClient = std::make_unique<LocalRssClient>(dataFile);
    return std::make_unique<RssPartitionWriter>(numPartitions, options, pool, std::move(rssClient));
  }
  return std::make_unique<LocalPartitionWriter>(numPartitions, options, pool, dataFile, localDirs);
}
} // namespace

struct ShuffleTestParams {
  ShuffleWriterType shuffleWriterType;
  PartitionWriterType partitionWriterType;
  arrow::Compression::type compressionType;
  int32_t compressionThreshold{0};
  int32_t mergeBufferSize{0};
  int32_t compressionBufferSize{0};
  bool useRadixSort{false};

  std::string toString() const {
    std::ostringstream out;
    out << "shuffleWriterType = " << shuffleWriterType << ", partitionWriterType = " << partitionWriterType
        << ", compressionType = " << compressionType << ", compressionThreshold = " << compressionThreshold
        << ", mergeBufferSize = " << mergeBufferSize << ", compressionBufferSize = " << compressionBufferSize
        << ", useRadixSort = " << (useRadixSort ? "true" : "false");
    return out.str();
  }
};

class VeloxShuffleWriterTestBase : public facebook::velox::test::VectorTestBase {
 public:
  virtual arrow::Status initShuffleWriterOptions() {
    RETURN_NOT_OK(setLocalDirsAndDataFile());
    return arrow::Status::OK();
  }

 protected:
  void setUp() {
    if (!isRegisteredNamedVectorSerde(facebook::velox::VectorSerde::Kind::kPresto)) {
      // RSS shuffle serde.
      facebook::velox::serializer::presto::PrestoVectorSerde::registerNamedVectorSerde();
    }
    // Set up test data.
    children1_ = {
        makeNullableFlatVector<int8_t>({1, 2, 3, std::nullopt, 4, std::nullopt, 5, 6, std::nullopt, 7}),
        makeNullableFlatVector<int8_t>({1, -1, std::nullopt, std::nullopt, -2, 2, std::nullopt, std::nullopt, 3, -3}),
        makeNullableFlatVector<int32_t>({1, 2, 3, 4, std::nullopt, 5, 6, 7, 8, std::nullopt}),
        makeNullableFlatVector<int64_t>(
            {std::nullopt,
             std::nullopt,
             std::nullopt,
             std::nullopt,
             std::nullopt,
             std::nullopt,
             std::nullopt,
             std::nullopt,
             std::nullopt,
             std::nullopt}),
        makeNullableFlatVector<float>(
            {-0.1234567,
             std::nullopt,
             0.1234567,
             std::nullopt,
             -0.142857,
             std::nullopt,
             0.142857,
             0.285714,
             0.428617,
             std::nullopt}),
        makeNullableFlatVector<bool>(
            {std::nullopt, true, false, std::nullopt, true, true, false, true, std::nullopt, std::nullopt}),
        makeFlatVector<facebook::velox::StringView>(
            {"alice0", "bob1", "alice2", "bob3", "Alice4", "Bob5", "AlicE6", "boB7", "ALICE8", "BOB9"}),
        makeNullableFlatVector<facebook::velox::StringView>(
            {"alice", "bob", std::nullopt, std::nullopt, "Alice", "Bob", std::nullopt, "alicE", std::nullopt, "boB"}),
        facebook::velox::BaseVector::create(facebook::velox::UNKNOWN(), 10, pool())};

    children2_ = {
        makeNullableFlatVector<int8_t>({std::nullopt, std::nullopt}),
        makeFlatVector<int8_t>({1, -1}),
        makeNullableFlatVector<int32_t>({100, std::nullopt}),
        makeFlatVector<int64_t>({1, 1}),
        makeFlatVector<float>({0.142857, -0.142857}),
        makeFlatVector<bool>({true, false}),
        makeFlatVector<facebook::velox::StringView>(
            {"bob",
             "alicealicealicealicealicealicealicealicealicealicealicealicealicealicealicealicealicealicealicealicealicealicealicealicealicealicealicealicealicealicealicealicealicealicealicealicealicealicealicealice"}),
        makeNullableFlatVector<facebook::velox::StringView>({std::nullopt, std::nullopt}),
        facebook::velox::BaseVector::create(facebook::velox::UNKNOWN(), 2, pool())};

    childrenNoNull_ = {
        makeFlatVector<int8_t>({0, 1}),
        makeFlatVector<int8_t>({0, -1}),
        makeFlatVector<int32_t>({0, 100}),
        makeFlatVector<int64_t>({0, 1}),
        makeFlatVector<float>({0, 0.142857}),
        makeFlatVector<bool>({false, true}),
        makeFlatVector<facebook::velox::StringView>({"", "alice"}),
        makeFlatVector<facebook::velox::StringView>({"alice", ""}),
    };

    largeString1_ = makeString(1024);
    childrenLargeBinary1_ = {
        makeFlatVector<int8_t>(std::vector<int8_t>(4096, 0)),
        makeFlatVector<int8_t>(std::vector<int8_t>(4096, 0)),
        makeFlatVector<int32_t>(std::vector<int32_t>(4096, 0)),
        makeFlatVector<int64_t>(std::vector<int64_t>(4096, 0)),
        makeFlatVector<float>(std::vector<float>(4096, 0)),
        makeFlatVector<bool>(std::vector<bool>(4096, true)),
        makeNullableFlatVector<facebook::velox::StringView>(
            std::vector<std::optional<facebook::velox::StringView>>(4096, largeString1_.c_str())),
        makeNullableFlatVector<facebook::velox::StringView>(
            std::vector<std::optional<facebook::velox::StringView>>(4096, std::nullopt)),
    };
    largeString2_ = makeString(4096);
    auto vectorToSpill = childrenLargeBinary2_ = {
        makeFlatVector<int8_t>(std::vector<int8_t>(2048, 0)),
        makeFlatVector<int8_t>(std::vector<int8_t>(2048, 0)),
        makeFlatVector<int32_t>(std::vector<int32_t>(2048, 0)),
        makeFlatVector<int64_t>(std::vector<int64_t>(2048, 0)),
        makeFlatVector<float>(std::vector<float>(2048, 0)),
        makeFlatVector<bool>(std::vector<bool>(2048, true)),
        makeNullableFlatVector<facebook::velox::StringView>(
            std::vector<std::optional<facebook::velox::StringView>>(2048, largeString2_.c_str())),
        makeNullableFlatVector<facebook::velox::StringView>(
            std::vector<std::optional<facebook::velox::StringView>>(2048, std::nullopt)),
    };
    childrenComplex_ = {
        makeNullableFlatVector<int32_t>({std::nullopt, 1}),
        makeRowVector({
            makeFlatVector<int32_t>({1, 3}),
            makeNullableFlatVector<facebook::velox::StringView>({std::nullopt, "de"}),
        }),
        makeNullableFlatVector<facebook::velox::StringView>({std::nullopt, "10 I'm not inline string"}),
        makeArrayVector<int64_t>({
            {1, 2, 3, 4, 5},
            {1, 2, 3},
        }),
        makeMapVector<int32_t, facebook::velox::StringView>(
            {{{1, "str1000"}, {2, "str2000"}}, {{3, "str3000"}, {4, "str4000"}}}),
    };

    inputVector1_ = makeRowVector(children1_);
    inputVector2_ = makeRowVector(children2_);
    inputVectorNoNull_ = makeRowVector(childrenNoNull_);
    inputVectorLargeBinary1_ = makeRowVector(childrenLargeBinary1_);
    inputVectorLargeBinary2_ = makeRowVector(childrenLargeBinary2_);
    inputVectorComplex_ = makeRowVector(childrenComplex_);
  }

  arrow::Status splitRowVector(
      VeloxShuffleWriter& shuffleWriter,
      facebook::velox::RowVectorPtr vector,
      int64_t memLimit = ShuffleWriter::kMinMemLimit) {
    std::shared_ptr<ColumnarBatch> cb = std::make_shared<VeloxColumnarBatch>(vector);
    return shuffleWriter.write(cb, memLimit);
  }

  // Create multiple local dirs and join with comma.
  arrow::Status setLocalDirsAndDataFile() {
    static const std::string kTestLocalDirsPrefix = "columnar-shuffle-test-";

    // Create first tmp dir and create data file.
    // To prevent tmpDirs from being deleted in the dtor, we need to store them.
    tmpDirs_.emplace_back();
    ARROW_ASSIGN_OR_RAISE(tmpDirs_.back(), arrow::internal::TemporaryDir::Make(kTestLocalDirsPrefix))
    ARROW_ASSIGN_OR_RAISE(dataFile_, createTempShuffleFile(tmpDirs_.back()->path().ToString()));
    localDirs_.push_back(tmpDirs_.back()->path().ToString());

    // Create second tmp dir.
    tmpDirs_.emplace_back();
    ARROW_ASSIGN_OR_RAISE(tmpDirs_.back(), arrow::internal::TemporaryDir::Make(kTestLocalDirsPrefix))
    localDirs_.push_back(tmpDirs_.back()->path().ToString());
    return arrow::Status::OK();
  }

  virtual std::shared_ptr<VeloxShuffleWriter> createShuffleWriter(arrow::MemoryPool* arrowPool) = 0;

  ShuffleWriterOptions shuffleWriterOptions_{};
  PartitionWriterOptions partitionWriterOptions_{};

  std::vector<std::unique_ptr<arrow::internal::TemporaryDir>> tmpDirs_;
  std::string dataFile_;
  std::vector<std::string> localDirs_;

  std::vector<facebook::velox::VectorPtr> children1_;
  std::vector<facebook::velox::VectorPtr> children2_;
  std::vector<facebook::velox::VectorPtr> childrenNoNull_;
  std::vector<facebook::velox::VectorPtr> childrenLargeBinary1_;
  std::vector<facebook::velox::VectorPtr> childrenLargeBinary2_;
  std::vector<facebook::velox::VectorPtr> childrenComplex_;

  facebook::velox::RowVectorPtr inputVector1_;
  facebook::velox::RowVectorPtr inputVector2_;
  facebook::velox::RowVectorPtr inputVectorNoNull_;
  std::string largeString1_;
  std::string largeString2_;
  facebook::velox::RowVectorPtr inputVectorLargeBinary1_;
  facebook::velox::RowVectorPtr inputVectorLargeBinary2_;
  facebook::velox::RowVectorPtr inputVectorComplex_;
};

class VeloxShuffleWriterTest : public ::testing::TestWithParam<ShuffleTestParams>, public VeloxShuffleWriterTestBase {
 public:
  arrow::Status initShuffleWriterOptions() override {
    RETURN_NOT_OK(VeloxShuffleWriterTestBase::initShuffleWriterOptions());

    ShuffleTestParams params = GetParam();
    shuffleWriterOptions_.useRadixSort = params.useRadixSort;
    shuffleWriterOptions_.sortEvictBufferSize = params.compressionBufferSize;
    partitionWriterOptions_.compressionType = params.compressionType;
    switch (partitionWriterOptions_.compressionType) {
      case arrow::Compression::UNCOMPRESSED:
        partitionWriterOptions_.compressionTypeStr = "none";
        break;
      case arrow::Compression::LZ4_FRAME:
        partitionWriterOptions_.compressionTypeStr = "lz4";
        break;
      case arrow::Compression::ZSTD:
        partitionWriterOptions_.compressionTypeStr = "zstd";
        break;
      default:
        break;
    };
    partitionWriterOptions_.compressionThreshold = params.compressionThreshold;
    partitionWriterOptions_.mergeBufferSize = params.mergeBufferSize;
    return arrow::Status::OK();
  }

  std::shared_ptr<VeloxShuffleWriter> createSpecificShuffleWriter(
      arrow::MemoryPool* arrowPool,
      std::unique_ptr<PartitionWriter> partitionWriter,
      ShuffleWriterOptions shuffleWriterOptions,
      uint32_t numPartitions,
      int32_t bufferSize) {
    if (shuffleWriterOptions.shuffleWriterType == ShuffleWriterType::kHashShuffle) {
      shuffleWriterOptions.bufferSize = bufferSize;
    }
    GLUTEN_ASSIGN_OR_THROW(
        auto shuffleWriter,
        VeloxShuffleWriter::create(
            GetParam().shuffleWriterType,
            numPartitions,
            std::move(partitionWriter),
            std::move(shuffleWriterOptions),
            pool_,
            arrowPool));
    return shuffleWriter;
  }

 protected:
  static void SetUpTestCase() {
    facebook::velox::memory::MemoryManager::testingSetInstance({});
  }

  virtual void SetUp() override {
    std::cout << "Running test with param: " << GetParam().toString() << std::endl;
    VeloxShuffleWriterTestBase::setUp();
  }

  void TearDown() override {
    if (file_ != nullptr && !file_->closed()) {
      GLUTEN_THROW_NOT_OK(file_->Close());
    }
  }

  static void checkFileExists(const std::string& fileName) {
    ASSERT_EQ(*arrow::internal::FileExists(*arrow::internal::PlatformFilename::FromString(fileName)), true);
  }

  std::shared_ptr<arrow::Schema> getArrowSchema(facebook::velox::RowVectorPtr& rowVector) {
    return toArrowSchema(rowVector->type(), pool());
  }

  void setReadableFile(const std::string& fileName) {
    if (file_ != nullptr && !file_->closed()) {
      GLUTEN_THROW_NOT_OK(file_->Close());
    }
    GLUTEN_ASSIGN_OR_THROW(file_, arrow::io::ReadableFile::Open(fileName))
  }

  void getRowVectors(
      arrow::Compression::type compressionType,
      std::shared_ptr<arrow::Schema> schema,
      std::vector<facebook::velox::RowVectorPtr>& vectors,
      std::shared_ptr<arrow::io::InputStream> in) {
    ShuffleReaderOptions options;
    options.compressionType = compressionType;
    auto codec = createArrowIpcCodec(options.compressionType, CodecBackend::NONE);
    auto rowType = facebook::velox::asRowType(gluten::fromArrowSchema(schema));
    switch (options.compressionType) {
      case arrow::Compression::type::UNCOMPRESSED:
        options.compressionTypeStr = "none";
        break;
      case arrow::Compression::type::LZ4_FRAME:
        options.compressionTypeStr = "lz4";
        break;
      case arrow::Compression::type::ZSTD:
        options.compressionTypeStr = "zstd";
        break;
      default:
        break;
    };
    auto veloxCompressionType = facebook::velox::common::stringToCompressionKind(options.compressionTypeStr);
    if (!facebook::velox::isRegisteredVectorSerde()) {
      facebook::velox::serializer::presto::PrestoVectorSerde::registerVectorSerde();
    }
    // Set batchSize to a large value to make all batches are merged by reader.
    auto deserializerFactory = std::make_unique<gluten::VeloxColumnarBatchDeserializerFactory>(
        schema,
        std::move(codec),
        veloxCompressionType,
        rowType,
        std::numeric_limits<int32_t>::max(),
        kDefaultReadBufferSize,
        defaultArrowMemoryPool().get(),
        pool_,
        GetParam().shuffleWriterType);
    auto reader = std::make_shared<VeloxShuffleReader>(std::move(deserializerFactory));
    auto iter = reader->readStream(in);
    while (iter->hasNext()) {
      auto vector = std::dynamic_pointer_cast<VeloxColumnarBatch>(iter->next())->getRowVector();
      vectors.emplace_back(vector);
    }
  }

  std::shared_ptr<arrow::io::ReadableFile> file_;
};

class SinglePartitioningShuffleWriter : public VeloxShuffleWriterTest {
 protected:
  void testShuffleWrite(VeloxShuffleWriter& shuffleWriter, std::vector<facebook::velox::RowVectorPtr> vectors) {
    for (auto& vector : vectors) {
      ASSERT_NOT_OK(splitRowVector(shuffleWriter, vector));
    }
    ASSERT_NOT_OK(shuffleWriter.stop());
    // verify data file
    checkFileExists(dataFile_);
    // verify output temporary files
    const auto& lengths = shuffleWriter.partitionLengths();
    ASSERT_EQ(lengths.size(), 1);

    auto schema = getArrowSchema(vectors[0]);
    std::vector<facebook::velox::RowVectorPtr> deserializedVectors;
    setReadableFile(dataFile_);
    GLUTEN_ASSIGN_OR_THROW(auto in, arrow::io::RandomAccessFile::GetStream(file_, 0, lengths[0]));
    getRowVectors(partitionWriterOptions_.compressionType, schema, deserializedVectors, in);

    ASSERT_EQ(deserializedVectors.size(), vectors.size());
    for (int32_t i = 0; i < deserializedVectors.size(); i++) {
      facebook::velox::test::assertEqualVectors(vectors[i], deserializedVectors[i]);
    }
  }

  std::shared_ptr<VeloxShuffleWriter> createShuffleWriter(arrow::MemoryPool* arrowPool) override {
    shuffleWriterOptions_.partitioning = Partitioning::kSingle;
    static const uint32_t kNumPartitions = 1;
    auto partitionWriter = createPartitionWriter(
        GetParam().partitionWriterType, kNumPartitions, dataFile_, localDirs_, partitionWriterOptions_, arrowPool);
    std::shared_ptr<VeloxShuffleWriter> shuffleWriter = createSpecificShuffleWriter(
        arrowPool, std::move(partitionWriter), std::move(shuffleWriterOptions_), kNumPartitions, 10);
    return shuffleWriter;
  }
};

class MultiplePartitioningShuffleWriter : public VeloxShuffleWriterTest {
 protected:
  void shuffleWriteReadMultiBlocks(
      VeloxShuffleWriter& shuffleWriter,
      int32_t expectPartitionLength,
      facebook::velox::TypePtr dataType,
      std::vector<std::vector<facebook::velox::RowVectorPtr>> expectedVectors) { /* blockId = pid, rowVector in block */
    ASSERT_NOT_OK(shuffleWriter.stop());
    // verify data file
    checkFileExists(dataFile_);
    // verify output temporary files
    const auto& lengths = shuffleWriter.partitionLengths();
    ASSERT_EQ(lengths.size(), expectPartitionLength);
    int64_t lengthSum = std::accumulate(lengths.begin(), lengths.end(), 0);
    auto schema = toArrowSchema(dataType, pool());
    setReadableFile(dataFile_);
    ASSERT_EQ(*file_->GetSize(), lengthSum);
    for (int32_t i = 0; i < expectPartitionLength; i++) {
      if (expectedVectors[i].size() == 0) {
        ASSERT_EQ(lengths[i], 0);
      } else {
        std::vector<facebook::velox::RowVectorPtr> deserializedVectors;
        GLUTEN_ASSIGN_OR_THROW(
            auto in, arrow::io::RandomAccessFile::GetStream(file_, i == 0 ? 0 : lengths[i - 1], lengths[i]));
        getRowVectors(partitionWriterOptions_.compressionType, schema, deserializedVectors, in);
        ASSERT_EQ(expectedVectors[i].size(), deserializedVectors.size());
        for (int32_t j = 0; j < expectedVectors[i].size(); j++) {
          facebook::velox::test::assertEqualVectors(expectedVectors[i][j], deserializedVectors[j]);
        }
      }
    }
  }

  void testShuffleWriteMultiBlocks(
      VeloxShuffleWriter& shuffleWriter,
      std::vector<facebook::velox::RowVectorPtr> vectors,
      int32_t expectPartitionLength,
      facebook::velox::TypePtr dataType,
      std::vector<std::vector<facebook::velox::RowVectorPtr>> expectedVectors) {
    for (auto& vector : vectors) {
      ASSERT_NOT_OK(splitRowVector(shuffleWriter, vector));
    }
    shuffleWriteReadMultiBlocks(shuffleWriter, expectPartitionLength, dataType, expectedVectors);
  }
};

class HashPartitioningShuffleWriter : public MultiplePartitioningShuffleWriter {
 protected:
  void SetUp() override {
    MultiplePartitioningShuffleWriter::SetUp();

    children1_.insert((children1_.begin()), makeFlatVector<int32_t>({1, 2, 2, 2, 2, 1, 1, 1, 2, 1}));
    hashInputVector1_ = makeRowVector(children1_);
    children2_.insert((children2_.begin()), makeFlatVector<int32_t>({2, 2}));
    hashInputVector2_ = makeRowVector(children2_);
  }

  std::shared_ptr<VeloxShuffleWriter> createShuffleWriter(arrow::MemoryPool* arrowPool) override {
    shuffleWriterOptions_.partitioning = Partitioning::kHash;
    static const uint32_t kNumPartitions = 2;
    auto partitionWriter = createPartitionWriter(
        GetParam().partitionWriterType, kNumPartitions, dataFile_, localDirs_, partitionWriterOptions_, arrowPool);
    std::shared_ptr<VeloxShuffleWriter> shuffleWriter = createSpecificShuffleWriter(
        arrowPool, std::move(partitionWriter), std::move(shuffleWriterOptions_), kNumPartitions, 4);
    return shuffleWriter;
  }

  std::vector<uint32_t> hashPartitionIds_{1, 2};

  facebook::velox::RowVectorPtr hashInputVector1_;
  facebook::velox::RowVectorPtr hashInputVector2_;
};

class RangePartitioningShuffleWriter : public MultiplePartitioningShuffleWriter {
 protected:
  void SetUp() override {
    MultiplePartitioningShuffleWriter::SetUp();

    auto pid1 = makeRowVector({makeFlatVector<int32_t>({0, 1, 0, 1, 0, 1, 0, 1, 0, 1})});
    auto rangeVector1 = makeRowVector(inputVector1_->children());
    compositeBatch1_ = VeloxColumnarBatch::compose(
        pool(), {std::make_shared<VeloxColumnarBatch>(pid1), std::make_shared<VeloxColumnarBatch>(rangeVector1)});

    auto pid2 = makeRowVector({makeFlatVector<int32_t>({0, 1})});
    auto rangeVector2 = makeRowVector(inputVector2_->children());
    compositeBatch2_ = VeloxColumnarBatch::compose(
        pool(), {std::make_shared<VeloxColumnarBatch>(pid2), std::make_shared<VeloxColumnarBatch>(rangeVector2)});
  }

  std::shared_ptr<VeloxShuffleWriter> createShuffleWriter(arrow::MemoryPool* arrowPool) override {
    shuffleWriterOptions_.partitioning = Partitioning::kRange;
    static const uint32_t kNumPartitions = 2;
    auto partitionWriter = createPartitionWriter(
        GetParam().partitionWriterType, kNumPartitions, dataFile_, localDirs_, partitionWriterOptions_, arrowPool);
    std::shared_ptr<VeloxShuffleWriter> shuffleWriter = createSpecificShuffleWriter(
        arrowPool, std::move(partitionWriter), std::move(shuffleWriterOptions_), kNumPartitions, 4);
    return shuffleWriter;
  }

  void testShuffleWriteMultiBlocks(
      VeloxShuffleWriter& shuffleWriter,
      std::vector<std::shared_ptr<ColumnarBatch>> batches,
      int32_t expectPartitionLength,
      facebook::velox::TypePtr dataType,
      std::vector<std::vector<facebook::velox::RowVectorPtr>> expectedVectors) { /* blockId = pid, rowVector in block */
    for (auto& batch : batches) {
      ASSERT_NOT_OK(shuffleWriter.write(batch, ShuffleWriter::kMinMemLimit));
    }
    shuffleWriteReadMultiBlocks(shuffleWriter, expectPartitionLength, dataType, expectedVectors);
  }

  std::shared_ptr<ColumnarBatch> compositeBatch1_;
  std::shared_ptr<ColumnarBatch> compositeBatch2_;
};

class RoundRobinPartitioningShuffleWriter : public MultiplePartitioningShuffleWriter {
 protected:
  std::shared_ptr<VeloxShuffleWriter> createShuffleWriter(arrow::MemoryPool* arrowPool) override {
    static const uint32_t kNumPartitions = 2;
    auto partitionWriter = createPartitionWriter(
        GetParam().partitionWriterType, kNumPartitions, dataFile_, localDirs_, partitionWriterOptions_, arrowPool);
    std::shared_ptr<VeloxShuffleWriter> shuffleWriter = createSpecificShuffleWriter(
        arrowPool, std::move(partitionWriter), std::move(shuffleWriterOptions_), kNumPartitions, 4);
    return shuffleWriter;
  }
};

class VeloxHashShuffleWriterMemoryTest : public VeloxShuffleWriterTestBase, public testing::Test {
 protected:
  static void SetUpTestCase() {
    facebook::velox::memory::MemoryManager::testingSetInstance({});
  }
  void SetUp() override {
    VeloxShuffleWriterTestBase::setUp();
  }

  std::shared_ptr<VeloxShuffleWriter> createShuffleWriter(uint32_t numPartitions, arrow::MemoryPool* arrowPool) {
    auto partitionWriter = createPartitionWriter(
        PartitionWriterType::kLocal, numPartitions, dataFile_, localDirs_, partitionWriterOptions_, arrowPool);
    GLUTEN_ASSIGN_OR_THROW(
        auto shuffleWriter,
        VeloxHashShuffleWriter::create(
            numPartitions, std::move(partitionWriter), std::move(shuffleWriterOptions_), pool_, arrowPool));
    return shuffleWriter;
  }

  std::shared_ptr<VeloxShuffleWriter> createShuffleWriter(arrow::MemoryPool* arrowPool) override {
    return createShuffleWriter(kDefaultShufflePartitions, arrowPool);
  }

  int64_t splitRowVectorAndSpill(
      VeloxShuffleWriter& shuffleWriter,
      std::vector<facebook::velox::RowVectorPtr> vectors,
      bool shrink) {
    for (auto vector : vectors) {
      ASSERT_NOT_OK(splitRowVector(shuffleWriter, vector));
    }

    auto targetEvicted = shuffleWriter.cachedPayloadSize();
    if (shrink) {
      targetEvicted += shuffleWriter.partitionBufferSize();
    }
    int64_t evicted;
    ASSERT_NOT_OK(shuffleWriter.reclaimFixedSize(targetEvicted, &evicted));

    return evicted;
  };

  static constexpr uint32_t kDefaultShufflePartitions = 2;
};

} // namespace gluten
