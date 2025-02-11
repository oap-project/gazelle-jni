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
#include <Core/Block.h>
#include <DataTypes/DataTypeDate32.h>
#include <DataTypes/DataTypeString.h>
#include <IO/ReadBufferFromFile.h>
#include <Processors/Executors/PullingPipelineExecutor.h>
#include <Processors/Formats/Impl/ArrowColumnToCHColumn.h>
#include <Processors/Formats/Impl/ParquetBlockInputFormat.h>
#include <QueryPipeline/QueryPipeline.h>
#include <QueryPipeline/QueryPipelineBuilder.h>
#include <Storages/Parquet/ParquetMeta.h>
#include <Storages/Parquet/VectorizedParquetRecordReader.h>
#include <Storages/SubstraitSource/SubstraitFileSource.h>
#include <benchmark/benchmark.h>
#include <parquet/arrow/reader.h>
#include <substrait/plan.pb.h>
#include <tests/gluten_test_util.h>
#include <Poco/Util/MapConfiguration.h>
#include <Common/DebugUtils.h>
#include <Common/QueryContext.h>

namespace
{

void BM_ColumnIndexRead_NoFilter(benchmark::State & state)
{
    using namespace DB;

    std::string file = local_engine::test::third_party_data(
        "benchmark/column_index/lineitem/part-00000-9395e12a-3620-4085-9677-c63b920353f4-c000.snappy.parquet");
    Block header{toBlockRowType(local_engine::test::readParquetSchema(file))};
    FormatSettings format_settings;
    Block res;
    for (auto _ : state)
    {
        local_engine::ParquetMetaBuilder metaBuilder{
            .collectPageIndex = true,
            .collectSkipRowGroup = false,
            .case_insensitive = format_settings.parquet.case_insensitive_column_matching,
            .allow_missing_columns = format_settings.parquet.allow_missing_columns};
        ReadBufferFromFilePRead fileReader(file);
        metaBuilder.build(&fileReader, &header, nullptr, [](UInt64 /*midpoint_offset*/) -> bool { return true; });
        local_engine::ColumnIndexRowRangesProvider provider{metaBuilder};
        auto format = std::make_shared<local_engine ::VectorizedParquetBlockInputFormat>(fileReader, header, provider, format_settings);
        auto pipeline = QueryPipeline(std::move(format));
        auto reader = std::make_unique<PullingPipelineExecutor>(pipeline);
        while (reader->pull(res))
        {
            // debug::headBlock(res);
        }
    }
}

void BM_ColumnIndexRead_Old(benchmark::State & state)
{
    using namespace DB;

    std::string file = local_engine::test::third_party_data(
        "benchmark/column_index/lineitem/part-00000-9395e12a-3620-4085-9677-c63b920353f4-c000.snappy.parquet");
    Block header{toBlockRowType(local_engine::test::readParquetSchema(file))};
    FormatSettings format_settings;
    Block res;
    for (auto _ : state)
    {
        ReadBufferFromFilePRead fileReader(file);
        auto format = std::make_shared<ParquetBlockInputFormat>(fileReader, header, format_settings, 1, 1, 8192);
        auto pipeline = QueryPipeline(std::move(format));
        auto reader = std::make_unique<PullingPipelineExecutor>(pipeline);
        while (reader->pull(res))
        {
            // debug::headBlock(res);
        }
    }
}

void BM_ParquetReadDate32(benchmark::State & state)
{
    using namespace DB;
    Block header{
        ColumnWithTypeAndName(DataTypeDate32().createColumn(), std::make_shared<DataTypeDate32>(), "l_shipdate"),
        ColumnWithTypeAndName(DataTypeDate32().createColumn(), std::make_shared<DataTypeDate32>(), "l_commitdate"),
        ColumnWithTypeAndName(DataTypeDate32().createColumn(), std::make_shared<DataTypeDate32>(), "l_receiptdate")};
    std::string file{GLUTEN_SOURCE_TPCH_DIR("lineitem/part-00000-d08071cb-0dfa-42dc-9198-83cb334ccda3-c000.snappy.parquet")};
    FormatSettings format_settings;
    Block res;
    for (auto _ : state)
    {
        auto in = std::make_unique<ReadBufferFromFile>(file);
        auto format = std::make_shared<ParquetBlockInputFormat>(*in, header, format_settings, 1, 1, 8192);
        auto pipeline = QueryPipeline(std::move(format));
        auto reader = std::make_unique<PullingPipelineExecutor>(pipeline);
        while (reader->pull(res))
        {
            // debug::headBlock(res);
        }
    }
}

void BM_OptimizedParquetReadString(benchmark::State & state)
{
    using namespace DB;
    using namespace local_engine;
    Block header{
        ColumnWithTypeAndName(DataTypeString().createColumn(), std::make_shared<DataTypeString>(), "l_returnflag"),
        ColumnWithTypeAndName(DataTypeString().createColumn(), std::make_shared<DataTypeString>(), "l_linestatus")};
    std::string file{GLUTEN_SOURCE_TPCH_URI("lineitem/part-00000-d08071cb-0dfa-42dc-9198-83cb334ccda3-c000.snappy.parquet")};
    Block res;

    for (auto _ : state)
    {
        substrait::ReadRel::LocalFiles files;
        substrait::ReadRel::LocalFiles::FileOrFiles * file_item = files.add_items();
        file_item->set_uri_file(file);
        substrait::ReadRel::LocalFiles::FileOrFiles::ParquetReadOptions parquet_format;
        file_item->mutable_parquet()->CopyFrom(parquet_format);

        auto builder = std::make_unique<QueryPipelineBuilder>();
        builder->init(
            Pipe(std::make_shared<local_engine::SubstraitFileSource>(local_engine::QueryContext::globalContext(), header, files)));
        auto pipeline = QueryPipelineBuilder::getPipeline(std::move(*builder));
        auto reader = PullingPipelineExecutor(pipeline);
        while (reader.pull(res))
        {
            // debug::headBlock(res);
        }
    }
}

void BM_OptimizedParquetReadDate32(benchmark::State & state)
{
    using namespace DB;
    using namespace local_engine;
    Block header{
        ColumnWithTypeAndName(DataTypeDate32().createColumn(), std::make_shared<DataTypeDate32>(), "l_shipdate"),
        ColumnWithTypeAndName(DataTypeDate32().createColumn(), std::make_shared<DataTypeDate32>(), "l_commitdate"),
        ColumnWithTypeAndName(DataTypeDate32().createColumn(), std::make_shared<DataTypeDate32>(), "l_receiptdate")};
    std::string file{GLUTEN_SOURCE_TPCH_URI("lineitem/part-00000-d08071cb-0dfa-42dc-9198-83cb334ccda3-c000.snappy.parquet")};
    Block res;

    for (auto _ : state)
    {
        substrait::ReadRel::LocalFiles files;
        substrait::ReadRel::LocalFiles::FileOrFiles * file_item = files.add_items();
        file_item->set_uri_file(file);
        substrait::ReadRel::LocalFiles::FileOrFiles::ParquetReadOptions parquet_format;
        file_item->mutable_parquet()->CopyFrom(parquet_format);

        auto builder = std::make_unique<QueryPipelineBuilder>();
        builder->init(
            Pipe(std::make_shared<local_engine::SubstraitFileSource>(local_engine::QueryContext::globalContext(), header, files)));
        auto pipeline = QueryPipelineBuilder::getPipeline(std::move(*builder));
        auto reader = PullingPipelineExecutor(pipeline);
        while (reader.pull(res))
        {
            // debug::headBlock(res);
        }
    }
}

substrait::ReadRel::LocalFiles createLocalFiles(const std::string & filename, const bool use_local_format)
{
    substrait::ReadRel::LocalFiles files;
    substrait::ReadRel::LocalFiles::FileOrFiles * file_item = files.add_items();
    file_item->set_uri_file("file://" + filename);
    file_item->set_start(0);
    file_item->set_length(std::filesystem::file_size(filename));
    const substrait::ReadRel::LocalFiles::FileOrFiles::ParquetReadOptions parquet_format;
    file_item->mutable_parquet()->CopyFrom(parquet_format);

    auto config = Poco::AutoPtr(new Poco::Util::MapConfiguration());
    config->setBool("use_local_format", use_local_format);
    local_engine::QueryContext::globalMutableContext()->setConfig(config);

    return files;
}

void doRead(const substrait::ReadRel::LocalFiles & files, const std::optional<DB::ActionsDAG> & pushDown, const DB::Block & header)
{
    const auto builder = std::make_unique<DB::QueryPipelineBuilder>();
    const auto source = std::make_shared<local_engine::SubstraitFileSource>(local_engine::QueryContext::globalContext(), header, files);
    source->setKeyCondition(pushDown, local_engine::QueryContext::globalContext());
    builder->init(DB::Pipe(source));
    auto pipeline = DB::QueryPipelineBuilder::getPipeline(std::move(*builder));
    auto reader = DB::PullingPipelineExecutor(pipeline);
    auto result = header.cloneEmpty();
    size_t total_rows = 0;
    while (reader.pull(result))
    {
#ifndef NDEBUG
        debug::headBlock(result);
#endif
        total_rows += result.rows();
    }
#ifndef NDEBUG
    std::cerr << "rows:" << total_rows << std::endl;
#endif
}

void BM_ColumnIndexRead_Filter_ReturnAllResult(benchmark::State & state)
{
    using namespace DB;

    const std::string filename = local_engine::test::third_party_data(
        "benchmark/column_index/lineitem/part-00000-9395e12a-3620-4085-9677-c63b920353f4-c000.snappy.parquet");
    const std::string filter1 = "l_shipdate is not null AND l_shipdate <= toDate32('1998-09-01')";
    const substrait::ReadRel::LocalFiles files = createLocalFiles(filename, true);
    const AnotherRowType schema = local_engine::test::readParquetSchema(filename);
    auto pushDown = local_engine::test::parseFilter(filter1, schema);
    const Block header = {toBlockRowType(schema)};

    for (auto _ : state)
        doRead(files, pushDown, header);
    local_engine::QueryContext::globalMutableContext()->setConfig(Poco::AutoPtr(new Poco::Util::MapConfiguration()));
}

void BM_ColumnIndexRead_Filter_ReturnHalfResult(benchmark::State & state)
{
    using namespace DB;

    const std::string filename = local_engine::test::third_party_data(
        "benchmark/column_index/lineitem/part-00000-9395e12a-3620-4085-9677-c63b920353f4-c000.snappy.parquet");
    const std::string filter1 = "l_orderkey is not null AND l_orderkey > 300977829";
    const substrait::ReadRel::LocalFiles files = createLocalFiles(filename, true);
    const AnotherRowType schema = local_engine::test::readParquetSchema(filename);
    auto pushDown = local_engine::test::parseFilter(filter1, schema);
    const Block header = {toBlockRowType(schema)};

    for (auto _ : state)
        doRead(files, pushDown, header);
    local_engine::QueryContext::globalMutableContext()->setConfig(Poco::AutoPtr(new Poco::Util::MapConfiguration()));
}

}

BENCHMARK(BM_ColumnIndexRead_Old)->Unit(benchmark::kMillisecond)->Iterations(20);
BENCHMARK(BM_ColumnIndexRead_NoFilter)->Unit(benchmark::kMillisecond)->Iterations(20);
BENCHMARK(BM_ColumnIndexRead_Filter_ReturnAllResult)->Unit(benchmark::kMillisecond)->Iterations(20);
BENCHMARK(BM_ColumnIndexRead_Filter_ReturnHalfResult)->Unit(benchmark::kMillisecond)->Iterations(20);
BENCHMARK(BM_ParquetReadDate32)->Unit(benchmark::kMillisecond)->Iterations(10);
BENCHMARK(BM_OptimizedParquetReadString)->Unit(benchmark::kMillisecond)->Iterations(10);
BENCHMARK(BM_OptimizedParquetReadDate32)->Unit(benchmark::kMillisecond)->Iterations(200);
