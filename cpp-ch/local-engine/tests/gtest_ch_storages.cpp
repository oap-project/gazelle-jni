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
#include <Functions/FunctionFactory.h>
#include <Parser/ParserContext.h>
#include <Parser/RelParsers/MergeTreeRelParser.h>
#include <Processors/Executors/PipelineExecutor.h>
#include <Processors/Executors/PullingPipelineExecutor.h>
#include <QueryPipeline/QueryPipelineBuilder.h>
#include <Storages/MergeTree/SparkMergeTreeMeta.h>
#include <Storages/SubstraitSource/SubstraitFileSource.h>
#include <google/protobuf/util/json_util.h>
#include <google/protobuf/wrappers.pb.h>
#include <gtest/gtest.h>
#include <substrait/plan.pb.h>
#include <Common/DebugUtils.h>
#include <Common/QueryContext.h>

using namespace DB;
using namespace local_engine;

TEST(TestBatchParquetFileSource, blob)
{
    GTEST_SKIP();
    Context::ConfigurationPtr config;
    config->setString("blob.storage_account_url", "http://127.0.0.1:10000/devstoreaccount1");
    config->setString("blob.container_name", "libch");
    config->setString("blob.container_already_exists", "true");
    config->setString(
        "blob.connection_string",
        "DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;AccountKey="
        "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;BlobEndpoint=http://127.0.0.1:10000/"
        "devstoreaccount1;");

    auto builder = std::make_unique<QueryPipelineBuilder>();
    substrait::ReadRel::LocalFiles files;
    substrait::ReadRel::LocalFiles::FileOrFiles * file = files.add_items();
    std::string file_path = "wasb://libch/parquet/lineitem/part-00000-f83d0a59-2bff-41bc-acde-911002bf1b33-c000.snappy.parquet";
    file->set_uri_file(file_path);
    substrait::ReadRel::LocalFiles::FileOrFiles::ParquetReadOptions parquet_format;
    file->mutable_parquet()->CopyFrom(parquet_format);

    const auto * type_string = "columns format version: 1\n"
                               "15 columns:\n"
                               "`l_partkey` Int64\n"
                               "`l_suppkey` Int64\n"
                               "`l_linenumber` Int32\n"
                               "`l_quantity` Float64\n"
                               "`l_extendedprice` Float64\n"
                               "`l_discount` Float64\n"
                               "`l_tax` Float64\n"
                               "`l_returnflag` String\n"
                               "`l_linestatus` String\n"
                               "`l_shipdate` Date\n"
                               "`l_commitdate` Date\n"
                               "`l_receiptdate` Date\n"
                               "`l_shipinstruct` String\n"
                               "`l_shipmode` String\n"
                               "`l_comment` String\n";
    auto names_and_types_list = NamesAndTypesList::parse(type_string);
    ColumnsWithTypeAndName columns;
    for (const auto & item : names_and_types_list)
    {
        ColumnWithTypeAndName col;
        col.column = item.type->createColumn();
        col.type = item.type;
        col.name = item.name;
        columns.emplace_back(std::move(col));
    }
    auto header = Block(std::move(columns));
    builder->init(Pipe(std::make_shared<local_engine::SubstraitFileSource>(QueryContext::globalContext(), header, files)));

    auto pipeline = QueryPipelineBuilder::getPipeline(std::move(*builder));
    auto executor = PullingPipelineExecutor(pipeline);
    auto result = header.cloneEmpty();
    size_t total_rows = 0;
    bool is_first = true;
    while (executor.pull(result))
    {
        if (is_first)
            debug::headBlock(result);
        total_rows += result.rows();
        is_first = false;
    }

    ASSERT_TRUE(total_rows > 0);
    std::cerr << "rows:" << total_rows << std::endl;
}

TEST(TestBatchParquetFileSource, s3)
{
    GTEST_SKIP();
    Context::ConfigurationPtr config;
    config->setString("s3.endpoint", "http://localhost:9000/tpch/");
    config->setString("s3.region", "us-east-1");
    config->setString("s3.access_key_id", "admin");
    config->setString("s3.secret_access_key", "password");

    auto builder = std::make_unique<QueryPipelineBuilder>();
    substrait::ReadRel::LocalFiles files;
    substrait::ReadRel::LocalFiles::FileOrFiles * file = files.add_items();
    std::string file_path = "s3://tpch/lineitem/part-00000-f83d0a59-2bff-41bc-acde-911002bf1b33-c000.snappy.parquet";
    file->set_uri_file(file_path);
    substrait::ReadRel::LocalFiles::FileOrFiles::ParquetReadOptions parquet_format;
    file->mutable_parquet()->CopyFrom(parquet_format);

    const auto * type_string = "columns format version: 1\n"
                               "15 columns:\n"
                               "`l_partkey` Int64\n"
                               "`l_suppkey` Int64\n"
                               "`l_linenumber` Int32\n"
                               "`l_quantity` Float64\n"
                               "`l_extendedprice` Float64\n"
                               "`l_discount` Float64\n"
                               "`l_tax` Float64\n"
                               "`l_returnflag` String\n"
                               "`l_linestatus` String\n"
                               "`l_shipdate` Date\n"
                               "`l_commitdate` Date\n"
                               "`l_receiptdate` Date\n"
                               "`l_shipinstruct` String\n"
                               "`l_shipmode` String\n"
                               "`l_comment` String\n";
    auto names_and_types_list = NamesAndTypesList::parse(type_string);
    ColumnsWithTypeAndName columns;
    for (const auto & item : names_and_types_list)
    {
        ColumnWithTypeAndName col;
        col.column = item.type->createColumn();
        col.type = item.type;
        col.name = item.name;
        columns.emplace_back(std::move(col));
    }
    auto header = Block(std::move(columns));
    builder->init(Pipe(std::make_shared<SubstraitFileSource>(QueryContext::globalContext(), header, files)));

    auto pipeline = QueryPipelineBuilder::getPipeline(std::move(*builder));
    auto executor = PullingPipelineExecutor(pipeline);
    auto result = header.cloneEmpty();
    size_t total_rows = 0;
    bool is_first = true;
    while (executor.pull(result))
    {
        if (is_first)
            debug::headBlock(result);
        total_rows += result.rows();
        is_first = false;
    }

    ASSERT_TRUE(total_rows > 0);
    std::cerr << "rows:" << total_rows << std::endl;
}

TEST(TestBatchParquetFileSource, local_file)
{
    GTEST_SKIP();
    auto builder = std::make_unique<QueryPipelineBuilder>();

    substrait::ReadRel::LocalFiles files;
    substrait::ReadRel::LocalFiles::FileOrFiles * file = files.add_items();
    file->set_uri_file(
        "file:///home/admin1/Documents/data/tpch/parquet/lineitem/part-00000-f83d0a59-2bff-41bc-acde-911002bf1b33-c000.snappy.parquet");
    substrait::ReadRel::LocalFiles::FileOrFiles::ParquetReadOptions parquet_format;
    file->mutable_parquet()->CopyFrom(parquet_format);
    file = files.add_items();
    file->set_uri_file(
        "file:///home/admin1/Documents/data/tpch/parquet/lineitem/part-00001-f83d0a59-2bff-41bc-acde-911002bf1b33-c000.snappy.parquet");
    file->mutable_parquet()->CopyFrom(parquet_format);
    file = files.add_items();
    file->set_uri_file(
        "file:///home/admin1/Documents/data/tpch/parquet/lineitem/part-00002-f83d0a59-2bff-41bc-acde-911002bf1b33-c000.snappy.parquet");
    file->mutable_parquet()->CopyFrom(parquet_format);

    const auto * type_string = "columns format version: 1\n"
                               "2 columns:\n"
                               //                               "`l_partkey` Int64\n"
                               //                               "`l_suppkey` Int64\n"
                               //                               "`l_linenumber` Int32\n"
                               //                               "`l_quantity` Float64\n"
                               //                               "`l_extendedprice` Float64\n"
                               "`l_discount` Float64\n"
                               "`l_tax` Float64\n";
    //                               "`l_returnflag` String\n"
    //                               "`l_linestatus` String\n"
    //                               "`l_shipdate` Date\n"
    //                               "`l_commitdate` Date\n"
    //                               "`l_receiptdate` Date\n"
    //                               "`l_shipinstruct` String\n"
    //                               "`l_shipmode` String\n"
    //                               "`l_comment` String\n";
    auto names_and_types_list = NamesAndTypesList::parse(type_string);
    ColumnsWithTypeAndName columns;
    for (const auto & item : names_and_types_list)
    {
        ColumnWithTypeAndName col;
        col.column = item.type->createColumn();
        col.type = item.type;
        col.name = item.name;
        columns.emplace_back(std::move(col));
    }
    auto header = Block(std::move(columns));
    builder->init(Pipe(std::make_shared<SubstraitFileSource>(QueryContext::globalContext(), header, files)));

    auto pipeline = QueryPipelineBuilder::getPipeline(std::move(*builder));
    auto executor = PullingPipelineExecutor(pipeline);
    auto result = header.cloneEmpty();
    size_t total_rows = 0;
    bool is_first = true;
    while (executor.pull(result))
    {
        if (is_first)
            debug::headBlock(result);
        total_rows += result.rows();
        is_first = false;
    }
    std::cerr << "rows:" << total_rows << std::endl;
    ASSERT_TRUE(total_rows == 59986052);
}

TEST(TestPrewhere, OptimizePrewhereCondition)
{
    String filter(R"({"scalarFunction":{"outputType":{"bool":{"nullability":"NULLABILITY_REQUIRED"}},"arguments":[{"value":{"scalarFunction":{"outputType":{"bool":{"nullability":"NULLABILITY_REQUIRED"}},"arguments":[{"value":{"scalarFunction":{"outputType":{"bool":{"nullability":"NULLABILITY_REQUIRED"}},"arguments":[{"value":{"scalarFunction":{"outputType":{"bool":{"nullability":"NULLABILITY_REQUIRED"}},   "arguments":[{"value":{"scalarFunction":{"functionReference":1,"outputType":{"bool":{"nullability":"NULLABILITY_REQUIRED"}},"arguments":[{"value":{"selection":{"directReference":{"structField":{"field":2}}}}},    {"value":{"literal":{"date":8766}}}]}}},{"value":{"scalarFunction":{"functionReference":2,"outputType":{"bool":{"nullability":"NULLABILITY_REQUIRED"}},    "arguments":[{"value":{"selection":{"directReference":{"structField":{"field":2}}}}},{"value":{"literal":{"date":9131}}}]}}}]}}},     {"value":{"scalarFunction":{"functionReference":3,"outputType":{"bool":{"nullability":"NULLABILITY_REQUIRED"}},"arguments":[{"value":{"selection":     {"directReference":{"structField":{}}}}},{"value":{"literal":{"decimal":{"value":"YAkAAAAAAAAAAAAAAAAAAA==","precision":15,"scale":2}}}}]}}}]}}},     {"value":{"scalarFunction":{"functionReference":4,"outputType":{"bool":{"nullability":"NULLABILITY_REQUIRED"}},"arguments":[{"value":{"selection":     {"directReference":{"structField":{"field":1}}}}},{"value":{"literal":{"decimal":{"value":"BQAAAAAAAAAAAAAAAAAAAA==","precision":15,"scale":2}}}}]}}}]}}},{"value":     {"scalarFunction":{"functionReference":5,"outputType":{"bool":{"nullability":"NULLABILITY_REQUIRED"}},"arguments":[{"value":{"selection":{"directReference":{"structField":     {"field":1}}}}},{"value":{"literal":{"decimal":{"value":"BwAAAAAAAAAAAAAAAAAAAA==","precision":15,"scale":2}}}}]}}}]}})");

    auto expr_ptr = std::make_unique<substrait::Expression>();
    auto s = google::protobuf::util::JsonStringToMessage(absl::string_view(filter), expr_ptr.get());
    ASSERT_TRUE(s.ok());

    auto plan_ptr = std::make_unique<substrait::Plan>();
    String extensions_str("{\"extensions\":[{\"extensionFunction\":{\"functionAnchor\":7,\"name\":\"sum:req_i32\"}},{\"extensionFunction\":{\"functionAnchor\":4,\"name\":\"lt:opt_date_date\"}},{\"extensionFunction\":{\"functionAnchor\":6,\"name\":\"multiply:opt_i32_i32\"}},{\"extensionFunction\":{\"name\":\"and:opt_bool_bool\"}},{\"extensionFunction\":{\"functionAnchor\":1,\"name\":\"gte:opt_dec\u003c15,2\u003e_dec\u003c15,2\u003e\"}},{\"extensionFunction\":{\"functionAnchor\":2,\"name\":\"lte:opt_dec\u003c15,2\u003e_dec\u003c15,2\u003e\"}},{\"extensionFunction\":{\"functionAnchor\":5,\"name\":\"lt:opt_dec\u003c15,2\u003e_dec\u003c15,2\u003e\"}},{\"extensionFunction\":{\"functionAnchor\":3,\"name\":\"gte:opt_date_date\"}}],\"relations\":[{\"root\":{\"input\":{\"aggregate\":{\"common\":{\"direct\":{}},\"input\":{\"project\":{\"common\":{\"emit\":{\"outputMapping\":[2]}},\"input\":{\"project\":{\"common\":{\"emit\":{\"outputMapping\":[5,6]}},\"input\":{\"read\":{\"common\":{\"direct\":{}},\"baseSchema\":{\"names\":[\"l_orderkey\",\"l_partkey\",\"l_quantity\",\"l_discount\",\"l_shipdate\"],\"struct\":{\"types\":[{\"i32\":{\"nullability\":\"NULLABILITY_REQUIRED\"}},{\"i32\":{\"nullability\":\"NULLABILITY_REQUIRED\"}},{\"decimal\":{\"scale\":2,\"precision\":15,\"nullability\":\"NULLABILITY_REQUIRED\"}},{\"decimal\":{\"scale\":2,\"precision\":15,\"nullability\":\"NULLABILITY_REQUIRED\"}},{\"date\":{\"nullability\":\"NULLABILITY_REQUIRED\"}}]},\"columnTypes\":[\"NORMAL_COL\",\"NORMAL_COL\",\"NORMAL_COL\",\"NORMAL_COL\",\"NORMAL_COL\"]},\"filter\":{\"scalarFunction\":{\"outputType\":{\"bool\":{\"nullability\":\"NULLABILITY_REQUIRED\"}},\"arguments\":[{\"value\":{\"scalarFunction\":{\"outputType\":{\"bool\":{\"nullability\":\"NULLABILITY_REQUIRED\"}},\"arguments\":[{\"value\":{\"scalarFunction\":{\"outputType\":{\"bool\":{\"nullability\":\"NULLABILITY_REQUIRED\"}},\"arguments\":[{\"value\":{\"scalarFunction\":{\"outputType\":{\"bool\":{\"nullability\":\"NULLABILITY_REQUIRED\"}},\"arguments\":[{\"value\":{\"scalarFunction\":{\"functionReference\":1,\"outputType\":{\"bool\":{\"nullability\":\"NULLABILITY_REQUIRED\"}},\"arguments\":[{\"value\":{\"selection\":{\"directReference\":{\"structField\":{\"field\":3}}}}},{\"value\":{\"literal\":{\"decimal\":{\"value\":\"BQAAAAAAAAAAAAAAAAAAAA==\",\"precision\":15,\"scale\":2}}}}]}}},{\"value\":{\"scalarFunction\":{\"functionReference\":2,\"outputType\":{\"bool\":{\"nullability\":\"NULLABILITY_REQUIRED\"}},\"arguments\":[{\"value\":{\"selection\":{\"directReference\":{\"structField\":{\"field\":3}}}}},{\"value\":{\"literal\":{\"decimal\":{\"value\":\"BwAAAAAAAAAAAAAAAAAAAA==\",\"precision\":15,\"scale\":2}}}}]}}}]}}},{\"value\":{\"scalarFunction\":{\"functionReference\":3,\"outputType\":{\"bool\":{\"nullability\":\"NULLABILITY_REQUIRED\"}},\"arguments\":[{\"value\":{\"selection\":{\"directReference\":{\"structField\":{\"field\":4}}}}},{\"value\":{\"literal\":{\"date\":8766}}}]}}}]}}},{\"value\":{\"scalarFunction\":{\"functionReference\":4,\"outputType\":{\"bool\":{\"nullability\":\"NULLABILITY_REQUIRED\"}},\"arguments\":[{\"value\":{\"selection\":{\"directReference\":{\"structField\":{\"field\":4}}}}},{\"value\":{\"literal\":{\"date\":9131}}}]}}}]}}},{\"value\":{\"scalarFunction\":{\"functionReference\":5,\"outputType\":{\"bool\":{\"nullability\":\"NULLABILITY_REQUIRED\"}},\"arguments\":[{\"value\":{\"selection\":{\"directReference\":{\"structField\":{\"field\":2}}}}},{\"value\":{\"literal\":{\"decimal\":{\"value\":\"YAkAAAAAAAAAAAAAAAAAAA==\",\"precision\":15,\"scale\":2}}}}]}}}]}},\"extensionTable\":{\"detail\":{\"@type\":\"type.googleapis.com/google.protobuf.StringValue\",\"value\":\"MergeTree;default\nlineitem\nvar/lib/clickhouse/data/tpch100_bak2/lineitem\n1\n574\n\"}}}},\"expressions\":[{\"selection\":{\"directReference\":{\"structField\":{}}}},{\"selection\":{\"directReference\":{\"structField\":{\"field\":1}}}}]}},\"expressions\":[{\"scalarFunction\":{\"functionReference\":6,\"outputType\":{\"i32\":{\"nullability\":\"NULLABILITY_REQUIRED\"}},\"arguments\":[{\"value\":{\"selection\":{\"directReference\":{\"structField\":{}}}}},{\"value\":{\"selection\":{\"directReference\":{\"structField\":{\"field\":1}}}}}]}}]}},\"groupings\":[{}],\"measures\":[{\"measure\":{\"functionReference\":7,\"phase\":\"AGGREGATION_PHASE_INITIAL_TO_INTERMEDIATE\",\"outputType\":{\"i64\":{\"nullability\":\"NULLABILITY_NULLABLE\"}},\"arguments\":[{\"value\":{\"selection\":{\"directReference\":{\"structField\":{}}}}}]}}]}},\"names\":[\"sum#909\"],\"outputSchema\":{\"types\":[{\"i64\":{\"nullability\":\"NULLABILITY_NULLABLE\"}}],\"nullability\":\"NULLABILITY_REQUIRED\"}}}]}");
    auto ext = google::protobuf::util::JsonStringToMessage(absl::string_view(extensions_str), plan_ptr.get());
    ASSERT_TRUE(ext.ok());

    const auto * type_string = "columns format version: 1\n"
                               "5 columns:\n"
                               "`l_quantity` decimal(15,2)\n"
                               "`l_discount` decimal(15,2)\n"
                               "`l_shipdate` date32\n"
                               "`l_orderkey` Int64\n"
                               "`l_partkey` Int64\n";
    auto names_and_types_list = NamesAndTypesList::parse(type_string);
    ColumnsWithTypeAndName columns;
    for (const auto & item : names_and_types_list)
    {
        ColumnWithTypeAndName col;
        col.column = item.type->createColumn();
        col.type = item.type;
        col.name = item.name;
        columns.emplace_back(std::move(col));
    }
    Block block(std::move(columns));

    ContextPtr context = QueryContext::globalContext();
    ParserContextPtr parser_context = ParserContext::build(context, *plan_ptr);
    SerializedPlanParser * parser = new SerializedPlanParser(parser_context);

    MergeTreeRelParser mergeTreeParser(parser_context, QueryContext::globalContext());

    mergeTreeParser.column_sizes["l_discount"] = 0;
    mergeTreeParser.column_sizes["l_quantity"] = 1;
    mergeTreeParser.column_sizes["l_partkey"] = 2;
    mergeTreeParser.column_sizes["l_orderkey"] = 3;
    mergeTreeParser.column_sizes["l_shipdate"] = 4;

    MergeTreeRelParser::Conditions res;
    std::set<Int64> pk_positions;
    mergeTreeParser.analyzeExpressions(res, *expr_ptr, pk_positions, block);
    res.sort();
    ASSERT_TRUE(res.front().table_columns.contains("l_discount"));
    ASSERT_TRUE(res.back().table_columns.contains("l_shipdate"));
}
