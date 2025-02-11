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
#include <gluten_test_util.h>
#include <incbin.h>
#include <Core/Settings.h>
#include <Interpreters/Context.h>
#include <Parser/LocalExecutor.h>
#include <Parser/SubstraitParserUtils.h>
#include <base/scope_guard.h>
#include <gtest/gtest.h>
#include <substrait/plan.pb.h>
#include <Common/DebugUtils.h>
#include <Common/GlutenConfig.h>
#include <Common/QueryContext.h>

using namespace local_engine;
using namespace DB;

INCBIN(_pr_18_2, SOURCE_DIR "/utils/extern-local-engine/tests/data/decimal_filter_push_down/18_2.json");
TEST(ColumnIndex, Decimal182)
{
    // [precision,scale] = [18,2]
    auto query_id = QueryContext::instance().initializeQuery("RowIndex");
    SCOPE_EXIT({ QueryContext::instance().finalizeQuery(query_id); });
    const auto context = QueryContext::instance().currentQueryContext();
    const auto config = ExecutorConfig::loadFromContext(context);
    EXPECT_TRUE(config.use_local_format) << "gtest need set use_local_format to true";

    constexpr std::string_view split_template
        = R"({"items":[{"uriFile":"{replace_local_files}","partitionIndex":"0","length":"488","parquet":{},"schema":{},"metadataColumns":[{}]}]})";
    const std::string file{test::gtest_uri("decimal_filter_push_down/18_2_flba.snappy.parquet")};
    auto [_, local_executor] = test::create_plan_and_executor(EMBEDDED_PLAN(_pr_18_2), split_template, file, context);

    EXPECT_TRUE(local_executor->hasNext());
    const Block & x = *local_executor->nextColumnar();
    debug::headBlock(x);
}

void readFile(
    std::string_view json_plan,
    std::string_view split_template,
    std::string_view file,
    const std::function<void(LocalExecutor &)> & callback,
    const TestSettings & test_settings = {{"input_format_parquet_allow_missing_columns", false}})
{
    auto query_id = QueryContext::instance().initializeQuery("RowIndex");
    SCOPE_EXIT({ QueryContext::instance().finalizeQuery(query_id); });
    const auto context = QueryContext::instance().currentQueryContext();
    for (const auto & x : test_settings)
        context->setSetting(x.first, x.second);
    auto [_, local_executor] = test::create_plan_and_executor(json_plan, split_template, file, context);
    callback(*local_executor);
}

INCBIN(_read_metadata, SOURCE_DIR "/utils/extern-local-engine/tests/json/parquet_metadata/read_metadata.row_index.json");
TEST(RowIndex, Basic)
{
    const std::string file{test::gtest_uri("metadata.rowindex.snappy.parquet")};
    constexpr std::string_view split_template
        = R"({"items":[{"uriFile":"{replace_local_files}","length":"1767","parquet":{},"partitionColumns":[{"key":"pb","value":"1003"}],"schema":{},"metadataColumns":[{"key":"input_file_name","value":"{replace_local_files}"},{"key":"input_file_block_length","value":"1767"},{"key":"input_file_block_start","value":"0"}],"properties":{"fileSize":"1767","modificationTime":"1736847651881"}}]})";
    readFile(
        EMBEDDED_PLAN(_read_metadata),
        split_template,
        file,
        [&](LocalExecutor & local_executor)
        {
            EXPECT_TRUE(local_executor.hasNext());
            debug::headBlock(*local_executor.nextColumnar());
        });
}

INCBIN(_rowindex_in, SOURCE_DIR "/utils/extern-local-engine/tests/json/parquet_metadata/rowindex_in.json");
TEST(RowIndex, In)
{
    const std::string file{test::gtest_uri("rowindex_in.snappy.parquet")};
    /// all row gorups are ignored
    constexpr std::string_view split_template_ignore_all_rg
        = R"({"items":[{"uriFile":"{replace_local_files}","length":"256","parquet":{},"schema":{},"metadataColumns":[{"key":"input_file_name","value":"{replace_local_files}"},{"key":"input_file_block_length","value":"256"},{"key":"input_file_block_start","value":"0"}],"properties":{"fileSize":"125451","modificationTime":"1737104830724"}}]})";

    readFile(
        EMBEDDED_PLAN(_rowindex_in),
        split_template_ignore_all_rg,
        file,
        [&](LocalExecutor & local_executor) { EXPECT_FALSE(local_executor.hasNext()); });

    constexpr std::string_view split_template
        = R"({"items":[{"uriFile":"{replace_local_files}","length":"125451","parquet":{},"schema":{},"metadataColumns":[{"key":"input_file_name","value":"{replace_local_files}"},{"key":"input_file_block_length","value":"256"},{"key":"input_file_block_start","value":"0"}],"properties":{"fileSize":"125451","modificationTime":"1737104830724"}}]})";

    readFile(
        EMBEDDED_PLAN(_rowindex_in),
        split_template,
        file,
        [&](LocalExecutor & local_executor)
        {
            EXPECT_TRUE(local_executor.hasNext());
            debug::headBlock(*local_executor.nextColumnar());
        });
}

INCBIN(_all_meta, SOURCE_DIR "/utils/extern-local-engine/tests/json/parquet_metadata/read_metadata.all.json");
TEST(RowIndex, AllMeta)
{
    const std::string file{test::gtest_uri("all_meta/part-00000-92bb25d0-7446-4f9b-8bdd-a6911d0d465a-c000.snappy.parquet")};
    constexpr std::string_view split_template
        = R"({"items":[{"uriFile":"{replace_local_files}","length":"1282","parquet":{},"schema":{},"metadataColumns":[{"key":"file_path","value":"{replace_local_files}"},{"key":"file_block_length","value":"1282"},{"key":"input_file_name","value":"{replace_local_files}"},{"key":"input_file_block_length","value":"1282"},{"key":"file_name","value":"part-00000-484a7344-cf25-4367-bf46-8123a6a7b71e-c000.snappy.parquet"},{"key":"file_modification_time","value":"2025-01-19 05:09:48.664"},{"key":"file_block_start","value":"0"},{"key":"input_file_block_start","value":"0"},{"key":"file_size","value":"1282"}],"properties":{"fileSize":"1282","modificationTime":"1737263388664"}}]})";

    readFile(
        EMBEDDED_PLAN(_all_meta),
        split_template,
        file,
        [&](LocalExecutor & local_executor)
        {
            EXPECT_TRUE(local_executor.hasNext());
            debug::headBlock(*local_executor.nextColumnar());
        });
}

INCBIN(_input_filename, SOURCE_DIR "/utils/extern-local-engine/tests/json/parquet_metadata/input_filename.json");
INCBIN(
    _input_filename_no_real_column, SOURCE_DIR "/utils/extern-local-engine/tests/json/parquet_metadata/input_filename_no_real_column.json");
TEST(RowIndex, InputFileName)
{
    const std::string file{test::gtest_uri("input_filename.snappy.parquet")};
    constexpr std::string_view split_template
        = R"({"items":[{"uriFile":"{replace_local_files}","length":"443","parquet":{},"schema":{},"metadataColumns":[{"key":"input_file_name","value":"{replace_local_files}"},{"key":"input_file_block_length","value":"443"},{"key":"input_file_block_start","value":"0"}],"properties":{"fileSize":"443","modificationTime":"1737445386987"}}]})";

    readFile(
        EMBEDDED_PLAN(_input_filename),
        split_template,
        file,
        [&](LocalExecutor & local_executor)
        {
            EXPECT_TRUE(local_executor.hasNext());
            debug::headBlock(*local_executor.nextColumnar());
        });

    readFile(
        EMBEDDED_PLAN(_input_filename_no_real_column),
        split_template,
        file,
        [&](LocalExecutor & local_executor)
        {
            EXPECT_TRUE(local_executor.hasNext());
            debug::headBlock(*local_executor.nextColumnar());
        });
}