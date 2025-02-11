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
#include <Parser/ParserContext.h>
#include <Parser/SerializedPlanParser.h>
#include <Parser/SubstraitParserUtils.h>
#include <gtest/gtest.h>
#include <Common/DebugUtils.h>
#include <Common/QueryContext.h>

namespace DB::Setting
{
extern const SettingsBool enable_named_columns_in_function_tuple;
}
using namespace local_engine;

using namespace DB;

// Plan for https://github.com/ClickHouse/ClickHouse/pull/54881
INCBIN(_pr_54881_, SOURCE_DIR "/utils/extern-local-engine/tests/json/clickhouse_pr_54881.json");
TEST(Clickhouse, PR54881)
{
    const auto context1 = DB::Context::createCopy(QueryContext::globalContext());
    // context1->setSetting("enable_named_columns_in_function_tuple", DB::Field(true));
    auto settings = context1->getSettingsRef();
    EXPECT_FALSE(settings[Setting::enable_named_columns_in_function_tuple])
        << "GLUTEN NEED set enable_named_columns_in_function_tuple to false";

    constexpr std::string_view split_template
        = R"({"items":[{"uriFile":"{replace_local_files}","partitionIndex":"0","length":"1529","parquet":{},"schema":{},"metadataColumns":[{}]}]})";

    const std::string file{test::gtest_uri("54881.snappy.parquet")};
    auto [_, local_executor] = test::create_plan_and_executor(EMBEDDED_PLAN(_pr_54881_), split_template, file, context1);

    EXPECT_TRUE(local_executor->hasNext());
    const Block & block = *local_executor->nextColumnar();

    debug::headBlock(block);

    EXPECT_EQ(2, block.columns());
    const auto & col_0 = *(block.getColumns()[0]);
    EXPECT_EQ(col_0.getInt(0), 9);
    EXPECT_EQ(col_0.getInt(1), 10);

    Field field;
    const auto & col_1 = *(block.getColumns()[1]);
    col_1.get(0, field);
    const Tuple & row_0 = field.safeGet<DB::Tuple>();
    EXPECT_EQ(2, row_0.size());

    Int64 actual{-1};
    EXPECT_TRUE(row_0[0].tryGet<Int64>(actual));
    EXPECT_EQ(9, actual);

    EXPECT_TRUE(row_0[1].tryGet<Int64>(actual));
    EXPECT_EQ(10, actual);

    col_1.get(1, field);
    const Tuple & row_1 = field.safeGet<DB::Tuple>();
    EXPECT_EQ(2, row_1.size());
    EXPECT_TRUE(row_1[0].tryGet<Int64>(actual));
    EXPECT_EQ(10, actual);

    EXPECT_TRUE(row_1[1].tryGet<Int64>(actual));
    EXPECT_EQ(11, actual);

    EXPECT_FALSE(local_executor->hasNext());
}

// Plan for https://github.com/ClickHouse/ClickHouse/pull/65234
INCBIN(_pr_65234_, SOURCE_DIR "/utils/extern-local-engine/tests/json/clickhouse_pr_65234.json");
TEST(Clickhouse, PR65234)
{
    const std::string split = R"({"items":[{"uriFile":"file:///foo","length":"84633","parquet":{},"schema":{},"metadataColumns":[{}]}]})";
    const auto plan = local_engine::JsonStringToMessage<substrait::Plan>(EMBEDDED_PLAN(_pr_65234_));
    auto parser_context = ParserContext::build(QueryContext::globalContext(), plan);
    SerializedPlanParser parser(parser_context);
    parser.addSplitInfo(local_engine::JsonStringToBinary<substrait::ReadRel::LocalFiles>(split));
    auto query_plan = parser.parse(plan);
}

INCBIN(_pr_68135_, SOURCE_DIR "/utils/extern-local-engine/tests/json/clickhouse_pr_68135.json");
TEST(Clickhouse, PR68135)
{
    constexpr std::string_view split_template
        = R"({"items":[{"uriFile":"{replace_local_files}","partitionIndex":"0","length":"461","parquet":{},"schema":{},"metadataColumns":[{}]}]})";
    const std::string file{test::gtest_uri("68135.snappy.parquet")};
    auto [_, local_executor] = test::create_plan_and_executor(EMBEDDED_PLAN(_pr_68135_), split_template, file);

    EXPECT_TRUE(local_executor->hasNext());
    const Block & x = *local_executor->nextColumnar();
    debug::headBlock(x);
}

INCBIN(_pr_68131_, SOURCE_DIR "/utils/extern-local-engine/tests/json/clickhouse_pr_68131.json");
TEST(Clickhouse, PR68131)
{
    constexpr std::string_view split_template
        = R"({"items":[{"uriFile":"{replace_local_files}","partitionIndex":"0","length":"289","parquet":{},"schema":{},"metadataColumns":[{}]}]})";
    auto [_, local_executor] = test::create_plan_and_executor(EMBEDDED_PLAN(_pr_68131_), split_template, test::gtest_uri("68131.parquet"));
    EXPECT_TRUE(local_executor->hasNext());
    const Block & x = *local_executor->nextColumnar();
    debug::headBlock(x);
}