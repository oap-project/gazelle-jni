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
#include <Columns/ColumnSet.h>
#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/DataTypeSet.h>
#include <Functions/FunctionFactory.h>
#include <Interpreters/Set.h>
#include <gtest/gtest.h>
#include <Common/BlockTypeUtils.h>
#include <Common/DebugUtils.h>
#include <Common/QueryContext.h>
#include "IO/ReadBufferFromString.h"

TEST(TestFunction, murmurHash2_64)
{
    using namespace DB;
    auto & factory = FunctionFactory::instance();
    auto function = factory.get("murmurHash2_64", local_engine::QueryContext::globalContext());
    auto type0 = DataTypeFactory::instance().get("String");
    auto column0 = type0->createColumn();
    column0->insert("A");
    column0->insert("A");
    column0->insert("B");
    column0->insert("c");

    auto column1 = type0->createColumn();
    column1->insert("X");
    column1->insert("X");
    column1->insert("Y");
    column1->insert("Z");

    ColumnsWithTypeAndName columns
        = {ColumnWithTypeAndName(std::move(column0), type0, "string0"), ColumnWithTypeAndName(std::move(column1), type0, "string0")};
    Block block(columns);
    std::cerr << "input:\n";
    debug::headBlock(block);
    auto executable = function->build(block.getColumnsWithTypeAndName());
    auto result = executable->execute(block.getColumnsWithTypeAndName(), executable->getResultType(), block.rows(), false);
    std::cerr << "output:\n";
    debug::headColumn(result);
    ASSERT_EQ(result->getUInt(0), result->getUInt(1));
}

TEST(TestFunction, toDateTime64)
{
    using namespace DB;
    auto & factory = FunctionFactory::instance();
    auto function = factory.get("toDateTime64", local_engine::QueryContext::globalContext());

    auto d0 = local_engine::STRING();
    auto c0 = d0->createColumn();
    c0->insert("2025-01-21 12:58:13.106");

    auto d1 = local_engine::UINT();
    auto c1 = d1->createColumnConst(1, 6);

    auto d2 = local_engine::STRING();
    auto c2 = d0->createColumnConst(1, "UTC");


    ColumnsWithTypeAndName columns
        = {ColumnWithTypeAndName(std::move(c0), d0, "string0"), ColumnWithTypeAndName(c1, d1, "int0"), ColumnWithTypeAndName(c2, d2, "tz")};

    Block block(columns);
    std::cerr << "input:\n";
    debug::headBlock(block);
    auto executable = function->build(block.getColumnsWithTypeAndName());
    auto result = executable->execute(block.getColumnsWithTypeAndName(), executable->getResultType(), block.rows(), false);
    std::cerr << "output:\n";
    debug::headColumn(result);

    DateTime64 time = 0;
    {
        std::string parsedTimeStamp = "2025-01-21 12:58:13.106";
        DB::ReadBufferFromString in(parsedTimeStamp);
        readDateTime64Text(time, 6, in, DateLUT::instance("UTC"));
    }
    Field expected = Field(DecimalField<DateTime64>(time, 6));

    ASSERT_EQ((*result.get())[0], expected);
}

TEST(TestFunction, In)
{
    using namespace DB;
    auto & factory = FunctionFactory::instance();
    auto function = factory.get("in", local_engine::QueryContext::globalContext());
    auto type0 = DataTypeFactory::instance().get("String");
    auto type_set = std::make_shared<DataTypeSet>();


    auto column1 = type0->createColumn();
    column1->insert("X");
    column1->insert("X");
    column1->insert("Y");
    column1->insert("Z");

    SizeLimits limit;
    auto set = std::make_shared<Set>(limit, true, false);
    Block col1_set_block;
    auto col1_set = type0->createColumn();
    col1_set->insert("X");
    col1_set->insert("Y");

    col1_set_block.insert(ColumnWithTypeAndName(std::move(col1_set), type0, "string0"));
    set->setHeader(col1_set_block.getColumnsWithTypeAndName());
    set->insertFromBlock(col1_set_block.getColumnsWithTypeAndName());
    set->finishInsert();
    PreparedSets::Hash empty;
    auto future_set = std::make_shared<FutureSetFromStorage>(empty, nullptr, std::move(set), std::nullopt);
    //TODO: WHY? after https://github.com/ClickHouse/ClickHouse/pull/63723 we need pass 4 instead of 1
    auto arg = ColumnSet::create(4, future_set);

    ColumnsWithTypeAndName columns
        = {ColumnWithTypeAndName(std::move(column1), type0, "string0"), ColumnWithTypeAndName(std::move(arg), type_set, "__set")};
    Block block(columns);
    std::cerr << "input:\n";
    debug::headBlock(block);
    auto executable = function->build(block.getColumnsWithTypeAndName());
    auto result = executable->execute(block.getColumnsWithTypeAndName(), executable->getResultType(), block.rows(), false);
    std::cerr << "output:\n";
    debug::headColumn(result);
    ASSERT_EQ(result->getUInt(3), 0);
}


TEST(TestFunction, NotIn1)
{
    using namespace DB;
    auto & factory = FunctionFactory::instance();
    auto function = factory.get("notIn", local_engine::QueryContext::globalContext());
    auto type0 = DataTypeFactory::instance().get("String");
    auto type_set = std::make_shared<DataTypeSet>();


    auto column1 = type0->createColumn();
    column1->insert("X");
    column1->insert("X");
    column1->insert("Y");
    column1->insert("Z");

    SizeLimits limit;
    auto set = std::make_shared<Set>(limit, true, false);
    Block col1_set_block;
    auto col1_set = type0->createColumn();
    col1_set->insert("X");
    col1_set->insert("Y");

    col1_set_block.insert(ColumnWithTypeAndName(std::move(col1_set), type0, "string0"));
    set->setHeader(col1_set_block.getColumnsWithTypeAndName());
    set->insertFromBlock(col1_set_block.getColumnsWithTypeAndName());
    set->finishInsert();
    PreparedSets::Hash empty;
    auto future_set = std::make_shared<FutureSetFromStorage>(empty, nullptr, std::move(set), std::nullopt);

    //TODO: WHY? after https://github.com/ClickHouse/ClickHouse/pull/63723 we need pass 4 instead of 1
    auto arg = ColumnSet::create(4, future_set);

    ColumnsWithTypeAndName columns
        = {ColumnWithTypeAndName(std::move(column1), type0, "string0"), ColumnWithTypeAndName(std::move(arg), type_set, "__set")};
    Block block(columns);
    std::cerr << "input:\n";
    debug::headBlock(block);
    auto executable = function->build(block.getColumnsWithTypeAndName());
    auto result = executable->execute(block.getColumnsWithTypeAndName(), executable->getResultType(), block.rows(), false);
    std::cerr << "output:\n";
    debug::headColumn(result);
    ASSERT_EQ(result->getUInt(3), 1);
}

TEST(TestFunction, NotIn2)
{
    using namespace DB;
    auto & factory = FunctionFactory::instance();
    auto function = factory.get("in", local_engine::QueryContext::globalContext());
    auto type0 = DataTypeFactory::instance().get("String");
    auto type_set = std::make_shared<DataTypeSet>();


    auto column1 = type0->createColumn();
    column1->insert("X");
    column1->insert("X");
    column1->insert("Y");
    column1->insert("Z");

    SizeLimits limit;
    auto set = std::make_shared<Set>(limit, true, false);
    Block col1_set_block;
    auto col1_set = type0->createColumn();
    col1_set->insert("X");
    col1_set->insert("Y");

    col1_set_block.insert(ColumnWithTypeAndName(std::move(col1_set), type0, "string0"));
    set->setHeader(col1_set_block.getColumnsWithTypeAndName());
    set->insertFromBlock(col1_set_block.getColumnsWithTypeAndName());
    set->finishInsert();
    PreparedSets::Hash empty;
    auto future_set = std::make_shared<FutureSetFromStorage>(empty, nullptr, std::move(set), std::nullopt);

    //TODO: WHY? after https://github.com/ClickHouse/ClickHouse/pull/63723 we need pass 4 instead of 1
    auto arg = ColumnSet::create(4, future_set);

    ColumnsWithTypeAndName columns
        = {ColumnWithTypeAndName(std::move(column1), type0, "string0"), ColumnWithTypeAndName(std::move(arg), type_set, "__set")};
    Block block(columns);
    std::cerr << "input:\n";
    debug::headBlock(block);
    auto executable = function->build(block.getColumnsWithTypeAndName());
    auto result = executable->execute(block.getColumnsWithTypeAndName(), executable->getResultType(), block.rows(), false);

    auto function_not = factory.get("not", local_engine::QueryContext::globalContext());
    auto type_bool = DataTypeFactory::instance().get("UInt8");
    ColumnsWithTypeAndName columns2 = {ColumnWithTypeAndName(result, type_bool, "string0")};
    Block block2(columns2);
    auto executable2 = function_not->build(block2.getColumnsWithTypeAndName());
    auto result2 = executable2->execute(block2.getColumnsWithTypeAndName(), executable2->getResultType(), block2.rows(), false);
    std::cerr << "output:\n";
    debug::headColumn(result2);
    ASSERT_EQ(result2->getUInt(3), 1);
}
