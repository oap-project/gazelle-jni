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

#include <Columns/IColumn.h>
#include <Core/Block.h>
#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/IDataType.h>
#include <Functions/FunctionFactory.h>
#include <benchmark/benchmark.h>
#include <Common/QueryContext.h>

using namespace DB;

static Block createDataBlock(size_t rows)
{
    auto type = DataTypeFactory::instance().get("Float64");
    auto column = type->createColumn();
    for (size_t i = 0; i < rows; ++i)
        column->insert(i * 1.0f);
    Block block;
    block.insert(ColumnWithTypeAndName(std::move(column), type, "d"));
    return std::move(block);
}

static void BM_CHCastFloatToInt(benchmark::State & state)
{
    using namespace DB;
    auto & factory = FunctionFactory::instance();
    auto function = factory.get("CAST", local_engine::QueryContext::globalContext());
    Block block = createDataBlock(30000000);
    DB::ColumnsWithTypeAndName args;
    args.emplace_back(block.getColumnsWithTypeAndName()[0]);
    DB::ColumnWithTypeAndName type_name_col;
    type_name_col.name = "Int64";
    type_name_col.column = DB::DataTypeString().createColumnConst(0, type_name_col.name);
    type_name_col.type = std::make_shared<DB::DataTypeString>();
    args.emplace_back(type_name_col);
    auto executable = function->build(args);
    for (auto _ : state) [[maybe_unused]]
        auto result = executable->execute(block.getColumnsWithTypeAndName(), executable->getResultType(), block.rows(), false);
}

static void BM_SparkCastFloatToInt(benchmark::State & state)
{
    using namespace DB;
    auto & factory = FunctionFactory::instance();
    auto function = factory.get("sparkCastFloatToInt64", local_engine::QueryContext::globalContext());
    Block block = createDataBlock(30000000);
    auto executable = function->build(block.getColumnsWithTypeAndName());
    for (auto _ : state) [[maybe_unused]]
        auto result = executable->execute(block.getColumnsWithTypeAndName(), executable->getResultType(), block.rows(), false);
}

BENCHMARK(BM_CHCastFloatToInt)->Unit(benchmark::kMillisecond)->Iterations(100);
BENCHMARK(BM_SparkCastFloatToInt)->Unit(benchmark::kMillisecond)->Iterations(100);