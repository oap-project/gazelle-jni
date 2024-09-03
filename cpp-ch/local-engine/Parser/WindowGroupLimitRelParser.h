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
#include <unordered_map>
#include <Core/Field.h>
#include <Core/SortDescription.h>
#include <DataTypes/IDataType.h>
#include <Interpreters/WindowDescription.h>
#include <Parser/AggregateFunctionParser.h>
#include <Parser/RelParser.h>
#include <Parser/SerializedPlanParser.h>
#include <Processors/QueryPlan/QueryPlan.h>
#include <Poco/Logger.h>
#include <Common/logger_useful.h>

namespace local_engine
{
/// Similar to WindowRelParser. Some differences
/// 1. cannot support aggregate functions. only support window functions: row_number, rank, dense_rank
/// 2. row_number, rank and dense_rank are mapped to new variants
/// 3. the output columns don't contain window function results
class WindowGroupLimitRelParser : public RelParser
{
public:
    explicit WindowGroupLimitRelParser(SerializedPlanParser * plan_parser_);
    ~WindowGroupLimitRelParser() override = default;
    DB::QueryPlanPtr
    parse(DB::QueryPlanPtr current_plan_, const substrait::Rel & rel, std::list<const substrait::Rel *> & rel_stack_) override;
    const substrait::Rel & getSingleInput(const substrait::Rel & rel) override { return rel.windowgrouplimit().input(); }

private:
    DB::QueryPlanPtr current_plan;
    String window_function_name;

    DB::WindowDescription buildWindowDescription(const substrait::WindowGroupLimitRel & win_rel_def);
    /// There is only one type of window frame at present.
    static DB::WindowFrame buildWindowFrame(const String & function_name);

    DB::SortDescription parsePartitionBy(const google::protobuf::RepeatedPtrField<substrait::Expression> & expressions);

    static DB::WindowFunctionDescription buildWindowFunctionDescription(const String & function_name, size_t limit);
};
}
