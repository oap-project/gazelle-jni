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

#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeString.h>
#include <Parser/FunctionParser.h>
#include <Common/CHUtil.h>


namespace DB
{

namespace ErrorCodes
{
extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
extern const int ILLEGAL_TYPE_OF_ARGUMENT;
}
}

namespace local_engine
{

class FunctionParserUtcTimestampTransform : public FunctionParser
{
public:
    explicit FunctionParserUtcTimestampTransform(ParserContextPtr parser_context_) : FunctionParser(parser_context_) { }
    ~FunctionParserUtcTimestampTransform() override = default;

    const DB::ActionsDAG::Node * parse(const substrait::Expression_ScalarFunction & substrait_func, DB::ActionsDAG & actions_dag) const override
    {
        /// Convert timezone value to clickhouse backend supported, i.e. GMT+8 -> Etc/GMT-8, +08:00 -> Etc/GMT-8
        if (substrait_func.arguments_size() != 2)
            throw DB::Exception(DB::ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, "Function {}'s must have 2 arguments", getName());

        const substrait::Expression & arg1 = substrait_func.arguments()[1].value();
        if (!arg1.has_literal() || !arg1.literal().has_string())
            throw DB::Exception(DB::ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,  "Function {}'s 2nd argument should be string literal", getName());

        const String & arg1_literal = arg1.literal().string();
        String time_zone_val = DateTimeUtil::convertTimeZone(arg1_literal);
        auto parsed_args = parseFunctionArguments(substrait_func, actions_dag);
        auto nullable_string_type = DB::makeNullable(std::make_shared<DB::DataTypeString>());
        const auto * time_zone_node = addColumnToActionsDAG(actions_dag, nullable_string_type, time_zone_val);
        const auto * result_node = toFunctionNode(actions_dag, getCHFunctionName(substrait_func), {parsed_args[0], time_zone_node});
        return convertNodeTypeIfNeeded(substrait_func, result_node, actions_dag);
    }
};
}
