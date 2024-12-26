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

#include <DataTypes/DataTypesNumber.h>
#include <Parser/FunctionParser.h>

namespace DB
{
namespace ErrorCodes
{
extern const int BAD_ARGUMENTS;
extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}
}

namespace local_engine
{

class FunctionParserTimestampSeconds : public FunctionParser
{
public:
    explicit FunctionParserTimestampSeconds(ParserContextPtr parser_context_) : FunctionParser(parser_context_) { }
    ~FunctionParserTimestampSeconds() override = default;

    static constexpr auto name = "timestamp_seconds";

    String getName() const override { return name; }

    const DB::ActionsDAG::Node * parse(const substrait::Expression_ScalarFunction & substrait_func, DB::ActionsDAG & actions_dag) const override
    {
        /// Parse timestamp_seconds(expr) as toDateTime64(expr, 6)
        auto parsed_args = parseFunctionArguments(substrait_func, actions_dag);
        if (parsed_args.size() != 1)
            throw DB::Exception(DB::ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, "Function {} requires exactly one argument", getName());

        const auto * arg = parsed_args[0];
        const auto * precision_node = addColumnToActionsDAG(actions_dag, std::make_shared<DB::DataTypeUInt8>(), 6);
        const auto * to_datetime64_node = toFunctionNode(actions_dag, "toDateTime64", {arg, precision_node});
        return convertNodeTypeIfNeeded(substrait_func, to_datetime64_node, actions_dag);
    }
};

static FunctionParserRegister<FunctionParserTimestampSeconds> register_timestamp_seconds;
}
