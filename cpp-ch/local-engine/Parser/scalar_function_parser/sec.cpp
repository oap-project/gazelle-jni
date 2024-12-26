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
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}
}

namespace local_engine
{

class FunctionParserSec : public FunctionParser
{
public:
    explicit FunctionParserSec(ParserContextPtr parser_context_) : FunctionParser(parser_context_) { }
    ~FunctionParserSec() override = default;

    static constexpr auto name = "sec";

    String getName() const override { return name; }

    const DB::ActionsDAG::Node * parse(
        const substrait::Expression_ScalarFunction & substrait_func,
        DB::ActionsDAG & actions_dag) const override
    {
        /// parse sec(x) as 1 / cos(x)
        auto parsed_args = parseFunctionArguments(substrait_func, actions_dag);
        if (parsed_args.size() != 1)
            throw DB::Exception(DB::ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, "Function {} requires exactly one arguments", getName());

        const auto * x = parsed_args[0];
        const auto * cos_node = toFunctionNode(actions_dag, "cos", {x});
        const auto * one_const_node = addColumnToActionsDAG(actions_dag, std::make_shared<DB::DataTypeFloat64>(), 1.0);
        const auto * result_node = toFunctionNode(actions_dag, "divide", {one_const_node, cos_node});

        return convertNodeTypeIfNeeded(substrait_func, result_node, actions_dag);;
    }
};

static FunctionParserRegister<FunctionParserSec> register_sec;

}
