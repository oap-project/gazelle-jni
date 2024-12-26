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
#include <Core/Field.h>
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

class FunctionParserSortArray : public FunctionParser
{
public:
    explicit FunctionParserSortArray(ParserContextPtr parser_context_) : FunctionParser(parser_context_) { }
    ~FunctionParserSortArray() override = default;

    static constexpr auto name = "sort_array";

    String getName() const override { return name; }

    const DB::ActionsDAG::Node * parse(
        const substrait::Expression_ScalarFunction & substrait_func,
        DB::ActionsDAG & actions_dag) const override
    {
        auto parsed_args = parseFunctionArguments(substrait_func, actions_dag);
        if (parsed_args.size() != 2)
            throw DB::Exception(DB::ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, "Function {} requires exactly two arguments", getName());

        const auto * array_arg = parsed_args[0];
        const auto * order_arg = parsed_args[1];

        const auto * sort_node = toFunctionNode(actions_dag, "sortArraySpark", {array_arg});
        const auto * reverse_sort_node = toFunctionNode(actions_dag, "reverseSortArraySpark", {array_arg});

        const auto * result_node = toFunctionNode(actions_dag, "if", {order_arg, sort_node, reverse_sort_node});
        return result_node;
    }
};

static FunctionParserRegister<FunctionParserSortArray> register_sort_array;

}
