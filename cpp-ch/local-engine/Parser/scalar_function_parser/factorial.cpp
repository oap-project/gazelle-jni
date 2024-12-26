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

#include <DataTypes/DataTypeNullable.h>
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

class FunctionParserFactorial : public FunctionParser
{
public:
    explicit FunctionParserFactorial(ParserContextPtr parser_context_) : FunctionParser(parser_context_) {}
    ~FunctionParserFactorial() override = default;

    static constexpr auto name = "factorial";

    String getName() const override { return name; }

    const DB::ActionsDAG::Node * parse(
        const substrait::Expression_ScalarFunction & substrait_func,
        DB::ActionsDAG & actions_dag) const override
    {
        /// parse factorial(x) as if (x > 20 || x < 0) null else factorial(x)
        auto parsed_args = parseFunctionArguments(substrait_func, actions_dag);
        if (parsed_args.size() != 1)
            throw DB::Exception(DB::ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, "Function {} requires exactly one arguments", getName());

        const auto * x = parsed_args[0];

        const auto * zero_const_node = addColumnToActionsDAG(actions_dag, std::make_shared<DB::DataTypeInt32>(), 0);
        const auto * twenty_const_node = addColumnToActionsDAG(actions_dag, std::make_shared<DB::DataTypeInt32>(), 20);

        const auto * greater_than_twenty_node = toFunctionNode(actions_dag, "greater", {x, twenty_const_node});
        const auto * less_than_zero_node = toFunctionNode(actions_dag, "less", {x, zero_const_node});
        const auto * or_node = toFunctionNode(actions_dag, "or", {greater_than_twenty_node, less_than_zero_node});

        // tricky: use tricky_if_node instead of x node to avoid exception: The maximum value for the input argument of function factorial is 20
        const auto * tricky_if_node = toFunctionNode(actions_dag, "if", {or_node, zero_const_node, x});
        const auto * factorial_node = toFunctionNode(actions_dag, "factorial", {tricky_if_node});
        const auto * null_const_node = addColumnToActionsDAG(actions_dag, makeNullable(factorial_node->result_type), DB::Field());

        const auto * result_node = toFunctionNode(actions_dag, "if", {or_node, null_const_node, factorial_node});

        return convertNodeTypeIfNeeded(substrait_func, result_node, actions_dag);;
    }
};

static FunctionParserRegister<FunctionParserFactorial> register_factorial;
}
