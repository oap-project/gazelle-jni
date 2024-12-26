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

class FunctionParserEqualNullSafe : public FunctionParser
{
public:
    explicit FunctionParserEqualNullSafe(ParserContextPtr parser_context_) : FunctionParser(parser_context_) { }
    ~FunctionParserEqualNullSafe() override = default;

    static constexpr auto name = "equal_null_safe";

    String getName() const override { return name; }

    const DB::ActionsDAG::Node * parse(const substrait::Expression_ScalarFunction & substrait_func, DB::ActionsDAG & actions_dag) const override
    {
        /// Parse equal_null_safe(left, right) as:
        /// if (isNull(left) && isNull(right))
        ///     return true
        /// else if (isNull(left) || isNull(right))
        ///     return false
        /// else
        ///     return equals(left, right)
        auto parsed_args = parseFunctionArguments(substrait_func, actions_dag);
        if (parsed_args.size() != 2)
            throw DB::Exception(DB::ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, "Function {} requires exactly two arguments", getName());

        const auto * left_arg = parsed_args[0];
        const auto * right_arg = parsed_args[1];
        const auto * is_left_null_node = toFunctionNode(actions_dag, "isNull", {left_arg});
        const auto * is_right_null_node = toFunctionNode(actions_dag, "isNull", {right_arg});
        const auto * is_both_null_node = toFunctionNode(actions_dag, "and", {is_left_null_node, is_right_null_node});
        const auto * is_either_null_node = toFunctionNode(actions_dag, "or", {is_left_null_node, is_right_null_node});
        const auto * equals_node = toFunctionNode(actions_dag, "equals", {left_arg, right_arg});

        auto result_type = std::make_shared<DB::DataTypeUInt8>();
        const auto * true_node = addColumnToActionsDAG(actions_dag, result_type, 1);
        const auto * false_node = addColumnToActionsDAG(actions_dag, result_type, 0);
        const auto * result_node
            = toFunctionNode(actions_dag, "multiIf", {is_both_null_node, true_node, is_either_null_node, false_node, equals_node});
        return convertNodeTypeIfNeeded(substrait_func, result_node, actions_dag);
    }
};

static FunctionParserRegister<FunctionParserEqualNullSafe> register_equal_null_safe;
}
