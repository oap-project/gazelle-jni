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
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeNullable.h>
#include <Functions/FunctionsMiscellaneous.h>
#include <Parser/FunctionParser.h>
#include <Common/Exception.h>
#include <Common/assert_cast.h>

namespace DB
{
namespace ErrorCodes
{
extern const int SIZES_OF_COLUMNS_DOESNT_MATCH;
};
};

namespace local_engine
{
using namespace DB;
class FunctionParserArrayExcept : public FunctionParser
{
public:
    FunctionParserArrayExcept(ParserContextPtr parser_context_) : FunctionParser(parser_context_) { }
    ~FunctionParserArrayExcept() override = default;

    static constexpr auto name = "array_except";
    String getName() const override { return name; }

    const DB::ActionsDAG::Node *
    parse(const substrait::Expression_ScalarFunction & substrait_func, DB::ActionsDAG & actions_dag) const override
    {
        auto parsed_args = parseFunctionArguments(substrait_func, actions_dag);
        if (parsed_args.size() != 2)
            throw Exception(DB::ErrorCodes::SIZES_OF_COLUMNS_DOESNT_MATCH, "Function {} requires exactly two arguments", getName());

        /// Parse spark array_except(arr1, arr2)
        /// if (arr1 == null || arr2 == null)
        ///    return null
        /// else
        ///    return arrayDistinctSpark(arrayFilter(x -> !has(assumeNotNull(arr2), x), assumeNotNull(arr1)))
        ///
        /// Note: we should use arrayDistinctSpark instead of arrayDistinct because of https://github.com/ClickHouse/ClickHouse/issues/69546
        const auto * arr1_arg = parsed_args[0];
        const auto * arr2_arg = parsed_args[1];
        const auto * arr1_not_null = toFunctionNode(actions_dag, "assumeNotNull", {arr1_arg});
        const auto * arr2_not_null = toFunctionNode(actions_dag, "assumeNotNull", {arr2_arg});
        // std::cout << "actions_dag:" << actions_dag.dumpDAG() << std::endl;

        // Create lambda function x -> !has(arr2, x)
        ActionsDAG lambda_actions_dag;
        const auto * arr2_in_lambda = &lambda_actions_dag.addInput(arr2_not_null->result_name, arr2_not_null->result_type);
        const auto & nested_type = assert_cast<const DataTypeArray &>(*removeNullable(arr1_not_null->result_type)).getNestedType();
        const auto * x_in_lambda = &lambda_actions_dag.addInput("x", nested_type);
        const auto * has_in_lambda = toFunctionNode(lambda_actions_dag, "has", {arr2_in_lambda, x_in_lambda});
        const auto * lambda_output = toFunctionNode(lambda_actions_dag, "not", {has_in_lambda});
        lambda_actions_dag.getOutputs().push_back(lambda_output);
        lambda_actions_dag.removeUnusedActions(Names(1, lambda_output->result_name));
        DB::Names captured_column_names{arr2_in_lambda->result_name};
        NamesAndTypesList lambda_arguments_names_and_types;
        lambda_arguments_names_and_types.emplace_back(x_in_lambda->result_name, x_in_lambda->result_type);
        DB::Names required_column_names = lambda_actions_dag.getRequiredColumnsNames();
        auto expression_actions_settings = DB::ExpressionActionsSettings::fromContext(getContext(), DB::CompileExpressions::yes);
        auto function_capture = std::make_shared<FunctionCaptureOverloadResolver>(
            std::move(lambda_actions_dag),
            expression_actions_settings,
            captured_column_names,
            lambda_arguments_names_and_types,
            lambda_output->result_type,
            lambda_output->result_name,
            false);
        const auto * lambda_function = &actions_dag.addFunction(function_capture, {arr2_not_null}, lambda_output->result_name);

        // Apply arrayFilter with the lambda function
        const auto * array_filter_node = toFunctionNode(actions_dag, "arrayFilter", {lambda_function, arr1_not_null});

        // Apply arrayDistinctSpark to the result of arrayFilter
        const auto * array_distinct_node = toFunctionNode(actions_dag, "arrayDistinctSpark", {array_filter_node});

        /// Return null if any of arr1 or arr2 is null
        const auto * arr1_is_null_node = toFunctionNode(actions_dag, "isNull", {arr1_arg});
        const auto * arr2_is_null_node = toFunctionNode(actions_dag, "isNull", {arr2_arg});
        const auto * null_array_node
            = addColumnToActionsDAG(actions_dag, std::make_shared<DataTypeNullable>(array_distinct_node->result_type), {});
        const auto * multi_if_node = toFunctionNode(
            actions_dag,
            "multiIf",
            {
                arr1_is_null_node,
                null_array_node,
                arr2_is_null_node,
                null_array_node,
                array_distinct_node,
            });
        return convertNodeTypeIfNeeded(substrait_func, multi_if_node, actions_dag);
    }
};

static FunctionParserRegister<FunctionParserArrayExcept> register_array_except;
}
