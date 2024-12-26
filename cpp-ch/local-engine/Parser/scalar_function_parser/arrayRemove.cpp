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
class FunctionParserArrayRemove : public FunctionParser
{
public:
    FunctionParserArrayRemove(ParserContextPtr parser_context_) : FunctionParser(parser_context_) { }
    ~FunctionParserArrayRemove() override = default;

    static constexpr auto name = "array_remove";
    String getName() const override { return name; }

    const DB::ActionsDAG::Node *
    parse(const substrait::Expression_ScalarFunction & substrait_func, DB::ActionsDAG & actions_dag) const override
    {
        auto parsed_args = parseFunctionArguments(substrait_func, actions_dag);
        if (parsed_args.size() != 2)
            throw Exception(DB::ErrorCodes::SIZES_OF_COLUMNS_DOESNT_MATCH, "Function {} requires exactly two arguments", getName());

        /// Parse spark array_remove(arr, elem)
        /// if (arr == null || elem == null) return null
        /// else return arrayFilter(x -> x != assumeNotNull(elem), assumeNotNull(arr))
        const auto * arr_arg = parsed_args[0];
        const auto * elem_arg = parsed_args[1];
        const auto * arr_not_null = toFunctionNode(actions_dag, "assumeNotNull", {arr_arg});
        const auto * elem_not_null = toFunctionNode(actions_dag, "assumeNotNull", {elem_arg});
        const auto & arr_not_null_type = assert_cast<const DataTypeArray &>(*arr_not_null->result_type);

        /// Create lambda function x -> ifNull(x != assumeNotNull(elem), 1)
        /// Note that notEquals in CH is not null safe, so we need to wrap it with ifNull
        ActionsDAG lambda_actions_dag;
        const auto * x_in_lambda = &lambda_actions_dag.addInput("x", arr_not_null_type.getNestedType());
        const auto * elem_in_lambda = &lambda_actions_dag.addInput(elem_not_null->result_name, elem_not_null->result_type);
        const auto * not_equals_in_lambda = toFunctionNode(lambda_actions_dag, "notEquals", {x_in_lambda, elem_in_lambda});
        const auto * const_one_in_lambda = addColumnToActionsDAG(lambda_actions_dag, std::make_shared<DataTypeUInt8>(), {1});
        const auto * if_null_in_lambda = toFunctionNode(lambda_actions_dag, "ifNull", {not_equals_in_lambda, const_one_in_lambda});
        const auto * lambda_output = if_null_in_lambda;
        lambda_actions_dag.getOutputs().push_back(lambda_output);
        lambda_actions_dag.removeUnusedActions(Names(1, lambda_output->result_name));
        DB::Names captured_column_names{elem_in_lambda->result_name};
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
        const auto * lambda_function = &actions_dag.addFunction(function_capture, {elem_not_null}, lambda_output->result_name);

        /// Apply arrayFilter with the lambda function
        const auto * array_filter = toFunctionNode(actions_dag, "arrayFilter", {lambda_function, arr_not_null});

        /// Return null if arr or elem is null
        const auto * arr_is_null = toFunctionNode(actions_dag, "isNull", {arr_arg});
        const auto * elem_is_null = toFunctionNode(actions_dag, "isNull", {elem_arg});
        const auto * arr_or_elem_is_null = toFunctionNode(actions_dag, "or", {arr_is_null, elem_is_null});
        const auto * null_array_node
            = addColumnToActionsDAG(actions_dag, std::make_shared<DataTypeNullable>(arr_not_null->result_type), {});
        const auto * if_node = toFunctionNode(actions_dag, "if", {arr_or_elem_is_null, null_array_node, array_filter});
        return convertNodeTypeIfNeeded(substrait_func, if_node, actions_dag);
    }
};

static FunctionParserRegister<FunctionParserArrayRemove> register_array_remove;
}
