
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
class FunctionParserArrayRepeat : public FunctionParser
{
public:
    FunctionParserArrayRepeat(ParserContextPtr parser_context_) : FunctionParser(parser_context_) { }
    ~FunctionParserArrayRepeat() override = default;

    static constexpr auto name = "array_repeat";
    String getName() const override { return name; }

    const DB::ActionsDAG::Node *
    parse(const substrait::Expression_ScalarFunction & substrait_func, DB::ActionsDAG & actions_dag) const override
    {
        auto parsed_args = parseFunctionArguments(substrait_func, actions_dag);
        if (parsed_args.size() != 2)
            throw Exception(DB::ErrorCodes::SIZES_OF_COLUMNS_DOESNT_MATCH, "Function {} requires exactly two arguments", getName());

        /// Parse spark array_repeat(elem, n)
        /// if (n == null) return null
        /// else return arrayMap(x -> elem, range(greatest(assumeNotNull(n))))
        const auto * elem_arg = parsed_args[0];
        const auto * n_arg = parsed_args[1];
        const auto * n_not_null_arg = toFunctionNode(actions_dag, "assumeNotNull", {n_arg});
        const auto * const_zero_node = addColumnToActionsDAG(actions_dag, n_not_null_arg->result_type, {0});
        const auto * greatest_node = toFunctionNode(actions_dag, "greatest", {n_not_null_arg, const_zero_node});
        const auto * range_node = toFunctionNode(actions_dag, "range", {greatest_node});
        const auto & range_type = assert_cast<const DataTypeArray &>(*removeNullable(range_node->result_type));

        // Create lambda function x -> elem
        ActionsDAG lambda_actions_dag;
        const auto * x_in_lambda = &lambda_actions_dag.addInput("x", range_type.getNestedType());
        const auto * elem_in_lambda = &lambda_actions_dag.addInput(elem_arg->result_name, elem_arg->result_type);
        const auto * lambda_output = elem_in_lambda;
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
        const auto * lambda_function = &actions_dag.addFunction(function_capture, {elem_arg}, lambda_output->result_name);

        /// Apply arrayMap with the lambda function
        const auto * array_map_node = toFunctionNode(actions_dag, "arrayMap", {lambda_function, range_node});

        /// Return null if n is null
        const auto * n_is_null_node = toFunctionNode(actions_dag, "isNull", {n_arg});
        const auto * null_array_node
            = addColumnToActionsDAG(actions_dag, std::make_shared<DataTypeNullable>(array_map_node->result_type), {});
        const auto * if_node = toFunctionNode(actions_dag, "if", {n_is_null_node, null_array_node, array_map_node});
        return convertNodeTypeIfNeeded(substrait_func, if_node, actions_dag);
    }
};

static FunctionParserRegister<FunctionParserArrayRepeat> register_array_repeat;
}
