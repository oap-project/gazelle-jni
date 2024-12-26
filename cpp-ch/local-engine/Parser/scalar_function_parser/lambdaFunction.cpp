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
#include <Functions/FunctionsMiscellaneous.h>
#include <Interpreters/ExpressionActions.h>
#include <Interpreters/ExpressionActionsSettings.h>
#include <Parser/ExpressionParser.h>
#include <Parser/FunctionParser.h>
#include <Parser/TypeParser.h>
#include <Poco/Logger.h>
#include <Common/Exception.h>
#include <Common/logger_useful.h>

#include <unordered_set>

namespace DB::ErrorCodes
{
extern const int LOGICAL_ERROR;
}

namespace local_engine
{
DB::NamesAndTypesList collectLambdaArguments(ParserContextPtr parser_context_, const substrait::Expression_ScalarFunction & substrait_func)
{
    DB::NamesAndTypesList lambda_arguments;
    std::unordered_set<String> collected_names;

    for (const auto & arg : substrait_func.arguments())
    {
        if (arg.value().has_scalar_function()
            && parser_context_->getFunctionNameInSignature(arg.value().scalar_function().function_reference()) == "namedlambdavariable")
        {
            auto [_, col_name_field] = LiteralParser::parse(arg.value().scalar_function().arguments()[0].value().literal());
            String col_name = col_name_field.safeGet<String>();
            if (collected_names.contains(col_name))
                continue;
            collected_names.insert(col_name);
            auto type = TypeParser::parseType(arg.value().scalar_function().output_type());
            lambda_arguments.emplace_back(col_name, type);
        }
    }
    return lambda_arguments;
}

/// Refer to `PlannerActionsVisitorImpl::visitLambda` for how to build a lambda function node.
class FunctionParserLambda : public FunctionParser
{
public:
    static constexpr auto name = "lambdafunction";
    explicit FunctionParserLambda(ParserContextPtr parser_context_) : FunctionParser(parser_context_) { }
    ~FunctionParserLambda() override = default;

    String getName() const override { return name; }

    String getCHFunctionName(const substrait::Expression_ScalarFunction & scalar_function) const override
    {
        throw DB::Exception(DB::ErrorCodes::LOGICAL_ERROR, "getCHFunctionName is not implemented for LambdaFunction");
    }

    const DB::ActionsDAG::Node *
    parse(const substrait::Expression_ScalarFunction & substrait_func, DB::ActionsDAG & actions_dag) const override
    {
        /// Some special cases, for example, `transform(arr, x -> concat(arr, array(x)))` refers to
        /// a column `arr` out of it directly. We need a `arr` as an input column for `lambda_actions_dag`
        DB::NamesAndTypesList parent_header;
        for (const auto * output_node : actions_dag.getOutputs())
            parent_header.emplace_back(output_node->result_name, output_node->result_type);
        DB::ActionsDAG lambda_actions_dag{parent_header};

        /// The first argument is the lambda function body, followings are the lambda arguments which is
        /// needed by the lambda function body.
        /// There could be a nested lambda function in the lambda function body, and it refer a variable from
        /// this outside lambda function's arguments. For an example, transform(number, x -> transform(letter, y -> struct(x, y))).
        /// Before parsing the lambda function body, we add lambda function arguments int actions dag at first.
        for (size_t i = 1; i < substrait_func.arguments().size(); ++i)
            (void)parseExpression(lambda_actions_dag, substrait_func.arguments()[i].value());
        const auto & substrait_lambda_body = substrait_func.arguments()[0].value();
        const auto * lambda_body_node = parseExpression(lambda_actions_dag, substrait_lambda_body);
        lambda_actions_dag.getOutputs().push_back(lambda_body_node);
        lambda_actions_dag.removeUnusedActions(DB::Names(1, lambda_body_node->result_name));

        DB::Names captured_column_names;
        DB::Names required_column_names = lambda_actions_dag.getRequiredColumnsNames();
        DB::ActionsDAG::NodeRawConstPtrs lambda_children;
        auto lambda_function_args = collectLambdaArguments(parser_context, substrait_func);
        const auto & lambda_actions_inputs = lambda_actions_dag.getInputs();

        std::unordered_map<String, const DB::ActionsDAG::Node *> parent_nodes;
        for (const auto & node : actions_dag.getNodes())
            parent_nodes[node.result_name] = &node;
        for (const auto & required_column_name : required_column_names)
        {
            if (std::find_if(
                    lambda_function_args.begin(),
                    lambda_function_args.end(),
                    [&required_column_name](const DB::NameAndTypePair & name_type) { return name_type.name == required_column_name; })
                == lambda_function_args.end())
            {
                auto it = std::find_if(
                    lambda_actions_inputs.begin(),
                    lambda_actions_inputs.end(),
                    [&required_column_name](const auto & node) { return node->result_name == required_column_name; });
                if (it == lambda_actions_inputs.end())
                    throw DB::Exception(DB::ErrorCodes::LOGICAL_ERROR, "Required column not found: {}", required_column_name);
                auto parent_node_it = parent_nodes.find(required_column_name);
                if (parent_node_it == parent_nodes.end())
                {
                    throw DB::Exception(
                        DB::ErrorCodes::LOGICAL_ERROR,
                        "Not found column {} in actions dag:\n{}",
                        required_column_name,
                        actions_dag.dumpDAG());
                }
                /// The nodes must be the ones in `actions_dag`, otherwise `ActionsDAG::evaluatePartialResult` will fail. Because nodes may have the
                /// same name but their addresses are different.
                lambda_children.push_back(parent_node_it->second);
                captured_column_names.push_back(required_column_name);
            }
        }
        auto expression_actions_settings = DB::ExpressionActionsSettings::fromContext(getContext(), DB::CompileExpressions::yes);
        auto function_capture = std::make_shared<DB::FunctionCaptureOverloadResolver>(
            std::move(lambda_actions_dag),
            expression_actions_settings,
            captured_column_names,
            lambda_function_args,
            lambda_body_node->result_type,
            lambda_body_node->result_name,
            false);

        const auto * result = &actions_dag.addFunction(function_capture, lambda_children, lambda_body_node->result_name);
        return result;
    }

protected:
    DB::ActionsDAG::NodeRawConstPtrs
    parseFunctionArguments(const substrait::Expression_ScalarFunction & substrait_func, DB::ActionsDAG & actions_dag) const override
    {
        throw DB::Exception(DB::ErrorCodes::LOGICAL_ERROR, "parseFunctionArguments is not implemented for LambdaFunction");
    }

    const DB::ActionsDAG::Node * convertNodeTypeIfNeeded(
        const substrait::Expression_ScalarFunction & substrait_func,
        const DB::ActionsDAG::Node * func_node,
        DB::ActionsDAG & actions_dag) const override
    {
        throw DB::Exception(DB::ErrorCodes::LOGICAL_ERROR, "convertNodeTypeIfNeeded is not implemented for NamedLambdaVariable");
    }
};

static FunctionParserRegister<FunctionParserLambda> register_lambda_function;


class NamedLambdaVariable : public FunctionParser
{
public:
    static constexpr auto name = "namedlambdavariable";
    explicit NamedLambdaVariable(ParserContextPtr parser_context_) : FunctionParser(parser_context_) { }
    ~NamedLambdaVariable() override = default;

    String getName() const override { return name; }

    String getCHFunctionName(const substrait::Expression_ScalarFunction & scalar_function) const override
    {
        throw DB::Exception(DB::ErrorCodes::LOGICAL_ERROR, "getCHFunctionName is not implemented for NamedLambdaVariable");
    }

    const DB::ActionsDAG::Node *
    parse(const substrait::Expression_ScalarFunction & substrait_func, DB::ActionsDAG & actions_dag) const override
    {
        auto [_, col_name_field] = parseLiteral(substrait_func.arguments()[0].value().literal());
        String col_name = col_name_field.safeGet<String>();

        auto type = TypeParser::parseType(substrait_func.output_type());
        const auto & inputs = actions_dag.getInputs();
        auto it = std::find_if(inputs.begin(), inputs.end(), [&col_name](const auto * node) { return node->result_name == col_name; });
        if (it == inputs.end())
            return &(actions_dag.addInput(col_name, type));
        return *it;
    }

protected:
    DB::ActionsDAG::NodeRawConstPtrs
    parseFunctionArguments(const substrait::Expression_ScalarFunction & substrait_func, DB::ActionsDAG & actions_dag) const override
    {
        throw DB::Exception(DB::ErrorCodes::LOGICAL_ERROR, "parseFunctionArguments is not implemented for NamedLambdaVariable");
    }

    const DB::ActionsDAG::Node * convertNodeTypeIfNeeded(
        const substrait::Expression_ScalarFunction & substrait_func,
        const DB::ActionsDAG::Node * func_node,
        DB::ActionsDAG & actions_dag) const override
    {
        throw DB::Exception(DB::ErrorCodes::LOGICAL_ERROR, "convertNodeTypeIfNeeded is not implemented for NamedLambdaVariable");
    }
};

static FunctionParserRegister<NamedLambdaVariable> register_named_lambda_variable;

}
