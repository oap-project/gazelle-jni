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
#include <Parser/FunctionParser.h>
#include <Poco/Logger.h>
#include <Common/Exception.h>
#include <Common/logger_useful.h>
#include <Rewriter/ExpressionRewriter.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}
}

namespace local_engine
{


class GetJSONObjectParser : public FunctionParser
{
public:
    static constexpr auto name = "get_json_object";

    explicit GetJSONObjectParser(ParserContextPtr parser_context_): FunctionParser(parser_context_) {}
    ~GetJSONObjectParser() override = default;

    String getName() const override { return name; }
    String getCHFunctionName(const substrait::Expression_ScalarFunction & scalar_function) const override
    {
        const auto & args = scalar_function.arguments();
        if (args[0].value().has_scalar_function()
            && args[0].value().scalar_function().function_reference() == SelfDefinedFunctionReference::GET_JSON_OBJECT)
        {
            return "sparkTupleElement";
        }
        return name;
    }

protected:
    /// Force to reuse the same flatten json column node
    DB::ActionsDAG::NodeRawConstPtrs parseFunctionArguments(
        const substrait::Expression_ScalarFunction & substrait_func,
        DB::ActionsDAG & actions_dag) const override
    {
        const auto & args = substrait_func.arguments();
        if (args.size() != 2)
        {
            throw DB::Exception(DB::ErrorCodes::BAD_ARGUMENTS, "Function {} requires 2 arguments", getCHFunctionName(substrait_func));
        }
        if (args[0].value().has_scalar_function()
            && args[0].value().scalar_function().function_reference() == SelfDefinedFunctionReference::GET_JSON_OBJECT)
        {
            auto flatten_json_column_name = getFlatterJsonColumnName(args[0].value());
            const auto * flatten_json_column_node = actions_dag.tryFindInOutputs(flatten_json_column_name);
            if (!flatten_json_column_node)
            {
                const auto flatten_function_pb = args[0].value().scalar_function();
                const auto * flatten_arg0 = parseExpression(actions_dag, flatten_function_pb.arguments(0).value());
                const auto * flatten_arg1 = parseExpression(actions_dag, flatten_function_pb.arguments(1).value());
                flatten_json_column_node = toFunctionNode(actions_dag, FlattenJSONStringOnRequiredFunction::name, flatten_json_column_name, {flatten_arg0, flatten_arg1});
                actions_dag.addOrReplaceInOutputs(*flatten_json_column_node);
            }
            return {flatten_json_column_node, parseExpression(actions_dag, args[1].value())};
        }
        else
        {
            return {parseExpression(actions_dag, args[0].value()), parseExpression(actions_dag, args[1].value())};
        }
    }

private:
    static String getFlatterJsonColumnName(const substrait::Expression & arg)
    {
        return arg.ShortDebugString();
    }
};

static FunctionParserRegister<GetJSONObjectParser> register_get_json_object_parser;
}
