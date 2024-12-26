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
#include <Parser/TypeParser.h>
#include <Common/BlockTypeUtils.h>
#include <Common/CHUtil.h>

namespace local_engine
{
class SparkFunctionRepeatParser : public FunctionParser
{
public:
    SparkFunctionRepeatParser(ParserContextPtr parser_context_) : FunctionParser(parser_context_) {}
    ~SparkFunctionRepeatParser() override = default;

    static constexpr auto name = "repeat";
    String getName() const override { return name; }
    String getCHFunctionName(const substrait::Expression_ScalarFunction &) const override { return name; }

    const DB::ActionsDAG::Node * parse(const substrait::Expression_ScalarFunction & substrait_func, DB::ActionsDAG & actions_dag) const override
    {
        // repeat. the field index must be unsigned integer in CH, cast the signed integer in substrait
        // which must be a positive value into unsigned integer here.
        DB::ActionsDAG::NodeRawConstPtrs parsed_args;
        auto ch_function_name = getCHFunctionName(substrait_func);
        const auto & args = substrait_func.arguments();
        parsed_args.emplace_back(parseExpression(actions_dag, args[0].value()));
        const auto * repeat_times_node = parseExpression(actions_dag, args[1].value());
        const auto cast_or_default_args
            = {repeat_times_node, expression_parser->addConstColumn(actions_dag, std::make_shared<DB::DataTypeString>(), "UInt32")};
        parsed_args.emplace_back(
            toFunctionNode(actions_dag, "accurateCastOrDefault", repeat_times_node->result_name, cast_or_default_args));
        const auto * func_node = toFunctionNode(actions_dag, ch_function_name, parsed_args);
        return convertNodeTypeIfNeeded(substrait_func, func_node, actions_dag);
    }
};
static FunctionParserRegister<SparkFunctionRepeatParser> register_repeat;
}
