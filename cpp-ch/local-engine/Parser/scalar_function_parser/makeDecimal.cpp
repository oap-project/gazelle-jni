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
#include <Common/Exception.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}
}

namespace local_engine
{
class SparkFunctionMakeDecimalParser : public FunctionParser
{
public:
    SparkFunctionMakeDecimalParser(ParserContextPtr parser_context_) : FunctionParser(parser_context_) {}
    ~SparkFunctionMakeDecimalParser() override = default;

    static constexpr auto name = "make_decimal";
    String getName() const override { return name; }
    String getCHFunctionName(const substrait::Expression_ScalarFunction & func) const override
    {
        const auto & args = func.arguments();
        if (args.size() < 2)
            throw DB::Exception(DB::ErrorCodes::BAD_ARGUMENTS, "make_decimal function requires at least two arguments");
        String ch_function_name = "makeDecimalSpark";
        if (args[1].value().literal().boolean())
            ch_function_name += "OrNull";
        return ch_function_name;
    }

    const DB::ActionsDAG::Node * parse(const substrait::Expression_ScalarFunction & substrait_func, DB::ActionsDAG & actions_dag) const override
    {
        DB::ActionsDAG::NodeRawConstPtrs parsed_args;
        const auto & args = substrait_func.arguments();
        parsed_args.emplace_back(parseExpression(actions_dag, args[0].value()));
        UInt32 precision = substrait_func.output_type().decimal().precision();
        UInt32 scale = substrait_func.output_type().decimal().scale();
        auto uint32_type = std::make_shared<DB::DataTypeUInt32>();
        parsed_args.emplace_back(addColumnToActionsDAG(actions_dag, uint32_type, precision));
        parsed_args.emplace_back(addColumnToActionsDAG(actions_dag, uint32_type, scale));

        auto ch_function_name = getCHFunctionName(substrait_func);
        const auto * func_node = toFunctionNode(actions_dag, ch_function_name, parsed_args);
        return convertNodeTypeIfNeeded(substrait_func, func_node, actions_dag);
    }
};
static FunctionParserRegister<SparkFunctionMakeDecimalParser> register_make_decimal;
}
