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
#include <DataTypes/DataTypeNumberBase.h>
#include <DataTypes/DataTypesNumber.h>
#include <Parser/FunctionParser.h>
#include <Parser/TypeParser.h>

namespace local_engine
{
class SparkFunctionIsNaNParser : public FunctionParser
{
public:
    SparkFunctionIsNaNParser(ParserContextPtr parser_context_) : FunctionParser(parser_context_) {}
    ~SparkFunctionIsNaNParser() override = default;

    static constexpr auto name = "isnan";
    String getName() const override { return name; }
    String getCHFunctionName(const substrait::Expression_ScalarFunction &) const override { return "isNaN"; }

    const DB::ActionsDAG::Node * parse(const substrait::Expression_ScalarFunction & substrait_func, DB::ActionsDAG & actions_dag) const override
    {
        // the result of isNaN(NULL) is NULL in CH, but false in Spark
        DB::ActionsDAG::NodeRawConstPtrs parsed_args;
        auto ch_function_name = getCHFunctionName(substrait_func);
        const auto & args = substrait_func.arguments();
        const DB::ActionsDAG::Node * arg_node = nullptr;
        if (args[0].value().has_cast())
        {
            arg_node = parseExpression(actions_dag, args[0].value().cast().input());
            auto result_type = DB::removeNullable(arg_node->result_type);
            if (DB::isString(*result_type))
                arg_node = toFunctionNode(actions_dag, "toFloat64OrZero", {arg_node});
            else
                arg_node = parseExpression(actions_dag, args[0].value());
        }
        else
            arg_node = parseExpression(actions_dag, args[0].value());

        DB::ActionsDAG::NodeRawConstPtrs ifnull_args = {arg_node, addColumnToActionsDAG(actions_dag, std::make_shared<DB::DataTypeInt32>(), 0)};
        parsed_args.emplace_back(toFunctionNode(actions_dag, "IfNull", ifnull_args));

        const auto * func_node = toFunctionNode(actions_dag, ch_function_name, parsed_args);
        return convertNodeTypeIfNeeded(substrait_func, func_node, actions_dag);
    }
};
static FunctionParserRegister<SparkFunctionIsNaNParser> register_isnan;
}

