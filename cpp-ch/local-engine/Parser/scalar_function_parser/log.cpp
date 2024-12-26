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

#include <Core/Field.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypesNumber.h>
#include <Parser/ExpressionParser.h>
#include <Parser/FunctionParser.h>
#include <Common/CHUtil.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int NOT_IMPLEMENTED;
}
}

namespace local_engine
{
using namespace DB;
class FunctionParserLog : public FunctionParser
{
public:
    explicit FunctionParserLog(ParserContextPtr parser_context_) : FunctionParser(parser_context_) {}
    ~FunctionParserLog() override = default;

    static constexpr auto name = "log";

    String getName() const override { return name; }

    const ActionsDAG::Node * parse(
        const substrait::Expression_ScalarFunction & substrait_func,
        ActionsDAG & actions_dag) const override
    {
        /*
            parse log(x, y) as
            if (x <= 0.0 || y <= 0.0)
                null
            else
                ln(y) / ln(x)
        */
        auto parsed_args = parseFunctionArguments(substrait_func, actions_dag);
        if (parsed_args.size() != 2)
            throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, "Function {} requires exactly two arguments", getName());

        const auto * x_node = parsed_args[0];
        const auto * y_node = parsed_args[1];

        const auto * ln_x_node = toFunctionNode(actions_dag, "ln", {x_node});
        const auto * ln_y_node = toFunctionNode(actions_dag, "ln", {y_node});
        auto result_type = std::make_shared<DataTypeFloat64>();

        const auto * null_const_node = addColumnToActionsDAG(actions_dag, makeNullable(result_type), Field());
        const auto * zero_const_node = addColumnToActionsDAG(actions_dag, result_type, 0.0);

        const auto * le_x_node = toFunctionNode(actions_dag, "lessOrEquals", {x_node, zero_const_node});
        const auto * le_y_node = toFunctionNode(actions_dag, "lessOrEquals", {y_node, zero_const_node});
        const auto * or_node = toFunctionNode(actions_dag, "or", {le_x_node, le_y_node});
        const auto * divide_node = toFunctionNode(actions_dag, "divide", {ln_y_node, ln_x_node});
        const auto * result_node = toFunctionNode(actions_dag, "if", {or_node, null_const_node, divide_node});

        return convertNodeTypeIfNeeded(substrait_func, result_node, actions_dag);
    }
};

static FunctionParserRegister<FunctionParserLog> register_log;
}
