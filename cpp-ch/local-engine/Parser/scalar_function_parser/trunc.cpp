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
#include <Interpreters/ActionsDAG.h>
#include <Parser/FunctionParser.h>
#include <Poco/String.h>

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

class FunctionParserTrunc : public FunctionParser
{
public:
    explicit FunctionParserTrunc(ParserContextPtr parser_context_) : FunctionParser(parser_context_) {}
    ~FunctionParserTrunc() override = default;

    static constexpr auto name = "trunc";

    String getName() const override { return name; }

    const DB::ActionsDAG::Node * parse(
        const substrait::Expression_ScalarFunction & substrait_func,
        DB::ActionsDAG & actions_dag) const override
    {
        auto parsed_args = parseFunctionArguments(substrait_func, actions_dag);
        if (parsed_args.size() != 2)
            throw DB::Exception(DB::ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, "Function {} requires two arguments", getName());

        const auto * date_arg = parsed_args[0];
        const auto & fmt_field = substrait_func.arguments().at(1);
        if (!fmt_field.value().has_literal() || !fmt_field.value().literal().has_string())
            throw DB::Exception(DB::ErrorCodes::BAD_ARGUMENTS, "Unsupported fmt argument, should be a string literal, but: {}", fmt_field.DebugString());

        const DB::ActionsDAG::Node * result_node = nullptr;
        const auto & field_value = Poco::toUpper(fmt_field.value().literal().string());
        if (field_value == "YEAR" || field_value == "YYYY" || field_value == "YY")
            result_node = toFunctionNode(actions_dag, "toStartOfYear", {date_arg});
        else if (field_value == "QUARTER")
            result_node = toFunctionNode(actions_dag, "toStartOfQuarter", {date_arg});
        else if (field_value == "MONTH" || field_value == "MM" || field_value == "MON")
            result_node = toFunctionNode(actions_dag, "toStartOfMonth", {date_arg});
        else if (field_value == "WEEK")
        {
            const auto * mode_node = addColumnToActionsDAG(actions_dag, std::make_shared<DB::DataTypeUInt8>(), 1);
            result_node = toFunctionNode(actions_dag, "toStartOfWeek", {date_arg, mode_node});
        }
        else
            throw DB::Exception(DB::ErrorCodes::BAD_ARGUMENTS, "Unsupported fmt argument: {}", field_value);
        return convertNodeTypeIfNeeded(substrait_func, result_node, actions_dag);
    }
};

static FunctionParserRegister<FunctionParserTrunc> register_trunc;
}
