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

#include <DataTypes/DataTypeString.h>
#include <Parser/FunctionParser.h>

namespace DB
{

namespace ErrorCodes
{
extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
extern const int ILLEGAL_TYPE_OF_ARGUMENT;
}
}


namespace local_engine
{

class FunctionParserTimestampAdd : public FunctionParser
{
public:
    explicit FunctionParserTimestampAdd(ParserContextPtr parser_context_) : FunctionParser(parser_context_) { }
    ~FunctionParserTimestampAdd() override = default;

    static constexpr auto name = "timestamp_add";

    String getName() const override { return name; }
    String getCHFunctionName(const substrait::Expression_ScalarFunction &) const override { return "timestamp_add"; }

    const DB::ActionsDAG::Node * parse(const substrait::Expression_ScalarFunction & substrait_func, DB::ActionsDAG & actions_dag) const override
    {
        auto parsed_args = parseFunctionArguments(substrait_func, actions_dag);
        if (parsed_args.size() < 3)
            throw DB::Exception(DB::ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, "Function {} requires at least three arguments", getName());

        const auto & unit_field = substrait_func.arguments().at(0);
        if (!unit_field.value().has_literal() || !unit_field.value().literal().has_string())
            throw DB::Exception(
                DB::ErrorCodes::BAD_ARGUMENTS, "Unsupported unit argument, should be a string literal, but: {}", unit_field.DebugString());

        String timezone;
        if (parsed_args.size() == 4)
        {
            const auto & timezone_field = substrait_func.arguments().at(3);
            if (!timezone_field.value().has_literal() || !timezone_field.value().literal().has_string())
            throw DB::Exception(
                DB::ErrorCodes::BAD_ARGUMENTS,
                "Unsupported timezone_field argument, should be a string literal, but: {}",
                timezone_field.DebugString());
            timezone = timezone_field.value().literal().string();
        }

        const auto & unit = Poco::toUpper(unit_field.value().literal().string());

        std::string ch_function_name;
        if (unit == "MICROSECOND")
            ch_function_name = "addMicroseconds";
        else if (unit == "MILLISECOND")
            ch_function_name = "addMilliseconds";
        else if (unit == "SECOND")
            ch_function_name = "addSeconds";
        else if (unit == "MINUTE")
            ch_function_name = "addMinutes";
        else if (unit == "HOUR")
            ch_function_name = "addHours";
        else if (unit == "DAY" || unit == "DAYOFYEAR")
            ch_function_name = "addDays";
        else if (unit == "WEEK")
            ch_function_name = "addWeeks";
        else if (unit == "MONTH")
            ch_function_name = "addMonths";
        else if (unit == "QUARTER")
            ch_function_name = "addQuarters";
        else if (unit == "YEAR")
            ch_function_name = "addYears";
        else
            throw DB::Exception(DB::ErrorCodes::BAD_ARGUMENTS, "Unsupported unit argument: {}", unit);

        if (timezone.empty())
            timezone = DateLUT::instance().getTimeZone();

        const auto * time_zone_node = addColumnToActionsDAG(actions_dag, std::make_shared<DB::DataTypeString>(), timezone);
        const DB::ActionsDAG::Node * result_node
            = toFunctionNode(actions_dag, ch_function_name, {parsed_args[2], parsed_args[1], time_zone_node});

        return convertNodeTypeIfNeeded(substrait_func, result_node, actions_dag);
    }
};
static FunctionParserRegister<FunctionParserTimestampAdd> register_timestamp_add;
}
