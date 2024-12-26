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
#include <Common/Exception.h>

namespace DB::ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

namespace local_engine
{
class SparkFunctionExtractParser : public FunctionParser
{
public:
    SparkFunctionExtractParser(ParserContextPtr parser_context_) : FunctionParser(parser_context_) {}
    ~SparkFunctionExtractParser() override = default;

    static constexpr auto name = "extract";
    String getName() const override { return name; }

    String getCHFunctionName(const substrait::Expression_ScalarFunction & func) const override
    {
        const auto & args = func.arguments();
        if (args.size() != 2)
            throw DB::Exception(DB::ErrorCodes::BAD_ARGUMENTS, "Spark function extract requires two args, function:{}", func.ShortDebugString());
        const auto & extract_field = args.at(0);
        String ch_function_name;
        if (extract_field.value().has_literal())
        {
            const auto & field_value = extract_field.value().literal().string();
            if (field_value == "YEAR")
                ch_function_name = "toYear"; // spark: extract(YEAR FROM) or year
            else if (field_value == "YEAR_OF_WEEK")
                ch_function_name = "toISOYear"; // spark: extract(YEAROFWEEK FROM)
            else if (field_value == "QUARTER")
                ch_function_name = "toQuarter"; // spark: extract(QUARTER FROM) or quarter
            else if (field_value == "MONTH")
                ch_function_name = "toMonth"; // spark: extract(MONTH FROM) or month
            else if (field_value == "WEEK_OF_YEAR")
                ch_function_name = "toISOWeek"; // spark: extract(WEEK FROM) or weekofyear
            else if (field_value == "WEEK_DAY")
                /// Spark WeekDay(date) (0 = Monday, 1 = Tuesday, ..., 6 = Sunday)
                /// Substrait: extract(WEEK_DAY from date)
                /// CH: toDayOfWeek(date, 1)
                ch_function_name = "toDayOfWeek";
            else if (field_value == "DAY_OF_WEEK")
                /// Spark: DayOfWeek(date) (1 = Sunday, 2 = Monday, ..., 7 = Saturday)
                /// Substrait: extract(DAY_OF_WEEK from date)
                /// CH: toDayOfWeek(date, 3)
                /// DAYOFWEEK is alias of function toDayOfWeek.
                /// This trick is to distinguish between extract fields DAY_OF_WEEK and WEEK_DAY in latter codes
                ch_function_name = "DAYOFWEEK";
            else if (field_value == "DAY")
                ch_function_name = "toDayOfMonth"; // spark: extract(DAY FROM) or dayofmonth
            else if (field_value == "DAY_OF_YEAR")
                ch_function_name = "toDayOfYear"; // spark: extract(DOY FROM) or dayofyear
            else if (field_value == "HOUR")
                ch_function_name = "toHour"; // spark: extract(HOUR FROM) or hour
            else if (field_value == "MINUTE")
                ch_function_name = "toMinute"; // spark: extract(MINUTE FROM) or minute
            else if (field_value == "SECOND")
                ch_function_name = "toSecond"; // spark: extract(SECOND FROM) or secondwithfraction
        }
        
        if (ch_function_name.empty())
            throw DB::Exception(DB::ErrorCodes::BAD_ARGUMENTS, "The first arg of spark extract function is wrong.");
        return ch_function_name;
    }

    const DB::ActionsDAG::Node * parse(const substrait::Expression_ScalarFunction & substrait_func, DB::ActionsDAG & actions_dag) const override
    {
        DB::ActionsDAG::NodeRawConstPtrs parsed_args;
        auto ch_function_name = getCHFunctionName(substrait_func);
        const auto & args = substrait_func.arguments();
 
        /// Skip the first arg of extract in substrait
        for (int i = 1; i < args.size(); i++)
            parsed_args.emplace_back(parseExpression(actions_dag, args[i].value()));

        /// Append extra mode argument for extract(WEEK_DAY from date) or extract(DAY_OF_WEEK from date) in substrait
        if (ch_function_name == "toDayOfWeek" || ch_function_name == "DAYOFWEEK")
        {
            UInt8 mode = ch_function_name == "toDayOfWeek" ? 1 : 3;
            auto mode_type = std::make_shared<DB::DataTypeUInt8>();
            parsed_args.emplace_back(addColumnToActionsDAG(actions_dag, mode_type, mode));
        }

        const DB::ActionsDAG::Node * func_node = nullptr;
        if (ch_function_name == "toYear")
        {
            auto arg_func_name = parsed_args[0]->function ? parsed_args[0]->function->getName() : "";
            if (arg_func_name == "sparkToDate" || arg_func_name == "sparkToDateTime" && parsed_args[0]->children.size() > 0)
            {
                const auto * child_node = parsed_args[0]->children[0];
                if (child_node && DB::isString(DB::removeNullable(child_node->result_type)))
                {
                    func_node = toFunctionNode(actions_dag, "sparkExtractYear", {child_node});
                }
            }
        }

        if (!func_node)
            func_node = toFunctionNode(actions_dag, ch_function_name, parsed_args);
        return convertNodeTypeIfNeeded(substrait_func, func_node, actions_dag);
    }
};
static FunctionParserRegister<SparkFunctionExtractParser> register_extract;
}

