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
#include <Parser/FunctionParser.h>
#include <Parser/scalar_function_parser/getTimestamp.h>

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

template<typename Name>
class FunctionParserUnixTimestamp : public FunctionParserGetTimestamp
{
public:
    explicit FunctionParserUnixTimestamp(ParserContextPtr parser_context_) : FunctionParserGetTimestamp(parser_context_) {}
    ~FunctionParserUnixTimestamp() override = default;

    static constexpr auto name = Name::name;
    String getName() const override { return Name::name; }

    const DB::ActionsDAG::Node * parse(
        const substrait::Expression_ScalarFunction & substrait_func,
        DB::ActionsDAG & actions_dag) const override
    {
        /*
        spark function: unix_timestamp(expr, fmt) / to_unix_timestamp(expr, fmt)
        1. If expr type is string, ch function = parseDateTime64InJodaSyntaxOrNull(expr, format)
        2. If expr type is date/TIMESTAMP, ch function = toUnixTimestamp(expr, format)
        3. Otherwise, throw exception
        */
        auto parsed_args = parseFunctionArguments(substrait_func, actions_dag);
        if (parsed_args.size() != 2)
            throw DB::Exception(DB::ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, "Function {} requires exactly two arguments", getName());

        const auto * expr_arg = parsed_args[0];
        const auto * fmt_arg = parsed_args[1];
        auto expr_type = removeNullable(expr_arg->result_type);
        if (isString(expr_type))
            return FunctionParserGetTimestamp::parse(substrait_func, actions_dag);

        const DateLUTImpl * date_lut = &DateLUT::instance();
        const auto * time_zone_node = addColumnToActionsDAG(actions_dag, std::make_shared<DB::DataTypeString>(), date_lut->getTimeZone());
        const DB::ActionsDAG::Node * result_node = nullptr;
        if (isDateOrDate32(expr_type))
            result_node = toFunctionNode(actions_dag, "sparkDateToUnixTimestamp", {expr_arg, time_zone_node});
        else if (isDateTime(expr_type) || isDateTime64(expr_type))
            result_node = toFunctionNode(actions_dag, "toUnixTimestamp", {expr_arg, time_zone_node});
        else
            throw DB::Exception(DB::ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Function {} requires expr type is string/date/timestamp", getName());

        return convertNodeTypeIfNeeded(substrait_func, result_node, actions_dag);
    }
};

struct FunctionNameUnixTimestamp
{
    static constexpr auto name = "unix_timestamp";
};

struct FunctionNameToUnixTimestamp
{
    static constexpr auto name = "to_unix_timestamp";
};

using FunctionParserForUnixTimestamp = FunctionParserUnixTimestamp<FunctionNameUnixTimestamp>;
using FunctionParseToUnixTimestamp = FunctionParserUnixTimestamp<FunctionNameToUnixTimestamp>;
static FunctionParserRegister<FunctionParserForUnixTimestamp> register_unix_timestamp;
static FunctionParserRegister<FunctionParseToUnixTimestamp> register_to_unix_timestamp;
}
