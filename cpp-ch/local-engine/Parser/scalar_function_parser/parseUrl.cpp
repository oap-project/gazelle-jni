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
#include "parseUrl.h"
#include <DataTypes/DataTypeString.h>
#include <DataTypes/IDataType.h>
#include <Common/Exception.h>

namespace DB
{
namespace ErrorCodes
{
extern const int BAD_ARGUMENTS;
extern const int NOT_IMPLEMENTED;
}
}

namespace local_engine
{
String ParseURLParser::getCHFunctionName(const substrait::Expression_ScalarFunction & substrait_func) const
{
    if (substrait_func.arguments().size() < 2)
    {
        throw DB::Exception(DB::ErrorCodes::BAD_ARGUMENTS, "parse_url() expects at least 2 arguments");
    }
    return selectCHFunctionName(substrait_func);
}

String ParseURLParser::getQueryPartName(const substrait::Expression & expr) const
{
    if (!expr.has_literal())
    {
        throw DB::Exception(DB::ErrorCodes::BAD_ARGUMENTS, "parse_url() expects a string literal as the 2nd argument");
    }

    auto [data_type, field] = parseLiteral(expr.literal());
    DB::WhichDataType ty_which(data_type);
    if (!ty_which.isString())
    {
        throw DB::Exception(DB::ErrorCodes::BAD_ARGUMENTS, "parse_url() 2nd argument must be a string literal");
    }

    return field.safeGet<String>();
}

const static String CH_URL_PROTOL_FUNCTION = "protocol";
const static String CH_URL_PATH_FUNCTION = "spark_parse_url_path";
const static String CH_URL_REF_FUNCTION = "spark_parse_url_ref";
const static String CH_URL_USERINFO_FUNCTION = "spark_parse_url_userinfo";
const static String CH_URL_FILE_FUNCTION = "spark_parse_url_file";
const static String CH_URL_AUTHORITY_FUNCTION = "spark_parse_url_authority";
const static String CH_URL_HOST_FUNCTION = "spark_parse_url_host";
const static String CH_URL_PARAMS_FUNCTION = "spark_parse_url_query";
const static String CH_URL_ONE_PARAM_FUNCTION = "spark_parse_url_one_query";
const static String CH_URL_INVALID_FUNCTION = "spark_parse_url_invalid";
String ParseURLParser::selectCHFunctionName(const substrait::Expression_ScalarFunction & substrait_func) const
{
    auto query_part_name = getQueryPartName(substrait_func.arguments(1).value());
    if (query_part_name == "QUERY")
    {
        if (substrait_func.arguments().size() == 2)
            return CH_URL_PARAMS_FUNCTION;
        else
            return CH_URL_ONE_PARAM_FUNCTION;
    }
    else if (query_part_name == "PROTOCOL")
        return CH_URL_PROTOL_FUNCTION;
    else if (query_part_name == "PATH")
        return CH_URL_PATH_FUNCTION;
    else if (query_part_name == "HOST")
        return CH_URL_HOST_FUNCTION;
    else if (query_part_name == "REF")
        return CH_URL_REF_FUNCTION;
    else if (query_part_name == "FILE")
        return CH_URL_FILE_FUNCTION;
    else if (query_part_name == "AUTHORITY")
        return CH_URL_AUTHORITY_FUNCTION;
    else if (query_part_name == "USERINFO")
        return CH_URL_USERINFO_FUNCTION;
    else
    {
        // throw DB::Exception(DB::ErrorCodes::BAD_ARGUMENTS, "Unknown query part name {} ", query_part_name);
        return CH_URL_INVALID_FUNCTION;
    }
}

DB::ActionsDAG::NodeRawConstPtrs ParseURLParser::parseFunctionArguments(
    const substrait::Expression_ScalarFunction & substrait_func, DB::ActionsDAG & actions_dag) const
{
    DB::ActionsDAG::NodeRawConstPtrs arg_nodes;
    arg_nodes.push_back(parseExpression(actions_dag, substrait_func.arguments(0).value()));
    for (Int32 i = 2; i < substrait_func.arguments().size(); ++i)
    {
        arg_nodes.push_back(parseExpression(actions_dag, substrait_func.arguments(i).value()));
    }
    return arg_nodes;
}

const DB::ActionsDAG::Node * ParseURLParser::convertNodeTypeIfNeeded(
    const substrait::Expression_ScalarFunction & substrait_func, const DB::ActionsDAG::Node * func_node, DB::ActionsDAG & actions_dag) const
{
    auto ch_function_name = getCHFunctionName(substrait_func);
    if (ch_function_name != CH_URL_PROTOL_FUNCTION)
    {
        return func_node;
    }
    // Empty string is converted to NULL.
    auto str_type = std::make_shared<DB::DataTypeString>();
    const auto * empty_str_node
        = &actions_dag.addColumn(DB::ColumnWithTypeAndName(str_type->createColumnConst(1, DB::Field("")), str_type, getUniqueName("")));
    return toFunctionNode(actions_dag, "nullIf", {func_node, empty_str_node});
}

FunctionParserRegister<ParseURLParser> register_scalar_function_parser_parse_url;
}
