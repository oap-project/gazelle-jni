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
#include "RelParser.h"

#include <string>
#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <DataTypes/DataTypeAggregateFunction.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/IDataType.h>
#include <Parser/ExpressionParser.h>
#include <google/protobuf/wrappers.pb.h>
#include <Poco/Logger.h>
#include <Poco/StringTokenizer.h>
#include <Common/Exception.h>
#include <Common/logger_useful.h>

namespace DB
{
namespace ErrorCodes
{
extern const int BAD_ARGUMENTS;
extern const int LOGICAL_ERROR;
}
}

namespace local_engine
{
using namespace DB;

RelParser::RelParser(ParserContextPtr parser_context_) : parser_context(parser_context_)
{
    expression_parser = std::make_unique<ExpressionParser>(parser_context);
}

String RelParser::getUniqueName(const std::string & name) const
{
    return expression_parser->getUniqueName(name);
}

const DB::ActionsDAG::Node * RelParser::parseArgument(ActionsDAG & action_dag, const substrait::Expression & rel) const
{
    return expression_parser->parseExpression(action_dag, rel);
}

const DB::ActionsDAG::Node * RelParser::parseExpression(ActionsDAG & action_dag, const substrait::Expression & rel) const
{
    return expression_parser->parseExpression(action_dag, rel);
}

DB::ActionsDAG RelParser::expressionsToActionsDAG(const std::vector<substrait::Expression> & expressions, const DB::Block & header) const
{
    return expression_parser->expressionsToActionsDAG(expressions, header);
}

std::pair<DataTypePtr, Field> RelParser::parseLiteral(const substrait::Expression_Literal & literal) const
{
    return LiteralParser().parse(literal);
}

const ActionsDAG::Node *
RelParser::buildFunctionNode(ActionsDAG & action_dag, const String & function, const DB::ActionsDAG::NodeRawConstPtrs & args) const
{
    return expression_parser->toFunctionNode(action_dag, function, args);
}

std::vector<const substrait::Rel *> RelParser::getInputs(const substrait::Rel & rel)
{
    auto input = getSingleInput(rel);
    if (!input)
        return {};
    return {*input};
}
AggregateFunctionPtr RelParser::getAggregateFunction(
    const String & name, DB::DataTypes arg_types, DB::AggregateFunctionProperties & properties, const DB::Array & parameters)
{
    auto & factory = DB::AggregateFunctionFactory::instance();
    auto action = NullsAction::EMPTY;

    String function_name = name;
    if (name == "avg" && isDecimal(removeNullable(arg_types[0])))
        function_name = "sparkAvg";
    else if (name == "avgPartialMerge")
    {
        if (auto agg_func = typeid_cast<const DB::DataTypeAggregateFunction *>(arg_types[0].get());
            !agg_func->getArgumentsDataTypes().empty() && DB::isDecimal(DB::removeNullable(agg_func->getArgumentsDataTypes()[0])))
        {
            function_name = "sparkAvgPartialMerge";
        }
    }

    return factory.get(function_name, action, arg_types, parameters, properties);
}

std::optional<String> RelParser::parseSignatureFunctionName(UInt32 function_ref) const
{
    return parser_context->getFunctionNameInSignature(function_ref);
}

std::optional<String> RelParser::parseFunctionName(const substrait::Expression_ScalarFunction & function) const
{
    auto func_name = expression_parser->safeGetFunctionName(function);
    if (func_name.empty())
        return {};
    return func_name;
}

DB::QueryPlanPtr
RelParser::parse(std::vector<DB::QueryPlanPtr> & input_plans_, const substrait::Rel & rel, std::list<const substrait::Rel *> & rel_stack_)
{
    assert(input_plans_.size() == 1);
    return parse(std::move(input_plans_[0]), rel, rel_stack_);
}

RelParserFactory & RelParserFactory::instance()
{
    static RelParserFactory factory;
    return factory;
}

void RelParserFactory::registerBuilder(UInt32 k, RelParserBuilder builder)
{
    auto it = builders.find(k);
    if (it != builders.end())
    {
        throw DB::Exception(DB::ErrorCodes::BAD_ARGUMENTS, "Duplicated builder key:{}", k);
    }
    builders[k] = builder;
}

RelParserFactory::RelParserBuilder RelParserFactory::getBuilder(UInt32 k)
{
    auto it = builders.find(k);
    if (it == builders.end())
    {
        throw DB::Exception(DB::ErrorCodes::BAD_ARGUMENTS, "Not found builder for key:{}", k);
    }
    return it->second;
}

void registerWindowRelParser(RelParserFactory & factory);
void registerWindowGroupLimitRelParser(RelParserFactory & factory);
void registerSortRelParser(RelParserFactory & factory);
void registerExpandRelParser(RelParserFactory & factory);
void registerAggregateParser(RelParserFactory & factory);
void registerProjectRelParser(RelParserFactory & factory);
void registerJoinRelParser(RelParserFactory & factory);
void registerFilterRelParser(RelParserFactory & factory);
void registerCrossRelParser(RelParserFactory & factory);
void registerFetchRelParser(RelParserFactory & factory);
void registerReadRelParser(RelParserFactory & factory);

void registerRelParsers()
{
    auto & factory = RelParserFactory::instance();
    registerWindowRelParser(factory);
    registerWindowGroupLimitRelParser(factory);
    registerSortRelParser(factory);
    registerExpandRelParser(factory);
    registerAggregateParser(factory);
    registerProjectRelParser(factory);
    registerJoinRelParser(factory);
    registerCrossRelParser(factory);
    registerFilterRelParser(factory);
    registerFetchRelParser(factory);
    registerReadRelParser(factory);
}
}
