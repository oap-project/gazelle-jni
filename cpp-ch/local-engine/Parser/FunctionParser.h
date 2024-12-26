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
#pragma once

#include <Core/Field.h>
#include <DataTypes/IDataType.h>
#include <Functions/FunctionFactory.h>
#include <Interpreters/ActionsDAG.h>
#include <Parser/ExpressionParser.h>
#include <Parser/ParserContext.h>
#include <Parser/SerializedPlanParser.h>
#include <base/types.h>
#include <substrait/algebra.pb.h>
#include <Common/IFactoryWithAliases.h>

namespace local_engine
{
class SerializedPlanParser;
class ExpressionParser;

/// Parse a single substrait scalar function
class FunctionParser
{
public:
    explicit FunctionParser(ParserContextPtr ctx);

    virtual ~FunctionParser() = default;

    virtual String getName() const = 0;

    /// This is a convenient interface for scalar functions, and should not be used for aggregate functions.
    /// It usally take following actions:
    /// - add const columns for literal arguments into actions_dag.
    /// - make pre-projections for input arguments. e.g. type conversion.
    /// - make a post-projection for the function result. e.g. type conversion.
    virtual const DB::ActionsDAG::Node *
    parse(const substrait::Expression_ScalarFunction & substrait_func, DB::ActionsDAG & actions_dag) const;

    virtual String getCHFunctionName(const substrait::Expression_ScalarFunction & substrait_func) const;

protected:
    /// Deprecated method
    virtual DB::ActionsDAG::NodeRawConstPtrs parseFunctionArguments(
        const substrait::Expression_ScalarFunction & substrait_func, const String & /*function_name*/, DB::ActionsDAG & actions_dag) const
    {
        return parseFunctionArguments(substrait_func, actions_dag);
    }

    virtual DB::ActionsDAG::NodeRawConstPtrs
    parseFunctionArguments(const substrait::Expression_ScalarFunction & substrait_func, DB::ActionsDAG & actions_dag) const;

    virtual const DB::ActionsDAG::Node * convertNodeTypeIfNeeded(
        const substrait::Expression_ScalarFunction & substrait_func,
        const DB::ActionsDAG::Node * func_node,
        DB::ActionsDAG & actions_dag) const;

    DB::ContextPtr getContext() const { return parser_context->queryContext(); }

    String getUniqueName(const String & name) const;

    const DB::ActionsDAG::Node *
    addColumnToActionsDAG(DB::ActionsDAG & actions_dag, const DB::DataTypePtr & type, const DB::Field & field) const;

    const DB::ActionsDAG::Node *
    toFunctionNode(DB::ActionsDAG & action_dag, const String & func_name, const DB::ActionsDAG::NodeRawConstPtrs & args) const;

    const DB::ActionsDAG::Node * toFunctionNode(
        DB::ActionsDAG & action_dag,
        const String & func_name,
        const String & result_name,
        const DB::ActionsDAG::NodeRawConstPtrs & args) const;

    const DB::ActionsDAG::Node * parseFunctionWithDAG(
        const substrait::Expression & rel, std::string & result_name, DB::ActionsDAG & actions_dag, bool keep_result = false) const;

    const DB::ActionsDAG::Node * parseExpression(DB::ActionsDAG & actions_dag, const substrait::Expression & rel) const;

    std::pair<DB::DataTypePtr, DB::Field> parseLiteral(const substrait::Expression_Literal & literal) const;

    ParserContextPtr parser_context;
    std::unique_ptr<ExpressionParser> expression_parser;
};

using FunctionParserPtr = std::shared_ptr<FunctionParser>;
using FunctionParserCreator = std::function<FunctionParserPtr(ParserContextPtr)>;

/// Creates FunctionParser by name.
class FunctionParserFactory : private boost::noncopyable, public DB::IFactoryWithAliases<FunctionParserCreator>
{
public:
    using Parsers = std::unordered_map<std::string, Value>;

    static FunctionParserFactory & instance();

    /// Register a function parser by its name.
    /// No locking, you must register all functions before usage of get.
    void registerFunctionParser(const String & name, Value value);

    template <typename Parser>
    void registerFunctionParser()
    {
        // std::cout << "register function parser with name:" << Parser::name << std::endl;
        auto creator = [](ParserContextPtr ctx) -> std::shared_ptr<FunctionParser> { return std::make_shared<Parser>(ctx); };
        registerFunctionParser(Parser::name, creator);
    }

    FunctionParserPtr get(const String & name, ParserContextPtr ctx);
    FunctionParserPtr tryGet(const String & name, ParserContextPtr ctx);
    const Parsers & getMap() const override { return parsers; }

private:
    Parsers parsers;

    /// Always empty
    Parsers case_insensitive_parsers;


    const Parsers & getCaseInsensitiveMap() const override { return case_insensitive_parsers; }

    String getFactoryName() const override { return "FunctionParserFactory"; }
};

template <typename Parser>
struct FunctionParserRegister
{
    FunctionParserRegister() { FunctionParserFactory::instance().registerFunctionParser<Parser>(); }
};

}
