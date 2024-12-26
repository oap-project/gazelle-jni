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
#include <map>
#include <optional>
#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/IAggregateFunction_fwd.h>
#include <Core/Field.h>
#include <DataTypes/IDataType.h>
#include <DataTypes/Serializations/ISerialization.h>
#include <Parser/ExpressionParser.h>
#include <Parser/ParserContext.h>
#include <Parser/SerializedPlanParser.h>
#include <Processors/QueryPlan/QueryPlan.h>
#include <base/types.h>
#include <substrait/plan.pb.h>

namespace local_engine
{
class ExpressionParser;
/// parse a single substrait relation
class RelParser
{
public:
    explicit RelParser(ParserContextPtr parser_context_);

    virtual ~RelParser() = default;
    virtual DB::QueryPlanPtr
    parse(DB::QueryPlanPtr current_plan_, const substrait::Rel & rel, std::list<const substrait::Rel *> & rel_stack_)
        = 0;
    virtual DB::QueryPlanPtr
    parse(std::vector<DB::QueryPlanPtr> & input_plans_, const substrait::Rel & rel, std::list<const substrait::Rel *> & rel_stack_);
    virtual std::vector<const substrait::Rel *> getInputs(const substrait::Rel & rel);
    virtual std::optional<const substrait::Rel *> getSingleInput(const substrait::Rel & rel) = 0;
    const std::vector<DB::IQueryPlanStep *> & getSteps() const { return steps; }

    static DB::AggregateFunctionPtr getAggregateFunction(
        const String & name, DB::DataTypes arg_types, DB::AggregateFunctionProperties & properties, const DB::Array & parameters = {});

    virtual std::vector<DB::QueryPlanPtr> extraPlans() { return {}; }

protected:
    // inline SerializedPlanParser * getPlanParser() const { return plan_parser; }
    inline DB::ContextPtr getContext() const { return parser_context->queryContext(); }

    String getUniqueName(const std::string & name) const;

    std::optional<String> parseSignatureFunctionName(UInt32 function_ref) const;
    // Get coresponding function name in ClickHouse.
    std::optional<String> parseFunctionName(const substrait::Expression_ScalarFunction & function) const;

    const DB::ActionsDAG::Node * parseArgument(DB::ActionsDAG & action_dag, const substrait::Expression & rel) const;

    const DB::ActionsDAG::Node * parseExpression(DB::ActionsDAG & action_dag, const substrait::Expression & rel) const;

    DB::ActionsDAG expressionsToActionsDAG(const std::vector<substrait::Expression> & expressions, const DB::Block & header) const;

    std::pair<DB::DataTypePtr, DB::Field> parseLiteral(const substrait::Expression_Literal & literal) const;
    // collect all steps for metrics
    std::vector<DB::IQueryPlanStep *> steps;

    const DB::ActionsDAG::Node *
    buildFunctionNode(DB::ActionsDAG & action_dag, const String & function, const DB::ActionsDAG::NodeRawConstPtrs & args) const;

    ParserContextPtr parser_context;
    std::unique_ptr<ExpressionParser> expression_parser;
};

class RelParserFactory
{
protected:
    RelParserFactory() = default;

public:
    using RelParserBuilder = std::function<std::shared_ptr<RelParser>(ParserContextPtr)>;
    static RelParserFactory & instance();
    void registerBuilder(UInt32 k, RelParserBuilder builder);
    RelParserBuilder getBuilder(UInt32 k);

private:
    std::map<UInt32, RelParserBuilder> builders;
};

void registerRelParsers();
}
