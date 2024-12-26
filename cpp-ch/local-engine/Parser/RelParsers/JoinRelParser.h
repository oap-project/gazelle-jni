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

#include <memory>
#include <unordered_set>
#include <Core/Joins.h>
#include <Interpreters/TableJoin.h>
#include <Parser/RelParsers/RelParser.h>
#include <substrait/algebra.pb.h>

namespace DB
{
class TableJoin;
}

namespace local_engine
{

class StorageJoinFromReadBuffer;

class JoinRelParser : public RelParser
{
public:
    explicit JoinRelParser(ParserContextPtr parser_context_);
    ~JoinRelParser() override = default;

    DB::QueryPlanPtr
    parse(DB::QueryPlanPtr query_plan, const substrait::Rel & sort_rel, std::list<const substrait::Rel *> & rel_stack_) override;

    DB::QueryPlanPtr parse(
        std::vector<DB::QueryPlanPtr> & input_plans_, const substrait::Rel & rel, std::list<const substrait::Rel *> & rel_stack_) override;

    std::vector<const substrait::Rel *> getInputs(const substrait::Rel & rel) override;
    std::optional<const substrait::Rel *> getSingleInput(const substrait::Rel & rel) override;
    std::vector<DB::QueryPlanPtr> extraPlans() override { return std::move(extra_plan_holder); }

private:
    DB::ContextPtr context;
    std::vector<DB::QueryPlanPtr> extra_plan_holder;


    DB::QueryPlanPtr parseJoin(const substrait::JoinRel & join, DB::QueryPlanPtr left, DB::QueryPlanPtr right);
    void renamePlanColumns(DB::QueryPlan & left, DB::QueryPlan & right, const StorageJoinFromReadBuffer & storage_join);
    void addConvertStep(DB::TableJoin & table_join, DB::QueryPlan & left, DB::QueryPlan & right);
    void collectJoinKeys(
        DB::TableJoin & table_join, const substrait::JoinRel & join_rel, const DB::Block & left_header, const DB::Block & right_header);

    bool applyJoinFilter(
        DB::TableJoin & table_join,
        const substrait::JoinRel & join_rel,
        DB::QueryPlan & left_plan,
        DB::QueryPlan & right_plan,
        bool allow_mixed_condition);

    void addPostFilter(DB::QueryPlan & plan, const substrait::JoinRel & join);

    void existenceJoinPostProject(DB::QueryPlan & plan, const DB::Names & left_input_cols);

    static std::unordered_set<DB::JoinTableSide>
    extractTableSidesFromExpression(const substrait::Expression & expr, const DB::Block & left_header, const DB::Block & right_header);

    bool couldRewriteToMultiJoinOnClauses(
        const DB::TableJoin::JoinOnClause & prefix_clause,
        std::vector<DB::TableJoin::JoinOnClause> & clauses,
        const substrait::JoinRel & join_rel,
        const DB::Block & left_header,
        const DB::Block & right_header);

    DB::QueryPlanPtr buildMultiOnClauseHashJoin(
        std::shared_ptr<DB::TableJoin> table_join,
        DB::QueryPlanPtr left_plan,
        DB::QueryPlanPtr right_plan,
        const std::vector<DB::TableJoin::JoinOnClause> & join_on_clauses);
    DB::QueryPlanPtr buildSingleOnClauseHashJoin(
        const substrait::JoinRel & join_rel,
        std::shared_ptr<DB::TableJoin> table_join,
        DB::QueryPlanPtr left_plan,
        DB::QueryPlanPtr right_plan);
};

}
