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
#include <Core/Settings.h>
#include <DataTypes/DataTypeFactory.h>
#include <Functions/FunctionFactory.h>
#include <Interpreters/HashJoin/HashJoin.h>
#include <Interpreters/TableJoin.h>
#include <Join/StorageJoinFromReadBuffer.h>
#include <Parsers/ASTIdentifier.h>
#include <Processors/Executors/PipelineExecutor.h>
#include <Processors/Executors/PullingPipelineExecutor.h>
#include <Processors/QueryPlan/ExpressionStep.h>
#include <Processors/QueryPlan/JoinStep.h>
#include <Processors/QueryPlan/Optimizations/QueryPlanOptimizationSettings.h>
#include <Processors/QueryPlan/QueryPlan.h>
#include <Processors/QueryPlan/ReadFromPreparedSource.h>
#include <Processors/Sources/SourceFromSingleChunk.h>
#include <QueryPipeline/QueryPipelineBuilder.h>
#include <Storages/MergeTree/SparkMergeTreeMeta.h>
#include <Storages/SubstraitSource/SubstraitFileSource.h>
#include <gtest/gtest.h>
#include <Common/DebugUtils.h>
#include <Common/QueryContext.h>

using namespace DB;
using namespace local_engine;

TEST(TestJoin, simple)
{
    auto global_context = local_engine::QueryContext::globalContext();
    local_engine::QueryContext::globalMutableContext()->setSetting("join_use_nulls", true);
    auto & factory = DB::FunctionFactory::instance();
    auto function = factory.get("murmurHash2_64", global_context);
    auto int_type = DataTypeFactory::instance().get("Int32");
    auto column0 = int_type->createColumn();
    column0->insert(1);
    column0->insert(2);
    column0->insert(3);
    column0->insert(4);

    auto column1 = int_type->createColumn();
    column1->insert(2);
    column1->insert(4);
    column1->insert(6);
    column1->insert(8);

    ColumnsWithTypeAndName columns
        = {ColumnWithTypeAndName(std::move(column0), int_type, "colA"), ColumnWithTypeAndName(std::move(column1), int_type, "colB")};
    Block left(columns);

    auto column3 = int_type->createColumn();
    column3->insert(1);
    column3->insert(2);
    column3->insert(3);
    column3->insert(5);

    auto column4 = int_type->createColumn();
    column4->insert(1);
    column4->insert(3);
    column4->insert(5);
    column4->insert(9);

    ColumnsWithTypeAndName columns2
        = {ColumnWithTypeAndName(std::move(column3), int_type, "colD"), ColumnWithTypeAndName(std::move(column4), int_type, "colC")};
    Block right(columns2);

    auto left_table = std::make_shared<SourceFromSingleChunk>(left);
    auto right_table = std::make_shared<SourceFromSingleChunk>(right);
    QueryPlan left_plan;
    left_plan.addStep(std::make_unique<ReadFromPreparedSource>(Pipe(left_table)));
    QueryPlan right_plan;
    right_plan.addStep(std::make_unique<ReadFromPreparedSource>(Pipe(right_table)));

    auto join = std::make_shared<TableJoin>(
        global_context->getSettingsRef(), global_context->getGlobalTemporaryVolume(), global_context->getTempDataOnDisk());
    join->setKind(JoinKind::Left);
    join->setStrictness(JoinStrictness::All);
    join->setColumnsFromJoinedTable(right.getNamesAndTypesList());
    join->addDisjunct();
    ASTPtr lkey = std::make_shared<ASTIdentifier>("colA");
    ASTPtr rkey = std::make_shared<ASTIdentifier>("colD");
    join->addOnKeys(lkey, rkey, false);
    for (const auto & column : join->columnsFromJoinedTable())
        join->addJoinedColumn(column);

    auto columns_from_left_table = left_plan.getCurrentHeader().getNamesAndTypesList();
    for (auto & column_from_joined_table : columns_from_left_table)
        join->setUsedColumn(column_from_joined_table, JoinTableSide::Left);

    auto left_keys = left.getNamesAndTypesList();
    join->addJoinedColumnsAndCorrectTypes(left_keys, true);
    std::cerr << "after join:\n";
    for (const auto & key : left_keys)
        std::cerr << key.dump() << std::endl;
    std::optional<ActionsDAG> left_convert_actions;
    std::optional<ActionsDAG> right_convert_actions;
    std::tie(left_convert_actions, right_convert_actions)
        = join->createConvertingActions(left.getColumnsWithTypeAndName(), right.getColumnsWithTypeAndName());

    if (right_convert_actions)
    {
        auto converting_step = std::make_unique<ExpressionStep>(right_plan.getCurrentHeader(), std::move(*right_convert_actions));
        converting_step->setStepDescription("Convert joined columns");
        right_plan.addStep(std::move(converting_step));
    }

    if (left_convert_actions)
    {
        auto converting_step = std::make_unique<ExpressionStep>(right_plan.getCurrentHeader(), std::move(*right_convert_actions));
        converting_step->setStepDescription("Convert joined columns");
        left_plan.addStep(std::move(converting_step));
    }
    auto hash_join = std::make_shared<HashJoin>(join, right_plan.getCurrentHeader());

    QueryPlanStepPtr join_step
        = std::make_unique<JoinStep>(left_plan.getCurrentHeader(), right_plan.getCurrentHeader(), hash_join, 8192, 8192, 1,  NameSet{}, false, false);

    std::cerr << "join step:" << join_step->getOutputHeader().dumpStructure() << std::endl;

    std::vector<QueryPlanPtr> plans;
    plans.emplace_back(std::make_unique<QueryPlan>(std::move(left_plan)));
    plans.emplace_back(std::make_unique<QueryPlan>(std::move(right_plan)));

    auto query_plan = QueryPlan();
    query_plan.unitePlans(std::move(join_step), {std::move(plans)});
    std::cerr << query_plan.getCurrentHeader().dumpStructure() << std::endl;
    ActionsDAG project{query_plan.getCurrentHeader().getNamesAndTypesList()};
    project.project(
        {NameWithAlias("colA", "colA"), NameWithAlias("colB", "colB"), NameWithAlias("colD", "colD"), NameWithAlias("colC", "colC")});
    QueryPlanStepPtr project_step = std::make_unique<ExpressionStep>(query_plan.getCurrentHeader(), std::move(project));
    query_plan.addStep(std::move(project_step));
    auto pipeline = query_plan.buildQueryPipeline(QueryPlanOptimizationSettings(), BuildQueryPipelineSettings());
    auto executable_pipe = QueryPipelineBuilder::getPipeline(std::move(*pipeline));
    PullingPipelineExecutor executor(executable_pipe);
    auto res = pipeline->getHeader().cloneEmpty();
    executor.pull(res);
    debug::headBlock(res);
}
