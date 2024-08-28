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
#include <substrait/algebra.pb.h>

#include <Parser/RelParser.h>
#include <Parser/SerializedPlanParser.h>
#include <Storages/MergeTree/MergeTreeTool.h>
#include <Storages/MergeTree/StorageMergeTreeFactory.h>


namespace DB
{
namespace ErrorCodes
{
extern const int LOGICAL_ERROR;
}
}

namespace local_engine
{
using namespace DB;

class MergeTreeRelParser : public RelParser
{
public:
    static CustomStorageMergeTreePtr parseStorage(
        const MergeTreeTable & merge_tree_table, ContextMutablePtr context, bool restore = false);

    // Create random table name and table path and use default storage policy.
    // In insert case, mergetree data can be upload after merges in default storage(Local Disk).
    static CustomStorageMergeTreePtr
    copyToDefaultPolicyStorage(MergeTreeTable merge_tree_table, ContextMutablePtr context);

    // Use same table path and data path as the originial table.
    static CustomStorageMergeTreePtr
    copyToVirtualStorage(MergeTreeTable merge_tree_table, ContextMutablePtr context);

    static MergeTreeTable parseMergeTreeTable(const substrait::ReadRel::ExtensionTable & extension_table);

    explicit MergeTreeRelParser(SerializedPlanParser * plan_paser_, const ContextPtr & context_)
        : RelParser(plan_paser_), context(context_), global_context(plan_paser_->global_context)
    {
    }

    ~MergeTreeRelParser() override = default;

    DB::QueryPlanPtr parse(DB::QueryPlanPtr query_plan, const substrait::Rel & rel, std::list<const substrait::Rel *> & rel_stack_) override
    {
        throw Exception(ErrorCodes::LOGICAL_ERROR, "MergeTreeRelParser can't call parse(), call parseReadRel instead.");
    }

    DB::QueryPlanPtr parseReadRel(
        DB::QueryPlanPtr query_plan, const substrait::ReadRel & read_rel, const substrait::ReadRel::ExtensionTable & extension_table);

    const substrait::Rel & getSingleInput(const substrait::Rel &) override
    {
        throw Exception(ErrorCodes::LOGICAL_ERROR, "MergeTreeRelParser can't call getSingleInput().");
    }

    String filterRangesOnDriver(const substrait::ReadRel & read_rel);

    struct Condition
    {
        explicit Condition(const substrait::Expression & node_) : node(node_) { }

        const substrait::Expression node;
        size_t columns_size = 0;
        NameSet table_columns;
        Int64 min_position_in_primary_key = std::numeric_limits<Int64>::max() - 1;

        auto tuple() const { return std::make_tuple(-min_position_in_primary_key, columns_size, table_columns.size()); }

        bool operator<(const Condition & rhs) const { return tuple() < rhs.tuple(); }
    };
    using Conditions = std::list<Condition>;

    // visable for test
    void analyzeExpressions(Conditions & res, const substrait::Expression & rel, std::set<Int64> & pk_positions, Block & block);

public:
    std::unordered_map<std::string, UInt64> column_sizes;

private:
    void parseToAction(ActionsDAG & filter_action, const substrait::Expression & rel, std::string & filter_name);
    PrewhereInfoPtr parsePreWhereInfo(const substrait::Expression & rel, Block & input);
    ActionsDAG optimizePrewhereAction(const substrait::Expression & rel, std::string & filter_name, Block & block);
    String getCHFunctionName(const substrait::Expression_ScalarFunction & substrait_func);
    void collectColumns(const substrait::Expression & rel, NameSet & columns, Block & block);
    UInt64 getColumnsSize(const NameSet & columns);

    const ContextPtr & context;
    ContextMutablePtr & global_context;
};

}
