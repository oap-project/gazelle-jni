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
#include <Parser/AggregateFunctionParser.h>
#include <Parser/RelParsers/RelParser.h>
#include <Poco/Logger.h>
#include <Common/logger_useful.h>


namespace local_engine
{
class AggregateRelParser : public RelParser
{
public:
    explicit AggregateRelParser(ParserContextPtr parser_context_);
    ~AggregateRelParser() override = default;
    DB::QueryPlanPtr
    parse(DB::QueryPlanPtr query_plan, const substrait::Rel & rel, std::list<const substrait::Rel *> & rel_stack_) override;
    std::optional<const substrait::Rel *> getSingleInput(const substrait::Rel & rel) override { return &rel.aggregate().input(); }

private:
    struct AggregateInfo
    {
        const substrait::AggregateRel::Measure * measure = nullptr;
        String measure_column_name;
        DB::Strings arg_column_names;
        DB::DataTypes arg_column_types;
        DB::Array params;
        String signature_function_name;
        String function_name;
        // If no combinator be applied on it, same as function_name
        String combinator_function_name;
        // For avoiding repeated builds.
        AggregateFunctionParser::CommonFunctionInfo parser_func_info;
        // For avoiding repeated builds.
        AggregateFunctionParserPtr function_parser;
    };

    Poco::Logger * logger = &Poco::Logger::get("AggregateRelParser");
    bool has_first_stage = false;
    bool has_inter_stage = false;
    bool has_final_stage = false;
    bool has_complete_stage = false;

    std::list<const substrait::Rel *> * rel_stack;
    DB::QueryPlanPtr plan = nullptr;
    const substrait::AggregateRel * aggregate_rel = nullptr;
    std::vector<AggregateInfo> aggregates;
    DB::Names grouping_keys;

    void setup(DB::QueryPlanPtr query_plan, const substrait::Rel & rel);
    void addPreProjection();
    void addMergingAggregatedStep();
    void addCompleteModeAggregatedStep();
    void addAggregatingStep();
    void addPostProjection();

    void buildAggregateDescriptions(DB::AggregateDescriptions & descriptions);
};
}
