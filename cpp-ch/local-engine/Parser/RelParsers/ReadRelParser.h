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
#include <Parser/RelParsers/RelParser.h>
#include <Common/JNIUtils.h>

namespace local_engine
{

class ReadRelParser : public RelParser
{
public:
    explicit ReadRelParser(ParserContextPtr parser_context_) : RelParser(parser_context_) { }
    ~ReadRelParser() override = default;

    DB::QueryPlanPtr
    parse(std::vector<DB::QueryPlanPtr> &, const substrait::Rel & rel, std::list<const substrait::Rel *> & rel_stack_) override
    {
        DB::QueryPlanPtr query_plan;
        return parse(std::move(query_plan), rel, rel_stack_);
    }

    DB::QueryPlanPtr parse(DB::QueryPlanPtr query_plan, const substrait::Rel & rel, std::list<const substrait::Rel *> & rel_stack) override;
    // This is source node, there is no input
    std::optional<const substrait::Rel *> getSingleInput(const substrait::Rel & rel) override { return {}; }

    static bool isReadRelFromLocalFile(const substrait::ReadRel & rel);
    static bool isReadRelFromJavaIter(const substrait::ReadRel & rel);
    static bool isReadRelFromMergeTree(const substrait::ReadRel & rel);
    static bool isReadRelFromRange(const substrait::ReadRel & rel);
    static bool isReadFromStreamKafka(const substrait::ReadRel & rel);

    void setInputIter(jobject input_iter_, bool is_materialze)
    {
        input_iter = input_iter_;
        is_input_iter_materialize = is_materialze;
    }

    void setSplitInfo(const String & split_info_) { split_info = split_info_; }

private:
    DB::QueryPlanStepPtr parseReadRelWithJavaIter(const substrait::ReadRel & rel);
    DB::QueryPlanStepPtr parseReadRelWithLocalFile(const substrait::ReadRel & rel);
    DB::QueryPlanStepPtr parseReadRelWithRange(const substrait::ReadRel & rel);

    jobject input_iter;
    bool is_input_iter_materialize;
    String split_info;
};
}
