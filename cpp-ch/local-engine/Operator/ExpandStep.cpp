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
#include "ExpandStep.h"
#include <memory>
#include <Core/ColumnWithTypeAndName.h>
#include <Core/ColumnsWithTypeAndName.h>
#include <Operator/ExpandTransform.h>
#include <Processors/IProcessor.h>
#include <QueryPipeline/Pipe.h>
#include <QueryPipeline/QueryPipelineBuilder.h>

namespace local_engine
{
static DB::ITransformingStep::Traits getTraits()
{
    return DB::ITransformingStep::Traits{
        {
            .returns_single_stream = true,
            .preserves_number_of_streams = false,
            .preserves_sorting = false,
        },
        {
            .preserves_number_of_rows = false,
        }};
}

ExpandStep::ExpandStep(const DB::Block & input_header, const ExpandField & project_set_exprs_)
    : DB::ITransformingStep(input_header, buildOutputHeader(input_header, project_set_exprs_), getTraits())
    , project_set_exprs(project_set_exprs_)
{
}

DB::Block ExpandStep::buildOutputHeader(const DB::Block &, const ExpandField & project_set_exprs_)
{
    DB::ColumnsWithTypeAndName cols;
    const auto & types = project_set_exprs_.getTypes();
    const auto & names = project_set_exprs_.getNames();

    chassert(names.size() == types.size());

    for (size_t i = 0; i < project_set_exprs_.getExpandCols(); ++i)
        cols.emplace_back(DB::ColumnWithTypeAndName(types[i], names[i]));

    return DB::Block(std::move(cols));
}

void ExpandStep::transformPipeline(DB::QueryPipelineBuilder & pipeline, const DB::BuildQueryPipelineSettings & /*settings*/)
{
    auto build_transform = [&](DB::OutputPortRawPtrs outputs)
    {
        DB::Processors new_processors;
        for (auto & output : outputs)
        {
            auto expand_op = std::make_shared<ExpandTransform>(input_headers.front(), *output_header, project_set_exprs);
            new_processors.push_back(expand_op);
            DB::connect(*output, expand_op->getInputs().front());
        }
        return new_processors;
    };
    pipeline.transform(build_transform);
}

void ExpandStep::describePipeline(DB::IQueryPlanStep::FormatSettings & settings) const
{
    if (!processors.empty())
        DB::IQueryPlanStep::describePipeline(processors, settings);
}

void ExpandStep::updateOutputHeader()
{
    output_header = buildOutputHeader(input_headers.front(), project_set_exprs);
}

}
