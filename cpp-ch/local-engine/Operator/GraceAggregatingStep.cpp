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

#include "GraceAggregatingStep.h"
#include <Interpreters/JoinUtils.h>
#include <Operator/GraceAggregatingTransform.h>
#include <Processors/Port.h>
#include <Processors/Transforms/AggregatingTransform.h>
#include <QueryPipeline/QueryPipelineBuilder.h>
#include <Common/CHUtil.h>

namespace DB::ErrorCodes
{
extern const int LOGICAL_ERROR;
}

namespace local_engine
{
static DB::ITransformingStep::Traits getTraits()
{
    return DB::ITransformingStep::Traits{
        {
            .preserves_number_of_streams = false,
            .preserves_sorting = false,
        },
        {
            .preserves_number_of_rows = false,
        }};
}

static DB::Block buildOutputHeader(const DB::Block & input_header_, const DB::Aggregator::Params params_, bool final)
{
    return params_.getHeader(input_header_, final);
}

GraceAggregatingStep::GraceAggregatingStep(
    DB::ContextPtr context_, const DB::Block & input_header, DB::Aggregator::Params params_, bool no_pre_aggregated_)
    : DB::ITransformingStep(input_header, buildOutputHeader(input_header, params_, false), getTraits())
    , context(context_)
    , params(std::move(params_))
    , no_pre_aggregated(no_pre_aggregated_)
{
}

void GraceAggregatingStep::transformPipeline(DB::QueryPipelineBuilder & pipeline, const DB::BuildQueryPipelineSettings &)
{
    if (params.max_bytes_before_external_group_by)
    {
        throw DB::Exception(DB::ErrorCodes::LOGICAL_ERROR, "max_bytes_before_external_group_by is not supported in GraceAggregatingStep");
    }
    auto num_streams = pipeline.getNumStreams();
    auto transform_params = std::make_shared<DB::AggregatingTransformParams>(pipeline.getHeader(), params, false);
    pipeline.resize(1);
    auto build_transform = [&](DB::OutputPortRawPtrs outputs)
    {
        DB::Processors new_processors;
        for (auto & output : outputs)
        {
            auto op
                = std::make_shared<GraceAggregatingTransform>(pipeline.getHeader(), transform_params, context, no_pre_aggregated, false);
            new_processors.push_back(op);
            DB::connect(*output, op->getInputs().front());
        }
        return new_processors;
    };
    pipeline.transform(build_transform);
    pipeline.resize(num_streams, true);
}

void GraceAggregatingStep::describeActions(DB::IQueryPlanStep::FormatSettings & settings) const
{
    params.explain(settings.out, settings.offset);
}

void GraceAggregatingStep::describeActions(DB::JSONBuilder::JSONMap & map) const
{
    params.explain(map);
}

void GraceAggregatingStep::updateOutputHeader()
{
    output_header = buildOutputHeader(input_headers.front(), params, false);
}


}
