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
#include <Operator/EarlyStopStep.h>
#include <Processors/Port.h>
#include <QueryPipeline/QueryPipelineBuilder.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}
}

namespace local_engine
{
static DB::ITransformingStep::Traits getTraits()
{
    return DB::ITransformingStep::Traits
    {
        {
            .preserves_number_of_streams = false,
            .preserves_sorting = false,
        },
        {
            .preserves_number_of_rows = false,
        }
    };
}

EarlyStopStep::EarlyStopStep(
    const DB::Block & input_header_)
    : DB::ITransformingStep(
        input_header_, transformHeader(input_header_), getTraits())
{
}

DB::Block EarlyStopStep::transformHeader(const DB::Block& input)
{
    return input.cloneEmpty();
}

void EarlyStopStep::transformPipeline(DB::QueryPipelineBuilder & pipeline, const DB::BuildQueryPipelineSettings & /*settings*/)
{
    pipeline.addSimpleTransform(
        [&](const DB::Block & header)
        {
            return std::make_shared<EarlyStopTransform>(header);
        });
}

void EarlyStopStep::describeActions(DB::IQueryPlanStep::FormatSettings & settings) const
{
    if (!processors.empty())
        DB::IQueryPlanStep::describePipeline(processors, settings);
}

void EarlyStopStep::updateOutputHeader()
{
    output_header = transformHeader(input_headers.front());
}

EarlyStopTransform::EarlyStopTransform(const DB::Block &header_)
    : DB::IProcessor({header_}, {EarlyStopStep::transformHeader(header_)})
{
}

EarlyStopTransform::Status EarlyStopTransform::prepare()
{
    auto & output = outputs.front();
    auto & input = inputs.front();
    if (!input.isFinished())
    {
        input.close();
    }
    output.finish();
    return Status::Finished;
}

void EarlyStopTransform::work()
{
}
}
