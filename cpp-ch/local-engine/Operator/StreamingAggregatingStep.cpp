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

#include "StreamingAggregatingStep.h"
#include <Processors/Port.h>
#include <Processors/Transforms/AggregatingTransform.h>
#include <QueryPipeline/QueryPipelineBuilder.h>
#include <Common/CHUtil.h>
#include <Common/GlutenConfig.h>
#include <Common/QueryContext.h>
#include <Common/Stopwatch.h>
#include <Common/formatReadable.h>
#include <Common/logger_useful.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}
}

namespace local_engine
{
StreamingAggregatingTransform::StreamingAggregatingTransform(
    DB::ContextPtr context_, const DB::Block & header_, DB::AggregatingTransformParamsPtr params_)
    : DB::IProcessor({header_}, {params_->getHeader()})
    , context(context_)
    , header(header_)
    , key_columns(params_->params.keys_size)
    , aggregate_columns(params_->params.aggregates_size)
    , params(params_)
{
    auto config = StreamingAggregateConfig::loadFromContext(context);
    aggregated_keys_before_evict = config.aggregated_keys_before_streaming_aggregating_evict;
    aggregated_keys_before_evict = PODArrayUtil::adjustMemoryEfficientSize(aggregated_keys_before_evict);
    max_allowed_memory_usage_ratio = config.max_memory_usage_ratio_for_streaming_aggregating;
    high_cardinality_threshold = config.high_cardinality_threshold_for_streaming_aggregating;
}

StreamingAggregatingTransform::~StreamingAggregatingTransform()
{
    LOG_INFO(
        logger,
        "Metrics. total_input_blocks: {}, total_input_rows: {},  total_output_blocks: {}, total_output_rows: {}, "
        "total_clear_data_variants_num: {}, total_aggregate_time: {}, total_convert_data_variants_time: {}, current mem usage: {}",
        total_input_blocks,
        total_input_rows,
        total_output_blocks,
        total_output_rows,
        total_clear_data_variants_num,
        total_aggregate_time,
        total_convert_data_variants_time,
        ReadableSize(currentThreadGroupMemoryUsage()));
}

StreamingAggregatingTransform::Status StreamingAggregatingTransform::prepare()
{
    auto & output = outputs.front();
    auto & input = inputs.front();
    if (output.isFinished() || isCancelled())
    {
        input.close();
        return Status::Finished;
    }

    if (has_output)
    {
        if (output.canPush())
        {
            LOG_DEBUG(
                logger,
                "Output one chunk. rows: {}, bytes: {}, current memory usage: {}",
                output_chunk.getNumRows(),
                ReadableSize(output_chunk.bytes()),
                ReadableSize(currentThreadGroupMemoryUsage()));
            total_output_rows += output_chunk.getNumRows();
            total_output_blocks++;
            if (!output_chunk.getNumRows())
                throw DB::Exception(DB::ErrorCodes::LOGICAL_ERROR, "Invalid output chunk");
            output.push(std::move(output_chunk));
            has_output = false;
        }
        return Status::PortFull;
    }

    if (has_input)
        return Status::Ready;

    if (input.isFinished())
    {
        /// to trigger the evict action anyway.
        input_finished = true;

        /// data is not cleared
        if (data_variants || (block_converter && block_converter->hasNext()))
        {
            has_input = true;
            return Status::Ready;
        }
        else
        {
            output.finish();
            return Status::Finished;
        }
    }

    input.setNeeded();
    if (!input.hasData())
    {
        return Status::NeedData;
    }
    input_chunk = input.pull(true);
    LOG_DEBUG(
        logger,
        "Input one new chunk. rows: {}, bytes: {}, current memory usage: {}",
        input_chunk.getNumRows(),
        ReadableSize(input_chunk.bytes()),
        ReadableSize(currentThreadGroupMemoryUsage()));
    total_input_rows += input_chunk.getNumRows();
    total_input_blocks++;
    has_input = true;
    return Status::Ready;
}

bool StreamingAggregatingTransform::needEvict()
{
    if (input_finished)
        return true;
    auto memory_soft_limit = DB::CurrentThread::getGroup()->memory_tracker.getSoftLimit();
    if (!memory_soft_limit)
        return false;
    auto max_mem_used = static_cast<size_t>(memory_soft_limit * max_allowed_memory_usage_ratio);
    auto current_result_rows = data_variants->size();
    /// avoid evict empty or too small aggregated results.
    if (current_result_rows < aggregated_keys_before_evict)
        return false;

    /// If the grouping keys is high cardinality, we should evict data variants early, and avoid to use a big
    /// hash table.
    if (static_cast<double>(total_output_rows) / total_input_rows > high_cardinality_threshold)
        return true;

    auto current_mem_used = currentThreadGroupMemoryUsage();
    if (per_key_memory_usage > 0)
    {
        /// When we know each key memory usage, we can take a more greedy memory usage strategy
        if (current_mem_used + per_key_memory_usage * current_result_rows >= max_mem_used)
        {
            LOG_INFO(
                logger,
                "Memory is overflow. current_mem_used: {}, max_mem_used: {}, per_key_memory_usage: {}, aggregator keys: {}, hash table type: {}",
                ReadableSize(current_mem_used),
                ReadableSize(max_mem_used),
                ReadableSize(per_key_memory_usage),
                current_result_rows,
                data_variants->type);
            return true;
        }
    }
    else
    {
        /// For safety, we should evict data variants when memory usage is overflow on half of max usage at the firs time.
        /// Usually, the peak memory usage to convert aggregated data variant into blocks is about double of the hash table.
        if (current_mem_used * 2 >= max_mem_used)
        {
            LOG_INFO(
                logger,
                "Memory is overflow on half of max usage. current_mem_used: {}, max_mem_used: {}, aggregator keys: {}, hash table type: {}",
                ReadableSize(current_mem_used),
                ReadableSize(max_mem_used),
                current_result_rows,
                data_variants->type);
            return true;
        }
    }
    return false;
}


void StreamingAggregatingTransform::work()
{
    auto pop_one_pending_block = [&]()
    {
        bool res = false;
        if (block_converter)
        {
            while (block_converter->hasNext())
            {
                has_output = false;
                Stopwatch convert_watch;
                auto block = block_converter->next();
                output_chunk = DB::convertToChunk(block);
                total_convert_data_variants_time += convert_watch.elapsedMicroseconds();
                if (!output_chunk.getNumRows())
                    continue;
                has_output = true;
                per_key_memory_usage = output_chunk.allocatedBytes() * 1.0 / output_chunk.getNumRows();
                res = true;
                break;
            }
            has_input = block_converter->hasNext();
            if (!block_converter->hasNext())
            {
                block_converter = nullptr;
            }
        }
        else
            has_input = false;
        return res;
    };

    if (has_input)
    {
        /// If there is a AggregateDataBlockConverter in working, generate one block and return.
        if (pop_one_pending_block())
            return;
        if (block_converter)
        {
            throw DB::Exception(DB::ErrorCodes::LOGICAL_ERROR, "block_converter should be null");
        }

        if (!data_variants)
        {
            data_variants = std::make_shared<DB::AggregatedDataVariants>();
        }

        has_input = false;
        if (input_chunk.getNumRows())
        {
            auto num_rows = input_chunk.getNumRows();
            Stopwatch watch;
            params->aggregator.executeOnBlock(
                input_chunk.detachColumns(), 0, num_rows, *data_variants, key_columns, aggregate_columns, no_more_keys);
            total_aggregate_time += watch.elapsedMicroseconds();
            input_chunk = {};
        }

        if (needEvict())
        {
            block_converter = std::make_unique<AggregateDataBlockConverter>(params->aggregator, data_variants, false);
            data_variants = nullptr;
            total_clear_data_variants_num++;
            pop_one_pending_block();
        }
    }
    else
    {
        throw DB::Exception(DB::ErrorCodes::LOGICAL_ERROR, "Invalid state");
    }
}

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

static DB::Block buildOutputHeader(const DB::Block & input_header_, const DB::Aggregator::Params params_)
{
    return params_.getHeader(input_header_, false);
}
StreamingAggregatingStep::StreamingAggregatingStep(
    const DB::ContextPtr & context_, const DB::Block & input_header, DB::Aggregator::Params params_)
    : DB::ITransformingStep(input_header, buildOutputHeader(input_header, params_), getTraits())
    , context(context_)
    , params(std::move(params_))
{
}

void StreamingAggregatingStep::transformPipeline(DB::QueryPipelineBuilder & pipeline, const DB::BuildQueryPipelineSettings &)
{
    if (params.max_bytes_before_external_group_by)
    {
        throw DB::Exception(
            DB::ErrorCodes::LOGICAL_ERROR, "max_bytes_before_external_group_by is not supported in StreamingAggregatingStep");
    }
    pipeline.dropTotalsAndExtremes();
    auto transform_params = std::make_shared<DB::AggregatingTransformParams>(pipeline.getHeader(), params, false);
    pipeline.resize(1);
    auto build_transform = [&](DB::OutputPortRawPtrs outputs)
    {
        DB::Processors new_processors;
        for (auto & output : outputs)
        {
            auto op = std::make_shared<StreamingAggregatingTransform>(context, pipeline.getHeader(), transform_params);
            new_processors.push_back(op);
            DB::connect(*output, op->getInputs().front());
        }
        return new_processors;
    };
    pipeline.transform(build_transform);
}

void StreamingAggregatingStep::describeActions(DB::IQueryPlanStep::FormatSettings & settings) const
{
    params.explain(settings.out, settings.offset);
}

void StreamingAggregatingStep::describeActions(DB::JSONBuilder::JSONMap & map) const
{
    params.explain(map);
}

void StreamingAggregatingStep::updateOutputHeader()
{
    output_header = buildOutputHeader(input_headers.front(), params);
}

}
