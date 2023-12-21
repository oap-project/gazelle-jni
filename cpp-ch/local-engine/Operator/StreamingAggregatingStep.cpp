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
#include <Processors/Transforms/AggregatingTransform.h>
#include <QueryPipeline/QueryPipelineBuilder.h>
#include <Common/CHUtil.h>
#include <Common/CurrentThread.h>
#include <Common/formatReadable.h>
#include <Common/Stopwatch.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}
}

namespace local_engine
{
StreamingAggregatingTransform::StreamingAggregatingTransform(DB::ContextPtr context_, const DB::Block &header_, DB::AggregatingTransformParamsPtr params_)
    : DB::IProcessor({header_}, {params_->getHeader()})
    , context(context_)
    , header(header_)
    , key_columns(params_->params.keys_size)
    , aggregate_columns(params_->params.aggregates_size)
    , params(params_)
{
    aggregated_keys_before_evict = context->getConfigRef().getUInt64("aggregated_keys_before_streaming_aggregating_evict", 1024);
    max_allowed_memory_usage_ratio = context->getConfigRef().getDouble("max_memory_usage_ratio_for_streaming_aggregating", 0.9);
    high_cardinality_threshold = context->getConfigRef().getDouble("high_cardinality_threshold_for_streaming_aggregating", 0.8);
}

StreamingAggregatingTransform::~StreamingAggregatingTransform()
{
    LOG_ERROR(
        logger,
        "Metrics. total_input_blocks: {}, total_input_rows: {},  total_output_blocks: {}, total_output_rows: {}, "
        "total_clear_data_variants_num: {}, total_aggregate_time: {}, total_convert_data_variants_time: {}",
        total_input_blocks,
        total_input_rows,
        total_output_blocks,
        total_output_rows,
        total_clear_data_variants_num,
        total_aggregate_time,
        total_convert_data_variants_time);
}

StreamingAggregatingTransform::Status StreamingAggregatingTransform::prepare()
{
    auto & output = outputs.front();
    auto & input = inputs.front();
    if (output.isFinished())
    {
        input.close();
        return Status::Finished;
    }
    if (has_output)
    {
        if (output.canPush())
        {
            LOG_ERROR(logger, "xxx push one chunk, rows: {}", output_chunk.getNumRows());
            total_output_rows += output_chunk.getNumRows();
            total_output_blocks++;
            output.push(std::move(output_chunk));
            has_output = false;
        }
        return Status::PortFull;
    }

    if (has_input)
        return Status::Ready;

    if (input.isFinished())
    {
        if (!data_variants)
        {
            output.finish();
            return Status::Finished;
        }
        else
        {
            return Status::Ready;
        }
    }

    input.setNeeded();
    if (!input.hasData())
    {
        return Status::NeedData;
    }
    input_chunk = input.pull(true);
    total_input_rows += input_chunk.getNumRows();
    total_input_blocks++;
    has_input = true;
    return Status::Ready;
}

bool StreamingAggregatingTransform::needEvict()
{
    /// More greedy memory usage strategy.
    if (!context->getSettingsRef().max_memory_usage)
        return false;
    auto max_mem_used = static_cast<size_t>(context->getSettingsRef().max_memory_usage * max_allowed_memory_usage_ratio);
    auto current_result_rows = data_variants->size();
    if (current_result_rows < aggregated_keys_before_evict)
        return false;
    
    /// If the grouping keys is high cardinality, we should evict data variants early, and avoid to use a big
    /// hash table.
    if (static_cast<double>(total_output_rows)/total_input_rows > high_cardinality_threshold)
        return true;

    auto current_mem_used = MemoryUtil::getCurrentMemoryUsage();
    if (per_key_memory_usage > 0)
    {
        if (current_mem_used + per_key_memory_usage * current_result_rows >= max_mem_used)
        {
            LOG_ERROR(
                logger,
                "Memory is overflow. current_mem_used: {}, max_mem_used: {}, per_key_memory_usage: {}, aggregator keys: {}",
                ReadableSize(current_mem_used),
                ReadableSize(max_mem_used),
                ReadableSize(per_key_memory_usage),
                current_result_rows);
            return true;
        }
    }
    else
    {
        if (current_mem_used * 2 >= max_mem_used)
        {
            LOG_ERROR(
                logger,
                "Memory is overflow on half of max usage. current_mem_used: {}, max_mem_used: {}, aggregator keys: {}",
                ReadableSize(current_mem_used),
                ReadableSize(max_mem_used),
                current_result_rows);
            return true;
        }
    }
    return false;
}


void StreamingAggregatingTransform::work()
{
    auto pop_one_pending_block = [&]()
    {
        #if AGGREGATE_UTIL_H
        if (blokck_converter && blokck_converter->hasNext())
        {
            Stopwatch convert_watch;
            auto block = blokck_converter->next();
            output_chunk = DB::convertToChunk(block);
            total_convert_data_variants_time += convert_watch.elapsedMicroseconds();
            has_output = true;
            has_input = blokck_converter->hasNext();
            if (output_chunk.getNumRows())
                per_key_memory_usage = output_chunk.allocatedBytes() * 1.0 / output_chunk.getNumRows();
            if (!has_input)
            {
                data_variants = nullptr;
                blokck_converter = nullptr;
            }
            return true;
        }
        return false;
        #else
        while (!pending_blocks.empty())
        {
            if (!pending_blocks.front().rows())
            {
                pending_blocks.pop_front();
                continue;
            }
            // downstream is GraceMergingAggregatedStep, don't need this bock_info.
            // make it be default value.
            pending_blocks.front().info = DB::BlockInfo();

            output_chunk = DB::convertToChunk(pending_blocks.front());
            pending_blocks.pop_front();
            has_output = true;
            has_input = !pending_blocks.empty();
            return true;
        }
        return false;
        #endif
    };

    if (has_input)
    {
        if (pop_one_pending_block())
            return;

        if (!input_chunk.getNumRows())
        {
            has_input = false;
            return;
        }

        if (!data_variants)
            data_variants = std::make_shared<DB::AggregatedDataVariants>();

        auto num_rows = input_chunk.getNumRows();
        Stopwatch watch;
        params->aggregator.executeOnBlock(
            input_chunk.detachColumns(), 0, num_rows, *data_variants, key_columns, aggregate_columns, no_more_keys);
        total_aggregate_time += watch.elapsedMicroseconds();
        has_input = false;

        if (needEvict())
        {
            #if AGGREGATE_UTIL_H
            LOG_ERROR(logger, "xxx 1) convert data variants to blocks. data_variants size: {}", data_variants->size());
            blokck_converter = std::make_unique<AggregateDataBlockConverter>(params->aggregator, data_variants, false);
            total_clear_data_variants_num++;
            #else
            Stopwatch convert_watch;
            /// When convert data variants to blocks, memory usage may be double.
            pending_blocks = params->aggregator.convertToBlocks(*data_variants, false, 1);

            size_t total_mem_used = 0;
            size_t total_rows = 0;
            for (const auto & block : pending_blocks)
            {
                total_mem_used += block.allocatedBytes();
                total_rows += block.rows();
            }
            if (total_rows)
                per_key_memory_usage = total_mem_used * 1.0 / total_rows;

            total_convert_data_variants_time += convert_watch.elapsedMicroseconds();
            total_clear_data_variants_num++;
            data_variants = nullptr;
            #endif
            pop_one_pending_block();
        }
    }
    else
    {
        // NOLINTNEXTLINE
        if (!data_variants || !data_variants->size())
        {
            has_output = false;
        }
        Stopwatch convert_watch;
        #if AGGREGATE_UTIL_H
        LOG_ERROR(logger, "xxx 2) convert data variants to blocks. data_variants size: {}", data_variants->size());
        blokck_converter = std::make_unique<AggregateDataBlockConverter>(params->aggregator, data_variants, false);
        #else
        pending_blocks = params->aggregator.convertToBlocks(*data_variants, false, 1);
        data_variants = nullptr;
        #endif
        total_clear_data_variants_num++;
        total_aggregate_time += convert_watch.elapsedMicroseconds();
        pop_one_pending_block();
    }
}

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

static DB::Block buildOutputHeader(const DB::Block & input_header_, const DB::Aggregator::Params params_)
{
    return params_.getHeader(input_header_, false);
}
StreamingAggregatingStep::StreamingAggregatingStep(
    DB::ContextPtr context_, const DB::DataStream & input_stream_, DB::Aggregator::Params params_)
    : DB::ITransformingStep(input_stream_, buildOutputHeader(input_stream_.header, params_), getTraits())
    , context(context_)
    , params(std::move(params_))
{
}

void StreamingAggregatingStep::transformPipeline(DB::QueryPipelineBuilder & pipeline, const DB::BuildQueryPipelineSettings &)
{
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
    return params.explain(settings.out, settings.offset);
}

void StreamingAggregatingStep::describeActions(DB::JSONBuilder::JSONMap & map) const
{
    params.explain(map);
}

void StreamingAggregatingStep::updateOutputStream()
{
    output_stream = createOutputStream(input_streams.front(), buildOutputHeader(input_streams.front().header, params), getDataStreamTraits());
}

}
