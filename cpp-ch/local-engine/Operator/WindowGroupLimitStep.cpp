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

#include "WindowGroupLimitStep.h"

#include <Columns/IColumn.h>
#include <Processors/Chunk.h>
#include <Processors/IProcessor.h>
#include <Processors/Port.h>
#include <QueryPipeline/QueryPipelineBuilder.h>
#include <Common/logger_useful.h>

namespace DB::ErrorCodes
{
extern const int LOGICAL_ERROR;
}

namespace local_engine
{

enum class WindowGroupLimitFunction
{
    RowNumber,
    Rank,
    DenseRank
};

template <WindowGroupLimitFunction function>
class WindowGroupLimitTransform : public DB::IProcessor
{
public:
    using Status = DB::IProcessor::Status;
    explicit WindowGroupLimitTransform(
        const DB::Block & header_, const std::vector<size_t> & partition_columns_, const std::vector<size_t> & sort_columns_, size_t limit_)
        : DB::IProcessor({header_}, {header_})
        , header(header_)
        , partition_columns(partition_columns_)
        , sort_columns(sort_columns_)
        , limit(limit_)
    {
    }
    ~WindowGroupLimitTransform() override = default;
    String getName() const override { return "WindowGroupLimitTransform"; }

    Status prepare() override
    {
        auto & output_port = outputs.front();
        auto & input_port = inputs.front();
        if (output_port.isFinished())
        {
            input_port.close();
            return Status::Finished;
        }

        if (has_output)
        {
            if (output_port.canPush())
            {
                output_port.push(std::move(output_chunk));
                has_output = false;
            }
            return Status::PortFull;
        }

        if (has_input)
            return Status::Ready;

        if (input_port.isFinished())
        {
            output_port.finish();
            return Status::Finished;
        }
        input_port.setNeeded();
        if (!input_port.hasData())
            return Status::NeedData;
        input_chunk = input_port.pull(true);
        has_input = true;
        return Status::Ready;
    }

    void work() override
    {
        if (!has_input) [[unlikely]]
            return;
        DB::Block block = header.cloneWithColumns(input_chunk.getColumns());
        size_t partition_start_row = 0;
        size_t chunk_rows = input_chunk.getNumRows();
        while (partition_start_row < chunk_rows)
        {
            auto next_partition_start_row = advanceNextPartition(input_chunk, partition_start_row);
            iteratePartition(input_chunk, partition_start_row, next_partition_start_row);
            partition_start_row = next_partition_start_row;
            // corner case, the partition end row is the last row of chunk.
            if (partition_start_row < chunk_rows)
            {
                current_row_rank_value = 1;
                if constexpr (function == WindowGroupLimitFunction::Rank)
                    current_peer_group_rows = 0;
                partition_start_row_columns = extractOneRowColumns(input_chunk, partition_start_row);
            }
        }

        if (!output_columns.empty() && output_columns[0]->size() > 0)
        {
            auto rows = output_columns[0]->size();
            output_chunk = DB::Chunk(std::move(output_columns), rows);
            output_columns.clear();
            has_output = true;
        }
        has_input = false;
    }

private:
    DB::Block header;
    // Which columns are used as the partition keys
    std::vector<size_t> partition_columns;
    // which columns are used as the order by keys, excluding partition columns.
    std::vector<size_t> sort_columns;
    // Limitations for each partition.
    size_t limit = 0;

    bool has_input = false;
    DB::Chunk input_chunk;
    bool has_output = false;
    DB::MutableColumns output_columns;
    DB::Chunk output_chunk;

    // We don't have window frame here. in fact all of frame are (unbounded preceding, current row]
    // the start value is 1
    size_t current_row_rank_value = 1;
    // rank need this to record how many rows in current peer group.
    // A peer group in a partition is defined as the rows have the same value on the sort columns.
    size_t current_peer_group_rows = 0;

    DB::Columns partition_start_row_columns;
    DB::Columns peer_group_start_row_columns;

    size_t advanceNextPartition(const DB::Chunk & chunk, size_t start_offset)
    {
        if (partition_start_row_columns.empty())
            partition_start_row_columns = extractOneRowColumns(chunk, start_offset);

        size_t max_row = chunk.getNumRows();
        for (size_t i = start_offset; i < max_row; ++i)
            if (!isRowEqual(partition_columns, partition_start_row_columns, 0, chunk.getColumns(), i))
                return i;
        return max_row;
    }

    static DB::Columns extractOneRowColumns(const DB::Chunk & chunk, size_t offset)
    {
        DB::Columns row;
        for (const auto & col : chunk.getColumns())
        {
            auto new_col = col->cloneEmpty();
            new_col->insertFrom(*col, offset);
            row.push_back(std::move(new_col));
        }
        return row;
    }

    static bool isRowEqual(
        const std::vector<size_t> & fields, const DB::Columns & left_cols, size_t loffset, const DB::Columns & right_cols, size_t roffset)
    {
        for (size_t i = 0; i < fields.size(); ++i)
        {
            const auto & field = fields[i];
            /// don't care about nan_direction_hint
            if (left_cols[field]->compareAt(loffset, roffset, *right_cols[field], 1))
                return false;
        }
        return true;
    }

    void iteratePartition(const DB::Chunk & chunk, size_t start_offset, size_t end_offset)
    {
        // Skip the rest rows int this partition.
        if (current_row_rank_value > limit)
            return;

        size_t chunk_rows = chunk.getNumRows();
        auto has_peer_group_ended = [&](size_t offset, size_t partition_end_offset, size_t chunk_rows_)
        { return offset < partition_end_offset || end_offset < chunk_rows_; };
        auto try_end_peer_group
            = [&](size_t peer_group_start_offset, size_t next_peer_group_start_offset, size_t partition_end_offset, size_t chunk_rows_)
        {
            if constexpr (function == WindowGroupLimitFunction::Rank)
            {
                current_peer_group_rows += next_peer_group_start_offset - peer_group_start_offset;
                if (has_peer_group_ended(next_peer_group_start_offset, partition_end_offset, chunk_rows_))
                {
                    current_row_rank_value += current_peer_group_rows;
                    current_peer_group_rows = 0;
                    peer_group_start_row_columns = extractOneRowColumns(chunk, next_peer_group_start_offset);
                }
            }
            else if constexpr (function == WindowGroupLimitFunction::DenseRank)
            {
                if (has_peer_group_ended(next_peer_group_start_offset, partition_end_offset, chunk_rows_))
                {
                    current_row_rank_value += 1;
                    peer_group_start_row_columns = extractOneRowColumns(chunk, next_peer_group_start_offset);
                }
            }
        };

        // This is a corner case. prev partition's last row is the last row of a chunk.
        if (start_offset >= end_offset)
        {
            assert(!start_offset);
            try_end_peer_group(start_offset, end_offset, end_offset, chunk_rows);
            return;
        }

        //  row_number is simple
        if constexpr (function == WindowGroupLimitFunction::RowNumber)
        {
            size_t rows = end_offset - start_offset;
            size_t limit_remained = limit - current_row_rank_value + 1;
            rows = rows > limit_remained ? limit_remained : rows;
            insertResultValue(chunk, start_offset, rows);

            current_row_rank_value += rows;
        }
        else
        {
            size_t peer_group_start_offset = start_offset;
            while (peer_group_start_offset < end_offset && current_row_rank_value <= limit)
            {
                auto next_peer_group_start_offset = advanceNextPeerGroup(chunk, peer_group_start_offset, end_offset);
                size_t group_rows = next_peer_group_start_offset - peer_group_start_offset;
                insertResultValue(chunk, peer_group_start_offset, group_rows);
                try_end_peer_group(peer_group_start_offset, next_peer_group_start_offset, end_offset, chunk_rows);
                peer_group_start_offset = next_peer_group_start_offset;
            }
        }
    }
    void insertResultValue(const DB::Chunk & chunk, size_t start_offset, size_t rows)
    {
        if (!rows)
            return;
        if (output_columns.empty())
            for (const auto & col : chunk.getColumns())
                output_columns.push_back(col->cloneEmpty());
        size_t i = 0;
        for (const auto & col : chunk.getColumns())
        {
            output_columns[i]->insertRangeFrom(*col, start_offset, rows);
            i += 1;
        }
    }
    size_t advanceNextPeerGroup(const DB::Chunk & chunk, size_t start_offset, size_t partition_end_offset)
    {
        if (peer_group_start_row_columns.empty())
            peer_group_start_row_columns = extractOneRowColumns(chunk, start_offset);
        for (size_t i = start_offset; i < partition_end_offset; ++i)
            if (!isRowEqual(sort_columns, peer_group_start_row_columns, 0, chunk.getColumns(), i))
                return i;
        return partition_end_offset;
    }
};

static DB::ITransformingStep::Traits getTraits()
{
    return DB::ITransformingStep::Traits{
        {
            .preserves_number_of_streams = false,
            .preserves_sorting = true,
        },
        {
            .preserves_number_of_rows = false,
        }};
}

WindowGroupLimitStep::WindowGroupLimitStep(
    const DB::Block & input_header_,
    const String & function_name_,
    const std::vector<size_t> & partition_columns_,
    const std::vector<size_t> & sort_columns_,
    size_t limit_)
    : DB::ITransformingStep(input_header_, input_header_, getTraits())
    , function_name(function_name_)
    , partition_columns(partition_columns_)
    , sort_columns(sort_columns_)
    , limit(limit_)
{
}

void WindowGroupLimitStep::describePipeline(DB::IQueryPlanStep::FormatSettings & settings) const
{
    if (!processors.empty())
        DB::IQueryPlanStep::describePipeline(processors, settings);
}

void WindowGroupLimitStep::updateOutputHeader()
{
    output_header = input_headers.front();
}


void WindowGroupLimitStep::transformPipeline(DB::QueryPipelineBuilder & pipeline, const DB::BuildQueryPipelineSettings & /*settings*/)
{
    if (function_name == "row_number")
    {
        pipeline.addSimpleTransform(
            [&](const DB::Block & header)
            {
                return std::make_shared<WindowGroupLimitTransform<WindowGroupLimitFunction::RowNumber>>(
                    header, partition_columns, sort_columns, limit);
            });
    }
    else if (function_name == "rank")
    {
        pipeline.addSimpleTransform(
            [&](const DB::Block & header) {
                return std::make_shared<WindowGroupLimitTransform<WindowGroupLimitFunction::Rank>>(
                    header, partition_columns, sort_columns, limit);
            });
    }
    else if (function_name == "dense_rank")
    {
        pipeline.addSimpleTransform(
            [&](const DB::Block & header)
            {
                return std::make_shared<WindowGroupLimitTransform<WindowGroupLimitFunction::DenseRank>>(
                    header, partition_columns, sort_columns, limit);
            });
    }
    else
    {
        throw DB::Exception(DB::ErrorCodes::LOGICAL_ERROR, "Unsupport function {} in WindowGroupLimit", function_name);
    }
}
}
