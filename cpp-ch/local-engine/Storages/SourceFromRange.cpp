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
#include "SourceFromRange.h"

#include <Columns/ColumnsNumber.h>
#include <DataTypes/DataTypesNumber.h>
#include <Processors/Chunk.h>

using namespace DB;

namespace DB::ErrorCodes
{
extern const int NUMBER_OF_COLUMNS_DOESNT_MATCH;
extern const int TYPE_MISMATCH;
}

namespace local_engine
{
SourceFromRange::SourceFromRange(const DB::Block & header_, Int64 start_, Int64 end_, Int64 step_, Int32 num_slices_, Int32 slice_index_, size_t max_block_size_)
    : DB::ISource(header_)
    , start(start_)
    , end(end_)
    , step(step_)
    , num_slices(num_slices_)
    , slice_index(slice_index_)
    , max_block_size(max_block_size_)
    , num_elements(getNumElements())
    , is_empty_range(start == end )
{
    const auto & header = getOutputs().front().getHeader();
    if (header.columns() != 1)
        throw Exception(ErrorCodes::NUMBER_OF_COLUMNS_DOESNT_MATCH, "Expected 1 column, got {}", header.columns());
    if (!header.getByPosition(0).type->equals(DataTypeInt64()))
        throw Exception(ErrorCodes::TYPE_MISMATCH, "Expected Int64 column, got {}", header.getByPosition(0).type->getName());
    if (step == 0)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Step cannot be zero");


    Int128 partition_start = (slice_index * num_elements) / num_slices * step + start;
    Int128 partition_end = (((slice_index + 1) * num_elements) / num_slices) * step + start;

    auto get_safe_margin = [](Int128 bi) -> Int64
    {
        if (bi <= std::numeric_limits<Int64>::max() && bi >= std::numeric_limits<Int64>::min())
            return static_cast<Int64>(bi);
        else if (bi > 0)
            return std::numeric_limits<Int64>::max();
        else
            return std::numeric_limits<Int64>::min();
    };

    safe_partition_start = get_safe_margin(partition_start);
    safe_partition_end = get_safe_margin(partition_end);
    current = safe_partition_start;
    previous = 0;
    overflow = false;
}

Int128 SourceFromRange::getNumElements() const
{
    const auto safe_start = static_cast<Int128>(start);
    const auto safe_end = static_cast<Int128>(end);
    if ((safe_end - safe_start) % step == 0 || (safe_end > safe_start) != (step > 0))
    {
        return (safe_end - safe_start) / step;
    }
    else
    {
        // the remainder has the same sign with range, could add 1 more
        return (safe_end - safe_start) / step + 1;
    }
}


DB::Chunk SourceFromRange::generate()
{
    if (is_empty_range)
        return {};

    if (overflow || (step > 0 && current >= safe_partition_end) || (step < 0 && current <= safe_partition_end))
        return {};


    auto column = DB::ColumnInt64::create();
    auto & data = column->getData();
    data.resize_exact(max_block_size);

    size_t row_i = 0;
    if (step > 0)
    {
        for (; current < safe_partition_end && !overflow && row_i < max_block_size; ++row_i)
        {
            previous = current;
            data[row_i] = current;
            current += step;
            overflow = current < previous;
        }
    }
    else
    {
        for (; current > safe_partition_end && !overflow && row_i < max_block_size; ++row_i)
        {
            previous = current;
            data[row_i] = current;
            current += step;
            overflow = current > previous;
        }
    }
    data.resize_exact(row_i);

    // std::cout << "gen rows:" << column->size() << std::endl;
    DB::Columns columns;
    columns.push_back(std::move(column));
    return DB::Chunk(std::move(columns), row_i);
}
}
