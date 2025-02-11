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

#include <deque>
#include <optional>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnsNumber.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/IDataType.h>
#include <Storages/Parquet/ParquetMeta.h>
#include <Storages/Parquet/RowRanges.h>
#include <base/types.h>

namespace local_engine
{

class RowIndexGenerator
{
    RowRanges row_ranges_;
    size_t start_rowRange_ = 0;

public:
    RowIndexGenerator(const RowRanges & row_ranges, UInt64 startingRowIdx) : row_ranges_(row_ranges)
    {
        assert(!row_ranges_.getRanges().empty());
        for (auto & range : row_ranges_.getRanges())
        {
            range.from += startingRowIdx;
            range.to += startingRowIdx;
        }
    }

    size_t populateRowIndices(Int64 * row_indices, size_t batchSize)
    {
        size_t count = 0;
        for (; start_rowRange_ < row_ranges_.getRanges().size(); ++start_rowRange_)
        {
            auto & range = row_ranges_.getRange(start_rowRange_);
            for (size_t j = range.from; j <= range.to; ++j)
            {
                *row_indices++ = j;
                if (++count >= batchSize)
                {
                    range.from = j + 1; // Advance range.from
                    return count;
                }
            }
        }
        return count;
    }
};

class VirtualColumnRowIndexReader
{
    const ColumnIndexRowRangesProvider & row_ranges_provider_;
    std::deque<Int32> row_groups_;
    std::optional<RowIndexGenerator> row_index_generator_;
    DB::DataTypePtr column_type_;

    std::optional<RowIndexGenerator> nextRowGroup()
    {
        while (!row_groups_.empty())
        {
            const Int32 row_group_index = row_groups_.front();
            auto result = row_ranges_provider_.getRowRanges(row_group_index);
            row_groups_.pop_front();
            if (result)
                return RowIndexGenerator{result.value(), row_ranges_provider_.getRowGroupStartIndex(row_group_index)};
        }
        return std::nullopt;
    }

public:
    VirtualColumnRowIndexReader(const ColumnIndexRowRangesProvider & row_ranges_provider, const DB::DataTypePtr & column_type)
        : row_ranges_provider_(row_ranges_provider)
        , row_groups_(row_ranges_provider.getReadRowGroups().begin(), row_ranges_provider.getReadRowGroups().end())
        , row_index_generator_(nextRowGroup())
        , column_type_(column_type)
    {
    }

    ~VirtualColumnRowIndexReader() = default;

    DB::ColumnPtr readBatch(const int64_t batch_size)
    {
        if (column_type_->isNullable())
        {
            auto internal_type = typeid_cast<const DB::DataTypeNullable &>(*column_type_).getNestedType();
            auto nested_column = readBatchNonNullable(internal_type, batch_size);
            auto nullmap_column = DB::ColumnUInt8::create(nested_column->size(), 0);
            return DB::ColumnNullable::create(nested_column, std::move(nullmap_column));
        }
        return readBatchNonNullable(column_type_, batch_size);
    }

    DB::ColumnPtr readBatchNonNullable(const DB::DataTypePtr & notNullType, const int64_t batch_size)
    {
        assert(DB::WhichDataType(notNullType).isInt64());
        auto column = DB::ColumnInt64::create(batch_size);
        DB::ColumnInt64::Container & vec = column->getData();
        int64_t readCount = 0;
        int64_t remaining = batch_size;
        while (remaining > 0 && row_index_generator_)
        {
            Int64 * pos = vec.data() + readCount;
            readCount += row_index_generator_->populateRowIndices(pos, remaining);
            remaining = batch_size - readCount;
            if (remaining > 0)
                row_index_generator_ = nextRowGroup();
        }

        if (remaining) // we know that we have read all the rows, but we can't fill the container
            vec.resize(readCount);
        assert(readCount + remaining == batch_size);
        assert(readCount == column->size());
        return column;
    }
};
}