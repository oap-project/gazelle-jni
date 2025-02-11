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

#include <Core/Block.h>
#include <Storages/Parquet/ColumnIndexFilter.h>
#include <Storages/Parquet/RowRanges.h>
#include <base/types.h>
#include <parquet/file_reader.h>

namespace local_engine
{

struct RowGroupInformation
{
    UInt32 index = 0;
    UInt64 start = 0;
    UInt64 total_compressed_size = 0;
    UInt64 total_size = 0;
    UInt64 num_rows = 0;
    UInt64 rowStartIndexOffset = 0;

    std::unique_ptr<ColumnIndexStore> columnIndexStore;
    RowRanges rowRanges;
};

struct ParquetMetaBuilder
{
    // control flag
    bool case_insensitive = false;
    bool allow_missing_columns = false;
    bool collectSkipRowGroup = false;
    bool collectPageIndex = false;

    //
    std::vector<RowGroupInformation> readRowGroups;

    // collectSkipRowGroup
    std::vector<Int32> skipRowGroups;

    // collectPageIndex
    std::vector<Int32> readColumns;

    ParquetMetaBuilder & build(
        DB::ReadBuffer * read_buffer,
        const DB::Block * readBlock,
        const ColumnIndexFilter * column_index_filter,
        const std::function<bool(UInt64)> & should_include_row_group);

    static std::unique_ptr<parquet::ParquetFileReader> openInputParquetFile(DB::ReadBuffer * read_buffer);

private:
    ParquetMetaBuilder &
    buildRequiredRowGroups(const parquet::FileMetaData & file_meta, const std::function<bool(UInt64)> & should_include_row_group);
    ParquetMetaBuilder & buildSkipRowGroup(const parquet::FileMetaData & file_meta);
    ParquetMetaBuilder & buildRowRange(
        parquet::ParquetFileReader & reader,
        const parquet::FileMetaData & file_meta,
        const DB::Block & readBlock,
        const ColumnIndexFilter * column_index_filter);

    static std::vector<Int32>
    pruneColumn(const DB::Block & header, const parquet::FileMetaData & metadata, bool case_insensitive, bool allow_missing_columns);
    static std::unique_ptr<ColumnIndexStore> collectColumnIndex(
        const parquet::RowGroupMetaData & rgMeta,
        parquet::RowGroupPageIndexReader & rowGroupPageIndex,
        const std::vector<Int32> & column_indices,
        bool case_insensitive = false);
};

namespace ParquetVirtualMeta
{
inline constexpr auto TMP_ROWINDEX = "_tmp_metadata_row_index";
inline bool hasMetaColumns(const DB::Block & header)
{
    return header.findByName(TMP_ROWINDEX) != nullptr;
}
inline DB::DataTypePtr getMetaColumnType(const DB::Block & header)
{
    return header.findByName(TMP_ROWINDEX)->type;
}
inline DB::Block removeMetaColumns(const DB::Block & header)
{
    DB::Block new_header;
    for (const auto & col : header)
        if (col.name != TMP_ROWINDEX)
            new_header.insert(col);
    return new_header;
}
}

class ColumnIndexRowRangesProvider
{
    ColumnIndexRowRangesProvider(std::vector<RowGroupInformation> rowGroupInfos, std::vector<Int32> readColumns)
        : startRowGroupIndex_(rowGroupInfos[0].index), rowGroupInfos_(std::move(rowGroupInfos)), readColumns_(std::move(readColumns))
    {
        for (const auto & rg : rowGroupInfos_)
            readRowGroups_.push_back(rg.index);

        /// Currently, we only filter row group based on the 'midpoint_offset' of the file, so that the row groups are continuous.
        /// We utilized this feature by using vectors instead of map.
        assert(rowGroupInfos_.size() == rowGroupInfos_.back().index - startRowGroupIndex_ + 1);
    }

public:
    /// Used in UT, in case of testing VirtualColumnRowIndexReader.
    explicit ColumnIndexRowRangesProvider(std::vector<RowGroupInformation> rowGroupInfos)
        : ColumnIndexRowRangesProvider(std::move(rowGroupInfos), {})
    {
    }
    explicit ColumnIndexRowRangesProvider(ParquetMetaBuilder & meta_collect)
        : ColumnIndexRowRangesProvider(std::move(meta_collect.readRowGroups), std::move(meta_collect.readColumns))
    {
    }

    std::optional<RowRanges> getRowRanges(Int32 row_group_index) const
    {
        auto index = adjustRowIndex(row_group_index);
        auto ranges = rowGroupInfos_[index].rowRanges;
        auto rgCount = rowGroupInfos_[index].num_rows;
        if (rgCount == 0 || ranges.rowCount() == 0)
            return std::nullopt;
        return ranges;
    }

    UInt64 getRowGroupStartIndex(Int32 row_group_index) const
    {
        return rowGroupInfos_[adjustRowIndex(row_group_index)].rowStartIndexOffset;
    }
    const ColumnIndexStore & getColumnIndexStore(Int32 row_group_index) const
    {
        if (!rowGroupInfos_[adjustRowIndex(row_group_index)].columnIndexStore)
            throw DB::Exception(DB::ErrorCodes::LOGICAL_ERROR, "ColumnIndexStore is not available");
        return *rowGroupInfos_[adjustRowIndex(row_group_index)].columnIndexStore;
    }

    const std::vector<Int32> & getReadRowGroups() const { return readRowGroups_; };
    const std::vector<Int32> & getReadColumns() const { return readColumns_; };

private:
    Int32 adjustRowIndex(Int32 row_group_index) const
    {
        Int32 realIndex = row_group_index - startRowGroupIndex_;
        assert(realIndex >= 0 || realIndex < static_cast<Int32>(rowGroupInfos_.size()));
        return realIndex;
    };

    const UInt32 startRowGroupIndex_;
    const std::vector<RowGroupInformation> rowGroupInfos_;
    std::vector<Int32> readRowGroups_;
    const std::vector<Int32> readColumns_;
};

}