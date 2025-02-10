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

#include "ParquetMeta.h"
#include <Formats/FormatSettings.h>
#include <Processors/Formats/Impl/ArrowBufferedStreams.h>
#include <Processors/Formats/Impl/ArrowFieldIndexUtil.h>
#include <Storages/Parquet/ArrowUtils.h>
#include <parquet/arrow/reader.h>
#include <parquet/arrow/schema.h>
#include <parquet/metadata.h>

namespace DB
{
namespace ErrorCodes
{
extern const int BAD_ARGUMENTS;
}
}


namespace local_engine
{
std::unique_ptr<parquet::ParquetFileReader> ParquetMetaBuilder::openInputParquetFile(DB::ReadBuffer * read_buffer)
{
    const DB::FormatSettings format_settings{
        .seekable_read = true,
    };
    std::atomic<int> is_stopped{0};
    auto arrow_file = asArrowFile(*read_buffer, format_settings, is_stopped, "Parquet", PARQUET_MAGIC_BYTES);

    return parquet::ParquetFileReader::Open(arrow_file, parquet::default_reader_properties(), nullptr);
}

std::vector<Int32> ParquetMetaBuilder::pruneColumn(
    const DB::Block & header, const parquet::FileMetaData & metadata, bool case_insensitive, bool allow_missing_columns)
{
    std::shared_ptr<arrow::Schema> schema;
    THROW_ARROW_NOT_OK(parquet::arrow::FromParquetSchema(metadata.schema(), &schema));

    DB::ArrowFieldIndexUtil field_util(case_insensitive, allow_missing_columns);
    auto index_mapping = field_util.findRequiredIndices(header, *schema, metadata);

    std::vector<Int32> column_indices;
    for (const auto & [clickhouse_header_index, parquet_indexes] : index_mapping)
        for (auto parquet_index : parquet_indexes)
            column_indices.push_back(parquet_index);
    return column_indices;
}

std::unique_ptr<ColumnIndexStore> ParquetMetaBuilder::collectColumnIndex(
    const parquet::RowGroupMetaData & rgMeta,
    parquet::RowGroupPageIndexReader & rowGroupPageIndex,
    const std::vector<Int32> & column_indices,
    bool case_insensitive)
{
    auto result = std::make_unique<ColumnIndexStore>();
    ColumnIndexStore & column_index_store = *result;
    column_index_store.reserve(column_indices.size());

    for (auto const column_index : column_indices)
    {
        const auto * col_desc = rgMeta.schema()->Column(column_index);
        const auto col_index = rowGroupPageIndex.GetColumnIndex(column_index);
        const auto offset_index = rowGroupPageIndex.GetOffsetIndex(column_index);
        const std::string columnName = case_insensitive ? boost::to_lower_copy(col_desc->name()) : col_desc->name();
        column_index_store[columnName] = ColumnIndex::create(col_desc, col_index, offset_index);
    }
    return result;
}


ParquetMetaBuilder & ParquetMetaBuilder::buildRequiredRowGroups(
    const parquet::FileMetaData & file_meta, const std::function<bool(UInt64)> & should_include_row_group)
{
    Int32 total_row_groups = file_meta.num_row_groups();

    readRowGroups.reserve(total_row_groups);

    auto get_column_start_offset = [&](const parquet::ColumnChunkMetaData & metadata_) -> Int64
    {
        Int64 offset = metadata_.data_page_offset();
        if (metadata_.has_dictionary_page() && offset > metadata_.dictionary_page_offset())
            offset = metadata_.dictionary_page_offset();
        return offset;
    };

    UInt64 rowStartIndexOffset = 0;
    for (int i = 0; i < total_row_groups; ++i)
    {
        const auto row_group_meta = file_meta.RowGroup(i);
        Int64 start_offset = get_column_start_offset(*row_group_meta->ColumnChunk(0));
        Int64 total_bytes = row_group_meta->total_compressed_size();

        if (!total_bytes)
            for (int j = 0; j < row_group_meta->num_columns(); ++j)
                total_bytes += row_group_meta->ColumnChunk(j)->total_compressed_size();

        const UInt64 midpoint_offset = static_cast<UInt64>(start_offset + total_bytes / 2);

        if (should_include_row_group(midpoint_offset))
        {
            RowGroupInformation info;
            info.index = i;
            info.num_rows = row_group_meta->num_rows();
            info.start = row_group_meta->file_offset();
            info.total_compressed_size = row_group_meta->total_compressed_size();
            info.total_size = row_group_meta->total_byte_size();
            info.rowStartIndexOffset = rowStartIndexOffset;
            readRowGroups.emplace_back(std::move(info));
        }
        rowStartIndexOffset += row_group_meta->num_rows();
    }
    return *this;
}

ParquetMetaBuilder & ParquetMetaBuilder::buildSkipRowGroup(const parquet::FileMetaData & file_meta)
{
    if (collectSkipRowGroup)
    {
        Int32 total_row_groups = file_meta.num_row_groups();

        std::vector<Int32> total_row_group_indices(total_row_groups);
        std::iota(total_row_group_indices.begin(), total_row_group_indices.end(), 0);

        std::vector<Int32> required_row_group_indices(readRowGroups.size());
        for (size_t i = 0; i < readRowGroups.size(); ++i)
            required_row_group_indices[i] = readRowGroups[i].index;

        std::ranges::set_difference(total_row_group_indices, required_row_group_indices, std::back_inserter(skipRowGroups));
    }
    return *this;
}

ParquetMetaBuilder & ParquetMetaBuilder::buildRowRange(
    parquet::ParquetFileReader & reader,
    const parquet::FileMetaData & file_meta,
    const DB::Block & readBlock,
    const ColumnIndexFilter * column_index_filter)
{
    if (collectPageIndex)
    {
        readColumns = pruneColumn(readBlock, file_meta, case_insensitive, allow_missing_columns);
        for (auto & row_group : readRowGroups)
        {
            const auto rgMeta = file_meta.RowGroup(row_group.index);
            const auto pageIndex = reader.GetPageIndexReader();
            const auto rowGroupPageIndex = pageIndex == nullptr ? nullptr : pageIndex->RowGroup(row_group.index);
            if (column_index_filter == nullptr || rowGroupPageIndex == nullptr)
                row_group.rowRanges = RowRanges::createSingle(row_group.num_rows);
            else
            {
                auto columnIndex = collectColumnIndex(*rgMeta, *rowGroupPageIndex, readColumns, case_insensitive);
                row_group.rowRanges = column_index_filter->calculateRowRanges(*columnIndex, row_group.num_rows);
                row_group.columnIndexStore = std::move(columnIndex);
            }
        }
    }
    return *this;
}

ParquetMetaBuilder & ParquetMetaBuilder::build(
    DB::ReadBuffer * read_buffer,
    const DB::Block * readBlock,
    const ColumnIndexFilter * column_index_filter,
    const std::function<bool(UInt64)> & should_include_row_group)
{
    auto reader = openInputParquetFile(read_buffer);
    const auto file_meta = reader->metadata();
    return buildRequiredRowGroups(*file_meta, should_include_row_group)
        .buildSkipRowGroup(*file_meta)
        .buildRowRange(*reader, *file_meta, *readBlock, column_index_filter);
}

}