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
#include "ParquetFormatFile.h"

#if USE_PARQUET

#include <memory>
#include <numeric>
#include <utility>

#include <Core/Settings.h>
#include <DataTypes/DataTypeNullable.h>
#include <Formats/FormatFactory.h>
#include <Formats/FormatSettings.h>
#include <IO/SeekableReadBuffer.h>
#include <Processors/Formats/Impl/ArrowBufferedStreams.h>
#include <Processors/Formats/Impl/ArrowColumnToCHColumn.h>
#include <Processors/Formats/Impl/ParquetBlockInputFormat.h>
#include <Storages/Parquet/VectorizedParquetRecordReader.h>
#include <parquet/arrow/reader.h>
#include <parquet/metadata.h>
#include <Common/Exception.h>

namespace DB
{
namespace Setting
{
extern const SettingsMaxThreads max_download_threads;
extern const SettingsMaxThreads max_parsing_threads;
}

namespace ErrorCodes
{
extern const int BAD_ARGUMENTS;
extern const int UNKNOWN_TYPE;
}
}

namespace local_engine
{

ParquetFormatFile::ParquetFormatFile(
    const DB::ContextPtr & context_,
    const substrait::ReadRel::LocalFiles::FileOrFiles & file_info_,
    const ReadBufferBuilderPtr & read_buffer_builder_,
    bool use_local_format_)
    : FormatFile(context_, file_info_, read_buffer_builder_), use_pageindex_reader(use_local_format_)
{
}

FormatFile::InputFormatPtr ParquetFormatFile::createInputFormat(const DB::Block & header)
{
    auto res = std::make_shared<FormatFile::InputFormat>();
    res->read_buffer = read_buffer_builder->build(file_info);

    std::vector<RowGroupInformation> required_row_groups;
    int total_row_groups = 0;
    if (auto * seekable_in = dynamic_cast<DB::SeekableReadBuffer *>(res->read_buffer.get()))
    {
        // reuse the read_buffer to avoid opening the file twice.
        // especially，the cost of opening a hdfs file is large.
        required_row_groups = collectRequiredRowGroups(seekable_in, total_row_groups);
        seekable_in->seek(0, SEEK_SET);
    }
    else
        required_row_groups = collectRequiredRowGroups(total_row_groups);

    auto format_settings = DB::getFormatSettings(context);

    std::vector<int> total_row_group_indices(total_row_groups);
    std::iota(total_row_group_indices.begin(), total_row_group_indices.end(), 0);

    std::vector<int> required_row_group_indices(required_row_groups.size());
    for (size_t i = 0; i < required_row_groups.size(); ++i)
        required_row_group_indices[i] = required_row_groups[i].index;

    std::vector<int> skip_row_group_indices;
    std::ranges::set_difference(total_row_group_indices, required_row_group_indices, std::back_inserter(skip_row_group_indices));

    format_settings.parquet.skip_row_groups = std::unordered_set<int>(skip_row_group_indices.begin(), skip_row_group_indices.end());

    const DB::Settings & settings = context->getSettingsRef();

    if (use_pageindex_reader && supportPageindexReader(header))
    {
        res->input = std::make_shared<VectorizedParquetBlockInputFormat>(*(res->read_buffer), header, format_settings);
    }
    else
    {
        res->input = std::make_shared<DB::ParquetBlockInputFormat>(
            *(res->read_buffer),
            header,
            format_settings,
            settings[DB::Setting::max_parsing_threads],
            settings[DB::Setting::max_download_threads],
            8192);
    }
    return res;
}

std::optional<size_t> ParquetFormatFile::getTotalRows()
{
    {
        std::lock_guard lock(mutex);
        if (total_rows)
            return total_rows;
    }

    int _;
    auto rowgroups = collectRequiredRowGroups(_);
    size_t rows = 0;
    for (const auto & rowgroup : rowgroups)
        rows += rowgroup.num_rows;

    {
        std::lock_guard lock(mutex);
        total_rows = rows;
        return total_rows;
    }
}

bool ParquetFormatFile::supportPageindexReader(const DB::Block & header)
{
    const auto result = std::ranges::find_if(
        header,
        [](DB::ColumnWithTypeAndName const & col)
        {
            const DB::DataTypePtr type_not_nullable = DB::removeNullable(col.type);
            const DB::WhichDataType which(type_not_nullable);
            return DB::isArray(which) || DB::isMap(which) || DB::isTuple(which);
        });

    return result == header.end();
}

std::vector<RowGroupInformation> ParquetFormatFile::collectRequiredRowGroups(int & total_row_groups) const
{
    auto in = read_buffer_builder->build(file_info);
    return collectRequiredRowGroups(in.get(), total_row_groups);
}

std::vector<RowGroupInformation> ParquetFormatFile::collectRequiredRowGroups(DB::ReadBuffer * read_buffer, int & total_row_groups) const
{
    const DB::FormatSettings format_settings{
        .seekable_read = true,
    };
    std::atomic<int> is_stopped{0};
    std::unique_ptr<parquet::arrow::FileReader> reader;
    auto status = parquet::arrow::OpenFile(
        asArrowFile(*read_buffer, format_settings, is_stopped, "Parquet", PARQUET_MAGIC_BYTES), arrow::default_memory_pool(), &reader);
    if (!status.ok())
        throw DB::Exception(DB::ErrorCodes::BAD_ARGUMENTS, "Open file({}) failed. {}", file_info.uri_file(), status.ToString());

    const auto file_meta = reader->parquet_reader()->metadata();
    total_row_groups = file_meta->num_row_groups();

    std::vector<RowGroupInformation> row_group_metadatas;
    row_group_metadatas.reserve(total_row_groups);

    auto get_column_start_offset = [&](const parquet::ColumnChunkMetaData & metadata_)
    {
        Int64 offset = metadata_.data_page_offset();
        if (metadata_.has_dictionary_page() && offset > metadata_.dictionary_page_offset())
            offset = metadata_.dictionary_page_offset();
        return offset;
    };

    for (int i = 0; i < total_row_groups; ++i)
    {
        const auto row_group_meta = file_meta->RowGroup(i);
        Int64 start_offset = 0;
        Int64 total_bytes = 0;
        start_offset = get_column_start_offset(*row_group_meta->ColumnChunk(0));
        total_bytes = row_group_meta->total_compressed_size();
        if (!total_bytes)
            for (int j = 0; j < row_group_meta->num_columns(); ++j)
                total_bytes += row_group_meta->ColumnChunk(j)->total_compressed_size();

        const UInt64 midpoint_offset = static_cast<UInt64>(start_offset + total_bytes / 2);
        /// Current row group has intersection with the required range.
        if (file_info.start() <= midpoint_offset && midpoint_offset < file_info.start() + file_info.length())
        {
            RowGroupInformation info;
            info.index = i;
            info.num_rows = row_group_meta->num_rows();
            info.start = row_group_meta->file_offset();
            info.total_compressed_size = row_group_meta->total_compressed_size();
            info.total_size = row_group_meta->total_byte_size();
            row_group_metadatas.emplace_back(std::move(info));
        }
    }
    return row_group_metadatas;
}

}
#endif
