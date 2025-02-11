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

#include <config.h>

#if USE_PARQUET

#include <Formats/FormatSettings.h>
#include <Processors/Formats/IInputFormat.h>
#include <Processors/Formats/Impl/ArrowColumnToCHColumn.h>
#include <parquet/arrow/reader_internal.h>
#include <parquet/arrow/schema.h>

namespace parquet
{
struct PageLocation;
}
namespace DB
{
class DataTypeNullable;
class Block;
}

namespace arrow
{
class ChunkedArray;
}
namespace local_engine
{
class RowRanges;
using ReadRanges = std::vector<arrow::io::ReadRange>;
using ReadSequence = std::vector<int64_t>;
using ColumnReadState = std::pair<ReadRanges, ReadSequence>;
using ColumnChunkPageRead = std::pair<std::unique_ptr<parquet::PageReader>, ReadSequence>;

class VectorizedParquetBlockInputFormat;
class ColumnIndexRowRangesProvider;

ColumnReadState buildAllRead(int64_t rg_count, const arrow::io::ReadRange & chunk_range);
ColumnReadState buildRead(
    int64_t rg_count,
    const arrow::io::ReadRange & chunk_range,
    const std::vector<parquet::PageLocation> & page_locations,
    const RowRanges & row_ranges);
std::shared_ptr<parquet::ArrowInputStream>
getStream(arrow::io::RandomAccessFile & reader, const std::vector<arrow::io::ReadRange> & ranges);

class ParquetReadState
{
    ReadSequence read_sequence_;
    int index_ = 0;

    void advance(const int64_t read_or_skip)
    {
        assert(hasMoreRead());
        assert(read_sequence_[index_] >= read_or_skip);
        read_sequence_[index_] -= read_or_skip;
        if (read_sequence_[index_] == 0)
            ++index_;
    }

public:
    explicit ParquetReadState(const ReadSequence & read_sequence) : read_sequence_(read_sequence) { }
    int64_t currentRead() const
    {
        assert(hasMoreRead());
        return read_sequence_[index_];
    }

    std::optional<int64_t> hasLastSkip() const
    {
        assert(!hasMoreRead());
        if (read_sequence_.back() < 0)
            return read_sequence_.back();
        return std::nullopt;
    }

    bool hasMoreRead() const
    {
        if (read_sequence_.back() < 0)
            return index_ < read_sequence_.size() - 1;
        return index_ < read_sequence_.size();
    }

    void skip(const int64_t skip)
    {
        assert(skip > 0);
        advance(-skip);
    }

    void read(const int64_t read)
    {
        assert(read > 0);
        advance(read);
    }
};

using BuildRead = std::function<ColumnReadState(const arrow::io::ReadRange & col_range)>;

class ParquetFileReaderExt
{
    /// Members
    std::shared_ptr<::arrow::io::RandomAccessFile> source_;
    int64_t source_size_;
    std::unique_ptr<parquet::ParquetFileReader> file_reader_;
    const DB::FormatSettings & format_settings_;
    const ColumnIndexRowRangesProvider & row_ranges_provider_;

    std::shared_ptr<parquet::FileMetaData> fileMeta() const { return file_reader_->metadata(); }
    ColumnChunkPageRead
    readColumnChunkPageBase(const parquet::RowGroupMetaData & rg, Int32 column_index, const BuildRead & build_read) const;

public:
    ParquetFileReaderExt(
        const std::shared_ptr<arrow::io::RandomAccessFile> & source,
        std::unique_ptr<parquet::ParquetFileReader> parquetFileReader,
        const ColumnIndexRowRangesProvider & row_ranges_provider,
        const DB::FormatSettings & format_settings);
    std::optional<ColumnChunkPageRead> nextRowGroup(int32_t row_group_index, int32_t column_index, const std::string & column_name) const;
    parquet::ParquetFileReader * fileReader() const { return file_reader_.get(); }
};

class PageIterator final : public parquet::arrow::FileColumnIterator
{
    ParquetFileReaderExt * reader_ext_;

public:
    PageIterator(const int column_index, ParquetFileReaderExt * reader_ext, const std::vector<Int32> & row_groups)
        : FileColumnIterator(column_index, reader_ext->fileReader(), row_groups), reader_ext_(reader_ext)
    {
    }

    ~PageIterator() override = default;

    std::optional<ColumnChunkPageRead> nextRowGroup();
};

class VectorizedColumnReader
{
    std::shared_ptr<arrow::Field> arrow_field_;
    PageIterator input_;
    std::shared_ptr<parquet::internal::RecordReader> record_reader_;
    std::unique_ptr<ParquetReadState> read_state_;

    void nextRowGroup();
    void setPageReader(std::unique_ptr<parquet::PageReader> reader, const ReadSequence & read_sequence);

public:
    VectorizedColumnReader(const parquet::arrow::SchemaField & field, ParquetFileReaderExt * reader, const std::vector<Int32> & row_groups);
    const std::string & columnName() const { return arrow_field_->name(); }
    std::shared_ptr<arrow::Field> arrowField() const { return arrow_field_; }
    bool hasMoreRead() const { return read_state_ && read_state_->hasMoreRead(); }
    std::shared_ptr<arrow::ChunkedArray> readBatch(int64_t batch_size);
};

class VectorizedParquetRecordReader
{
    const DB::Block parquet_header_;
    const DB::FormatSettings format_settings_;
    DB::ArrowColumnToCHColumn arrow_column_to_ch_column_;

    std::unique_ptr<ParquetFileReaderExt> file_reader_;

    // parquet::arrow::SchemaManifest manifest_;
    /// columns to read from Parquet file.
    std::vector<VectorizedColumnReader> column_readers_;

    friend class VectorizedParquetBlockInputFormat;

    static parquet::arrow::SchemaManifest createSchemaManifest(const parquet::FileMetaData & metadata);

public:
    VectorizedParquetRecordReader(const DB::Block & header, const DB::FormatSettings & format_settings);
    ~VectorizedParquetRecordReader() = default;

    bool initialize(
        const std::shared_ptr<arrow::io::RandomAccessFile> & arrow_file,
        const ColumnIndexRowRangesProvider & row_ranges_provider,
        const std::shared_ptr<parquet::FileMetaData> & metadata = nullptr);
    DB::Chunk nextBatch();

    bool initialized() const { return file_reader_ != nullptr; }

    void reset()
    {
        column_readers_.clear();
        file_reader_.reset();
    }
};

/// InputFormat

class VectorizedParquetBlockInputFormat final : public DB::IInputFormat
{
    std::atomic<int> is_stopped{0};
    VectorizedParquetRecordReader record_reader_;
    const ColumnIndexRowRangesProvider & row_ranges_provider_;

protected:
    void onCancel() noexcept override { is_stopped = 1; }

public:
    VectorizedParquetBlockInputFormat(
        DB::ReadBuffer & in_,
        const DB::Block & header_,
        const ColumnIndexRowRangesProvider & row_ranges_provider,
        const DB::FormatSettings & format_settings);
    String getName() const override { return "VectorizedParquetBlockInputFormat"; }
    void resetParser() override;

private:
    DB::Chunk read() override;
};
}
#endif
