#pragma once
#include <config.h>

#if USE_PARQUET

#include <cstddef>
#include <Formats/FormatSettings.h>
#include <Processors/Formats/IInputFormat.h>
#include <Processors/Formats/Impl/ArrowColumnToCHColumn.h>
#include <parquet/arrow/reader_internal.h>
#include <parquet/arrow/schema.h>


namespace DB
{
class Block;
}

namespace arrow
{
class ChunkedArray;
}
namespace local_engine
{
class VectorizedParquetRecordReader;
class VectorizedParquetBlockInputFormat;

class VectorizedColumnReader
{
private:
    std::shared_ptr<arrow::Field> arrowField_;
    parquet::arrow::FileColumnIterator input_;
    std::shared_ptr<parquet::internal::RecordReader> record_reader_;
    void NextRowGroup();
    friend class VectorizedParquetRecordReader;

public:
    VectorizedColumnReader(
        const parquet::arrow::SchemaField & field, parquet::ParquetFileReader * reader, const std::vector<int32_t> & row_groups);
    std::shared_ptr<arrow::ChunkedArray> readBatch(size_t batch_size);
};

class VectorizedParquetRecordReader
{
private:
    const DB::FormatSettings format_settings_;
    DB::ArrowColumnToCHColumn arrowColumnToCHColumn_;

    std::unique_ptr<parquet::ParquetFileReader> parquetFileReader_;
    parquet::arrow::SchemaManifest manifest_;
    /// columns to read from Parquet file.
    std::vector<VectorizedColumnReader> columnVectors_;

    friend class VectorizedParquetBlockInputFormat;

public:
    VectorizedParquetRecordReader(const DB::Block & header, const DB::FormatSettings & format_settings);
    ~VectorizedParquetRecordReader() = default;
    void initialize(
        const DB::Block & header,
        const std::shared_ptr<arrow::io::RandomAccessFile> & arrow_file,
        const std::shared_ptr<parquet::FileMetaData> & metadata = nullptr);
    DB::Chunk nextBatch();

    bool initialized() { return parquetFileReader_ != nullptr; }

    void reset()
    {
        columnVectors_.clear();
        parquetFileReader_.reset();
    }
};


/// InputFormat

class VectorizedParquetBlockInputFormat : public DB::IInputFormat
{
private:
    std::atomic<int> is_stopped{0};
    DB::BlockMissingValues block_missing_values;
    VectorizedParquetRecordReader recordReader_;

protected:
    void onCancel() override { is_stopped = 1; }

public:
    VectorizedParquetBlockInputFormat(DB::ReadBuffer & in_, const DB::Block & header_, const DB::FormatSettings & format_settings_);

    String getName() const override { return "VectorizedParquetBlockInputFormat"; }
    void resetParser() override;
    const DB::BlockMissingValues & getMissingValues() const override;

private:
    DB::Chunk generate() override;
};

}
#endif
