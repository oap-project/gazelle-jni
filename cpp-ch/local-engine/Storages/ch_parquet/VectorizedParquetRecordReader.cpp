#include "VectorizedParquetRecordReader.h"

#if USE_PARQUET
#include <optional>
#include <Processors/Chunk.h>
#include <Processors/Formats/Impl/ArrowBufferedStreams.h>
#include <Processors/Formats/Impl/ArrowFieldIndexUtil.h>
#include <Storages/ch_parquet/ArrowUtils.h>
#include <boost/iterator/counting_iterator.hpp>
#include <parquet/column_reader.h>
#include <parquet/file_reader.h>


namespace local_engine
{
VectorizedColumnReader::VectorizedColumnReader(
    const parquet::arrow::SchemaField & field, parquet::ParquetFileReader * reader, const std::vector<int32_t> & row_groups)
    : arrowField_(field.field)
    , input_(field.column_index, reader, row_groups)
    , record_reader_(parquet::internal::RecordReader::Make(
          input_.descr(), ComputeLevelInfo(input_.descr()), default_arrow_pool(), arrowField_->type()->id() == ::arrow::Type::DICTIONARY))
{
    NextRowGroup();
}

void VectorizedColumnReader::NextRowGroup()
{
    /// where IO happens!
    record_reader_->SetPageReader(input_.NextChunk());
}

std::shared_ptr<arrow::ChunkedArray> VectorizedColumnReader::readBatch(size_t batch_size)
{
    record_reader_->Reset();

    // pre-allocate memory
    record_reader_->Reserve(batch_size);
    while (batch_size > 0 && record_reader_->HasMoreData())
    {
        int64_t records_read = record_reader_->ReadRecords(batch_size);
        batch_size -= records_read;
        if (records_read == 0)
            NextRowGroup();
    }
    std::shared_ptr<arrow::ChunkedArray> result;
    THROW_ARROW_NOT_OK(
        parquet::arrow::TransferColumnData(record_reader_.get(), arrowField_, input_.descr(), default_arrow_pool(), &result));
    return result;
}

VectorizedParquetRecordReader::VectorizedParquetRecordReader(const DB::Block & header, const DB::FormatSettings & format_settings)
    : format_settings_(format_settings)
    , arrowColumnToCHColumn_(
          header,
          "Parquet",
          format_settings.parquet.allow_missing_columns,
          format_settings.null_as_default,
          format_settings.date_time_overflow_behavior,
          format_settings.parquet.case_insensitive_column_matching)
{
}

void VectorizedParquetRecordReader::initialize(
    const DB::Block & header,
    const std::shared_ptr<arrow::io::RandomAccessFile> & arrow_file,
    const std::shared_ptr<parquet::FileMetaData> & metadata)
{
    parquetFileReader_ = parquet::ParquetFileReader::Open(arrow_file, parquet::default_reader_properties(), metadata);
    parquet::ArrowReaderProperties properties;
    const parquet::SchemaDescriptor * parquet_schema = parquetFileReader_->metadata()->schema();
    auto keyValueMetadata = parquetFileReader_->metadata()->key_value_metadata();
    THROW_ARROW_NOT_OK(parquet::arrow::SchemaManifest::Make(parquet_schema, keyValueMetadata, properties, &manifest_));
    std::vector<std::shared_ptr<arrow::Field>> fields;
    fields.reserve(manifest_.schema_fields.size());
    for (auto const & schema_field : manifest_.schema_fields)
        fields.emplace_back(schema_field.field);
    arrow::Schema schema(fields, keyValueMetadata);

    /// column pruning
    DB::ArrowFieldIndexUtil field_util(
        format_settings_.parquet.case_insensitive_column_matching, format_settings_.parquet.allow_missing_columns);
    std::vector<int32_t> column_indices = field_util.findRequiredIndices(header, schema);
    THROW_ARROW_NOT_OK_OR_ASSIGN(std::vector<int> field_indices, manifest_.GetFieldIndices(column_indices));

    /// row groups pruning
    std::vector<int32_t> row_groups(
        boost::counting_iterator<int32_t>(0), boost::counting_iterator<int32_t>(parquetFileReader_->metadata()->num_row_groups()));
    if (!format_settings_.parquet.skip_row_groups.empty())
    {
        row_groups.erase(
            std::remove_if(
                row_groups.begin(), row_groups.end(), [&](int32_t i) { return format_settings_.parquet.skip_row_groups.contains(i); }),
            row_groups.end());
    }

    assert(!row_groups.empty());
    columnVectors_.reserve(field_indices.size());
    for (auto const & fieldIndex : field_indices)
    {
        auto const & field = manifest_.schema_fields[fieldIndex];
        assert(field.column_index >= 0);
        columnVectors_.emplace_back(field, parquetFileReader_.get(), row_groups);
    }
}

DB::Chunk VectorizedParquetRecordReader::nextBatch()
{
    assert(initialized());
    ::arrow::ChunkedArrayVector columns(columnVectors_.size());
    DB::ArrowColumnToCHColumn::NameToColumnPtr name_to_column_ptr;
    for (auto & columnVector : columnVectors_)
    {
        std::shared_ptr<arrow::ChunkedArray> arrow_column = columnVector.readBatch(format_settings_.parquet.max_block_size);
        std::string column_name = columnVector.arrowField_->name();
        if (format_settings_.parquet.case_insensitive_column_matching)
            boost::to_lower(column_name);
        name_to_column_ptr[column_name] = arrow_column;
    }
    size_t num_rows = name_to_column_ptr.begin()->second->length();
    if (num_rows > 0)
    {
        DB::Chunk result;
        arrowColumnToCHColumn_.arrowColumnsToCHChunk(result, name_to_column_ptr, num_rows, nullptr);
        return result;
    }
    return {};
}

/// input format
VectorizedParquetBlockInputFormat::VectorizedParquetBlockInputFormat(
    DB::ReadBuffer & in_, const DB::Block & header_, const DB::FormatSettings & format_settings)
    : DB::IInputFormat(header_, &in_), recordReader_(getPort().getHeader(), format_settings)
{
}

void VectorizedParquetBlockInputFormat::resetParser()
{
    IInputFormat::resetParser();
    recordReader_.reset();
    block_missing_values.clear();
}

const DB::BlockMissingValues & VectorizedParquetBlockInputFormat::getMissingValues() const
{
    return block_missing_values;
}

DB::Chunk VectorizedParquetBlockInputFormat::generate()
{
    block_missing_values.clear();

    if (is_stopped)
        return {};

    if (!recordReader_.initialized())
    {
        auto arrow_file = DB::asArrowFile(*in, recordReader_.format_settings_, is_stopped, "Parquet", PARQUET_MAGIC_BYTES);
        if (is_stopped)
            return {};
        recordReader_.initialize(getPort().getHeader(), arrow_file);
    }
    return recordReader_.nextBatch();
}
}
#endif
