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
#include <functional>
#include <memory>

#include <Columns/ColumnNullable.h>
#include <Columns/ColumnTuple.h>
#include <Columns/ColumnsNumber.h>
#include <DataTypes/DataTypeDecimalBase.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypesDecimal.h>
#include <IO/ReadBufferFromString.h>
#include <IO/ReadHelpers.h>
#include <QueryPipeline/Pipe.h>
#include <Storages/Parquet/ColumnIndexFilter.h>
#include <Storages/SubstraitSource/FormatFile.h>
#include <Storages/SubstraitSource/ParquetFormatFile.h>
#include <Storages/SubstraitSource/SubstraitFileSource.h>
#include <boost/algorithm/string/case_conv.hpp>
#include <substrait/plan.pb.h>
#include <Poco/URI.h>
#include <Common/CHUtil.h>
#include <Common/Exception.h>
#include <Common/GlutenStringUtils.h>
#include <Common/typeid_cast.h>

namespace DB
{
namespace ErrorCodes
{
extern const int UNKNOWN_TYPE;
extern const int LOGICAL_ERROR;
}
}

namespace local_engine
{

/// When run query "select count(*) from t", there is no any column to be read. The only necessary information is the number of rows.
/// To handle these cases, we build blocks with a const virtual column to indicate how many rows are in it.
static DB::Block getRealHeader(const DB::Block & header)
{
    return header ? header : BlockUtil::buildRowCountHeader();
}

static std::vector<FormatFilePtr> initializeFiles(const substrait::ReadRel::LocalFiles & file_infos, const DB::ContextPtr & context)
{
    if (file_infos.items().empty())
        return {};
    std::vector<FormatFilePtr> files;
    const Poco::URI file_uri(file_infos.items().Get(0).uri_file());
    ReadBufferBuilderPtr read_buffer_builder = ReadBufferBuilderFactory::instance().createBuilder(file_uri.getScheme(), context);
    for (const auto & item : file_infos.items())
        files.emplace_back(FormatFileUtil::createFile(context, read_buffer_builder, item));
    return files;
}

static DB::Block initReadHeader(const DB::Block & block, const FormatFiles & files)
{
    if (files.empty())
        return block;
    const auto & partitions = files[0]->getFilePartitionValues();
    const auto & fileMetaColumns = files[0]->fileMetaColumns();
    DB::ColumnsWithTypeAndName result_columns;
    std::ranges::copy_if(
        block.getColumnsWithTypeAndName(),
        std::back_inserter(result_columns),
        [&partitions, &fileMetaColumns](const auto & column)
        { return !partitions.contains(column.name) && !fileMetaColumns.virtualColumn(column.name); });
    return result_columns;
}

SubstraitFileSource::SubstraitFileSource(
    const DB::ContextPtr & context_, const DB::Block & outputHeader_, const substrait::ReadRel::LocalFiles & file_infos)
    : DB::SourceWithKeyCondition(getRealHeader(outputHeader_), false)
    , files(initializeFiles(file_infos, context_))
    , outputHeader(outputHeader_)
    , readHeader(initReadHeader(outputHeader, files))
{
}

void SubstraitFileSource::setKeyCondition(const std::optional<DB::ActionsDAG> & filter_actions_dag, DB::ContextPtr context_)
{
    setKeyConditionImpl(filter_actions_dag, context_, readHeader);
    if (filter_actions_dag)
        column_index_filter = std::make_shared<ColumnIndexFilter>(filter_actions_dag.value(), context_);
}

DB::Chunk SubstraitFileSource::generate()
{
    while (true)
    {
        if (!tryPrepareReader())
        {
            /// all files finished
            return {};
        }

        DB::Chunk chunk;
        if (file_reader->pull(chunk))
            return chunk;

        /// try to read from next file
        file_reader.reset();
    }
}

bool SubstraitFileSource::tryPrepareReader()
{
    if (isCancelled())
        return false;

    if (file_reader)
        return true;

    while (current_file_index < files.size())
    {
        auto current_file = files[current_file_index];
        current_file_index += 1;
        /// For the files do not support split strategy, the task with not 0 offset will generate empty data
        if (!current_file->supportSplit() && current_file->getStartOffset())
            continue;

        if (!readHeader)
        {
            if (auto totalRows = current_file->getTotalRows())
                file_reader = std::make_unique<ConstColumnsFileReader>(current_file, outputHeader, *totalRows);
            else
            {
                /// If we can't get total rows from file metadata (i.e. text/json format file), adding a dummy column to
                /// indicate the number of rows.
                file_reader = NormalFileReader::create(current_file, getRealHeader(readHeader), getRealHeader(outputHeader));
            }
        }
        else
            file_reader = NormalFileReader::create(current_file, readHeader, outputHeader, key_condition, column_index_filter);
        if (file_reader)
            return true;
    }
    return false;
}


void SubstraitFileSource::onCancel() noexcept
{
    if (file_reader)
        file_reader->cancel();
}

DB::Columns BaseReader::addVirtualColumn(DB::Chunk dataChunk, size_t rowNum) const
{
    // dataChunk may be empty
    const size_t rows = dataChunk.empty() ? rowNum : dataChunk.getNumRows();
    assert(rows && "read 0 rows from file");

    auto read_columns = dataChunk.detachColumns();
    const auto & columns = outputHeader.getColumnsWithTypeAndName();
    const auto & normalized_partition_values = file->getFileNormalizedPartitionValues();

    DB::Columns res_columns;
    res_columns.reserve(columns.size());
    std::ranges::transform(
        columns,
        std::back_inserter(res_columns),
        [&](const auto & column) -> DB::ColumnPtr
        {
            if (readHeader.has(column.name))
                return read_columns[readHeader.getPositionByName(column.name)];
            if (auto it = normalized_partition_values.find(boost::to_lower_copy(column.name)); it != normalized_partition_values.end())
                return createPartitionColumn(it->second, column.type, rows);
            if (file->fileMetaColumns().virtualColumn(column.name))
                return file->fileMetaColumns().createMetaColumn(column.name, column.type, rows);
            throw DB::Exception(
                DB::ErrorCodes::LOGICAL_ERROR, "Not found column = {} when reading file: {}.", column.name, file->getURIPath());
        });
    return res_columns;
}

DB::ColumnPtr BaseReader::createConstColumn(DB::DataTypePtr data_type, const DB::Field & field, size_t rows)
{
    auto nested_type = DB::removeNullable(data_type);
    auto column = nested_type->createColumnConst(rows, field);

    if (data_type->isNullable())
        column = DB::ColumnNullable::create(column, DB::ColumnUInt8::create(rows, 0));
    return column;
}

DB::ColumnPtr BaseReader::createPartitionColumn(const String & value, const DB::DataTypePtr & type, size_t rows)
{
    if (GlutenStringUtils::isNullPartitionValue(value))
    {
        if (!type->isNullable())
            throw DB::Exception(DB::ErrorCodes::LOGICAL_ERROR, "Partition column is null value,but column data type is not nullable.");
        auto nested_type = static_cast<const DB::DataTypeNullable &>(*type).getNestedType();
        auto column = nested_type->createColumnConstWithDefaultValue(rows);
        return DB::ColumnNullable::create(column, DB::ColumnUInt8::create(rows, 1));
    }
    else
    {
        auto field = buildFieldFromString(value, type);
        return createConstColumn(type, field, rows);
    }
}

#define BUILD_INT_FIELD(type) \
    [](DB::ReadBuffer & in, const String &) \
    { \
        type value = 0; \
        DB::readIntText(value, in); \
        return DB::Field(value); \
    }

#define BUILD_FP_FIELD(type) \
    [](DB::ReadBuffer & in, const String &) \
    { \
        type value = 0.0; \
        DB::readFloatText(value, in); \
        return DB::Field(value); \
    }

DB::Field BaseReader::buildFieldFromString(const String & str_value, DB::DataTypePtr type)
{
    using FieldBuilder = std::function<DB::Field(DB::ReadBuffer &, const String &)>;
    static std::map<std::string, FieldBuilder> field_builders
        = {{"Int8", BUILD_INT_FIELD(Int8)},
           {"Int16", BUILD_INT_FIELD(Int16)},
           {"Int32", BUILD_INT_FIELD(Int32)},
           {"Int64", BUILD_INT_FIELD(Int64)},
           {"Float32", BUILD_FP_FIELD(Float32)},
           {"Float64", BUILD_FP_FIELD(Float64)},
           {"String", [](DB::ReadBuffer &, const String & val) { return DB::Field(val); }},
           {"Date",
            [](DB::ReadBuffer & in, const String &)
            {
                DayNum value;
                readDateText(value, in);
                return DB::Field(value);
            }},
           {"Date32",
            [](DB::ReadBuffer & in, const String &)
            {
                ExtendedDayNum value;
                readDateText(value, in);
                return DB::Field(value.toUnderType());
            }},
           {"Bool",
            [](DB::ReadBuffer & in, const String &)
            {
                bool value;
                readBoolTextWord(value, in, true);
                return DB::Field(value);
            }},
           {"DateTime64(6)",
            [](DB::ReadBuffer &, const String & s)
            {
                std::string decoded; // s: "2023-07-12 05%3A05%3A33.798" (spark encoded it) => decoded: "2023-07-12 05:05:33.798"
                Poco::URI::decode(s, decoded);

                std::string to_read;
                if (decoded.length() > 23) // we see cases when spark mistakely? encode the URI twice, so we need to decode twice
                    Poco::URI::decode(decoded, to_read);
                else
                    to_read = decoded;

                DB::ReadBufferFromString read_buffer(to_read);
                DB::DateTime64 value;
                DB::readDateTime64Text(value, 6, read_buffer);
                return DB::Field(value);
            }}

        };

    auto nested_type = DB::removeNullable(type);
    DB::ReadBufferFromString read_buffer(str_value);
    auto it = field_builders.find(nested_type->getName());
    if (it == field_builders.end())
    {
        DB::WhichDataType which(nested_type->getTypeId());
        if (which.isDecimal32())
        {
            const auto & dataTypeDecimal = static_cast<const DB::DataTypeDecimal<DB::Decimal32> &>(*nested_type);
            DB::Decimal32 value = dataTypeDecimal.parseFromString(str_value);
            return DB::DecimalField<DB::Decimal32>(value, dataTypeDecimal.getScale());
        }
        else if (which.isDecimal64())
        {
            const auto & dataTypeDecimal = static_cast<const DB::DataTypeDecimal<DB::Decimal64> &>(*nested_type);
            DB::Decimal64 value = dataTypeDecimal.parseFromString(str_value);
            return DB::DecimalField<DB::Decimal64>(value, dataTypeDecimal.getScale());
        }
        else if (which.isDecimal128())
        {
            const auto & dataTypeDecimal = static_cast<const DB::DataTypeDecimal<DB::Decimal128> &>(*nested_type);
            DB::Decimal128 value = dataTypeDecimal.parseFromString(str_value);
            return DB::DecimalField<DB::Decimal128>(value, dataTypeDecimal.getScale());
        }
        else if (which.isDecimal256())
        {
            const auto & dataTypeDecimal = static_cast<const DB::DataTypeDecimal<DB::Decimal256> &>(*nested_type);
            DB::Decimal256 value = dataTypeDecimal.parseFromString(str_value);
            return DB::DecimalField<DB::Decimal256>(value, dataTypeDecimal.getScale());
        }

        throw DB::Exception(DB::ErrorCodes::UNKNOWN_TYPE, "Unsupported data type {}", nested_type->getName());
    }
    return it->second(read_buffer, str_value);
}

ConstColumnsFileReader::ConstColumnsFileReader(const FormatFilePtr & file_, const DB::Block & header_, size_t blockSize)
    : BaseReader(file_, {}, header_), remained_rows(file->getTotalRows().value()), block_size(blockSize)
{
}

bool ConstColumnsFileReader::pull(DB::Chunk & chunk)
{
    if (isCancelled())
        return false;

    if (!remained_rows)
        return false;

    size_t to_read_rows = 0;
    if (remained_rows < block_size)
    {
        to_read_rows = remained_rows;
        remained_rows = 0;
    }
    else
    {
        to_read_rows = block_size;
        remained_rows -= block_size;
    }

    /// If the original output header is empty, build a block to represent the row count.
    DB::Columns res_columns
        = outputHeader.columns() > 0 ? addVirtualColumn({}, to_read_rows) : BlockUtil::buildRowCountChunk(to_read_rows).detachColumns();

    chunk = DB::Chunk(std::move(res_columns), to_read_rows);
    return true;
}

std::unique_ptr<NormalFileReader> NormalFileReader::create(
    const FormatFilePtr & file,
    const DB::Block & to_read_header_,
    const DB::Block & output_header_,
    const std::shared_ptr<const DB::KeyCondition> & key_condition,
    const ColumnIndexFilterPtr & column_index_filter)
{
    FormatFile::InputFormatPtr input_format;
    if (auto * parquetFile = dynamic_cast<ParquetFormatFile *>(file.get()))
    {
        /// Apply key condition to the reader.
        /// If use_local_format is true, column_index_filter will be used  otherwise it will be ignored
        input_format = parquetFile->createInputFormat(to_read_header_, key_condition, column_index_filter);
    }
    else
    {
        input_format = file->createInputFormat(to_read_header_);
        if (key_condition)
            input_format->inputFormat().setKeyCondition(key_condition);
    }
    if (!input_format)
        return nullptr;
    return std::make_unique<NormalFileReader>(file, to_read_header_, output_header_, input_format);
}

NormalFileReader::NormalFileReader(
    const FormatFilePtr & file_,
    const DB::Block & to_read_header_,
    const DB::Block & output_header_,
    const FormatFile::InputFormatPtr & input_format_)
    : BaseReader(file_, to_read_header_, output_header_), input_format(input_format_)
{
}

bool NormalFileReader::pull(DB::Chunk & chunk)
{
    if (isCancelled())
        return false;

    /// read read real data chunk from input.
    DB::Chunk dataChunk = input_format->generate();
    const size_t rows = dataChunk.getNumRows();
    if (!rows)
        return false;
    chunk = DB::Chunk(addVirtualColumn(std::move(dataChunk)), rows);
    return true;
}

}
