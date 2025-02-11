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

#include <memory>
#include <optional>
#include <vector>
#include <Core/Block.h>
#include <IO/ReadBuffer.h>
#include <Interpreters/Context.h>
#include <Parser/TypeParser.h>
#include <Processors/Formats/IInputFormat.h>
#include <Storages/SubstraitSource/ReadBufferBuilder.h>
#include <substrait/plan.pb.h>

namespace DB
{
namespace ErrorCodes
{
extern const int NOT_IMPLEMENTED;
}
}

namespace local_engine
{

class FormatFile;
using substraitInputFile = substrait::ReadRel::LocalFiles::FileOrFiles;

class FileMetaColumns
{
public:
    inline static const std::string FILE_PATH = "file_path";
    inline static const std::string FILE_NAME = "file_name";
    inline static const std::string FILE_BLOCK_START = "file_block_start";
    inline static const std::string FILE_BLOCK_LENGTH = "file_block_length";
    inline static const std::string FILE_SIZE = "file_size";
    inline static const std::string FILE_MODIFICATION_TIME = "file_modification_time";

    inline static const std::string METADATA_NAME = "_metadata";

    static inline const std::string INPUT_FILE_NAME = "input_file_name";
    static inline const std::string INPUT_FILE_BLOCK_START = "input_file_block_start";
    static inline const std::string INPUT_FILE_BLOCK_LENGTH = "input_file_block_length";

    static inline std::unordered_set INPUT_FILE_COLUMNS_SET = {INPUT_FILE_NAME, INPUT_FILE_BLOCK_START, INPUT_FILE_BLOCK_LENGTH};
    static inline std::unordered_set METADATA_COLUMNS_SET
        = {FILE_PATH, FILE_NAME, FILE_BLOCK_START, FILE_BLOCK_LENGTH, FILE_SIZE, FILE_MODIFICATION_TIME};

    /// Caution: only used in InputFileNameParser
    static bool isVirtualColumn(const std::string & column_name)
    {
        return METADATA_COLUMNS_SET.contains(column_name) || INPUT_FILE_COLUMNS_SET.contains(column_name);
    }
    static bool hasVirtualColumns(const DB::Block & block)
    {
        return std::ranges::any_of(block, [](const auto & column) { return isVirtualColumn(column.name); });
    }

    static DB::Block removeVirtualColumns(const DB::Block & block)
    {
        DB::ColumnsWithTypeAndName result_columns;
        std::ranges::copy_if(
            block.getColumnsWithTypeAndName(),
            std::back_inserter(result_columns),
            [](const auto & column) { return !isVirtualColumn(column.name); });
        return result_columns;
    }
    ///

    explicit FileMetaColumns(const substraitInputFile & file);
    DB::ColumnPtr createMetaColumn(const String & columnName, const DB::DataTypePtr & type, size_t rows) const;

    bool virtualColumn(const std::string & column_name) const { return metadata_columns_map.contains(column_name); }

protected:
    static std::map<std::string, std::function<DB::Field(const std::string &)>> BASE_METADATA_EXTRACTORS;

    /// InputFileName, InputFileBlockStart and InputFileBlockLength,
    static std::map<std::string, std::function<DB::Field(const substraitInputFile &)>> INPUT_FUNCTION_EXTRACTORS;

    std::unordered_map<String, DB::Field> metadata_columns_map;
};

class FormatFile
{
public:
    class InputFormat
    {
    protected:
        std::unique_ptr<DB::ReadBuffer> read_buffer;
        DB::InputFormatPtr input;

    public:
        virtual ~InputFormat() = default;
        DB::IInputFormat & inputFormat() const { return *input; }
        void cancel() const noexcept { return input->cancel(); }
        virtual DB::Chunk generate() { return input->generate(); }
        InputFormat(std::unique_ptr<DB::ReadBuffer> read_buffer_, const DB::InputFormatPtr & input_)
            : read_buffer(std::move(read_buffer_)), input(input_)
        {
        }
    };
    using InputFormatPtr = std::shared_ptr<InputFormat>;

    FormatFile(DB::ContextPtr context_, const substraitInputFile & file_info_, const ReadBufferBuilderPtr & read_buffer_builder_);
    virtual ~FormatFile() = default;

    /// Create a new input format for reading this file
    virtual InputFormatPtr createInputFormat(const DB::Block & header) = 0;

    /// Spark would split a large file into small segements and read in different tasks
    /// If this file doesn't support the split feacture, only the task with offset 0 will generate data.
    virtual bool supportSplit() const { return false; }

    /// Try to get rows from file metadata
    virtual std::optional<size_t> getTotalRows() { return {}; }

    virtual String getFileFormat() const = 0;

    /// Get partition keys from a file path
    const std::map<String, String> & getFilePartitionValues() const { return partition_values; }
    const std::map<String, String> & getFileNormalizedPartitionValues() const { return normalized_partition_values; }

    String getURIPath() const { return file_info.uri_file(); }
    size_t getStartOffset() const { return file_info.start(); }
    const FileMetaColumns & fileMetaColumns() const { return meta_columns; }

protected:
    DB::ContextPtr context;
    const substraitInputFile file_info;
    ReadBufferBuilderPtr read_buffer_builder;
    std::map<String, String> partition_values;
    /// partition keys are normalized to lower cases for partition column case-insensitive matching
    std::map<String, String> normalized_partition_values;
    std::shared_ptr<const DB::KeyCondition> key_condition;
    const FileMetaColumns meta_columns;
};

using FormatFilePtr = std::shared_ptr<FormatFile>;
using FormatFiles = std::vector<FormatFilePtr>;

class FormatFileUtil
{
public:
    static FormatFilePtr
    createFile(DB::ContextPtr context, ReadBufferBuilderPtr read_buffer_builder, const substrait::ReadRel::LocalFiles::FileOrFiles & file);
};
}
