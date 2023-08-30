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
#include <Columns/IColumn.h>
#include <IO/PeekableReadBuffer.h>
#include <IO/ReadBuffer.h>
#include <Processors/Formats/IRowInputFormat.h>
#include <Processors/Formats/Impl/CSVRowInputFormat.h>
#include <Storages/SubstraitSource/FormatFile.h>

namespace local_engine
{
/// Read file from excel export.

class ExcelTextFormatFile : public FormatFile
{
public:
    explicit ExcelTextFormatFile(
        DB::ContextPtr context_, const substrait::ReadRel::LocalFiles::FileOrFiles & file_info_, ReadBufferBuilderPtr read_buffer_builder_, String optimization_)
        : FormatFile(context_, file_info_, read_buffer_builder_)
    {
        this->optimization = optimization_;
    }

    ~ExcelTextFormatFile() override = default;

    FormatFile::InputFormatPtr createInputFormat(const DB::Block & header) override;

    bool supportSplit() const override { return true; }
    DB::String getFileFormat() const override { return "exceltext"; }

private:
    String optimization;
    DB::FormatSettings createFormatSettings();
};


class ExcelRowInputFormat final : public DB::CSVRowInputFormat
{
public:
    ExcelRowInputFormat(
        const DB::Block & header_,
        std::shared_ptr<DB::PeekableReadBuffer> & buf_,
        const DB::RowInputFormatParams & params_,
        const DB::FormatSettings & format_settings_,
        DB::Names & input_field_names_,
        String escape_);

    String getName() const override { return "ExcelRowInputFormat"; }

private:
    String escape;
};

class ExcelTextFormatReader final : public DB::CSVFormatReader
{
public:
    ExcelTextFormatReader(DB::PeekableReadBuffer & buf_, DB::Names & input_field_names_, const DB::FormatSettings & format_settings_);

    std::vector<String> readNames() override;
    std::vector<String> readTypes() override;
    void skipFieldDelimiter() override;
    void skipRowEndDelimiter() override;
    bool readField(DB::IColumn & column, const DB::DataTypePtr & type, const DB::SerializationPtr & serialization, bool is_last_file_column, const String & column_name) override;

private:
    void preSkipNullValue();
    bool isEndOfLine();
    static void skipEndOfLine(DB::ReadBuffer & in);
    static void skipWhitespacesAndTabs(DB::ReadBuffer & in, bool allow_whitespace_or_tab_as_delimiter);


    std::vector<String> input_field_names;
};
}
