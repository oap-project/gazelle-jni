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
#include "TextFormatFile.h"

#if USE_HIVE
#include <memory>
#include <Formats/FormatFactory.h>
#include <Formats/FormatSettings.h>
#include <Processors/Formats/IRowInputFormat.h>
#include <Processors/Formats/Impl/HiveTextRowInputFormat.h>
#include <Poco/URI.h>


namespace local_engine
{

TextFormatFile::TextFormatFile(
    DB::ContextPtr context_, const substrait::ReadRel::LocalFiles::FileOrFiles & file_info_, ReadBufferBuilderPtr read_buffer_builder_)
    : FormatFile(context_, file_info_, read_buffer_builder_)
{
}

FormatFile::InputFormatPtr TextFormatFile::createInputFormat(const DB::Block & header)
{
    auto read_buffer = read_buffer_builder->buildWithCompressionWrapper(file_info);

    /// Initialize format params
    size_t max_block_size = file_info.text().max_block_size();
    DB::RowInputFormatParams params = {.max_block_size = max_block_size};

    /// Initialize format settings
    DB::FormatSettings format_settings = DB::getFormatSettings(context);
    format_settings.hive_text.input_field_names = getSchema().getNames();

    std::string text_field_delimiter = file_info.text().field_delimiter();
    format_settings.hive_text.fields_delimiter = file_info.text().field_delimiter()[0];
    format_settings.csv.empty_as_default = file_info.text().empty_as_default();
    format_settings.csv.allow_whitespace_or_tab_as_delimiter = true;
    format_settings.csv.use_default_on_bad_values = true;
    format_settings.csv.skip_trailing_empty_lines = true;
    format_settings.csv.allow_variable_number_of_columns = true;
    char quote = *file_info.text().quote().data();
    if (quote == '\'')
    {
        format_settings.csv.allow_single_quotes = true;
        format_settings.csv.allow_double_quotes = false;
    }
    else if (quote == '"')
    {
        format_settings.csv.allow_single_quotes = false;
        format_settings.csv.allow_double_quotes = true;
    }
    else
    {
        format_settings.csv.allow_single_quotes = false;
        format_settings.csv.allow_double_quotes = false;
    }
    auto input = std::make_shared<DB::HiveTextRowInputFormat>(header, *read_buffer, params, format_settings);
    return std::make_shared<InputFormat>(std::move(read_buffer), input);
}

}
#endif
