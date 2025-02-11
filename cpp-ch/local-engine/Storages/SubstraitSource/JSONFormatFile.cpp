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
#include "JSONFormatFile.h"


#include <Formats/FormatFactory.h>
#include <Formats/FormatSettings.h>
#include <Processors/Formats/Impl/JSONEachRowRowInputFormat.h>


namespace local_engine
{

JSONFormatFile::JSONFormatFile(
    DB::ContextPtr context_, const substrait::ReadRel::LocalFiles::FileOrFiles & file_info_, ReadBufferBuilderPtr read_buffer_builder_)
    : FormatFile(context_, file_info_, read_buffer_builder_)
{
}

FormatFile::InputFormatPtr JSONFormatFile::createInputFormat(const DB::Block & header)
{
    auto read_buffer = read_buffer_builder->buildWithCompressionWrapper(file_info);

    DB::FormatSettings format_settings = DB::getFormatSettings(context);
    format_settings.with_names_use_header = true;
    format_settings.skip_unknown_fields = true;
    size_t max_block_size = file_info.json().max_block_size();
    DB::RowInputFormatParams in_params = {max_block_size};
    std::shared_ptr<DB::JSONEachRowRowInputFormat> json_input_format
        = std::make_shared<DB::JSONEachRowRowInputFormat>(*read_buffer, header, in_params, format_settings, false);

    return std::make_shared<InputFormat>(std::move(read_buffer), json_input_format);
}

}
