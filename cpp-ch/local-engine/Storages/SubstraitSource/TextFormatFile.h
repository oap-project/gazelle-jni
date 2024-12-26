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

#include "config.h"


#if USE_HIVE
#include <memory>
#include <Storages/SubstraitSource/FormatFile.h>

namespace local_engine
{
class TextFormatFile : public FormatFile
{
public:
    explicit TextFormatFile(
        DB::ContextPtr context_, const substrait::ReadRel::LocalFiles::FileOrFiles & file_info_, ReadBufferBuilderPtr read_buffer_builder_);
    ~TextFormatFile() override = default;

    FormatFile::InputFormatPtr createInputFormat(const DB::Block & header) override;

    DB::NamesAndTypesList getSchema() const
    {
        const auto & schema = file_info.schema();
        auto header = TypeParser::buildBlockFromNamedStructWithoutDFS(schema);
        return header.getNamesAndTypesList();
    }

    bool supportSplit() const override { return true; }
    String getFileFormat() const override { return "HiveText"; }
};

}
#endif
