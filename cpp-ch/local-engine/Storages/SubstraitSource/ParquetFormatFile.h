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

#if USE_PARQUET
#include <Storages/Parquet/ParquetMeta.h>
#include <Storages/SubstraitSource/FormatFile.h>

namespace local_engine
{


class ParquetFormatFile : public FormatFile
{
public:
    explicit ParquetFormatFile(
        const DB::ContextPtr & context_,
        const substrait::ReadRel::LocalFiles::FileOrFiles & file_info_,
        const ReadBufferBuilderPtr & read_buffer_builder_,
        bool use_local_format_);
    ~ParquetFormatFile() override = default;

    InputFormatPtr createInputFormat(const DB::Block & /*header*/) override
    {
        throw DB::Exception(DB::ErrorCodes::LOGICAL_ERROR, "Use createInputFormat with key_condition and column_index_filter");
    }

    InputFormatPtr createInputFormat(
        const DB::Block & header,
        const std::shared_ptr<const DB::KeyCondition> & key_condition = nullptr,
        const ColumnIndexFilterPtr & column_index_filter = nullptr) const;

    std::optional<size_t> getTotalRows() override;

    bool supportSplit() const override { return true; }

    String getFileFormat() const override { return "Parquet"; }

    static bool onlyHasFlatType(const DB::Block & header);

private:
    bool use_pageindex_reader;
    std::mutex mutex;
    std::optional<size_t> total_rows;
};

}
#endif
