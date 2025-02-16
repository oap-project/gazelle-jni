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

#include <Columns/IColumn.h>
#include <Core/Block.h>
#include <Core/Field.h>
#include <Processors/Chunk.h>
#include <Storages/SubstraitSource/FormatFile.h>
#include <base/types.h>

namespace local_engine
{
class ColumnIndexFilter;
using ColumnIndexFilterPtr = std::shared_ptr<ColumnIndexFilter>;

class BaseReader
{
public:
    explicit BaseReader(const FormatFilePtr & file_, const DB::Block & to_read_header_, const DB::Block & header_)
        : file(file_), readHeader(to_read_header_), outputHeader(header_)
    {
    }
    virtual ~BaseReader() = default;

    void cancel()
    {
        bool already_cancelled = is_cancelled.exchange(true, std::memory_order_acq_rel);
        if (!already_cancelled)
            onCancel();
    }

    virtual bool pull(DB::Chunk & chunk) = 0;
    bool isCancelled() const { return is_cancelled.load(std::memory_order_acquire); }

protected:
    virtual void onCancel() { };

    DB::Columns addVirtualColumn(DB::Chunk dataChunk, size_t rowNum = 0) const;

    FormatFilePtr file;
    DB::Block readHeader;
    DB::Block outputHeader;

    std::atomic<bool> is_cancelled{false};


    static DB::ColumnPtr createConstColumn(DB::DataTypePtr type, const DB::Field & field, size_t rows);
    static DB::ColumnPtr createPartitionColumn(const String & value, const DB::DataTypePtr & type, size_t rows);
    static DB::Field buildFieldFromString(const String & value, DB::DataTypePtr type);
};

class NormalFileReader : public BaseReader
{
public:
    static std::unique_ptr<NormalFileReader> create(
        const FormatFilePtr & file,
        const DB::Block & to_read_header_,
        const DB::Block & output_header_,
        const std::shared_ptr<const DB::KeyCondition> & key_condition = nullptr,
        const ColumnIndexFilterPtr & column_index_filter = nullptr);
    NormalFileReader(
        const FormatFilePtr & file_,
        const DB::Block & to_read_header_,
        const DB::Block & output_header_,
        const FormatFile::InputFormatPtr & input_format_);
    ~NormalFileReader() override = default;

    bool pull(DB::Chunk & chunk) override;

private:
    void onCancel() override { input_format->cancel(); }
    FormatFile::InputFormatPtr input_format;
};

class ConstColumnsFileReader : public BaseReader
{
public:
    ConstColumnsFileReader(const FormatFilePtr & file_, const DB::Block & header_, size_t blockSize = DB::DEFAULT_BLOCK_SIZE);
    ~ConstColumnsFileReader() override = default;

    bool pull(DB::Chunk & chunk) override;

private:
    size_t remained_rows;
    const size_t block_size;
};

class SubstraitFileSource : public DB::SourceWithKeyCondition
{
public:
    SubstraitFileSource(const DB::ContextPtr & context_, const DB::Block & header_, const substrait::ReadRel::LocalFiles & file_infos);
    ~SubstraitFileSource() override = default;

    String getName() const override { return "SubstraitFileSource"; }

    void setKeyCondition(const std::optional<DB::ActionsDAG> & filter_actions_dag, DB::ContextPtr context_) override;

protected:
    DB::Chunk generate() override;

private:
    bool tryPrepareReader();
    void onCancel() noexcept override;
    FormatFiles files;

    DB::Block outputHeader; /// Sample header may contain partitions columns and file meta-columns
    DB::Block readHeader; /// Sample header doesn't include partition columns and file meta-columns

    UInt32 current_file_index = 0;

    std::unique_ptr<BaseReader> file_reader;
    ColumnIndexFilterPtr column_index_filter;
};
}
