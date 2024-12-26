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

#include <Core/Block.h>
#include <Core/Field.h>
#include <Interpreters/Context.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Processors/Chunk.h>
#include <Processors/Executors/PushingPipelineExecutor.h>
#include <Processors/ISimpleTransform.h>
#include <Processors/Sinks/SinkToStorage.h>
#include <Storages/NativeOutputWriter.h>
#include <Storages/Output/OutputFormatFile.h>
#include <Storages/PartitionedSink.h>
#include <base/types.h>
#include <Common/ArenaUtils.h>
#include <Common/BlockTypeUtils.h>
#include <Common/CHUtil.h>
#include <Common/FieldVisitorsAccurateComparison.h>

namespace local_engine
{

class NormalFileWriter : public NativeOutputWriter
{
public:
    static std::unique_ptr<NativeOutputWriter> create(
        const DB::ContextPtr & context, const std::string & file_uri, const DB::Block & preferred_schema, const std::string & format_hint);

    NormalFileWriter(const OutputFormatFilePtr & file_, const DB::ContextPtr & context_);
    ~NormalFileWriter() override = default;

    void write(DB::Block & block) override;
    void close() override;

private:
    OutputFormatFilePtr file;
    DB::ContextPtr context;

    OutputFormatFile::OutputFormatPtr output_format;
    std::unique_ptr<DB::QueryPipeline> pipeline;
    std::unique_ptr<DB::PushingPipelineExecutor> writer;
};

OutputFormatFilePtr createOutputFormatFile(
    const DB::ContextPtr & context, const std::string & file_uri, const DB::Block & preferred_schema, const std::string & format_hint);

struct DeltaStats
{
    size_t row_count;
    std::vector<DB::Field> min;
    std::vector<DB::Field> max;
    std::vector<Int64> null_count;
    std::set<size_t> partition_index;

    static DeltaStats create(const DB::Block & output, const DB::Names & partition)
    {
        size_t size = output.columns() - partition.size();
        std::set<size_t> partition_index;
        std::ranges::transform(
            partition,
            std::inserter(partition_index, partition_index.end()),
            [&](const auto & name) { return output.getPositionByName(name); });
        assert(partition_index.size() == partition.size());
        return DeltaStats(size, partition_index);
    }
    static DB::Block statsHeader(const DB::Block & output, const DB::Names & partition, DB::ColumnsWithTypeAndName && statsHeaderBase)
    {
        std::set<std::string> partition_index;
        std::ranges::transform(partition, std::inserter(partition_index, partition_index.end()), [&](const auto & name) { return name; });

        assert(partition_index.size() == partition.size());

        auto appendBase = [&](const std::string & prefix)
        {
            for (const auto & column : output.getColumnsWithTypeAndName())
                if (!partition_index.contains(column.name))
                    statsHeaderBase.emplace_back(wrapNullableType(column.type), prefix + column.name);
        };
        appendBase("min_");
        appendBase("max_");
        for (const auto & column : output.getColumnsWithTypeAndName())
            if (!partition_index.contains(column.name))
                statsHeaderBase.emplace_back(BIGINT(), "null_count_" + column.name);

        return makeBlockHeader(statsHeaderBase);
    }

    explicit DeltaStats(size_t size, const std::set<size_t> & partition_index_ = {})
        : row_count(0), min(size), max(size), null_count(size, 0), partition_index(partition_index_)
    {
        assert(size > 0);
    }

    bool initialized() const { return row_count > 0; }

    void update(const DB::Chunk & chunk)
    {
        assert(chunk.getNumRows() > 0);
        const auto & columns = chunk.getColumns();
        assert(columns.size() == min.size() + partition_index.size());
        for (size_t i = 0, col = 0; col < columns.size(); ++col)
        {
            if (partition_index.contains(col))
                continue;

            const auto & column = columns[col];
            Int64 null_count = 0;
            if (const auto * nullable_column = typeid_cast<const DB::ColumnNullable *>(column.get()))
            {
                const auto & null_map = nullable_column->getNullMapData();
                null_count = std::ranges::count_if(null_map, [](UInt8 value) { return value != 0; });
            }
            this->null_count[i] += null_count;

            DB::Field min_value, max_value;
            if (const auto * column_nullable = typeid_cast<const DB::ColumnNullable *>(column.get()))
                column_nullable->getExtremesNullLast(min_value, max_value);
            else
                column->getExtremes(min_value, max_value);

            assert(min[i].isNull() || min_value.getType() == min[i].getType());
            assert(max[i].isNull() || max_value.getType() == max[i].getType());

            if (!initialized())
            {
                min[i] = min_value;
                max[i] = max_value;
            }
            else
            {
                min[i] = applyVisitor(DB::FieldVisitorAccurateLess(), min[i], min_value) ? min[i] : min_value;
                max[i] = applyVisitor(DB::FieldVisitorAccurateLess(), max[i], max_value) ? max_value : max[i];
            }
            ++i;
        }

        row_count += chunk.getNumRows();
    }

    void merge(const DeltaStats & right)
    {
        assert(min.size() == right.min.size());
        assert(partition_index == right.partition_index);

        for (size_t i = 0; i < min.size(); ++i)
        {
            null_count[i] += right.null_count[i];
            min[i] = std::min(min[i], right.min[i]);
            max[i] = std::max(max[i], right.max[i]);
        }
    }
};

class WriteStatsBase : public DB::ISimpleTransform
{
public:
    /// visible for UTs
    static const std::string NO_PARTITION_ID;

protected:
    bool all_chunks_processed_ = false; /// flag to determine if we have already processed all chunks
    virtual DB::Chunk final_result() = 0;

public:
    WriteStatsBase(const DB::Block & input_header_, const DB::Block & output_header_)
        : ISimpleTransform(input_header_, output_header_, true)
    {
    }

    Status prepare() override
    {
        if (input.isFinished() && !output.isFinished() && !has_input && !all_chunks_processed_)
        {
            all_chunks_processed_ = true;
            /// return Ready to call transform() for generating filling rows after latest chunk was processed
            return Status::Ready;
        }

        return ISimpleTransform::prepare();
    }

    void transform(DB::Chunk & chunk) override
    {
        if (all_chunks_processed_)
            chunk = final_result();
        else
            chunk = {};
    }
};

class WriteStats : public WriteStatsBase
{
    DB::MutableColumns columns_;

    enum ColumnIndex
    {
        filename,
        partition_id,
        record_count,
        stats_column_start = record_count + 1
    };
    static DB::ColumnsWithTypeAndName statsHeaderBase()
    {
        return {{STRING(), "filename"}, {STRING(), "partition_id"}, {BIGINT(), "record_count"}};
    }

protected:
    DB::Chunk final_result() override
    {
        size_t rows = columns_[filename]->size();
        return DB::Chunk(std::move(columns_), rows);
    }

public:
    WriteStats(const DB::Block & input_header_, const DB::Block & output_header_)
        : WriteStatsBase(input_header_, output_header_), columns_(output_header_.cloneEmptyColumns())
    {
    }
    static std::shared_ptr<WriteStats> create(const DB::Block & input, const DB::Names & partition)
    {
        return std::make_shared<WriteStats>(input, DeltaStats::statsHeader(input, partition, statsHeaderBase()));
    }

    String getName() const override { return "WriteStats"; }

    void collectStats(const String & filename, const String & partition_dir, const DeltaStats & stats) const
    {
        const std::string & partition = partition_dir.empty() ? WriteStatsBase::NO_PARTITION_ID : partition_dir;
        size_t columnSize = stats.min.size();
        assert(columns_.size() == stats_column_start + columnSize * 3);
        assert(stats.min.size() == stats.max.size() && stats.min.size() == stats.null_count.size());

        columns_[ColumnIndex::filename]->insertData(filename.c_str(), filename.size());
        columns_[partition_id]->insertData(partition.c_str(), partition.size());
        auto & countColData = static_cast<DB::ColumnVector<Int64> &>(*columns_[record_count]).getData();
        countColData.emplace_back(stats.row_count);

        for (int i = 0; i < columnSize; ++i)
        {
            size_t offset = stats_column_start + i;
            columns_[offset]->insert(stats.min[i]);
            columns_[columnSize + offset]->insert(stats.max[i]);
            auto & nullCountData = static_cast<DB::ColumnVector<Int64> &>(*columns_[(columnSize * 2) + offset]).getData();
            nullCountData.emplace_back(stats.null_count[i]);
        }
    }
};

struct FileNameGenerator
{
    // Align with org.apache.spark.sql.execution.FileNamePlaceHolder
    static const std::vector<std::string> SUPPORT_PLACEHOLDERS;
    // Align with placeholders above
    const std::vector<bool> need_to_replace;
    const std::string file_pattern;

    FileNameGenerator(const std::string & file_pattern) : file_pattern(file_pattern), need_to_replace(compute_need_to_replace(file_pattern))
    {
    }

    std::vector<bool> compute_need_to_replace(const std::string & file_pattern)
    {
        std::vector<bool> result;
        for (const std::string & placeholder : SUPPORT_PLACEHOLDERS)
            if (file_pattern.find(placeholder) != std::string::npos)
                result.push_back(true);
            else
                result.push_back(false);
        return result;
    }

    std::string generate(const std::string & bucket = "") const
    {
        std::string result = file_pattern;
        if (need_to_replace[0]) // {id}
            result = pattern_format(SUPPORT_PLACEHOLDERS[0], toString(DB::UUIDHelpers::generateV4()));
        if (need_to_replace[1]) // {bucket}
            result = pattern_format(SUPPORT_PLACEHOLDERS[1], bucket);
        return result;
    }

    std::string pattern_format(const std::string & arg, const std::string & replacement) const
    {
        std::string format_str = file_pattern;
        size_t pos = format_str.find(arg);
        while (pos != std::string::npos)
        {
            format_str.replace(pos, arg.length(), replacement);
            pos = format_str.find(arg, pos + arg.length());
        }
        return format_str;
    }
};

class SubstraitFileSink final : public DB::SinkToStorage
{
    const std::string partition_id_;
    const bool bucketed_write_;
    const std::string relative_path_;
    OutputFormatFilePtr format_file_;
    OutputFormatFile::OutputFormatPtr output_format_;
    std::shared_ptr<WriteStats> stats_;
    DeltaStats delta_stats_;

    static std::string makeAbsoluteFilename(const std::string & base_path, const std::string & partition_id, const std::string & relative)
    {
        if (partition_id.empty())
            return fmt::format("{}/{}", base_path, relative);
        return fmt::format("{}/{}/{}", base_path, partition_id, relative);
    }

public:
    explicit SubstraitFileSink(
        const DB::ContextPtr & context,
        const std::string & base_path,
        const std::string & partition_id,
        const bool bucketed_write,
        const std::string & relative,
        const std::string & format_hint,
        const DB::Block & header,
        const std::shared_ptr<WriteStatsBase> & stats,
        const DeltaStats & delta_stats)
        : SinkToStorage(header)
        , partition_id_(partition_id)
        , bucketed_write_(bucketed_write)
        , relative_path_(relative)
        , format_file_(createOutputFormatFile(context, makeAbsoluteFilename(base_path, partition_id, relative), header, format_hint))
        , stats_(std::dynamic_pointer_cast<WriteStats>(stats))
        , delta_stats_(delta_stats)
    {
    }

    String getName() const override { return "SubstraitFileSink"; }

protected:
    void consume(DB::Chunk & chunk) override
    {
        delta_stats_.update(chunk);
        if (!output_format_) [[unlikely]]
            output_format_ = format_file_->createOutputFormat();

        const DB::Block & input_header = getHeader();
        if (bucketed_write_)
        {
            chunk.erase(input_header.columns() - 1);
            const DB::ColumnsWithTypeAndName & cols = input_header.getColumnsWithTypeAndName();
            DB::ColumnsWithTypeAndName without_bucket_cols(cols.begin(), cols.end() - 1);
            DB::Block without_bucket_header = DB::Block(without_bucket_cols);
            output_format_->output->write(materializeBlock(without_bucket_header.cloneWithColumns(chunk.detachColumns())));
        }
        else
            output_format_->output->write(materializeBlock(input_header.cloneWithColumns(chunk.detachColumns())));
    }
    void onFinish() override
    {
        if (output_format_) [[unlikely]]
        {
            output_format_->finalizeOutput();
            assert(delta_stats_.row_count > 0);
            if (stats_)
                stats_->collectStats(relative_path_, partition_id_, delta_stats_);
        }
    }
};

class SparkPartitionedBaseSink : public DB::PartitionedSink
{
public:
    static const std::string DEFAULT_PARTITION_NAME;
    static const std::string BUCKET_COLUMN_NAME;

    static bool isBucketedWrite(const DB::Block & input_header)
    {
        return input_header.has(BUCKET_COLUMN_NAME) && input_header.getPositionByName(BUCKET_COLUMN_NAME) == input_header.columns() - 1;
    }

    /// visible for UTs
    static DB::ASTPtr make_partition_expression(const DB::Names & partition_columns, const DB::Block & input_header)
    {
        /// Parse the following expression into ASTs
        /// cancat('/col_name=', 'toString(col_name)')
        bool add_slash = false;
        DB::ASTs arguments;
        for (const auto & column : partition_columns)
        {
            // partition_column=
            std::string key = add_slash ? fmt::format("/{}=", column) : fmt::format("{}=", column);
            add_slash = true;
            arguments.emplace_back(std::make_shared<DB::ASTLiteral>(key));

            // ifNull(toString(partition_column), DEFAULT_PARTITION_NAME)
            // FIXME if toString(partition_column) is empty
            auto column_ast = std::make_shared<DB::ASTIdentifier>(column);
            DB::ASTs if_null_args{
                makeASTFunction("toString", DB::ASTs{column_ast}), std::make_shared<DB::ASTLiteral>(DEFAULT_PARTITION_NAME)};
            arguments.emplace_back(makeASTFunction("ifNull", std::move(if_null_args)));
        }
        if (isBucketedWrite(input_header))
        {
            DB::ASTs args{std::make_shared<DB::ASTLiteral>("%05d"), std::make_shared<DB::ASTIdentifier>(BUCKET_COLUMN_NAME)};
            arguments.emplace_back(DB::makeASTFunction("printf", std::move(args)));
        }
        assert(!arguments.empty());
        if (arguments.size() == 1)
            return arguments[0];
        return DB::makeASTFunction("concat", std::move(arguments));
    }

    DB::SinkPtr createSinkForPartition(const String & partition_id) override
    {
        if (bucketed_write_)
        {
            std::string bucket_val = partition_id.substr(partition_id.length() - 5, 5);
            std::string real_partition_id = partition_id.substr(0, partition_id.length() - 5);
            return createSinkForPartition(real_partition_id, bucket_val);
        }
        return createSinkForPartition(partition_id, "");
    }

    virtual DB::SinkPtr createSinkForPartition(const String & partition_id, const String & bucket) = 0;

protected:
    DB::ContextPtr context_;
    std::shared_ptr<WriteStatsBase> stats_;
    DeltaStats empty_delta_stats_;
    bool bucketed_write_;

public:
    SparkPartitionedBaseSink(
        const DB::ContextPtr & context,
        const DB::Names & partition_by,
        const DB::Block & input_header,
        const std::shared_ptr<WriteStatsBase> & stats)
        : PartitionedSink(make_partition_expression(partition_by, input_header), context, input_header)
        , context_(context)
        , stats_(stats)
        , bucketed_write_(isBucketedWrite(input_header))
        , empty_delta_stats_(DeltaStats::create(input_header, partition_by))
    {
    }
};

class SubstraitPartitionedFileSink final : public SparkPartitionedBaseSink
{
    const std::string base_path_;
    const FileNameGenerator generator_;
    const DB::Block input_header_;
    const DB::Block sample_block_;
    const std::string format_hint_;

public:
    SubstraitPartitionedFileSink(
        const DB::ContextPtr & context,
        const DB::Names & partition_by,
        const DB::Block & input_header,
        const DB::Block & sample_block,
        const std::string & base_path,
        const FileNameGenerator & generator,
        const std::string & format_hint,
        const std::shared_ptr<WriteStatsBase> & stats)
        : SparkPartitionedBaseSink(context, partition_by, input_header, stats)
        , base_path_(base_path)
        , generator_(generator)
        , sample_block_(sample_block)
        , input_header_(input_header)
        , format_hint_(format_hint)
    {
    }

    DB::SinkPtr createSinkForPartition(const String & partition_id, const String & bucket) override
    {
        assert(stats_);
        bool bucketed_write = !bucket.empty();
        std::string filename = bucketed_write ? generator_.generate(bucket) : generator_.generate();
        const auto partition_path = fmt::format("{}/{}", partition_id, filename);
        validatePartitionKey(partition_path, true);
        return std::make_shared<SubstraitFileSink>(
            context_, base_path_, partition_id, bucketed_write, filename, format_hint_, sample_block_, stats_, empty_delta_stats_);
    }
    String getName() const override { return "SubstraitPartitionedFileSink"; }
};
}
