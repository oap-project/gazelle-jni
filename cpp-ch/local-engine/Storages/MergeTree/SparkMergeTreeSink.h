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

#include <Processors/ISimpleTransform.h>
#include <Processors/Sinks/SinkToStorage.h>
#include <Storages/MergeTree/SparkMergeTreeMeta.h>
#include <Storages/MergeTree/SparkMergeTreeWriteSettings.h>
#include <Storages/MergeTree/SparkStorageMergeTree.h>
#include <Storages/Output/NormalFileWriter.h>
#include <Common/BlockTypeUtils.h>

namespace local_engine
{

struct MergeTreeTable;
using SparkStorageMergeTreePtr = std::shared_ptr<SparkStorageMergeTree>;
class SinkHelper;
using SinkHelperPtr = std::shared_ptr<SinkHelper>;

// TODO: Remove ConcurrentDeque
template <typename T>
class ConcurrentDeque
{
public:
    std::optional<T> pop_front()
    {
        std::lock_guard<std::mutex> lock(mtx);

        if (deq.empty())
            return {};

        T t = deq.front();
        deq.pop_front();
        return t;
    }

    void emplace_back(T value)
    {
        std::lock_guard<std::mutex> lock(mtx);
        deq.emplace_back(value);
    }

    void emplace_back(const std::vector<T> & values)
    {
        std::lock_guard<std::mutex> lock(mtx);
        deq.insert(deq.end(), values.begin(), values.end());
    }

    void emplace_front(T value)
    {
        std::lock_guard<std::mutex> lock(mtx);
        deq.emplace_front(value);
    }

    size_t size()
    {
        std::lock_guard<std::mutex> lock(mtx);
        return deq.size();
    }

    bool empty()
    {
        std::lock_guard<std::mutex> lock(mtx);
        return deq.empty();
    }

    /// !!! unsafe get, only called when background tasks are finished
    const std::deque<T> & unsafeGet() const { return deq; }

private:
    std::deque<T> deq;
    mutable std::mutex mtx;
};

//
struct PartWithStats
{
    DB::MergeTreeDataPartPtr data_part;
    std::shared_ptr<DeltaStats> delta_stats; // maybe null
};
//
class SinkHelper
{
protected:
    SparkStorageMergeTreePtr data;
    bool isRemoteStorage;

    ConcurrentDeque<PartWithStats> new_parts;
    std::unordered_set<String> tmp_parts{};
    ThreadPool thread_pool;

public:
    const SparkMergeTreeWriteSettings write_settings;
    const DB::StorageMetadataPtr metadata_snapshot;

protected:
    virtual SparkStorageMergeTree & dest_storage() { return *data; }

    void doMergePartsAsync(const std::vector<PartWithStats> & merge_parts_with_stats);
    void finalizeMerge();
    virtual void cleanup() { }
    virtual void commit(const DB::ReadSettings & read_settings, const DB::WriteSettings & write_settings) { }
    void saveMetadata(const DB::ContextPtr & context);
    SparkWriteStorageMergeTree & dataRef() const { return assert_cast<SparkWriteStorageMergeTree &>(*data); }

public:
    const std::deque<PartWithStats> & unsafeGet() const { return new_parts.unsafeGet(); }
    void
    writeTempPart(DB::BlockWithPartition & block_with_partition, PartWithStats part_with_stats, const DB::ContextPtr & context, int part_num);
    void checkAndMerge(bool force = false);
    void finish(const DB::ContextPtr & context);

    virtual ~SinkHelper() = default;
    SinkHelper(const SparkStorageMergeTreePtr & data_, const SparkMergeTreeWriteSettings & write_settings_, bool isRemoteStorage_);
};

class DirectSinkHelper : public SinkHelper
{
protected:
    void cleanup() override;

public:
    explicit DirectSinkHelper(
        const SparkStorageMergeTreePtr & data_, const SparkMergeTreeWriteSettings & write_settings_, bool isRemoteStorage_)
        : SinkHelper(data_, write_settings_, isRemoteStorage_)
    {
    }
};

class CopyToRemoteSinkHelper : public SinkHelper
{
    SparkStorageMergeTreePtr dest;

protected:
    void commit(const DB::ReadSettings & read_settings, const DB::WriteSettings & write_settings) override;
    SparkStorageMergeTree & dest_storage() override { return *dest; }
    const SparkStorageMergeTreePtr & temp_storage() const { return data; }

public:
    explicit CopyToRemoteSinkHelper(
        const SparkStorageMergeTreePtr & temp, const SparkStorageMergeTreePtr & dest_, const SparkMergeTreeWriteSettings & write_settings_)
        : SinkHelper(temp, write_settings_, true), dest(dest_)
    {
        assert(data != dest);
    }
};

class MergeTreeStats : public WriteStatsBase
{
    DB::MutableColumns columns_;

    enum ColumnIndex
    {
        part_name,
        partition_id,
        record_count,
        marks_count,
        size_in_bytes,
        stats_column_start = size_in_bytes + 1
    };

    static DB::ColumnsWithTypeAndName statsHeaderBase()
    {
        return {
            {STRING(), "part_name"},
            {STRING(), "partition_id"},
            {BIGINT(), "record_count"},
            {BIGINT(), "marks_count"},
            {BIGINT(), "size_in_bytes"}};
    }

protected:
    DB::Chunk final_result() override
    {
        size_t rows = columns_[part_name]->size();
        return DB::Chunk(std::move(columns_), rows);
    }

public:
    explicit MergeTreeStats(const DB::Block & input, const DB::Block & output)
        : WriteStatsBase(input, output), columns_(output.cloneEmptyColumns())
    {
    }
    static std::shared_ptr<MergeTreeStats> create(const DB::Block & input, const DB::Names & partition)
    {
        return std::make_shared<MergeTreeStats>(input, DeltaStats::statsHeader(input, partition, statsHeaderBase()));
    }
    String getName() const override { return "MergeTreeStats"; }

    void collectStats(const std::deque<PartWithStats> & parts, const std::string & partition_dir) const
    {
        if (parts.empty())
            return;
        const std::string & partition = partition_dir.empty() ? WriteStatsBase::NO_PARTITION_ID : partition_dir;
        size_t columnSize = parts[0].delta_stats->min.size();
        assert(columns_.size() == stats_column_start + columnSize * 3);
        assert(std::ranges::all_of(
            parts,
            [&](const auto & part)
            {
                return part.delta_stats->min.size() == columnSize && part.delta_stats->max.size() == columnSize
                    && part.delta_stats->null_count.size() == columnSize;
            }));

        const size_t size = parts.size() + columns_[part_name]->size();
        columns_[part_name]->reserve(size);
        columns_[partition_id]->reserve(size);

        columns_[record_count]->reserve(size);
        auto & countColData = static_cast<DB::ColumnVector<Int64> &>(*columns_[record_count]).getData();

        columns_[marks_count]->reserve(size);
        auto & marksColData = static_cast<DB::ColumnVector<Int64> &>(*columns_[marks_count]).getData();

        columns_[size_in_bytes]->reserve(size);
        auto & bytesColData = static_cast<DB::ColumnVector<Int64> &>(*columns_[size_in_bytes]).getData();

        for (const auto & part : parts)
        {
            std::string part_name_without_partition
                = partition_dir.empty() ? part.data_part->name : part.data_part->name.substr(partition_dir.size() + 1);
            columns_[part_name]->insertData(part_name_without_partition.c_str(), part_name_without_partition.size());
            columns_[partition_id]->insertData(partition.c_str(), partition.size());

            countColData.emplace_back(part.data_part->rows_count);
            marksColData.emplace_back(part.data_part->getMarksCount());
            bytesColData.emplace_back(part.data_part->getBytesOnDisk());

            for (int i = 0; i < columnSize; ++i)
            {
                size_t offset = stats_column_start + i;
                columns_[offset]->insert(part.delta_stats->min[i]);
                columns_[columnSize + offset]->insert(part.delta_stats->max[i]);
                auto & nullCountData = static_cast<DB::ColumnVector<Int64> &>(*columns_[(columnSize * 2) + offset]).getData();
                nullCountData.emplace_back(part.delta_stats->null_count[i]);
            }
        }
    }
};

class SparkMergeTreeSink : public DB::SinkToStorage
{
public:
    using SinkStatsOption = std::optional<std::shared_ptr<MergeTreeStats>>;
    using DeltaStatsOption = std::optional<DeltaStats>;
    static DB::SinkToStoragePtr create(
        const MergeTreeTable & merge_tree_table,
        const SparkMergeTreeWriteSettings & write_settings_,
        const DB::ContextMutablePtr & context,
        const DeltaStatsOption & delta_stats = {},
        const SinkStatsOption & stats = {});

    explicit SparkMergeTreeSink(
        const SinkHelperPtr & sink_helper_,
        const DB::ContextPtr & context_,
        const DeltaStatsOption & delta_stats,
        const SinkStatsOption & stats,
        size_t min_block_size_rows,
        size_t min_block_size_bytes)
        : SinkToStorage(sink_helper_->metadata_snapshot->getSampleBlock())
        , context(context_)
        , sink_helper(sink_helper_)
        , stats_(stats)
        , squashing(sink_helper_->metadata_snapshot->getSampleBlock(), min_block_size_rows, min_block_size_bytes)
        , empty_delta_stats_(delta_stats)
    {
    }
    ~SparkMergeTreeSink() override = default;

    String getName() const override { return "SparkMergeTreeSink"; }
    void consume(DB::Chunk & chunk) override;
    void onStart() override;
    void onFinish() override;

    const SinkHelper & sinkHelper() const { return *sink_helper; }

private:
    void write(const DB::Chunk & chunk);

    DB::ContextPtr context;
    SinkHelperPtr sink_helper;
    std::optional<std::shared_ptr<MergeTreeStats>> stats_;
    DB::Squashing squashing;
    DB::Chunk squashed_chunk;
    DeltaStatsOption empty_delta_stats_;
    int part_num = 1;
};


class SparkMergeTreePartitionedFileSink final : public SparkPartitionedBaseSink
{
    const SparkMergeTreeWriteSettings write_settings_;
    MergeTreeTable table;

public:
    SparkMergeTreePartitionedFileSink(
        const DB::Block & input_header,
        const DB::Names & partition_by,
        const MergeTreeTable & merge_tree_table,
        const SparkMergeTreeWriteSettings & write_settings,
        const DB::ContextPtr & context,
        const std::shared_ptr<WriteStatsBase> & stats)
        : SparkPartitionedBaseSink(context, partition_by, input_header, stats), write_settings_(write_settings), table(merge_tree_table)
    {
    }

    DB::SinkPtr createSinkForPartition(const String & partition_id) override
    {
        SparkMergeTreeWriteSettings write_settings{write_settings_};

        assert(write_settings.partition_settings.partition_dir.empty());
        assert(write_settings.partition_settings.bucket_dir.empty());
        write_settings.partition_settings.part_name_prefix = fmt::format(
            "{}/{}{}", partition_id, toString(DB::UUIDHelpers::generateV4()), write_settings.partition_settings.part_name_prefix);
        write_settings.partition_settings.partition_dir = partition_id;

        return SparkMergeTreeSink::create(
            table, write_settings, context_->getQueryContext(), empty_delta_stats_, {std::dynamic_pointer_cast<MergeTreeStats>(stats_)});
    }

    // TODO implement with bucket
    DB::SinkPtr createSinkForPartition(const String & partition_id, const String & bucket) override
    {
        return createSinkForPartition(partition_id);
    }
};

}
