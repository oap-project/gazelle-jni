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

#include "MergeTreeTool.h"


#include <Processors/Sinks/SinkToStorage.h>
#include <Storages/MergeTree/CustomStorageMergeTree.h>
#include <Common/CHUtil.h>
#include <Common/GlutenSettings.h>

namespace local_engine
{
struct MergeTreeTable;
using CustomStorageMergeTreePtr = std::shared_ptr<CustomStorageMergeTree>;

#define MERGE_TREE_WRITE_RELATED_SETTINGS(M, ALIAS, UNIQ) \
    M(String, part_name_prefix, , "The part name prefix for writing data", UNIQ) \
    M(String, partition_dir, , "The parition directory for writing data", UNIQ) \
    M(String, bucket_dir, , "The bucket directory for writing data", UNIQ)

DECLARE_GLUTEN_SETTINGS(MergeTreePartitionWriteSettings, MERGE_TREE_WRITE_RELATED_SETTINGS)

struct GlutenMergeTreeWriteSettings
{
    MergeTreePartitionWriteSettings partition_settings;
    bool merge_after_insert{true};
    bool insert_without_local_storage{false};
    size_t merge_min_size = 1024 * 1024 * 1024;
    size_t merge_limit_parts = 10;

    void load(const DB::ContextPtr & context)
    {
        const DB::Settings & settings = context->getSettingsRef();
        merge_after_insert = settings.get(MERGETREE_MERGE_AFTER_INSERT).safeGet<bool>();
        insert_without_local_storage = settings.get(MERGETREE_INSERT_WITHOUT_LOCAL_STORAGE).safeGet<bool>();

        if (Field limit_size_field; settings.tryGet("optimize.minFileSize", limit_size_field))
            merge_min_size = limit_size_field.safeGet<Int64>() <= 0 ? merge_min_size : limit_size_field.safeGet<Int64>();

        if (Field limit_cnt_field; settings.tryGet("mergetree.max_num_part_per_merge_task", limit_cnt_field))
            merge_limit_parts = limit_cnt_field.safeGet<Int64>() <= 0 ? merge_limit_parts : limit_cnt_field.safeGet<Int64>();
    }
};

class SparkMergeTreeDataWriter
{
public:
    explicit SparkMergeTreeDataWriter(MergeTreeData & data_) : data(data_), log(getLogger(data.getLogName() + " (Writer)")) { }
    MergeTreeDataWriter::TemporaryPart writeTempPart(
        DB::BlockWithPartition & block_with_partition,
        const DB::StorageMetadataPtr & metadata_snapshot,
        const ContextPtr & context,
        const MergeTreePartitionWriteSettings & write_settings,
        int part_num) const;

private:
    MergeTreeData & data;
    LoggerPtr log;
};

class SparkStorageMergeTree final : public CustomStorageMergeTree
{
public:
    SparkStorageMergeTree(const MergeTreeTable & table_, const StorageInMemoryMetadata & metadata, const ContextMutablePtr & context_)
        : CustomStorageMergeTree(
              StorageID(table_.database, table_.table),
              table_.relative_path,
              metadata,
              false,
              context_,
              "",
              MergingParams(),
              buildMergeTreeSettings(table_.table_configs),
              false /*has_force_restore_data_flag*/)
        , table(table_)
        , writer(*this)
    {
    }

    SinkToStoragePtr
    write(const ASTPtr & query, const StorageMetadataPtr & /*metadata_snapshot*/, ContextPtr context, bool async_insert) override;

    SparkMergeTreeDataWriter & getWriter() { return writer; }

private:
    MergeTreeTable table;
    SparkMergeTreeDataWriter writer;
};


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

    void emplace_back(std::vector<T> values)
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

class SinkHelper
{
protected:
    CustomStorageMergeTreePtr data;
    bool isRemoteStorage;

    ConcurrentDeque<DB::MergeTreeDataPartPtr> new_parts;
    std::unordered_set<String> tmp_parts{};
    ThreadPool thread_pool;

public:
    const GlutenMergeTreeWriteSettings write_settings;
    const DB::StorageMetadataPtr metadata_snapshot;

protected:
    virtual CustomStorageMergeTree & dest_storage() { return *data; }

    void doMergePartsAsync(const std::vector<DB::MergeTreeDataPartPtr> & prepare_merge_parts);
    void finalizeMerge();
    virtual void cleanup() { }
    virtual void commit(const ReadSettings & read_settings, const WriteSettings & write_settings) { }
    void saveMetadata(const DB::ContextPtr & context);
    SparkStorageMergeTree & dataRef() const { return assert_cast<SparkStorageMergeTree &>(*data); }

public:
    const std::deque<DB::MergeTreeDataPartPtr> & unsafeGet() const { return new_parts.unsafeGet(); }

    void writeTempPart(DB::BlockWithPartition & block_with_partition, const ContextPtr & context, int part_num);
    void checkAndMerge(bool force = false);
    void finish(const DB::ContextPtr & context);

    virtual ~SinkHelper() = default;
    SinkHelper(const CustomStorageMergeTreePtr & data_, const GlutenMergeTreeWriteSettings & write_settings_, bool isRemoteStorage_);
};

class DirectSinkHelper : public SinkHelper
{
protected:
    void cleanup() override;

public:
    explicit DirectSinkHelper(
        const CustomStorageMergeTreePtr & data_, const GlutenMergeTreeWriteSettings & write_settings_, bool isRemoteStorage_)
        : SinkHelper(data_, write_settings_, isRemoteStorage_)
    {
    }
};

class CopyToRemoteSinkHelper : public SinkHelper
{
    CustomStorageMergeTreePtr dest;

protected:
    void commit(const ReadSettings & read_settings, const WriteSettings & write_settings) override;
    CustomStorageMergeTree & dest_storage() override { return *dest; }
    const CustomStorageMergeTreePtr & temp_storage() const { return data; }

public:
    explicit CopyToRemoteSinkHelper(
        const CustomStorageMergeTreePtr & temp,
        const CustomStorageMergeTreePtr & dest_,
        const GlutenMergeTreeWriteSettings & write_settings_)
        : SinkHelper(temp, write_settings_, true), dest(dest_)
    {
        assert(data != dest);
    }
};

using SinkHelperPtr = std::shared_ptr<SinkHelper>;

class SparkMergeTreeSink : public DB::SinkToStorage
{
public:
    static SinkHelperPtr create(
        const MergeTreeTable & merge_tree_table,
        const GlutenMergeTreeWriteSettings & write_settings_,
        const DB::ContextMutablePtr & context);

    explicit SparkMergeTreeSink(const SinkHelperPtr & sink_helper_, const ContextPtr & context_)
        : SinkToStorage(sink_helper_->metadata_snapshot->getSampleBlock()), context(context_), sink_helper(sink_helper_)
    {
    }
    ~SparkMergeTreeSink() override = default;

    String getName() const override { return "SparkMergeTreeSink"; }
    void consume(Chunk & chunk) override;
    void onStart() override;
    void onFinish() override;

    const SinkHelper & sinkHelper() const { return *sink_helper; }

private:
    ContextPtr context;
    SinkHelperPtr sink_helper;

    int part_num = 1;
};

}
