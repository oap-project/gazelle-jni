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

#include <Interpreters/Context.h>
#include <Interpreters/Squashing.h>
#include <Storages/MergeTree/IMergeTreeDataPart.h>
#include <Storages/MergeTree/MergeTreeDataWriter.h>
#include <Storages/MergeTree/MergeTreeTool.h>
#include <Storages/MergeTree/SparkMergeTreeSink.h>
#include <Storages/MergeTree/StorageMergeTreeFactory.h>
#include <Common/CHUtil.h>

namespace DB
{
struct BlockWithPartition;
class MergeTreeData;
struct StorageSnapshot;
using StorageSnapshotPtr = std::shared_ptr<StorageSnapshot>;
}

namespace local_engine
{

struct PartInfo
{
    String part_name;
    size_t mark_count;
    size_t disk_size;
    size_t row_count;
    std::unordered_map<String, String> partition_values;
    String bucket_id;

    bool operator<(const PartInfo & rhs) const { return disk_size < rhs.disk_size; }
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

class SinkHelper;
using SinkHelperPtr = std::shared_ptr<SinkHelper>;
class SinkHelper
{
protected:
    const GlutenMergeTreeWriteSettings write_settings;
    CustomStorageMergeTreePtr data;
    bool isRemoteStorage;

    ConcurrentDeque<DB::MergeTreeDataPartPtr> new_parts;
    std::unordered_set<String> tmp_parts{};
    ThreadPool thread_pool;

public:
    const DB::StorageMetadataPtr metadata_snapshot;
    const DB::Block header;

protected:
    void doMergePartsAsync(const std::vector<DB::MergeTreeDataPartPtr> & prepare_merge_parts);
    virtual void cleanup() { }

public:
    void emplacePart(const DB::MergeTreeDataPartPtr & part) { new_parts.emplace_back(part); }

    const std::deque<DB::MergeTreeDataPartPtr> & unsafeGet() const { return new_parts.unsafeGet(); }
    void checkAndMerge(bool force = false);
    void finalizeMerge();

    virtual ~SinkHelper() = default;
    explicit SinkHelper(
        const CustomStorageMergeTreePtr & data_, const GlutenMergeTreeWriteSettings & write_settings_, bool isRemoteStorage_);
    static SinkHelperPtr create(
        const MergeTreeTable & merge_tree_table,
        const GlutenMergeTreeWriteSettings & write_settings_,
        const DB::ContextMutablePtr & context);

    virtual CustomStorageMergeTree & dest_storage() { return *data; }
    CustomStorageMergeTree & dataRef() const { return *data; }

    virtual void commit(const ReadSettings & read_settings, const WriteSettings & write_settings) { }
    void saveMetadata(const DB::ContextPtr & context);
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

public:
    explicit CopyToRemoteSinkHelper(
        const CustomStorageMergeTreePtr & temp,
        const CustomStorageMergeTreePtr & dest_,
        const GlutenMergeTreeWriteSettings & write_settings_)
        : SinkHelper(temp, write_settings_, true), dest(dest_)
    {
        assert(data != dest);
    }

    CustomStorageMergeTree & dest_storage() override { return *dest; }
    const CustomStorageMergeTreePtr & temp_storage() const { return data; }

    void commit(const ReadSettings & read_settings, const WriteSettings & write_settings) override;
};

class SparkMergeTreeWriter
{
public:
    static String partInfosToJson(const std::vector<PartInfo> & part_infos);
    SparkMergeTreeWriter(
        const MergeTreeTable & merge_tree_table, const GlutenMergeTreeWriteSettings & write_settings_, const DB::ContextPtr & context_);

    void write(const DB::Block & block);
    void finalize();
    std::vector<PartInfo> getAllPartInfo() const;

private:
    DB::MergeTreeDataWriter::TemporaryPart
    writeTempPartAndFinalize(DB::BlockWithPartition & block_with_partition, const DB::StorageMetadataPtr & metadata_snapshot) const;
    bool chunkToPart(Chunk && plan_chunk);
    bool blockToPart(Block & block);

    const GlutenMergeTreeWriteSettings write_settings;
    SinkHelperPtr dataWrapper;
    DB::ContextPtr context;
    std::unordered_map<String, String> partition_values;


    std::unique_ptr<DB::Squashing> squashing;

    int part_num = 1;
};
}
