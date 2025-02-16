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

#include <Processors/QueryPlan/ReadFromMergeTree.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <Storages/MergeTree/MergeTreeDataSelectExecutor.h>
#include <Storages/MergeTree/MergeTreeDataWriter.h>
#include <Storages/MergeTree/MergeTreeSettings.h>
#include <Storages/MergeTree/RangesInDataPart.h>
#include <Storages/MergeTree/SparkMergeTreeMeta.h>
#include <Storages/MutationCommands.h>
#include <Storages/StorageMergeTree.h>
#include <Common/GlutenSettings.h>

namespace local_engine
{
struct SparkMergeTreeWritePartitionSettings;

class SparkMergeTreeDataWriter
{
public:
    explicit SparkMergeTreeDataWriter(DB::MergeTreeData & data_) : data(data_), log(getLogger(data.getLogName() + " (Writer)")) { }
    DB::MergeTreeDataWriter::TemporaryPart writeTempPart(
        DB::BlockWithPartition & block_with_partition,
        const DB::StorageMetadataPtr & metadata_snapshot,
        const DB::ContextPtr & context,
        const std::string & part_dir) const;

private:
    DB::MergeTreeData & data;
    LoggerPtr log;
};

class SparkStorageMergeTree : public DB::MergeTreeData
{
    friend class MergeSparkMergeTreeTask;

    struct SparkMutationsSnapshot : public IMutationsSnapshot
    {
        SparkMutationsSnapshot() = default;

        DB::MutationCommands getAlterMutationCommandsForPart(const MergeTreeData::DataPartPtr & part) const override { return {}; }
        std::shared_ptr<MergeTreeData::IMutationsSnapshot> cloneEmpty() const override
        {
            return std::make_shared<SparkMutationsSnapshot>();
        }

        DB::NameSet getAllUpdatedColumns() const override { return {}; }
    };

public:
    static void wrapRangesInDataParts(DB::ReadFromMergeTree & source, const DB::RangesInDataParts & ranges);
    static void analysisPartsByRanges(DB::ReadFromMergeTree & source, const DB::RangesInDataParts & ranges_in_data_parts);
    std::string getName() const override;
    std::vector<DB::MergeTreeMutationStatus> getMutationsStatus() const override;
    bool scheduleDataProcessingJob(DB::BackgroundJobsAssignee & executor) override;
    std::map<std::string, DB::MutationCommands> getUnfinishedMutationCommands() const override;
    std::vector<DB::MergeTreeDataPartPtr> loadDataPartsWithNames(const std::unordered_set<std::string> & parts);
    void removePartFromMemory(const MergeTreeData::DataPart & part_to_detach);
    void prefetchPartDataFile(const std::unordered_set<std::string> & parts) const;

    DB::MergeTreeDataSelectExecutor reader;
    DB::MergeTreeDataMergerMutator merger_mutator;

protected:
    SparkStorageMergeTree(
        const DB::StorageID & table_id_,
        const String & relative_data_path_,
        const DB::StorageInMemoryMetadata & metadata,
        bool attach,
        const DB::ContextMutablePtr & context_,
        const String & date_column_name,
        const MergingParams & merging_params_,
        std::unique_ptr<DB::MergeTreeSettings> settings_,
        bool has_force_restore_data_flag = false);

private:
    static std::atomic<int> part_num;
    SimpleIncrement increment;

    void prefetchPartFiles(const std::unordered_set<std::string> & parts, String file_name) const;
    void prefetchMetaDataFile(const std::unordered_set<std::string> & parts) const;
    void startBackgroundMovesIfNeeded() override;
    std::unique_ptr<DB::MergeTreeSettings> getDefaultSettings() const override;
    LoadPartResult loadDataPart(
        const DB::MergeTreePartInfo & part_info, const String & part_name, const DB::DiskPtr & part_disk_ptr, DB::MergeTreeDataPartState to_state);

protected:
    void dropPartNoWaitNoThrow(const String & part_name) override;
    void dropPart(const String & part_name, bool detach, DB::ContextPtr context) override;
    void dropPartition(const DB::ASTPtr & partition, bool detach, DB::ContextPtr context) override;
    DB::PartitionCommandsResultInfo
    attachPartition(const DB::ASTPtr & partition, const DB::StorageMetadataPtr & metadata_snapshot, bool part, DB::ContextPtr context) override;
    void replacePartitionFrom(const DB::StoragePtr & source_table, const DB::ASTPtr & partition, bool replace, DB::ContextPtr context) override;
    void movePartitionToTable(const DB::StoragePtr & dest_table, const DB::ASTPtr & partition, DB::ContextPtr context) override;
    bool partIsAssignedToBackgroundOperation(const DataPartPtr & part) const override;
    void attachRestoredParts(MutableDataPartsVector && /*parts*/) override { throw std::runtime_error("not implement"); }
    UInt64 getNumberOnFlyDataMutations() const override { throw std::runtime_error("not implement"); }
    UInt64 getNumberOnFlyMetadataMutations() const override { throw std::runtime_error("not implement"); }

public:
    MutationsSnapshotPtr getMutationsSnapshot(const IMutationsSnapshot::Params & /*params*/) const override
    {
        return std::make_shared<SparkMutationsSnapshot>();
    };
};

class SparkWriteStorageMergeTree final : public SparkStorageMergeTree
{
    static std::unique_ptr<DB::MergeTreeSettings>
    buildMergeTreeSettings(const DB::ContextMutablePtr & context, const MergeTreeTableSettings & config);

public:
    SparkWriteStorageMergeTree(const MergeTreeTable & table_, const DB::StorageInMemoryMetadata & metadata, const DB::ContextMutablePtr & context_)
        : SparkStorageMergeTree(
              DB::StorageID(table_.database, table_.table),
              table_.relative_path,
              metadata,
              false,
              context_,
              "",
              MergingParams(),
              buildMergeTreeSettings(context_, table_.table_configs),
              false /*has_force_restore_data_flag*/)
        , table(table_)
        , writer(*this)
    {
    }

    DB::SinkToStoragePtr
    write(const DB::ASTPtr & query, const DB::StorageMetadataPtr & /*metadata_snapshot*/, DB::ContextPtr context, bool async_insert) override;

    SparkMergeTreeDataWriter & getWriter() { return writer; }

private:
    MergeTreeTable table;
    SparkMergeTreeDataWriter writer;
};

}
