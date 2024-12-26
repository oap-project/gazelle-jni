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

#include <Interpreters/MergeTreeTransactionHolder.h>
#include <Storages/MergeTree/IExecutableTask.h>
#include <Storages/MergeTree/MergeMutateSelectedEntry.h>
#include <Storages/MergeTree/MergeTask.h>

namespace local_engine
{
class SparkStorageMergeTree;

class MergeSparkMergeTreeTask : public DB::IExecutableTask
{
public:
    MergeSparkMergeTreeTask(
        SparkStorageMergeTree & storage_,
        DB::StorageMetadataPtr metadata_snapshot_,
        bool deduplicate_,
        DB::Names deduplicate_by_columns_,
        bool cleanup_,
        DB::MergeMutateSelectedEntryPtr merge_mutate_entry_,
        DB::TableLockHolder table_lock_holder_,
        DB::IExecutableTask::TaskResultCallback & task_result_callback_)
        : storage(storage_)
        , metadata_snapshot(std::move(metadata_snapshot_))
        , deduplicate(deduplicate_)
        , deduplicate_by_columns(std::move(deduplicate_by_columns_))
        , cleanup(cleanup_)
        , merge_mutate_entry(std::move(merge_mutate_entry_))
        , table_lock_holder(std::move(table_lock_holder_))
        , task_result_callback(task_result_callback_)
    {
        for (auto & item : merge_mutate_entry->future_part->parts)
            priority.value += item->getBytesOnDisk();
    }

    bool executeStep() override;
    void onCompleted() override;
    DB::StorageID getStorageID() const override;
    Priority getPriority() const override { return priority; }
    String getQueryId() const override { return getStorageID().getShortName() + "::" + merge_mutate_entry->future_part->name; }

    void setCurrentTransaction(DB::MergeTreeTransactionHolder && txn_holder_, DB::MergeTreeTransactionPtr && txn_)
    {
        txn_holder = std::move(txn_holder_);
        txn = std::move(txn_);
    }
    void cancel() noexcept override;

private:
    void prepare();
    void finish();

    enum class State
    {
        NEED_PREPARE,
        NEED_EXECUTE,
        NEED_FINISH,

        SUCCESS
    };

    State state{State::NEED_PREPARE};

    SparkStorageMergeTree & storage;

    DB::StorageMetadataPtr metadata_snapshot;
    bool deduplicate;
    DB::Names deduplicate_by_columns;
    bool cleanup;
    DB::MergeMutateSelectedEntryPtr merge_mutate_entry{nullptr};
    DB::TableLockHolder table_lock_holder;
    DB::FutureMergedMutatedPartPtr future_part{nullptr};
    DB::MergeTreeData::MutableDataPartPtr new_part;
    std::unique_ptr<Stopwatch> stopwatch_ptr{nullptr};
    using MergeListEntryPtr = std::unique_ptr<DB::MergeListEntry>;
    MergeListEntryPtr merge_list_entry;

    Priority priority;

    std::function<void(const DB::ExecutionStatus &)> write_part_log;
    std::function<void()> transfer_profile_counters_to_initial_query;
    DB::IExecutableTask::TaskResultCallback task_result_callback;
    DB::MergeTaskPtr merge_task{nullptr};

    DB::MergeTreeTransactionHolder txn_holder;
    DB::MergeTreeTransactionPtr txn;

    ProfileEvents::Counters profile_counters;

    DB::ContextMutablePtr task_context;

    DB::ContextMutablePtr createTaskContext() const;
};


using MergeSparkMergeTreeTaskPtr = std::shared_ptr<MergeSparkMergeTreeTask>;

}
