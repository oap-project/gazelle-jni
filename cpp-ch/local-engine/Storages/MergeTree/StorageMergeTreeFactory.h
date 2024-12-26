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
#include <Interpreters/MergeTreeTransaction.h>
#include <Storages/MergeTree/SparkMergeTreeMeta.h>
#include <Storages/MergeTree/SparkStorageMergeTree.h>
#include <Poco/LRUCache.h>
#include <Common/GlutenConfig.h>
#include <Common/QueryContext.h>

namespace local_engine
{
using SparkStorageMergeTreePtr = std::shared_ptr<SparkStorageMergeTree>;

class DataPartStorageHolder
{
public:
    DataPartStorageHolder(const DB::DataPartPtr& data_part, const SparkStorageMergeTreePtr& storage)
        : storage_(storage),
          data_part_(data_part)
    {
    }

    [[nodiscard]] DB::DataPartPtr dataPart() const
    {
        return data_part_;
    }

    [[nodiscard]] SparkStorageMergeTreePtr storage() const
    {
        return storage_;
    }

    ~DataPartStorageHolder()
    {
        storage_->removePartFromMemory(*data_part_);
    }

private:
    SparkStorageMergeTreePtr storage_;
    DB::DataPartPtr data_part_;
};

using DataPartStorageHolderPtr = std::shared_ptr<DataPartStorageHolder>;
using storage_map_cache = Poco::LRUCache<std::string, std::pair<SparkStorageMergeTreePtr, MergeTreeTable>>;
using datapart_map_cache = Poco::LRUCache<std::string, std::shared_ptr<Poco::LRUCache<std::string, DataPartStorageHolderPtr>>>;

class StorageMergeTreeFactory
{
public:
    static StorageMergeTreeFactory & instance();
    static void freeStorage(const DB::StorageID & id, const String & snapshot_id = "");
    static SparkStorageMergeTreePtr
    getStorage(const DB::StorageID& id, const String & snapshot_id, MergeTreeTable merge_tree_table,
        const std::function<SparkStorageMergeTreePtr()> & creator);
    static DB::DataPartsVector getDataPartsByNames(const DB::StorageID & id, const String & snapshot_id, std::unordered_set<String> part_name);
    static void init_cache_map()
    {
        auto config = MergeTreeConfig::loadFromContext(QueryContext::globalContext());
        auto & storage_map_v = storage_map;
        if (!storage_map_v)
        {
            storage_map_v = std::make_unique<storage_map_cache>(config.table_metadata_cache_max_count);
        }
        else
        {
            storage_map_v->clear();
        }
        auto & datapart_map_v = datapart_map;
        if (!datapart_map_v)
        {
            datapart_map_v = std::make_unique<datapart_map_cache>(config.table_metadata_cache_max_count);
        }
        else
        {
            datapart_map_v->clear();
        }
    }
    static void clear()
    {
        if (storage_map) storage_map->clear();
        if (datapart_map) datapart_map->clear();
    }

    static String getTableName(const DB::StorageID & id, const String & snapshot_id);

private:
    static std::unique_ptr<storage_map_cache> storage_map;
    static std::unique_ptr<datapart_map_cache> datapart_map;

    static std::recursive_mutex storage_map_mutex;
    static std::recursive_mutex datapart_mutex;
};

}
