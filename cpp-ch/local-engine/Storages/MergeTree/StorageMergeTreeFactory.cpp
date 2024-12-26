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
#include "StorageMergeTreeFactory.h"

#include <Storages/MergeTree/SparkMergeTreeMeta.h>
#include <Storages/MergeTree/SparkStorageMergeTree.h>
#include <Common/GlutenConfig.h>

namespace local_engine
{
using namespace DB;
String StorageMergeTreeFactory::getTableName(const StorageID & id, const String & snapshot_id)
{
    auto table_name = id.database_name + "." + id.table_name;
    // for optimize table
    if (!snapshot_id.empty())
        table_name += "_" + snapshot_id;

    return table_name;
}


StorageMergeTreeFactory & StorageMergeTreeFactory::instance()
{
    static StorageMergeTreeFactory ret;
    return ret;
}

void StorageMergeTreeFactory::freeStorage(const StorageID & id, const String & snapshot_id)
{
    auto table_name = getTableName(id, snapshot_id);

    {
        std::lock_guard lock(storage_map_mutex);
        if (storage_map->has(table_name))
            storage_map->remove(table_name);
    }

    {
        std::lock_guard lock(datapart_mutex);
        if (datapart_map->has(table_name))
            datapart_map->remove(table_name);
    }
}


SparkStorageMergeTreePtr StorageMergeTreeFactory::getStorage(
    const StorageID & id, const String & snapshot_id, MergeTreeTable merge_tree_table,
    const std::function<SparkStorageMergeTreePtr()> & creator)
{
    const auto table_name = getTableName(id, snapshot_id);
    std::lock_guard lock(storage_map_mutex);

    // merge_tree_table.parts.clear();
    if (storage_map->has(table_name) && !storage_map->get(table_name)->second.sameTable(merge_tree_table))
    {
        freeStorage(id);
        std::lock_guard lock_datapart(datapart_mutex);
        if (datapart_map->has(table_name))
            datapart_map->remove(table_name);
    }
    if (!storage_map->has(table_name))
        storage_map->add(table_name, {creator(), merge_tree_table});
    return storage_map->get(table_name)->first;
}

DataPartsVector
StorageMergeTreeFactory::getDataPartsByNames(const StorageID & id, const String & snapshot_id, std::unordered_set<String> part_name)
{
    DataPartsVector res;
    auto table_name = getTableName(id, snapshot_id);
    auto config = MergeTreeConfig::loadFromContext(QueryContext::globalContext());
    std::lock_guard lock(datapart_mutex);
    std::unordered_set<String> missing_names;
    if (!datapart_map->has(table_name)) [[unlikely]]
    {
        auto cache = std::make_shared<Poco::LRUCache<std::string, DataPartStorageHolderPtr>>(config.table_part_metadata_cache_max_count);
        datapart_map->add(table_name, cache);
    }

    // find the missing cache part name
    for (const auto & name : part_name)
    {
        if (!(*datapart_map->get(table_name))->has(name))
        {
            missing_names.emplace(name);
        }
        else
        {
            res.emplace_back((*datapart_map->get(table_name))->get(name)->get()->dataPart());
        }
    }

    if (!missing_names.empty())
    {
        SparkStorageMergeTreePtr storage_merge_tree;
        {
            std::lock_guard storage_lock(storage_map_mutex);
            storage_merge_tree = storage_map->get(table_name)->first;
        }
        auto missing_parts = storage_merge_tree->loadDataPartsWithNames(missing_names);
        for (auto & part : missing_parts)
        {
            res.emplace_back(part);
            (*datapart_map->get(table_name))->add(part->name, std::make_shared<DataPartStorageHolder>(part, storage_merge_tree));
        }
    }
    return res;
}
// will be inited in native init phase
std::unique_ptr<storage_map_cache> StorageMergeTreeFactory::storage_map = nullptr;
std::unique_ptr<datapart_map_cache> StorageMergeTreeFactory::datapart_map = nullptr;
std::recursive_mutex StorageMergeTreeFactory::storage_map_mutex;
std::recursive_mutex StorageMergeTreeFactory::datapart_mutex;

}
