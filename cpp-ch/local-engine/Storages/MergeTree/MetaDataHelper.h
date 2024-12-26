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

#include <Storages/MergeTree/SparkMergeTreeMeta.h>
#include <Storages/MergeTree/StorageMergeTreeFactory.h>

namespace local_engine
{


bool isMergeTreePartMetaDataFile(const String & file_name);

void restoreMetaData(const SparkStorageMergeTreePtr & storage, const MergeTreeTableInstance & mergeTreeTable, const DB::Context & context);

void saveFileStatus(
    const DB::MergeTreeData & storage, const DB::ContextPtr & context, const String & part_name, DB::IDataPartStorage & data_part_storage);

DB::MergeTreeDataPartPtr mergeParts(
    std::vector<DB::DataPartPtr> selected_parts,
    const String & new_part_uuid,
    SparkStorageMergeTree & storage,
    const String & partition_dir,
    const String & bucket_dir);
}
