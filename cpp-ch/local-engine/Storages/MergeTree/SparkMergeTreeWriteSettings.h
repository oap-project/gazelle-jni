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

#include <Common/GlutenSettings.h>

namespace local_engine
{
#define MERGE_TREE_WRITE_RELATED_SETTINGS(M, ALIAS) \
    M(String, part_name_prefix, , "The part name prefix for writing data") \
    M(String, partition_dir, , "The partition directory for writing data") \
    M(String, bucket_dir, , "The bucket directory for writing data")

DECLARE_GLUTEN_SETTINGS(SparkMergeTreeWritePartitionSettings, MERGE_TREE_WRITE_RELATED_SETTINGS)

struct SparkMergeTreeWriteSettings
{
    SparkMergeTreeWritePartitionSettings partition_settings;
    bool merge_after_insert{true};
    bool insert_without_local_storage{false};
    bool is_optimize_task{false};
    size_t merge_min_size = 1024 * 1024 * 1024; // 1GB
    size_t merge_limit_parts = 10;

    explicit SparkMergeTreeWriteSettings(const DB::ContextPtr & context);
};

struct MergeTreeConf
{
    inline static const String CH_CONF{"merge_tree"};
    inline static const String GLUTEN_CONF{"mergetree"};

    inline static const String OPTIMIZE_TASK{GLUTEN_CONF + ".optimize_task"};
    // inline static const String MAX_NUM_PART_PER_MERGE_TASK{GLUTEN_CONF + ".max_num_part_per_merge_task"};
};
}