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

#include "SparkMergeTreeWriteSettings.h"

#include <Core/Settings.h>
#include <Interpreters/Context.h>
#include <Common/CHUtil.h>

namespace local_engine
{

IMPLEMENT_GLUTEN_SETTINGS(SparkMergeTreeWritePartitionSettings, MERGE_TREE_WRITE_RELATED_SETTINGS)

SparkMergeTreeWriteSettings::SparkMergeTreeWriteSettings(const DB::ContextPtr & context)
    : partition_settings(SparkMergeTreeWritePartitionSettings::get(context))
{
    const DB::Settings & settings = context->getSettingsRef();
    merge_after_insert = settings.get(MERGETREE_MERGE_AFTER_INSERT).safeGet<bool>();
    insert_without_local_storage = settings.get(MERGETREE_INSERT_WITHOUT_LOCAL_STORAGE).safeGet<bool>();

    if (DB::Field limit_size_field; settings.tryGet("optimize.minFileSize", limit_size_field))
        merge_min_size = limit_size_field.safeGet<Int64>() <= 0 ? merge_min_size : limit_size_field.safeGet<Int64>();

    if (DB::Field limit_cnt_field; settings.tryGet("mergetree.max_num_part_per_merge_task", limit_cnt_field))
        merge_limit_parts = limit_cnt_field.safeGet<Int64>() <= 0 ? merge_limit_parts : limit_cnt_field.safeGet<Int64>();

    if (settingsEqual(context->getSettingsRef(), MergeTreeConf::OPTIMIZE_TASK, "true"))
        is_optimize_task = true;
}

}
