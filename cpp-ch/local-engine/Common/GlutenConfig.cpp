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
#include "GlutenConfig.h"

#include <Core/Settings.h>
#include <Interpreters/Context.h>
#include <Parser/SubstraitParserUtils.h>
#include <config.pb.h>
#include <Poco/Util/AbstractConfiguration.h>
#include <Common/CHUtil.h>
#include <Common/DebugUtils.h>
#include <Common/logger_useful.h>

namespace local_engine
{

void SparkConfigs::updateConfig(const DB::ContextMutablePtr & context, std::string_view plan)
{
    update(
        plan,
        [&](const ConfigMap & spark_conf_map)
        {
            // configs cannot be updated per query
            // settings can be updated per query
            auto settings = context->getSettingsCopy(); // make a copy

            // TODO: Remove BackendInitializerUtil::initSettings
            BackendInitializerUtil::initSettings(spark_conf_map, settings);
            context->setSettings(settings);
        });
}

void SparkConfigs::update(std::string_view plan, const std::function<void(const ConfigMap &)> & callback, bool processStart)
{
    auto configMaps = local_engine::BinaryToMessage<gluten::ConfigMap>(plan);
    if (!processStart)
        debug::dumpMessage(configMaps, "Update Config Map Plan");
    callback(configMaps.configs());
}

MemoryConfig MemoryConfig::loadFromContext(const DB::ContextPtr & context)
{
    MemoryConfig config;
    config.extra_memory_hard_limit = context->getConfigRef().getUInt64(EXTRA_MEMORY_HARD_LIMIT, 0);
    config.off_heap_per_task = context->getConfigRef().getUInt64(CH_TASK_MEMORY, 0);
    config.spill_mem_ratio = context->getConfigRef().getDouble(SPILL_MEM_RATIO, 0.9);
    return config;
}

GraceMergingAggregateConfig GraceMergingAggregateConfig::loadFromContext(const DB::ContextPtr & context)
{
    GraceMergingAggregateConfig config;
    config.max_grace_aggregate_merging_buckets = context->getConfigRef().getUInt64(MAX_GRACE_AGGREGATE_MERGING_BUCKETS, 32);
    config.throw_on_overflow_grace_aggregate_merging_buckets
        = context->getConfigRef().getBool(THROW_ON_OVERFLOW_GRACE_AGGREGATE_MERGING_BUCKETS, false);
    config.aggregated_keys_before_extend_grace_aggregate_merging_buckets
        = context->getConfigRef().getUInt64(AGGREGATED_KEYS_BEFORE_EXTEND_GRACE_AGGREGATE_MERGING_BUCKETS, 8192);
    config.max_pending_flush_blocks_per_grace_aggregate_merging_bucket
        = context->getConfigRef().getUInt64(MAX_PENDING_FLUSH_BLOCKS_PER_GRACE_AGGREGATE_MERGING_BUCKET, 1_MiB);
    config.max_allowed_memory_usage_ratio_for_aggregate_merging
        = context->getConfigRef().getDouble(MAX_ALLOWED_MEMORY_USAGE_RATIO_FOR_AGGREGATE_MERGING, 0.9);
    config.enable_spill_test = context->getConfigRef().getBool(ENABLE_SPILL_TEST, false);
    return config;
}

StreamingAggregateConfig StreamingAggregateConfig::loadFromContext(const DB::ContextPtr & context)
{
    StreamingAggregateConfig config;
    config.aggregated_keys_before_streaming_aggregating_evict
        = context->getConfigRef().getUInt64(AGGREGATED_KEYS_BEFORE_STREAMING_AGGREGATING_EVICT, 1024);
    config.max_memory_usage_ratio_for_streaming_aggregating
        = context->getConfigRef().getDouble(MAX_MEMORY_USAGE_RATIO_FOR_STREAMING_AGGREGATING, 0.9);
    config.high_cardinality_threshold_for_streaming_aggregating
        = context->getConfigRef().getDouble(HIGH_CARDINALITY_THRESHOLD_FOR_STREAMING_AGGREGATING, 0.8);
    config.enable_streaming_aggregating = context->getConfigRef().getBool(ENABLE_STREAMING_AGGREGATING, true);
    return config;
}

JoinConfig JoinConfig::loadFromContext(const DB::ContextPtr & context)
{
    JoinConfig config;
    config.prefer_multi_join_on_clauses = context->getConfigRef().getBool(PREFER_MULTI_JOIN_ON_CLAUSES, true);
    config.multi_join_on_clauses_build_side_rows_limit
        = context->getConfigRef().getUInt64(MULTI_JOIN_ON_CLAUSES_BUILD_SIDE_ROWS_LIMIT, 10000000);
    return config;
}

ExecutorConfig ExecutorConfig::loadFromContext(const DB::ContextPtr & context)
{
    ExecutorConfig config;
    config.dump_pipeline = context->getConfigRef().getBool(DUMP_PIPELINE, false);
    config.use_local_format = context->getConfigRef().getBool(USE_LOCAL_FORMAT, false);
    return config;
}

S3Config S3Config::loadFromContext(const DB::ContextPtr & context)
{
    S3Config config;

    if (context->getConfigRef().has("S3_LOCAL_CACHE_ENABLE"))
    {
        LOG_WARNING(&Poco::Logger::get("S3Config"), "Config {} has deprecated.", S3_LOCAL_CACHE_ENABLE);

        config.s3_local_cache_enabled = context->getConfigRef().getBool(S3_LOCAL_CACHE_ENABLE, false);
        config.s3_local_cache_max_size = context->getConfigRef().getUInt64(S3_LOCAL_CACHE_MAX_SIZE, 100_GiB);
        config.s3_local_cache_cache_path = context->getConfigRef().getString(S3_LOCAL_CACHE_CACHE_PATH, "");
        config.s3_gcs_issue_compose_request = context->getConfigRef().getBool(S3_GCS_ISSUE_COMPOSE_REQUEST, false);
    }

    return config;
}
MergeTreeConfig MergeTreeConfig::loadFromContext(const DB::ContextPtr & context)
{
    MergeTreeConfig config;
    config.table_part_metadata_cache_max_count = context->getConfigRef().getUInt64(TABLE_PART_METADATA_CACHE_MAX_COUNT, 5000);
    config.table_metadata_cache_max_count = context->getConfigRef().getUInt64(TABLE_METADATA_CACHE_MAX_COUNT, 500);
    return config;
}
GlutenJobSchedulerConfig GlutenJobSchedulerConfig::loadFromContext(const DB::ContextPtr & context)
{
    GlutenJobSchedulerConfig config;
    config.job_scheduler_max_threads = context->getConfigRef().getUInt64(JOB_SCHEDULER_MAX_THREADS, 10);
    return config;
}
MergeTreeCacheConfig MergeTreeCacheConfig::loadFromContext(const DB::ContextPtr & context)
{
    MergeTreeCacheConfig config;
    config.enable_data_prefetch = context->getConfigRef().getBool(ENABLE_DATA_PREFETCH, config.enable_data_prefetch);
    return config;
}

WindowConfig WindowConfig::loadFromContext(const DB::ContextPtr & context)
{
    WindowConfig config;
    config.aggregate_topk_sample_rows = context->getConfigRef().getUInt64(WINDOW_AGGREGATE_TOPK_SAMPLE_ROWS, 5000);
    config.aggregate_topk_high_cardinality_threshold
        = context->getConfigRef().getDouble(WINDOW_AGGREGATE_TOPK_HIGH_CARDINALITY_THRESHOLD, 0.6);
    return config;
}
}
