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

#include <Interpreters/Context_fwd.h>
#include <base/unit.h>
#include <google/protobuf/map.h>

namespace Poco::Util
{
class AbstractConfiguration;
}
namespace DB
{
struct ReadSettings;
}
namespace local_engine
{

struct SparkConfigs
{
    using ConfigMap = google::protobuf::Map<std::string, std::string>;
    static void updateConfig(const DB::ContextMutablePtr &, std::string_view);
    static void update(std::string_view plan, const std::function<void(const ConfigMap &)> & callback, bool processStart = false);
};

struct MemoryConfig
{
    inline static const String EXTRA_MEMORY_HARD_LIMIT = "extra_memory_hard_limit";
    inline static const String CH_TASK_MEMORY = "off_heap_per_task";
    inline static const String SPILL_MEM_RATIO = "spill_mem_ratio";

    size_t extra_memory_hard_limit = 0;
    size_t off_heap_per_task = 0;
    double spill_mem_ratio = 0.9;

    static MemoryConfig loadFromContext(const DB::ContextPtr & context);
};

struct GraceMergingAggregateConfig
{
    inline static const String MAX_GRACE_AGGREGATE_MERGING_BUCKETS = "max_grace_aggregate_merging_buckets";
    inline static const String THROW_ON_OVERFLOW_GRACE_AGGREGATE_MERGING_BUCKETS = "throw_on_overflow_grace_aggregate_merging_buckets";
    inline static const String AGGREGATED_KEYS_BEFORE_EXTEND_GRACE_AGGREGATE_MERGING_BUCKETS
        = "aggregated_keys_before_extend_grace_aggregate_merging_buckets";
    inline static const String MAX_PENDING_FLUSH_BLOCKS_PER_GRACE_AGGREGATE_MERGING_BUCKET
        = "max_pending_flush_blocks_per_grace_aggregate_merging_bucket";
    inline static const String MAX_ALLOWED_MEMORY_USAGE_RATIO_FOR_AGGREGATE_MERGING
        = "max_allowed_memory_usage_ratio_for_aggregate_merging";
    inline static const String ENABLE_SPILL_TEST = "enable_grace_aggregate_spill_test";

    size_t max_grace_aggregate_merging_buckets = 32;
    bool throw_on_overflow_grace_aggregate_merging_buckets = false;
    size_t aggregated_keys_before_extend_grace_aggregate_merging_buckets = 8192;
    size_t max_pending_flush_blocks_per_grace_aggregate_merging_bucket = 1_MiB;
    double max_allowed_memory_usage_ratio_for_aggregate_merging = 0.9;
    bool enable_spill_test = false;

    static GraceMergingAggregateConfig loadFromContext(const DB::ContextPtr & context);
};

struct StreamingAggregateConfig
{
    inline static const String AGGREGATED_KEYS_BEFORE_STREAMING_AGGREGATING_EVICT = "aggregated_keys_before_streaming_aggregating_evict";
    inline static const String MAX_MEMORY_USAGE_RATIO_FOR_STREAMING_AGGREGATING = "max_memory_usage_ratio_for_streaming_aggregating";
    inline static const String HIGH_CARDINALITY_THRESHOLD_FOR_STREAMING_AGGREGATING
        = "high_cardinality_threshold_for_streaming_aggregating";
    inline static const String ENABLE_STREAMING_AGGREGATING = "enable_streaming_aggregating";

    size_t aggregated_keys_before_streaming_aggregating_evict = 1024;
    double max_memory_usage_ratio_for_streaming_aggregating = 0.9;
    double high_cardinality_threshold_for_streaming_aggregating = 0.8;
    bool enable_streaming_aggregating = true;

    static StreamingAggregateConfig loadFromContext(const DB::ContextPtr & context);
};

struct JoinConfig
{
    /// If the join condition is like `t1.k = t2.k and (t1.id1 = t2.id2 or t1.id2 = t2.id2)`, try to join with multi
    /// join on clauses `(t1.k = t2.k and t1.id1 = t2.id2) or (t1.k = t2.k or t1.id2 = t2.id2)`
    inline static const String PREFER_MULTI_JOIN_ON_CLAUSES = "prefer_multi_join_on_clauses";
    /// Only hash join supports multi join on clauses, the right table cannot be too large. If the row number of right
    /// table is larger then this limit, this transform will not work.
    inline static const String MULTI_JOIN_ON_CLAUSES_BUILD_SIDE_ROWS_LIMIT = "multi_join_on_clauses_build_side_row_limit";

    bool prefer_multi_join_on_clauses = true;
    size_t multi_join_on_clauses_build_side_rows_limit = 10000000;

    static JoinConfig loadFromContext(const DB::ContextPtr & context);
};

struct ExecutorConfig
{
    inline static const String DUMP_PIPELINE = "dump_pipeline";
    inline static const String USE_LOCAL_FORMAT = "use_local_format";

    bool dump_pipeline = false;
    bool use_local_format = false;

    static ExecutorConfig loadFromContext(const DB::ContextPtr & context);
};

struct S3Config
{
    inline static const String S3_LOCAL_CACHE_ENABLE = "s3.local_cache.enabled";
    inline static const String S3_LOCAL_CACHE_MAX_SIZE = "s3.local_cache.max_size";
    inline static const String S3_LOCAL_CACHE_CACHE_PATH = "s3.local_cache.cache_path";
    inline static const String S3_GCS_ISSUE_COMPOSE_REQUEST = "s3.gcs_issue_compose_request";

    bool s3_local_cache_enabled = false;
    size_t s3_local_cache_max_size = 100_GiB;
    String s3_local_cache_cache_path = "";
    bool s3_gcs_issue_compose_request = false;

    static S3Config loadFromContext(const DB::ContextPtr & context);
};

struct MergeTreeConfig
{
    inline static const String TABLE_PART_METADATA_CACHE_MAX_COUNT = "table_part_metadata_cache_max_count";
    inline static const String TABLE_METADATA_CACHE_MAX_COUNT = "table_metadata_cache_max_count";

    size_t table_part_metadata_cache_max_count = 5000;
    size_t table_metadata_cache_max_count = 500;

    static MergeTreeConfig loadFromContext(const DB::ContextPtr & context);
};

struct GlutenJobSchedulerConfig
{
    inline static const String JOB_SCHEDULER_MAX_THREADS = "job_scheduler_max_threads";

    size_t job_scheduler_max_threads = 10;

    static GlutenJobSchedulerConfig loadFromContext(const DB::ContextPtr & context);
};

struct MergeTreeCacheConfig
{
    inline static const String ENABLE_DATA_PREFETCH = "enable_data_prefetch";

    bool enable_data_prefetch = true;

    static MergeTreeCacheConfig loadFromContext(const DB::ContextPtr & context);
};

struct WindowConfig
{
public:
    inline static const String WINDOW_AGGREGATE_TOPK_SAMPLE_ROWS = "window.aggregate_topk_sample_rows";
    inline static const String WINDOW_AGGREGATE_TOPK_HIGH_CARDINALITY_THRESHOLD = "window.aggregate_topk_high_cardinality_threshold";
    size_t aggregate_topk_sample_rows = 5000;
    double aggregate_topk_high_cardinality_threshold = 0.6;
    static WindowConfig loadFromContext(const DB::ContextPtr & context);
};

namespace PathConfig
{
inline constexpr const char * USE_CURRENT_DIRECTORY_AS_TMP = "use_current_directory_as_tmp";
inline constexpr const char * DEFAULT_TEMP_FILE_PATH = "/tmp/libch";
};
}
