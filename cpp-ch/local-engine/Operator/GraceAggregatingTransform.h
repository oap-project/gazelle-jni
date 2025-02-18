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
#include <Core/Block.h>
#include <Interpreters/Aggregator.h>
#include <Interpreters/Context.h>
#include <Interpreters/JoinUtils.h>
#include <Processors/Chunk.h>
#include <Processors/IProcessor.h>
#include <Processors/Transforms/AggregatingTransform.h>
#include <Poco/Logger.h>
#include <Common/MemorySpillScheduler.h>
#include <Common/AggregateUtil.h>


namespace local_engine
{
/**
 * GraceAggregatingTransform is use to aggregate original data or merging intermediate aggregating result. It support spilling data into disk
 * when the memory usage is over limit.
 * If the input data is original data, no_pre_aggregated is true. If it's intermediate aggregating result, no_pre_aggregated is false.
 * IF the output data is final aggregating result, final is true, otherwise it false.
 */
class GraceAggregatingTransform : public DB::IProcessor
{
public:
    using Status = DB::IProcessor::Status;
    explicit GraceAggregatingTransform(
        const DB::Block & header_,
        DB::AggregatingTransformParamsPtr params_,
        DB::ContextPtr context_,
        bool no_pre_aggregated_,
        bool final_output_);
    ~GraceAggregatingTransform() override;

    Status prepare() override;
    void work() override;
    String getName() const override { return "GraceAggregatingTransform"; }

private:
    bool no_pre_aggregated;
    bool final_output;
    DB::Block header;
    DB::Block output_header;
    DB::ColumnRawPtrs key_columns;
    DB::Aggregator::AggregateColumns aggregate_columns;
    DB::AggregatingTransformParamsPtr params;
    DB::ContextPtr context;
    DB::TemporaryDataOnDiskScopePtr tmp_data_disk;
    DB::AggregatedDataVariantsPtr current_data_variants = nullptr;
    size_t current_bucket_index = 0;

    /// Followings are configurations defined in context config.
    // max buckets number, default is 32
    size_t max_buckets = 0;
    // If the buckets number is overflow the max_buckets, throw exception or not.
    bool throw_on_overflow_buckets = false;
    // Even the memory usage has reached the limit, we still allow to aggregate some more keys before
    // extend the buckets.
    size_t aggregated_keys_before_extend_buckets = 8196;
    // The ratio of memory usage to the total memory usage of the whole query.
    double max_allowed_memory_usage_ratio = 0.9;
    // configured by max_pending_flush_blocks_per_grace_merging_bucket
    size_t max_pending_flush_blocks_per_bucket = 0;

    struct BufferFileStream
    {
        /// store the intermediate result blocks.
        std::list<DB::Block> intermediate_blocks;
        /// Only be used when there is no pre-aggregated step, store the original input blocks.
        std::list<DB::Block> original_blocks;
        /// store the intermediate result blocks.
        std::optional<DB::TemporaryBlockStreamHolder> intermediate_file_stream;
        /// Only be used when there is no pre-aggregated step
        std::optional<DB::TemporaryBlockStreamHolder> original_file_stream;
        size_t pending_bytes = 0;
    };
    std::unordered_map<size_t, BufferFileStream> buckets;

    size_t getBucketsNum() const { return buckets.size(); }
    bool extendBuckets();
    void rehashDataVariants();
    DB::Blocks scatterBlock(const DB::Block & block);
    /// Add a block into a bucket, if the pending bytes reaches limit, flush it into disk.
    void addBlockIntoFileBucket(size_t bucket_index, const DB::Block & block, bool is_original_block);
    void flushBuckets();
    size_t flushBucket(size_t bucket_index);
    /// Load blocks from disk and merge them into a new hash table, make a new AggregateDataBlockConverter
    /// to generate output blocks.
    std::unique_ptr<AggregateDataBlockConverter> prepareBucketOutputBlocks(size_t bucket);
    /// Pass current_final_blocks into a new AggregateDataBlockConverter to generate output blocks.
    std::unique_ptr<AggregateDataBlockConverter> currentDataVariantToBlockConverter(bool final);
    void checkAndSetupCurrentDataVariants();
    /// Merge one block into current_data_variants.
    void mergeOneBlock(const DB::Block & block, bool is_original_block);

    // spill control
    bool isMemoryOverflow();
    DB::ProcessorMemoryStats getMemoryStats() override;
    bool spillOnSize(size_t bytes) override;
    bool force_spill = false; // a force flag to trigger spill
    bool force_spill_on_bytes = 0;

    bool input_finished = false;
    bool has_input = false;
    DB::Chunk input_chunk;
    bool has_output = false;
    DB::Chunk output_chunk;
    DB::BlocksList current_final_blocks;
    std::unique_ptr<AggregateDataBlockConverter> block_converter = nullptr;
    bool no_more_keys = false;
    bool enable_spill_test = false;

    double per_key_memory_usage = 0;

    // metrics
    size_t total_input_blocks = 0;
    size_t total_input_rows = 0;
    size_t total_output_blocks = 0;
    size_t total_output_rows = 0;
    size_t total_spill_disk_bytes = 0;
    size_t total_spill_disk_time = 0;
    size_t total_read_disk_time = 0;
    size_t total_scatter_time = 0;

    Poco::Logger * logger = &Poco::Logger::get("GraceMergingAggregatedTransform");
};
}
