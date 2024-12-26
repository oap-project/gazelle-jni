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
#include <Interpreters/Aggregator.h>
#include <Processors/Transforms/AggregatingTransform.h>

namespace DB
{
/// Util class to access private methods of Aggregator.
class GlutenAggregatorUtil
{
public:
    static Int32 getBucketsNum(AggregatedDataVariants & data_variants);
    static std::optional<Block>
    safeConvertOneBucketToBlock(Aggregator & aggregator, AggregatedDataVariants & variants, Arena * arena, bool final, Int32 bucket);
    static void safeReleaseOneBucket(AggregatedDataVariants & variants, Int32 bucket);
};
}

namespace local_engine
{
/// Act like a iterator to convert AggregatedDataVariants to block stream.
/// Once data_variants is passed to this class, it should not be changed elsewhere.
class AggregateDataBlockConverter
{
public:
    explicit AggregateDataBlockConverter(DB::Aggregator & aggregator_, DB::AggregatedDataVariantsPtr data_variants_, bool final_);
    ~AggregateDataBlockConverter() = default;
    bool hasNext();
    DB::Block next();

private:
    DB::Aggregator & aggregator;
    DB::AggregatedDataVariantsPtr data_variants;
    /// Convert the aggregated states into final data or not.
    bool final;
    Int32 buckets_num = 0;
    Int32 current_bucket = 0;
    DB::BlocksList output_blocks;
};

class AggregatorParamsHelper
{
public:
    enum class Algorithm
    {
        GlutenGraceAggregate,
        CHTwoStageAggregate
    };
    enum class Mode
    {
        INIT_TO_PARTIAL,
        INIT_TO_COMPLETED,
        PARTIAL_TO_PARTIAL,
        PARTIAL_TO_FINISHED,
    };

    // for using grace aggregating, never enable ch spill, otherwise there will be data lost.
    static DB::Aggregator::Params buildParams(
        const DB::ContextPtr & context,
        const DB::Names & grouping_keys,
        const DB::AggregateDescriptions & agg_descriptions,
        Mode mode,
        Algorithm algorithm = Algorithm::GlutenGraceAggregate);
    static bool compare(const DB::Aggregator::Params & lhs, const DB::Aggregator::Params & rhs);
};

}
