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
#include <Parser/aggregate_function_parser/PercentileParserBase.h>

namespace local_engine
{
using namespace DB;
/*
spark: percentile(col, percentage, [, frequency])
1. When percentage is an array literal, spark returns an array of percentiles, corresponding to CH: quantilesExact(percentage[0], ...)(col)
1. Otherwise spark return a single percentile, corresponding to CH: quantileExact(percentage)(col)
*/
class PercentileParser : public PercentileParserBase
{
public:
    static constexpr auto name = "percentile";

    explicit PercentileParser(ParserContextPtr parser_context_) : PercentileParserBase(parser_context_) { }

    String getName() const override { return name; }
    String getCHSingularName() const override { return "quantileExactWeightedInterpolated"; }
    String getCHPluralName() const override { return "quantilesExactWeightedInterpolated"; }

    /// spark percentile(col, percentile[s], frequency)
    size_t expectedArgumentsNumberInFirstStage() const override { return 3; }

    /// intermediate result: struct{col, percentile[s]}
    size_t expectedTupleElementsNumberInSecondStage() const override { return 2; }

    ColumnNumbers getArgumentsThatAreParameters() const override { return {1}; }
    DB::Array getDefaultFunctionParametersImpl() const override { return {1}; }
};

static const AggregateFunctionParserRegister<PercentileParser> register_percentile;
}
