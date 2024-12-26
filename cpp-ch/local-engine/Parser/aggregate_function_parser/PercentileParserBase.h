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
#include <Parser/AggregateFunctionParser.h>

namespace local_engine
{
class PercentileParserBase : public AggregateFunctionParser
{
public:
    explicit PercentileParserBase(ParserContextPtr parser_context_) : AggregateFunctionParser(parser_context_) { }

    String getCHFunctionName(const CommonFunctionInfo & func_info) const override;
    String getCHFunctionName(DB::DataTypes & types) const override;
    DB::Array parseFunctionParameters(
        const CommonFunctionInfo & /*func_info*/,
        DB::ActionsDAG::NodeRawConstPtrs & arg_nodes,
        DB::ActionsDAG & actions_dag) const override;
    DB::Array getDefaultFunctionParameters() const override { return getDefaultFunctionParametersImpl(); }

protected:
    virtual String getCHSingularName() const = 0;
    virtual String getCHPluralName() const = 0;

    /// Expected arguments number of substrait function in first stage
    virtual size_t expectedArgumentsNumberInFirstStage() const = 0;

    /// Expected number of element types wrapped in struct type as intermidate result type of current aggregate function in second stage
    /// Refer to L327 in backends-clickhouse/src/main/scala/io/glutenproject/execution/CHHashAggregateExecTransformer.scala
    virtual size_t expectedTupleElementsNumberInSecondStage() const = 0;

    /// Get argument indexes in first stage substrait function which should be treated as parameters in CH aggregate function.
    /// Note: the indexes are 0-based, and we must guarantee the order of returned parameters matches the order of parameters in CH aggregate function
    virtual DB::ColumnNumbers getArgumentsThatAreParameters() const = 0;

    virtual DB::Array getDefaultFunctionParametersImpl() const = 0;

    /// Utils functions
    void assertArgumentsSize(substrait::AggregationPhase phase, size_t size, size_t expect) const;
    const substrait::Expression::Literal & assertAndGetLiteral(substrait::AggregationPhase phase, const substrait::Expression & expr) const;

    /// percentage index in substrait function arguments(both in first and second stage), which is always 1
    /// All derived implementations must obey this rule
    static constexpr size_t PERCENTAGE_INDEX = 1;
};

}
