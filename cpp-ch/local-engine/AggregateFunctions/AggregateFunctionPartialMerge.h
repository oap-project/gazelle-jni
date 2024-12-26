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

#include <AggregateFunctions/IAggregateFunction.h>
#include <Columns/ColumnAggregateFunction.h>
#include <DataTypes/DataTypeAggregateFunction.h>
#include <Common/assert_cast.h>
#include <Common/typeid_cast.h>


namespace DB
{
namespace ErrorCodes
{
extern const int ILLEGAL_TYPE_OF_ARGUMENT;
}
}

namespace local_engine
{
/**
 * this class is copied from AggregateFunctionMerge with little enhancement.
 * we use this PartialMerge for both spark PartialMerge and Final
 */
class AggregateFunctionPartialMerge final : public DB::IAggregateFunctionHelper<AggregateFunctionPartialMerge>
{
private:
    DB::AggregateFunctionPtr nested_func;

public:
    AggregateFunctionPartialMerge(const DB::AggregateFunctionPtr & nested_, const DB::DataTypePtr & argument, const DB::Array & params_)
        : DB::IAggregateFunctionHelper<AggregateFunctionPartialMerge>({argument}, params_, createResultType(nested_)), nested_func(nested_)
    {
        const DB::DataTypeAggregateFunction * data_type = typeid_cast<const DB::DataTypeAggregateFunction *>(argument.get());

        if (!data_type || !nested_func->haveSameStateRepresentation(*data_type->getFunction()))
            throw DB::Exception(
                DB::ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "Illegal type {} of argument for aggregate function {}, "
                "expected {} or equivalent type",
                argument->getName(),
                getName(),
                getStateType()->getName());
    }

    String getName() const override { return nested_func->getName() + "PartialMerge"; }

    static DB::DataTypePtr createResultType(const DB::AggregateFunctionPtr & nested_) { return nested_->getResultType(); }

    const DB::DataTypePtr & getResultType() const override { return nested_func->getResultType(); }

    bool isVersioned() const override { return nested_func->isVersioned(); }

    size_t getDefaultVersion() const override { return nested_func->getDefaultVersion(); }

    DB::DataTypePtr getStateType() const override { return nested_func->getStateType(); }

    void create(DB::AggregateDataPtr __restrict place) const override { nested_func->create(place); }

    void destroy(DB::AggregateDataPtr __restrict place) const noexcept override { nested_func->destroy(place); }

    bool hasTrivialDestructor() const override { return nested_func->hasTrivialDestructor(); }

    size_t sizeOfData() const override { return nested_func->sizeOfData(); }

    size_t alignOfData() const override { return nested_func->alignOfData(); }

    void add(DB::AggregateDataPtr __restrict place, const DB::IColumn ** columns, size_t row_num, DB::Arena * arena) const override
    {
        nested_func->merge(place, assert_cast<const DB::ColumnAggregateFunction &>(*columns[0]).getData()[row_num], arena);
    }

    void merge(DB::AggregateDataPtr __restrict place, DB::ConstAggregateDataPtr rhs, DB::Arena * arena) const override
    {
        nested_func->merge(place, rhs, arena);
    }

    void serialize(DB::ConstAggregateDataPtr __restrict place, DB::WriteBuffer & buf, std::optional<size_t> version) const override
    {
        nested_func->serialize(place, buf, version);
    }

    void deserialize(DB::AggregateDataPtr __restrict place, DB::ReadBuffer & buf, std::optional<size_t> version, DB::Arena * arena) const override
    {
        nested_func->deserialize(place, buf, version, arena);
    }

    void insertResultInto(DB::AggregateDataPtr __restrict place, DB::IColumn & to, DB::Arena * arena) const override
    {
        nested_func->insertResultInto(place, to, arena);
    }

    bool allocatesMemoryInArena() const override { return nested_func->allocatesMemoryInArena(); }

    DB::AggregateFunctionPtr getNestedFunction() const override { return nested_func; }
    /// If the aggregate phase is `INTEMEDIATE_TO_INTERMEDIATE`, partial merge combinator is applied. In this case, the actual result column's
    /// representation is `xxxPartialMerge`. It will make block structure check fail somewhere, since the expected column's represiontation is
    /// `xxx` without partial merge. The represiontaions of `xxxPartialMerge` and `xxx` are the same actually.
    const IAggregateFunction & getBaseAggregateFunctionWithSameStateRepresentation() const override { return *nested_func; }
};
}
