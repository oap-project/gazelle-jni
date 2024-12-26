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

#include <cstddef>
#include <AggregateFunctions/IAggregateFunction_fwd.h>
#include <Columns/ColumnAggregateFunction.h>
#include <Columns/ColumnVector.h>
#include <DataTypes/DataTypesNumber.h>
#include <IO/VarInt.h>
#include <base/types.h>
#include "Common/Exception.h"
#include <Common/assert_cast.h>


#include <IO/ReadHelpers.h>
#include <Interpreters/BloomFilter.h>

namespace DB::ErrorCodes
{
extern const int BAD_ARGUMENTS;
}

namespace local_engine
{

struct AggregateFunctionGroupBloomFilterData
{
    bool initted = false;
    // small default value because BloomFilter has no default ctor
    DB::BloomFilter bloom_filter = DB::BloomFilter(100, 2, 0);
    static const char * name() { return "groupBloomFilter"; }

    void read(DB::ReadBuffer & in)
    {
        UInt64 filter_size, filter_hashes, seed = 0;
        readVarUInt(filter_size, in);
        readVarUInt(filter_hashes, in);
        readVarUInt(seed, in);
        if unlikely (filter_size == 0)
        {
            initted = false;
        }
        else
        {
            bloom_filter = DB::BloomFilter(DB::BloomFilterParameters(filter_size, filter_hashes, seed));
            auto & v = bloom_filter.getFilter();
            in.readStrict(reinterpret_cast<char *>(v.data()), v.size() * sizeof(v[0]));
            initted = true;
        }
    }

    void write(DB::WriteBuffer & out) const
    {
        if likely (initted)
        {
            writeVarUInt(bloom_filter.getSize(), out);
            writeVarUInt(bloom_filter.getHashes(), out);
            writeVarUInt(bloom_filter.getSeed(), out);
            const auto & v = bloom_filter.getFilter();

            out.write(reinterpret_cast<const char *>(v.data()), v.size() * sizeof(v[0]));
        }
        else
        {
            writeVarUInt(0, out);
            writeVarUInt(0, out);
            writeVarUInt(0, out);
        }
    }
};

// Aggreate Int64 values into a bloom filter.
// For groupFunctionBloomFilter, we don't actually care about the final Int result(currently always return BF byte size).
// We just need its intermediate state, ,i.e. groupFunctionFilterState.
template <typename T, typename Data>
class AggregateFunctionGroupBloomFilter final : public DB::IAggregateFunctionDataHelper<Data, AggregateFunctionGroupBloomFilter<T, Data>>
{
public:
    explicit AggregateFunctionGroupBloomFilter(
        const DB::DataTypes & argument_types_, const DB::Array & parameters_, size_t filter_size_, size_t filter_hashes_, size_t seed_)
        : DB::IAggregateFunctionDataHelper<Data, AggregateFunctionGroupBloomFilter<T, Data>>(argument_types_, parameters_, createResultType())
        , filter_size(filter_size_)
        , filter_hashes(filter_hashes_)
        , seed(seed_)
    {
    }

    String getName() const override { return Data::name(); }

    static DB::DataTypePtr createResultType() { return std::make_shared<DB::DataTypeNumber<T>>(); }

    bool allocatesMemoryInArena() const override { return false; }

    void checkFilterSize(size_t filter_size_) const
    {
        if (filter_size_ <= 0)
        {
            throw DB::Exception(DB::ErrorCodes::BAD_ARGUMENTS, "filter_size being LTE 0 means bloom filter is not properly initialized");
        }
    }

    void add(DB::AggregateDataPtr __restrict place, const DB::IColumn ** columns, size_t row_num, DB::Arena *) const override
    {
        if unlikely (!this->data(place).initted)
        {
            checkFilterSize(filter_size);
            this->data(place).bloom_filter = DB::BloomFilter(DB::BloomFilterParameters(filter_size, filter_hashes, seed));
            this->data(place).initted = true;
        }

        T x = assert_cast<const DB::ColumnVector<T> &>(*columns[0]).getData()[row_num];
        this->data(place).bloom_filter.add(reinterpret_cast<const char *>(&x), sizeof(T));
    }

    void merge(DB::AggregateDataPtr __restrict place, DB::ConstAggregateDataPtr rhs, DB::Arena *) const override
    {
        // Skip un-initted values
        if (!this->data(rhs).initted)
        {
            return;
        }
        const auto & bloom_other = this->data(rhs).bloom_filter;
        const auto & filter_other = bloom_other.getFilter();
        if (!this->data(place).initted)
        {
            // We use filter_other's size/hashes/seed to avoid passing these parameters around to construct AggregateFunctionGroupBloomFilter.
            checkFilterSize(bloom_other.getSize());
            this->data(place).bloom_filter = DB::BloomFilter(DB::BloomFilterParameters(bloom_other.getSize(), bloom_other.getHashes(), bloom_other.getSeed()));
            this->data(place).initted = true;
        }
        auto & filter_self = this->data(place).bloom_filter.getFilter();
        for (size_t i = 0; i < filter_other.size(); ++i)
        {
            if (filter_other[i])
            {
                filter_self[i] |= filter_other[i];
            }
        }
    }

    void serialize(DB::ConstAggregateDataPtr __restrict place, DB::WriteBuffer & buf, std::optional<size_t> /* version */) const override
    {
        this->data(place).write(buf);
    }

    void deserialize(DB::AggregateDataPtr __restrict place, DB::ReadBuffer & buf, std::optional<size_t> /* version */, DB::Arena *) const override
    {
        this->data(place).read(buf);
    }

    void insertResultInto(DB::AggregateDataPtr __restrict /*place*/, DB::IColumn & to, DB::Arena *) const override
    {
        assert_cast<DB::ColumnVector<T> &>(to).getData().push_back(static_cast<T>(filter_size));
    }

private:
    size_t filter_size;
    size_t filter_hashes;
    size_t seed;
};

}
