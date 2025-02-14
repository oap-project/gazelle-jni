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
#include <AggregateFunctions/AggregateFunctionAvg.h>
#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/FactoryHelpers.h>
#include <AggregateFunctions/Helpers.h>
#include <Core/Settings.h>
#include <Common/CHUtil.h>
#include <Common/GlutenDecimalUtils.h>
#include <Common/GlutenSettings.h>

namespace DB
{
struct Settings;

namespace ErrorCodes
{
extern const int ILLEGAL_TYPE_OF_ARGUMENT;
}
}

namespace local_engine
{
using namespace DB;


DataTypePtr getSparkAvgReturnType(const DataTypePtr & arg_type)
{
    const UInt32 precision_value = std::min<size_t>(getDecimalPrecision(*arg_type) + 4, DecimalUtils::max_precision<Decimal128>);
    const auto scale_value = std::min(getDecimalScale(*arg_type) + 4, precision_value);
    return createDecimal<DataTypeDecimal>(precision_value, scale_value);
}

template <typename T, bool SPARK35>
requires is_decimal<T>
class AggregateFunctionSparkAvg final : public AggregateFunctionAvg<T>
{
public:
    using Base = AggregateFunctionAvg<T>;

    explicit AggregateFunctionSparkAvg(const DataTypes & argument_types_, UInt32 num_scale_, UInt32 round_scale_)
        : Base(argument_types_, createResultType(argument_types_, num_scale_, round_scale_), num_scale_)
        , num_scale(num_scale_)
        , round_scale(round_scale_)
    {
    }

    DataTypePtr createResultType(const DataTypes & argument_types_, UInt32 num_scale_, UInt32 /*round_scale_*/)
    {
        const DataTypePtr & data_type = argument_types_[0];
        const UInt32 precision_value = std::min<size_t>(getDecimalPrecision(*data_type) + 4, DecimalUtils::max_precision<Decimal128>);
        const auto scale_value = std::min(num_scale_ + 4, precision_value);
        return createDecimal<DataTypeDecimal>(precision_value, scale_value);
    }

    void insertResultInto(AggregateDataPtr __restrict place, IColumn & to, Arena *) const override
    {
        const DataTypePtr & result_type = this->getResultType();
        auto result_scale = getDecimalScale(*result_type);
        WhichDataType which(result_type);
        if (which.isDecimal32())
        {
            assert_cast<ColumnDecimal<Decimal32> &>(to).getData().push_back(
                divideDecimalAndUInt(this->data(place), num_scale, result_scale, round_scale));
        }
        else if (which.isDecimal64())
        {
            assert_cast<ColumnDecimal<Decimal64> &>(to).getData().push_back(
                divideDecimalAndUInt(this->data(place), num_scale, result_scale, round_scale));
        }
        else if (which.isDecimal128())
        {
            assert_cast<ColumnDecimal<Decimal128> &>(to).getData().push_back(
                divideDecimalAndUInt(this->data(place), num_scale, result_scale, round_scale));
        }
        else
        {
            assert_cast<ColumnDecimal<Decimal256> &>(to).getData().push_back(
                divideDecimalAndUInt(this->data(place), num_scale, result_scale, round_scale));
        }
    }

    String getName() const override { return "sparkAvg"; }

private:
    Int128 NO_SANITIZE_UNDEFINED
    divideDecimalAndUInt(AvgFraction<AvgFieldType<T>, UInt64> avg, UInt32 num_scale, UInt32 result_scale, UInt32 round_scale) const
    {
        auto value = avg.numerator.value;
        if (result_scale > num_scale)
        {
            auto diff = DecimalUtils::scaleMultiplier<AvgFieldType<T>>(result_scale - num_scale);
            value = value * diff;
        }
        else if (result_scale < num_scale)
        {
            auto diff = DecimalUtils::scaleMultiplier<AvgFieldType<T>>(num_scale - result_scale);
            value = value / diff;
        }

        auto result = value / avg.denominator;

        if constexpr (SPARK35)
            return result;

        if (round_scale > result_scale)
            return result;

        auto round_diff = DecimalUtils::scaleMultiplier<AvgFieldType<T>>(result_scale - round_scale);
        return (result + round_diff / 2) / round_diff * round_diff;
    }

private:
    UInt32 num_scale;
    UInt32 round_scale;
};

template <bool Data, typename... TArgs>
static IAggregateFunction * createWithDecimalType(const IDataType & argument_type, TArgs && ... args)
{
    WhichDataType which(argument_type);
    if (which.idx == TypeIndex::Decimal32) return new AggregateFunctionSparkAvg<Decimal32, Data>(args...);
    if (which.idx == TypeIndex::Decimal64) return new AggregateFunctionSparkAvg<Decimal64, Data>(args...);
    if (which.idx == TypeIndex::Decimal128) return new AggregateFunctionSparkAvg<Decimal128, Data>(args...);
    if (which.idx == TypeIndex::Decimal256) return new AggregateFunctionSparkAvg<Decimal256, Data>(args...);
    if constexpr (AggregateFunctionSparkAvg<DateTime64, Data>::DateTime64Supported)
        if (which.idx == TypeIndex::DateTime64) return new AggregateFunctionSparkAvg<DateTime64, Data>(args...);
    return nullptr;
}

AggregateFunctionPtr createAggregateFunctionSparkAvg(
    const std::string & name, const DataTypes & argument_types, const Array & parameters, const Settings * settings)
{
    assertNoParameters(name, parameters);
    assertUnary(name, argument_types);

    AggregateFunctionPtr res;
    const DataTypePtr & data_type = argument_types[0];
    if (!isDecimal(data_type))
        throw Exception(
            ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Illegal type {} of argument for aggregate function {}", data_type->getName(), name);

    std::string version;
    if (tryGetString(*settings, "spark_version", version) && version.starts_with("3.5"))
    {
        res.reset(createWithDecimalType<true>(*data_type, argument_types, getDecimalScale(*data_type), 0));
        return res;
    }

    bool allowPrecisionLoss = settings->get(DECIMAL_OPERATIONS_ALLOW_PREC_LOSS).safeGet<bool>();
    const UInt32 p1 = DB::getDecimalPrecision(*data_type);
    const UInt32 s1 = DB::getDecimalScale(*data_type);
    auto [p2, s2] = GlutenDecimalUtils::LONG_DECIMAL;
    auto [_, round_scale] = GlutenDecimalUtils::dividePrecisionScale(p1, s1, p2, s2, allowPrecisionLoss);

    res.reset(createWithDecimalType<false>(*data_type, argument_types, getDecimalScale(*data_type), round_scale));
    return res;
}

void registerAggregateFunctionSparkAvg(AggregateFunctionFactory & factory)
{
    factory.registerFunction("sparkAvg", createAggregateFunctionSparkAvg);
}

}
