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

#include <Columns/ColumnNullable.h>
#include <Columns/ColumnVector.h>
#include <Columns/ColumnsNumber.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/IDataType.h>
#include <Functions/FunctionFactory.h>
#include <Functions/IFunction.h>
#include <Common/NaNUtils.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int TYPE_MISMATCH;
}
}

namespace local_engine
{

template <typename T, typename Name, T int_max_value, T int_min_value>
class SparkFunctionCastFloatToInt : public DB::IFunction
{
public:
    size_t getNumberOfArguments() const override { return 1; }
    static constexpr auto name = Name::name;
    static DB::FunctionPtr create(DB::ContextPtr) { return std::make_shared<SparkFunctionCastFloatToInt>(); }
    SparkFunctionCastFloatToInt() = default;
    ~SparkFunctionCastFloatToInt() override = default;
    String getName() const override { return name; }
    bool useDefaultImplementationForConstants() const override { return true; }
    bool isSuitableForShortCircuitArgumentsExecution(const DB::DataTypesWithConstInfo & /*arguments*/) const override { return true; }

    DB::DataTypePtr getReturnTypeImpl(const DB::DataTypes &) const override
    {
        if constexpr (std::is_integral_v<T>)
        {
            return DB::makeNullable(std::make_shared<const DB::DataTypeNumber<T>>());
        }
        else
            throw DB::Exception(DB::ErrorCodes::TYPE_MISMATCH, "Function {}'s return type should be Int", name);
    }

    DB::ColumnPtr executeImpl(const DB::ColumnsWithTypeAndName & arguments, const DB::DataTypePtr & result_type, size_t) const override
    {
        if (arguments.size() != 1)
            throw DB::Exception(DB::ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, "Function {}'s arguments number must be 1", name);
        
        if (!isFloat(removeNullable(arguments[0].type)))
            throw DB::Exception(DB::ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Function {}'s 1st argument must be float type", name);
        
        DB::ColumnPtr src_col = arguments[0].column;
        size_t size = src_col->size();

        auto res_col = DB::ColumnVector<T>::create(size, 0);
        auto null_map_col = DB::ColumnUInt8::create(size, 0);

        switch(removeNullable(arguments[0].type)->getTypeId())
        {
            case DB::TypeIndex::Float32:
            {
                executeInternal<Float32>(src_col, res_col->getData(), null_map_col->getData());
                break;
            }
            case DB::TypeIndex::Float64:
            {
                executeInternal<Float64>(src_col, res_col->getData(), null_map_col->getData());
                break;
            }
        }
        return DB::ColumnNullable::create(std::move(res_col), std::move(null_map_col));
    }

    template <typename F>
    void executeInternal(const DB::ColumnPtr & src, DB::PaddedPODArray<T> & data, DB::PaddedPODArray<UInt8> & null_map_data) const
    {
        const DB::ColumnVector<F> * src_vec = assert_cast<const DB::ColumnVector<F> *>(src.get());
        for (size_t i = 0; i < src_vec->size(); ++i)
        {
            F element = src_vec->getElement(i);
            if (isNaN(element) || !isFinite(element))
                null_map_data[i] = 1;
            else if (element > int_max_value)
                data[i] = int_max_value;
            else if (element < int_min_value)
                data[i] = int_min_value;
            else
                data[i] = static_cast<T>(element);
        }
    }
};

}