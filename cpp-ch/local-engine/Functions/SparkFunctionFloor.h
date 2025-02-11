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
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/IDataType.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionsRound.h>

#if USE_MULTITARGET_CODE
#include <immintrin.h>
#endif

namespace local_engine
{
template <typename T>
requires std::is_floating_point_v<T>
static void checkAndSetNullable(T & t, UInt8 & null_flag)
{
    UInt8 is_nan = (t != t);
    UInt8 is_inf = 0;
    if constexpr (std::is_same_v<T, float>)
        is_inf = ((*reinterpret_cast<const uint32_t *>(&t) & 0b01111111111111111111111111111111) == 0b01111111100000000000000000000000);
    else
        is_inf
            = ((*reinterpret_cast<const uint64_t *>(&t) & 0b0111111111111111111111111111111111111111111111111111111111111111)
               == 0b0111111111110000000000000000000000000000000000000000000000000000);

    null_flag = is_nan | is_inf;

    /* Equivalent code:
    if (null_flag)
        t = 0;
    */
    if constexpr (std::is_same_v<T, float>)
    {
        UInt32 * uint_data = reinterpret_cast<UInt32 *>(&t);
        *uint_data &= ~(-null_flag);
    }
    else
    {
        UInt64 * uint_data = reinterpret_cast<UInt64 *>(&t);
        *uint_data &= ~(-null_flag);
    }
}

DECLARE_AVX2_SPECIFIC_CODE(

    inline void checkFloat32AndSetNullables(Float32 * data, UInt8 * null_map, size_t size) {
        const __m256 inf = _mm256_set1_ps(INFINITY);
        const __m256 neg_inf = _mm256_set1_ps(-INFINITY);
        const __m256 zero = _mm256_set1_ps(0.0f);

        size_t i = 0;
        for (; i + 7 < size; i += 8)
        {
            __m256 values = _mm256_loadu_ps(&data[i]);

            __m256 is_inf = _mm256_cmp_ps(values, inf, _CMP_EQ_OQ);
            __m256 is_neg_inf = _mm256_cmp_ps(values, neg_inf, _CMP_EQ_OQ);
            __m256 is_nan = _mm256_cmp_ps(values, values, _CMP_NEQ_UQ);
            __m256 is_null = _mm256_or_ps(_mm256_or_ps(is_inf, is_neg_inf), is_nan);
            __m256 new_values = _mm256_blendv_ps(values, zero, is_null);

            _mm256_storeu_ps(&data[i], new_values);

            UInt32 mask = static_cast<UInt32>(_mm256_movemask_ps(is_null));
            for (size_t j = 0; j < 8; ++j)
            {
                UInt8 null_flag = (mask & 1U);
                null_map[i + j] = null_flag;
                mask >>= 1;
            }
        }

        for (; i < size; ++i)
            checkAndSetNullable(data[i], null_map[i]);
    }

    inline void checkFloat64AndSetNullables(Float64 * data, UInt8 * null_map, size_t size) {
        const __m256d inf = _mm256_set1_pd(INFINITY);
        const __m256d neg_inf = _mm256_set1_pd(-INFINITY);
        const __m256d zero = _mm256_set1_pd(0.0);

        size_t i = 0;
        for (; i + 3 < size; i += 4)
        {
            __m256d values = _mm256_loadu_pd(&data[i]);

            __m256d is_inf = _mm256_cmp_pd(values, inf, _CMP_EQ_OQ);
            __m256d is_neg_inf = _mm256_cmp_pd(values, neg_inf, _CMP_EQ_OQ);
            __m256d is_nan = _mm256_cmp_pd(values, values, _CMP_NEQ_UQ);
            __m256d is_null = _mm256_or_pd(_mm256_or_pd(is_inf, is_neg_inf), is_nan);
            __m256d new_values = _mm256_blendv_pd(values, zero, is_null);

            _mm256_storeu_pd(&data[i], new_values);

            UInt32 mask = static_cast<UInt32>(_mm256_movemask_pd(is_null));
            for (size_t j = 0; j < 4; ++j)
            {
                UInt8 null_flag = (mask & 1U);
                null_map[i + j] = null_flag;
                mask >>= 1;
            }
        }

        for (; i < size; ++i)
            checkAndSetNullable(data[i], null_map[i]);
    }

)

template <typename T, DB::ScaleMode scale_mode>
requires std::is_floating_point_v<T>
struct SparkFloatFloorImpl
{
private:
    static_assert(!DB::is_decimal<T>);
    template <
        DB::Vectorize vectorize =
#ifdef __SSE4_1__
            DB::Vectorize::Yes
#else
            DB::Vectorize::No
#endif
        >
    using Op = DB::FloatRoundingComputation<T, DB::RoundingMode::Floor, scale_mode, vectorize>;
    using Data = std::array<T, Op<>::data_count>;

public:
    static void apply(const DB::PaddedPODArray<T> & in, size_t scale, DB::PaddedPODArray<T> & out, DB::PaddedPODArray<UInt8> & null_map)
    {
        auto mm_scale = Op<>::prepare(scale);
        const size_t data_count = std::tuple_size<Data>();
        const T * end_in = in.data() + in.size();
        const T * limit = in.data() + in.size() / data_count * data_count;
        const T * __restrict p_in = in.data();
        T * __restrict p_out = out.data();
        while (p_in < limit)
        {
            Op<>::compute(p_in, mm_scale, p_out);
            p_in += data_count;
            p_out += data_count;
        }

        if (p_in < end_in)
        {
            Data tmp_src{{}};
            Data tmp_dst;
            size_t tail_size_bytes = (end_in - p_in) * sizeof(*p_in);
            memcpy(&tmp_src, p_in, tail_size_bytes);
            Op<>::compute(reinterpret_cast<T *>(&tmp_src), mm_scale, reinterpret_cast<T *>(&tmp_dst));
            memcpy(p_out, &tmp_dst, tail_size_bytes);
        }

#if USE_MULTITARGET_CODE
        if (isArchSupported(DB::TargetArch::AVX2))
        {
            if constexpr (std::is_same_v<T, Float32>)
            {
                TargetSpecific::AVX2::checkFloat32AndSetNullables(out.data(), null_map.data(), out.size());
                return;
            }
            else if constexpr (std::is_same_v<T, Float64>)
            {
                TargetSpecific::AVX2::checkFloat64AndSetNullables(out.data(), null_map.data(), out.size());
                return;
            }
        }
#endif
        for (size_t i = 0; i < out.size(); ++i)
             checkAndSetNullable(out[i], null_map[i]);
    }
};

class SparkFunctionFloor : public DB::FunctionFloor
{
    static DB::Scale getScaleArg(const DB::ColumnsWithTypeAndName & arguments)
    {
        if (arguments.size() == 2)
        {
            const DB::IColumn & scale_column = *arguments[1].column;
            if (!isColumnConst(scale_column))
                throw DB::Exception(DB::ErrorCodes::ILLEGAL_COLUMN, "Scale argument for rounding functions must be constant");

            DB::Field scale_field = assert_cast<const DB::ColumnConst &>(scale_column).getField();
            if (scale_field.getType() != DB::Field::Types::UInt64 && scale_field.getType() != DB::Field::Types::Int64)
                throw DB::Exception(DB::ErrorCodes::ILLEGAL_COLUMN, "Scale argument for rounding functions must have integer type");

            Int64 scale64 = scale_field.safeGet<Int64>();
            if (scale64 > std::numeric_limits<DB::Scale>::max() || scale64 < std::numeric_limits<DB::Scale>::min())
                throw DB::Exception(DB::ErrorCodes::ARGUMENT_OUT_OF_BOUND, "Scale argument for rounding function is too large");

            return scale64;
        }
        return 0;
    }

public:
    static constexpr auto name = "sparkFloor";
    static DB::FunctionPtr create(DB::ContextPtr) { return std::make_shared<SparkFunctionFloor>(); }
    SparkFunctionFloor() = default;
    ~SparkFunctionFloor() override = default;
    String getName() const override { return name; }

    DB::ColumnNumbers getArgumentsThatAreAlwaysConstant() const override { return {1}; }

    DB::DataTypePtr getReturnTypeImpl(const DB::ColumnsWithTypeAndName & arguments) const override
    {
        auto result_type = DB::FunctionFloor::getReturnTypeImpl(arguments);
        return makeNullable(result_type);
    }

    DB::ColumnPtr
    executeImpl(const DB::ColumnsWithTypeAndName & arguments, const DB::DataTypePtr & result_type, size_t input_rows) const override
    {
        const DB::ColumnWithTypeAndName & first_arg = arguments[0];
        DB::Scale scale_arg = getScaleArg(arguments);
        switch (first_arg.type->getTypeId())
        {
            case DB::TypeIndex::Float32:
                return executeInternal<Float32>(first_arg.column, scale_arg);
            case DB::TypeIndex::Float64:
                return executeInternal<Float64>(first_arg.column, scale_arg);
            default:
                DB::ColumnPtr res = DB::FunctionFloor::executeImpl(arguments, result_type, input_rows);
                DB::MutableColumnPtr null_map_col = DB::ColumnUInt8::create(first_arg.column->size(), 0);
                return DB::ColumnNullable::create(std::move(res), std::move(null_map_col));
        }
    }

    template <typename T>
    static DB::ColumnPtr executeInternal(const DB::ColumnPtr & col_arg, const DB::Scale & scale_arg)
    {
        const auto * col = checkAndGetColumn<DB::ColumnVector<T>>(col_arg.get());
        auto col_res = DB::ColumnVector<T>::create(col->size());
        DB::MutableColumnPtr null_map_col = DB::ColumnUInt8::create(col->size(), 0);
        DB::PaddedPODArray<T> & vec_res = col_res->getData();
        DB::PaddedPODArray<UInt8> & null_map_data = assert_cast<DB::ColumnVector<UInt8> *>(null_map_col.get())->getData();
        if (!vec_res.empty())
        {
            if (scale_arg == 0)
            {
                size_t scale = 1;
                SparkFloatFloorImpl<T, DB::ScaleMode::Zero>::apply(col->getData(), scale, vec_res, null_map_data);
            }
            else if (scale_arg > 0)
            {
                size_t scale = intExp10(scale_arg);
                SparkFloatFloorImpl<T, DB::ScaleMode::Positive>::apply(col->getData(), scale, vec_res, null_map_data);
            }
            else
            {
                size_t scale = intExp10(-scale_arg);
                SparkFloatFloorImpl<T, DB::ScaleMode::Negative>::apply(col->getData(), scale, vec_res, null_map_data);
            }
        }
        return DB::ColumnNullable::create(std::move(col_res), std::move(null_map_col));
    }
};
}
