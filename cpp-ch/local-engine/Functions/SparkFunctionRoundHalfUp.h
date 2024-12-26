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

#include <Functions/FunctionsRound.h>

namespace DB::ErrorCodes
{
extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}

namespace local_engine
{

template <typename T, DB::Vectorize vectorize>
class BaseFloatRoundingHalfUpComputation;

#ifdef __SSE4_1__

/// vectorized implementation for x86

template <>
class BaseFloatRoundingHalfUpComputation<Float32, DB::Vectorize::Yes>
{
public:
    using ScalarType = Float32;
    using VectorType = __m128;
    static const size_t data_count = 4;

    static VectorType load(const ScalarType * in) { return _mm_loadu_ps(in); }
    static VectorType load1(const ScalarType in) { return _mm_load1_ps(&in); }
    static void store(ScalarType * out, VectorType val) { _mm_storeu_ps(out, val); }
    static VectorType multiply(VectorType val, VectorType scale) { return _mm_mul_ps(val, scale); }
    static VectorType divide(VectorType val, VectorType scale) { return _mm_div_ps(val, scale); }
    template <DB::RoundingMode mode>
    static VectorType apply(VectorType val)
    {
        ScalarType tempFloatsIn[data_count];
        ScalarType tempFloatsOut[data_count];
        store(tempFloatsIn, val);
        for (size_t i = 0; i < data_count; ++i)
            tempFloatsOut[i] = std::roundf(tempFloatsIn[i]);

        return load(tempFloatsOut);
    }

    static VectorType prepare(size_t scale) { return load1(scale); }
};

template <>
class BaseFloatRoundingHalfUpComputation<Float64, DB::Vectorize::Yes>
{
public:
    using ScalarType = Float64;
    using VectorType = __m128d;
    static const size_t data_count = 2;

    static VectorType load(const ScalarType * in) { return _mm_loadu_pd(in); }
    static VectorType load1(const ScalarType in) { return _mm_load1_pd(&in); }
    static void store(ScalarType * out, VectorType val) { _mm_storeu_pd(out, val); }
    static VectorType multiply(VectorType val, VectorType scale) { return _mm_mul_pd(val, scale); }
    static VectorType divide(VectorType val, VectorType scale) { return _mm_div_pd(val, scale); }
    template <DB::RoundingMode mode>
    static VectorType apply(VectorType val)
    {
        ScalarType tempFloatsIn[data_count];
        ScalarType tempFloatsOut[data_count];
        store(tempFloatsIn, val);
        for (size_t i = 0; i < data_count; ++i)
            tempFloatsOut[i] = std::round(tempFloatsIn[i]);

        return load(tempFloatsOut);
    }

    static VectorType prepare(size_t scale) { return load1(scale); }
};

/// end __SSE4_1__
#endif

/// Sequential implementation for ARM. Also used for scalar arguments

template <typename T>
class BaseFloatRoundingHalfUpComputation<T, DB::Vectorize::No>
{
public:
    using ScalarType = T;
    using VectorType = T;
    static const size_t data_count = 1;

    static VectorType load(const ScalarType * in) { return *in; }
    static VectorType load1(const ScalarType in) { return in; }
    static VectorType store(ScalarType * out, ScalarType val) { return *out = val;}
    static VectorType multiply(VectorType val, VectorType scale) { return val * scale; }
    static VectorType divide(VectorType val, VectorType scale) { return val / scale; }
    template <DB::RoundingMode mode>
    static VectorType apply(VectorType val)
    {
        if constexpr (std::is_same_v<ScalarType, Float32>)
        {
            return std::roundf(val);
        }
        else
        {
            return std::round(val);
        }
    }

    static VectorType prepare(size_t scale)
    {
        return load1(scale);
    }
};

template <>
class BaseFloatRoundingHalfUpComputation<BFloat16, DB::Vectorize::No>
{
public:
    using ScalarType = BFloat16;
    using VectorType = BFloat16;
    static const size_t data_count = 1;

    static VectorType load(const ScalarType * in) { return *in; }
    static VectorType load1(const ScalarType in) { return in; }
    static VectorType store(ScalarType * out, ScalarType val) { return *out = val;}
    static VectorType multiply(VectorType val, VectorType scale) { return val * scale; }
    static VectorType divide(VectorType val, VectorType scale) { return val / scale; }
    template <DB::RoundingMode mode>
    static VectorType apply(VectorType val)
    {
        return BFloat16(std::roundf(static_cast<Float32>(val)));
    }

    static VectorType prepare(size_t scale)
    {
        return load1(BFloat16(static_cast<Float32>(scale)));
    }
};


/** Implementation of low-level round-off functions for floating-point values.
  */
template <typename T, DB::RoundingMode rounding_mode, DB::ScaleMode scale_mode, DB::Vectorize vectorize>
class FloatRoundingHalfUpComputation : public BaseFloatRoundingHalfUpComputation<T, vectorize>
{
    using Base = BaseFloatRoundingHalfUpComputation<T, vectorize>;

public:
    static inline void compute(const T * __restrict in, const typename Base::VectorType & scale, T * __restrict out)
    {
        auto val = Base::load(in);

        if (scale_mode == DB::ScaleMode::Positive)
            val = Base::multiply(val, scale);
        else if (scale_mode == DB::ScaleMode::Negative)
            val = Base::divide(val, scale);

        val = Base::template apply<rounding_mode>(val);

        if (scale_mode == DB::ScaleMode::Positive)
            val = Base::divide(val, scale);
        else if (scale_mode == DB::ScaleMode::Negative)
            val = Base::multiply(val, scale);

        Base::store(out, val);
    }
};


/** Implementing high-level rounding functions.
  */
template <typename T, DB::RoundingMode rounding_mode, DB::ScaleMode scale_mode>
struct FloatRoundingHalfUpImpl
{
private:
    static_assert(!DB::is_decimal<T>);

    template <DB::Vectorize vectorize =
#ifdef __SSE4_1__
    std::is_same_v<T, BFloat16> ? DB::Vectorize::No : DB::Vectorize::Yes
#else
    DB::Vectorize::No
#endif
    >
    using Op = FloatRoundingHalfUpComputation<T, rounding_mode, scale_mode, vectorize>;
    using Data = std::array<T, Op<>::data_count>;
    using ColumnType = DB::ColumnVector<T>;
    using Container = typename ColumnType::Container;

public:
    static NO_INLINE void apply(const Container & in, size_t scale, Container & out)
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
    }
};


/** Select the appropriate processing algorithm depending on the scale.
  */
template <typename T, DB::RoundingMode rounding_mode, DB::TieBreakingMode tie_breaking_mode>
struct DispatcherRoundingHalfUp
{
    template <DB::ScaleMode scale_mode>
    using FunctionRoundingImpl = std::conditional_t<
        std::is_floating_point_v<T> || std::is_same_v<T, BFloat16>,
        FloatRoundingHalfUpImpl<T, rounding_mode, scale_mode>,
        DB::IntegerRoundingImpl<T, rounding_mode, scale_mode, tie_breaking_mode>>;

    static DB::ColumnPtr apply(const DB::IColumn * col_general, DB::Scale scale_arg)
    {
        const auto * const col = checkAndGetColumn<DB::ColumnVector<T>>(col_general);
        auto col_res = DB::ColumnVector<T>::create();

        typename DB::ColumnVector<T>::Container & vec_res = col_res->getData();
        vec_res.resize_exact(col->getData().size());

        if (!vec_res.empty())
        {
            if (scale_arg == 0)
            {
                size_t scale = 1;
                FunctionRoundingImpl<DB::ScaleMode::Zero>::apply(col->getData(), scale, vec_res);
            }
            else if (scale_arg > 0)
            {
                size_t scale = intExp10(scale_arg);
                FunctionRoundingImpl<DB::ScaleMode::Positive>::apply(col->getData(), scale, vec_res);
            }
            else
            {
                size_t scale = intExp10(-scale_arg);
                FunctionRoundingImpl<DB::ScaleMode::Negative>::apply(col->getData(), scale, vec_res);
            }
        }

        return col_res;
    }
};

template <DB::is_decimal T, DB::RoundingMode rounding_mode, DB::TieBreakingMode tie_breaking_mode>
struct DispatcherRoundingHalfUp<T, rounding_mode, tie_breaking_mode>
{
public:
    static DB::ColumnPtr apply(const DB::IColumn * col_general, DB::Scale scale_arg)
    {
        const auto * const col = checkAndGetColumn<DB::ColumnDecimal<T>>(col_general);
        const typename DB::ColumnDecimal<T>::Container & vec_src = col->getData();

        auto col_res = DB::ColumnDecimal<T>::create(vec_src.size(), col->getScale());
        auto & vec_res = col_res->getData();

        if (!vec_res.empty())
            DB::DecimalRoundingImpl<T, rounding_mode, tie_breaking_mode>::apply(col->getData(), col->getScale(), vec_res, scale_arg);

        return col_res;
    }
};

/** A template for functions that round the value of an input parameter of type
  * (U)Int8/16/32/64, Float32/64 or Decimal32/64/128, and accept an additional optional parameter (default is 0).
  */
template <typename Name, DB::RoundingMode rounding_mode, DB::TieBreakingMode tie_breaking_mode>
class FunctionRoundingHalfUp : public DB::IFunction
{
public:
    static constexpr auto name = "roundHalfUp";
    static DB::FunctionPtr create(DB::ContextPtr) { return std::make_shared<FunctionRoundingHalfUp>(); }

    String getName() const override { return name; }

    bool isVariadic() const override { return true; }
    size_t getNumberOfArguments() const override { return 0; }
    bool isSuitableForShortCircuitArgumentsExecution(const DB::DataTypesWithConstInfo & /*arguments*/) const override { return false; }

    /// Get result types by argument types. If the function does not apply to these arguments, throw an exception.
    DB::DataTypePtr getReturnTypeImpl(const DB::DataTypes & arguments) const override
    {
        if ((arguments.empty()) || (arguments.size() > 2))
            throw DB::Exception(
                DB::ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                "Number of arguments for function {} doesn't match: passed {}, should be 1 or 2.",
                getName(),
                arguments.size());

        for (const auto & type : arguments)
            if (!isNumber(type))
                throw DB::Exception(
                    DB::ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Illegal type {} of argument of function {}", arguments[0]->getName(), getName());

        return arguments[0];
    }

    static DB::Scale getScaleArg(const DB::ColumnsWithTypeAndName & arguments)
    {
        if (arguments.size() == 2)
        {
            const DB::IColumn & scale_column = *arguments[1].column;
            if (!isColumnConst(scale_column))
                throw DB::Exception(DB::ErrorCodes::ILLEGAL_COLUMN, "DB::Scale argument for rounding functions must be constant");

            DB::Field scale_field = assert_cast<const DB::ColumnConst &>(scale_column).getField();
            if (scale_field.getType() != DB::Field::Types::UInt64 && scale_field.getType() != DB::Field::Types::Int64)
                throw DB::Exception(DB::ErrorCodes::ILLEGAL_COLUMN, "DB::Scale argument for rounding functions must have integer type");

            Int64 scale64 = scale_field.safeGet<Int64>();
            if (scale64 > std::numeric_limits<DB::Scale>::max() || scale64 < std::numeric_limits<DB::Scale>::min())
                throw DB::Exception(DB::ErrorCodes::ARGUMENT_OUT_OF_BOUND, "DB::Scale argument for rounding function is too large");

            return scale64;
        }
        return 0;
    }

    bool useDefaultImplementationForConstants() const override { return true; }
    DB::ColumnNumbers getArgumentsThatAreAlwaysConstant() const override { return {1}; }

    DB::ColumnPtr executeImpl(const DB::ColumnsWithTypeAndName & arguments, const DB::DataTypePtr &, size_t /*input_rows_count*/) const override
    {
        const DB::ColumnWithTypeAndName & column = arguments[0];
        DB::Scale scale_arg = getScaleArg(arguments);

        DB::ColumnPtr res;
        auto call = [&](const auto & types) -> bool
        {
            using Types = std::decay_t<decltype(types)>;
            using DataType = typename Types::LeftType;

            if constexpr (DB::IsDataTypeNumber<DataType> || DB::IsDataTypeDecimal<DataType>)
            {
                using FieldType = typename DataType::FieldType;
                res = DispatcherRoundingHalfUp<FieldType, rounding_mode, tie_breaking_mode>::apply(column.column.get(), scale_arg);
                return true;
            }
            return false;
        };

        if (!callOnIndexAndDataType<void>(column.type->getTypeId(), call))
            throw DB::Exception(DB::ErrorCodes::ILLEGAL_COLUMN, "Illegal column {} of argument of function {}", column.name, getName());

        return res;
    }

    bool hasInformationAboutMonotonicity() const override { return true; }

    Monotonicity getMonotonicityForRange(const DB::IDataType &, const DB::Field &, const DB::Field &) const override
    {
        return {.is_monotonic = true, .is_always_monotonic = true};
    }
};


struct NameRoundHalfUp
{
    static constexpr auto name = "roundHalfUp";
};

using FunctionRoundHalfUp = FunctionRoundingHalfUp<NameRoundHalfUp, DB::RoundingMode::Round, DB::TieBreakingMode::Auto>;

}
