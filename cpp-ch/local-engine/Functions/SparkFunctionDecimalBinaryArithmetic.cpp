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
#include "SparkFunctionDecimalBinaryArithmetic.h"

#include <Columns/ColumnDecimal.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnsNumber.h>
#include <Core/DecimalFunctions.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypesDecimal.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/IFunction.h>
#include <Functions/castTypeToEither.h>
#include <Common/CurrentThread.h>
#include <Common/Stopwatch.h>

namespace DB
{
namespace ErrorCodes
{
extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
extern const int ILLEGAL_TYPE_OF_ARGUMENT;
extern const int ILLEGAL_COLUMN;
extern const int TYPE_MISMATCH;
extern const int LOGICAL_ERROR;
}

}

namespace local_engine
{
using namespace DB;

namespace
{
enum class OpCase : uint8_t
{
    Vector,
    LeftConstant,
    RightConstant
};

enum class OpMode : uint8_t
{
    Default,
    Effect
};

template <bool is_plus_minus, bool is_multiply, bool is_division, bool is_modulo>
bool calculateWith256(const IDataType & left, const IDataType & right)
{
    const size_t p1 = getDecimalPrecision(left);
    const size_t s1 = getDecimalScale(left);
    const size_t p2 = getDecimalPrecision(right);
    const size_t s2 = getDecimalScale(right);

    size_t precision;
    if constexpr (is_plus_minus)
        precision = std::max(s1, s2) + std::max(p1 - s1, p2 - s2) + 1;
    else if constexpr (is_multiply)
        precision = p1 + p2 + 1;
    else if constexpr (is_division)
        precision = p1 - s1 + s2 + std::max(static_cast<size_t>(6), s1 + p2 + 1);
    else if constexpr (is_modulo)
        precision = std::min(p1 - s1, p2 - s2) + std::max(s1, s2);
    else
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Not supported.");

    return precision > DataTypeDecimal128::maxPrecision();
}

template <typename Operation, OpMode Mode>
struct SparkDecimalBinaryOperation
{
private:
    static constexpr bool is_plus_minus = SparkIsOperation<Operation>::plus || SparkIsOperation<Operation>::minus;
    static constexpr bool is_multiply = SparkIsOperation<Operation>::multiply;
    static constexpr bool is_division = SparkIsOperation<Operation>::division;
    static constexpr bool is_modulo = SparkIsOperation<Operation>::modulo;

public:
    template <typename A, typename B, typename R>
    static ColumnPtr executeDecimal(const ColumnsWithTypeAndName & arguments, const A & left, const B & right, const R & result)
    {
        using LeftDataType = std::decay_t<decltype(left)>; // e.g. DataTypeDecimal<Decimal32>
        using RightDataType = std::decay_t<decltype(right)>; // e.g. DataTypeDecimal<Decimal32>
        using ResultDataType = std::decay_t<decltype(result)>; // e.g. DataTypeDecimal<Decimal32>

        using ColVecLeft = ColumnVectorOrDecimal<typename LeftDataType::FieldType>;
        using ColVecRight = ColumnVectorOrDecimal<typename RightDataType::FieldType>;

        const ColumnPtr left_col = arguments[0].column;
        const ColumnPtr right_col = arguments[1].column;

        const auto * const col_left_raw = left_col.get();
        const auto * const col_right_raw = right_col.get();

        const size_t col_left_size = col_left_raw->size();

        const ColumnConst * const col_left_const = checkAndGetColumnConst<ColVecLeft>(col_left_raw);
        const ColumnConst * const col_right_const = checkAndGetColumnConst<ColVecRight>(col_right_raw);

        const ColVecLeft * const col_left = checkAndGetColumn<ColVecLeft>(col_left_raw);
        const ColVecRight * const col_right = checkAndGetColumn<ColVecRight>(col_right_raw);

        if constexpr (Mode == OpMode::Effect)
        {
            return executeDecimalImpl<LeftDataType, RightDataType, ResultDataType, NativeType<typename ResultDataType::FieldType>>(
                left, right, col_left_const, col_right_const, col_left, col_right, col_left_size, result);
        }

        if (calculateWith256<is_plus_minus, is_multiply, is_division, is_modulo>(*arguments[0].type.get(), *arguments[1].type.get()))
        {
            return executeDecimalImpl<LeftDataType, RightDataType, ResultDataType, Int256, true>(
                left, right, col_left_const, col_right_const, col_left, col_right, col_left_size, result);
        }

        size_t max_scale = getMaxScaled(left.getScale(), right.getScale(), result.getScale());
        if (is_division && max_scale - left.getScale() + max_scale > DataTypeDecimal<typename ResultDataType::FieldType>::maxPrecision())
        {
            return executeDecimalImpl<LeftDataType, RightDataType, ResultDataType, Int256, true>(
                left, right, col_left_const, col_right_const, col_left, col_right, col_left_size, result);
        }
        else
        {
            return executeDecimalImpl<LeftDataType, RightDataType, ResultDataType, NativeType<typename ResultDataType::FieldType>>(
                left, right, col_left_const, col_right_const, col_left, col_right, col_left_size, result);
        }
    }

private:
    // ResultDataType e.g. DataTypeDecimal<Decimal32>
    template <class LeftDataType, class RightDataType, class ResultDataType, class ScaleDataType, bool CalculateWith256 = false>
    static ColumnPtr executeDecimalImpl(
        const auto & left,
        const auto & right,
        const ColumnConst * const col_left_const,
        const ColumnConst * const col_right_const,
        const auto * const col_left,
        const auto * const col_right,
        size_t col_left_size,
        const ResultDataType & resultDataType)
    {
        using LeftFieldType = typename LeftDataType::FieldType;
        using RightFieldType = typename RightDataType::FieldType;
        using ResultFieldType = typename ResultDataType::FieldType;

        using ColVecResult = ColumnVectorOrDecimal<ResultFieldType>;

        size_t max_scale = getMaxScaled(left.getScale(), right.getScale(), resultDataType.getScale());

        ScaleDataType scale_left = [&]
        {
            if constexpr (is_multiply)
                return ScaleDataType{1};

            // cast scale same to left
            auto diff_scale = max_scale - left.getScale();
            if constexpr (is_division)
                return DecimalUtils::scaleMultiplier<ScaleDataType>(diff_scale + max_scale);
            else
                return DecimalUtils::scaleMultiplier<ScaleDataType>(diff_scale);
        }();

        const ScaleDataType scale_right = [&]
        {
            if constexpr (is_multiply)
                return ScaleDataType{1};
            else
                return DecimalUtils::scaleMultiplier<ScaleDataType>(max_scale - right.getScale());
        }();


        bool calculate_with_256 = false;
        if constexpr (CalculateWith256)
            calculate_with_256 = true;
        else
        {
            auto p1 = left.getPrecision();
            auto p2 = right.getPrecision();
            if (DataTypeDecimal<LeftFieldType>::maxPrecision() < p1 + max_scale - left.getScale()
                || DataTypeDecimal<RightFieldType>::maxPrecision() < p2 + max_scale - right.getScale())
                calculate_with_256 = true;
        }

        ColumnUInt8::MutablePtr col_null_map_to = ColumnUInt8::create(col_left_size, false);
        ColumnUInt8::Container * vec_null_map_to = &col_null_map_to->getData();

        typename ColVecResult::MutablePtr col_res = ColVecResult::create(0, resultDataType.getScale());
        auto & vec_res = col_res->getData();
        vec_res.resize(col_left_size);

        if (col_left && col_right)
        {
            if (calculate_with_256)
            {
                process<OpCase::Vector, true>(
                    col_left->getData(),
                    col_right->getData(),
                    vec_res,
                    scale_left,
                    scale_right,
                    *vec_null_map_to,
                    resultDataType,
                    max_scale);
            }
            else
            {
                process<OpCase::Vector, false>(
                    col_left->getData(),
                    col_right->getData(),
                    vec_res,
                    scale_left,
                    scale_right,
                    *vec_null_map_to,
                    resultDataType,
                    max_scale);
            }
        }
        else if (col_left_const && col_right)
        {
            LeftFieldType const_left = col_left_const->getValue<LeftFieldType>();

            if (calculate_with_256)
            {
                process<OpCase::LeftConstant, true>(
                    const_left, col_right->getData(), vec_res, scale_left, scale_right, *vec_null_map_to, resultDataType, max_scale);
            }
            else
            {
                process<OpCase::LeftConstant, false>(
                    const_left, col_right->getData(), vec_res, scale_left, scale_right, *vec_null_map_to, resultDataType, max_scale);
            }
        }
        else if (col_left && col_right_const)
        {
            RightFieldType const_right = col_right_const->getValue<RightFieldType>();
            if (calculate_with_256)
            {
                process<OpCase::RightConstant, true>(
                    col_left->getData(), const_right, vec_res, scale_left, scale_right, *vec_null_map_to, resultDataType, max_scale);
            }
            else
            {
                process<OpCase::RightConstant, false>(
                    col_left->getData(), const_right, vec_res, scale_left, scale_right, *vec_null_map_to, resultDataType, max_scale);
            }
        }
        else
        {
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Not supported.");
        }

        return ColumnNullable::create(std::move(col_res), std::move(col_null_map_to));
    }

    template <OpCase op_case, bool CalculateWith256, typename ResultContainerType, typename ResultDataType, typename ScaleDataType>
    static static void NO_INLINE process(
        const auto & a,
        const auto & b,
        ResultContainerType & result_container,
        const ScaleDataType & scale_a,
        const ScaleDataType & scale_b,
        ColumnUInt8::Container & vec_null_map_to,
        const ResultDataType & resultDataType,
        size_t max_scale)
    {
        using NativeResultType = NativeType<typename ResultDataType::FieldType>;

        size_t size;
        if constexpr (op_case == OpCase::LeftConstant)
            size = b.size();
        else
            size = a.size();

        if constexpr (op_case == OpCase::Vector)
        {
            for (size_t i = 0; i < size; ++i)
            {
                NativeResultType res;
                if (calculate<CalculateWith256>(
                        unwrap<op_case, OpCase::LeftConstant>(a, i),
                        unwrap<op_case, OpCase::RightConstant>(b, i),
                        scale_a,
                        scale_b,
                        res,
                        resultDataType,
                        max_scale))
                    result_container[i] = res;
                else
                    vec_null_map_to[i] = static_cast<UInt8>(1);
            }
        }
        else if constexpr (op_case == OpCase::LeftConstant)
        {
            ScaleDataType scaled_a = applyScaled(unwrap<op_case, OpCase::LeftConstant>(a, 0), scale_a);
            for (size_t i = 0; i < size; ++i)
            {
                NativeResultType res;
                if (calculate<CalculateWith256>(
                        scaled_a,
                        unwrap<op_case, OpCase::RightConstant>(b, i),
                        static_cast<ScaleDataType>(0),
                        scale_b,
                        res,
                        resultDataType,
                        max_scale))
                    result_container[i] = res;
                else
                    vec_null_map_to[i] = static_cast<UInt8>(1);
            }
        }
        else if constexpr (op_case == OpCase::RightConstant)
        {
            ScaleDataType scaled_b = applyScaled(unwrap<op_case, OpCase::RightConstant>(b, 0), scale_b);

            for (size_t i = 0; i < size; ++i)
            {
                NativeResultType res;
                if (calculate<CalculateWith256>(
                        unwrap<op_case, OpCase::LeftConstant>(a, i),
                        scaled_b,
                        scale_a,
                        static_cast<ScaleDataType>(0),
                        res,
                        resultDataType,
                        max_scale))
                    result_container[i] = res;
                else
                    vec_null_map_to[i] = static_cast<UInt8>(1);
            }
        }
    }

    // ResultNativeType = Int32/64/128/256
    template <bool CalculateWith256, typename LeftNativeType, typename RightNativeType, typename NativeResultType, typename ResultDataType, typename ScaleDataType>
    static NO_SANITIZE_UNDEFINED bool calculate(
        const LeftNativeType l,
        const RightNativeType r,
        const ScaleDataType & scale_left,
        const ScaleDataType & scale_right,
        NativeResultType & res,
        const ResultDataType & resultDataType,
        size_t max_scale)
    {
        if constexpr (CalculateWith256)
            return calculateImpl<Int256>(l, r, scale_left, scale_right, res, resultDataType, max_scale);
        else if (is_division)
            return calculateImpl<Int128>(l, r, scale_left, scale_right, res, resultDataType, max_scale);
        else
            return calculateImpl<NativeResultType>(l, r, scale_left, scale_right, res, resultDataType, max_scale);
    }

    template <typename CalcType, typename LeftNativeType, typename RightNativeType, typename NativeResultType, typename ResultDataType, typename ScaleDataType>
    static NO_SANITIZE_UNDEFINED bool calculateImpl(
        const LeftNativeType & l,
        const RightNativeType & r,
        const ScaleDataType & scale_left,
        const ScaleDataType & scale_right,
        NativeResultType & res,
        const ResultDataType & resultDataType,
        size_t max_scale)
    {
        CalcType scaled_l = applyScaled(static_cast<CalcType>(l), static_cast<CalcType>(scale_left));
        CalcType scaled_r = applyScaled(static_cast<CalcType>(r), static_cast<CalcType>(scale_right));

        CalcType c_res = 0;
        auto success = Operation::template apply<CalcType>(scaled_l, scaled_r, c_res);
        if (!success)
            return false;

        auto result_scale = resultDataType.getScale();
        auto scale_diff = max_scale - result_scale;
        chassert(scale_diff >= 0);
        if (scale_diff)
        {
            auto scaled_diff = DecimalUtils::scaleMultiplier<CalcType>(scale_diff);
            DecimalDivideImpl::apply<CalcType>(c_res, scaled_diff, c_res);
        }

        // check overflow
        if constexpr (std::is_same_v<CalcType, Int256> || is_division)
        {
            auto max_value = intExp10OfSize<CalcType>(resultDataType.getPrecision());
            if (c_res <= -max_value || c_res >= max_value)
                return false;
        }

        res = static_cast<NativeResultType>(c_res);

        return true;
    }

    template <OpCase op_case, OpCase target, class E>
    static auto unwrap(const E & elem, size_t i)
    {
        if constexpr (op_case == target)
            return elem.value;
        else
            return elem[i].value;
    }

    template <typename NativeType, typename ScaleType>
    static ScaleType applyScaled(const NativeType & l, const ScaleType & scale)
    {
        if (scale > 1)
            return common::mulIgnoreOverflow(l, scale);

        return static_cast<ScaleType>(l);
    }

    static size_t getMaxScaled(const size_t left_scale, const size_t right_scale, const size_t result_scale)
    {
        if constexpr (is_multiply)
            return left_scale + right_scale;
        else
            return std::max(result_scale, std::max(left_scale, right_scale));
    }
};


template <class Operation, typename Name, OpMode Mode = OpMode::Default>
class SparkFunctionDecimalBinaryArithmetic final : public IFunction
{
    static constexpr bool is_plus_minus = SparkIsOperation<Operation>::plus || SparkIsOperation<Operation>::minus;
    static constexpr bool is_multiply = SparkIsOperation<Operation>::multiply;
    static constexpr bool is_division = SparkIsOperation<Operation>::division;
    static constexpr bool is_modulo = SparkIsOperation<Operation>::modulo;

public:
    static constexpr auto name = Name::name;

    static FunctionPtr create(ContextPtr context_) { return std::make_shared<SparkFunctionDecimalBinaryArithmetic>(context_); }

    explicit SparkFunctionDecimalBinaryArithmetic(ContextPtr context_) : context(context_) { }

    String getName() const override { return name; }
    size_t getNumberOfArguments() const override { return 3; }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return false; }
    bool useDefaultImplementationForConstants() const override { return true; }
    ColumnNumbers getArgumentsThatAreAlwaysConstant() const override { return {2}; }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        if (arguments.size() != 3)
            throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, "Function '{}' expects 3 arguments", getName());

        if (!isDecimal(arguments[0].type) || !isDecimal(arguments[1].type) || !isDecimal(arguments[2].type))
            throw Exception(
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "Illegal type {} {} {} of argument of function {}",
                arguments[0].type->getName(),
                arguments[1].type->getName(),
                arguments[2].type->getName(),
                getName());

        return std::make_shared<DataTypeNullable>(arguments[2].type);
    }

    // executeImpl2
    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t) const override
    {
        const auto & left_argument = arguments[0];
        const auto & right_argument = arguments[1];

        const auto * left_generic = left_argument.type.get();
        const auto * right_generic = right_argument.type.get();

        ColumnPtr res;
        const bool valid = castBothTypes(
            left_generic,
            right_generic,
            removeNullable(arguments[2].type).get(),
            [&](const auto & left, const auto & right, const auto & result) {
                return (res = SparkDecimalBinaryOperation<Operation, Mode>::template executeDecimal<>(arguments, left, right, result))
                    != nullptr;
            });

        if (!valid)
        {
            // This is a logical error, because the types should have been checked
            // by getReturnTypeImpl().
            throw Exception(
                ErrorCodes::LOGICAL_ERROR,
                "Arguments of '{}' have incorrect data types: '{}' of type '{}',"
                " '{}' of type '{}'",
                getName(),
                left_argument.name,
                left_argument.type->getName(),
                right_argument.name,
                right_argument.type->getName());
        }

        return res;
    }

private:
    template <typename F>
    static bool castBothTypes(const IDataType * left, const IDataType * right, const IDataType * result, F && f)
    {
        return castType(
            left,
            [&](const auto & left_)
            {
                return castType(
                    right,
                    [&](const auto & right_) { return castType(result, [&](const auto & result_) { return f(left_, right_, result_); }); });
            });
    }

    static bool castType(const IDataType * type, auto && f)
    {
        using Types = TypeList<DataTypeDecimal32, DataTypeDecimal64, DataTypeDecimal128, DataTypeDecimal256>;
        return castTypeToEither(Types{}, type, std::forward<decltype(f)>(f));
    }

    ContextPtr context;
};

struct NameSparkDecimalPlus
{
    static constexpr auto name = "sparkDecimalPlus";
};
struct NameSparkDecimalPlusEffect
{
    static constexpr auto name = "sparkDecimalPlusEffect";
};
struct NameSparkDecimalMinus
{
    static constexpr auto name = "sparkDecimalMinus";
};
struct NameSparkDecimalMinusEffect
{
    static constexpr auto name = "sparkDecimalMinusEffect";
};
struct NameSparkDecimalMultiply
{
    static constexpr auto name = "sparkDecimalMultiply";
};
struct NameSparkDecimalMultiplyEffect
{
    static constexpr auto name = "sparkDecimalMultiplyEffect";
};
struct NameSparkDecimalDivide
{
    static constexpr auto name = "sparkDecimalDivide";
};
struct NameSparkDecimalDivideEffect
{
    static constexpr auto name = "sparkDecimalDivideEffect";
};
struct NameSparkDecimalModulo
{
    static constexpr auto name = "NameSparkDecimalModulo";
};
struct NameSparkDecimalModuloEffect
{
    static constexpr auto name = "NameSparkDecimalModuloEffect";
};


using DecimalPlus = SparkFunctionDecimalBinaryArithmetic<DecimalPlusImpl, NameSparkDecimalPlus>;
using DecimalMinus = SparkFunctionDecimalBinaryArithmetic<DecimalMinusImpl, NameSparkDecimalMinus>;
using DecimalMultiply = SparkFunctionDecimalBinaryArithmetic<DecimalMultiplyImpl, NameSparkDecimalMultiply>;
using DecimalDivide = SparkFunctionDecimalBinaryArithmetic<DecimalDivideImpl, NameSparkDecimalDivide>;
using DecimalModulo = SparkFunctionDecimalBinaryArithmetic<DecimalModuloImpl, NameSparkDecimalModulo>;

using DecimalPlusEffect = SparkFunctionDecimalBinaryArithmetic<DecimalPlusImpl, NameSparkDecimalPlusEffect, OpMode::Effect>;
using DecimalMinusEffect = SparkFunctionDecimalBinaryArithmetic<DecimalMinusImpl, NameSparkDecimalMinusEffect, OpMode::Effect>;
using DecimalMultiplyEffect = SparkFunctionDecimalBinaryArithmetic<DecimalMultiplyImpl, NameSparkDecimalMultiplyEffect, OpMode::Effect>;
using DecimalDivideEffect = SparkFunctionDecimalBinaryArithmetic<DecimalDivideImpl, NameSparkDecimalDivideEffect, OpMode::Effect>;
using DecimalModuloEffect = SparkFunctionDecimalBinaryArithmetic<DecimalModuloImpl, NameSparkDecimalModuloEffect, OpMode::Effect>;
}

REGISTER_FUNCTION(SparkDecimalFunctionArithmetic)
{
    factory.registerFunction<DecimalPlus>();
    factory.registerFunction<DecimalMinus>();
    factory.registerFunction<DecimalMultiply>();
    factory.registerFunction<DecimalDivide>();
    factory.registerFunction<DecimalModulo>();

    factory.registerFunction<DecimalPlusEffect>();
    factory.registerFunction<DecimalMinusEffect>();
    factory.registerFunction<DecimalMultiplyEffect>();
    factory.registerFunction<DecimalDivideEffect>();
    factory.registerFunction<DecimalModuloEffect>();
}
}
