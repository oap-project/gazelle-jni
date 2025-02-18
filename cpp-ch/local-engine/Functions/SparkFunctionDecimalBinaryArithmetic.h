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

#include "SparkFunctionDecimalBinaryOperator.h"

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

#if USE_EMBEDDED_COMPILER
#include <DataTypes/Native.h>
#include <llvm/IR/IRBuilder.h>
#endif

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

template <typename Op1, typename Op2>
struct IsSameOperation
{
    static constexpr bool value = std::is_same_v<Op1, Op2>;
};

template <typename Op>
struct SparkIsOperation
{
    static constexpr bool plus = IsSameOperation<Op, DecimalPlusImpl>::value;
    static constexpr bool minus = IsSameOperation<Op, DecimalMinusImpl>::value;
    static constexpr bool plus_minus = IsSameOperation<Op, DecimalPlusImpl>::value || IsSameOperation<Op, DecimalMinusImpl>::value;
    static constexpr bool multiply = IsSameOperation<Op, DecimalMultiplyImpl>::value;
    static constexpr bool division = IsSameOperation<Op, DecimalDivideImpl>::value;
    static constexpr bool modulo = IsSameOperation<Op, DecimalModuloImpl>::value;
};

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


template <typename Operation, OpMode Mode>
struct SparkDecimalBinaryOperation
{
private:
    static constexpr bool is_plus_minus = SparkIsOperation<Operation>::plus_minus;
    static constexpr bool is_multiply = SparkIsOperation<Operation>::multiply;
    static constexpr bool is_division = SparkIsOperation<Operation>::division;
    static constexpr bool is_modulo = SparkIsOperation<Operation>::modulo;

public:
    static size_t getMaxScaled(size_t left_scale, size_t right_scale, size_t result_scale)
    {
        if constexpr (is_multiply)
            return left_scale + right_scale;
        else
            return std::max(result_scale, std::max(left_scale, right_scale));
    }

    template <typename LeftDataType, typename RightDataType, typename ResultDataType>
    static bool shouldPromoteTo256(const LeftDataType & left_type, const RightDataType & right_type, const ResultDataType & result_type)
    {
        auto p1 = left_type.getPrecision();
        auto s1 = left_type.getScale();
        auto p2 = right_type.getPrecision();
        auto s2 = right_type.getScale();

        size_t precision;
        if constexpr (is_plus_minus)
            precision = std::max<size_t>(s1, s2) + std::max<size_t>(p1 - s1, p2 - s2) + 1;
        else if constexpr (is_multiply)
            precision = p1 + p2 + 1;
        else if constexpr (is_division)
            precision = p1 - s1 + s2 + std::max<size_t>(6, s1 + p2 + 1);
        else if constexpr (is_modulo)
            precision = std::min<size_t>(p1 - s1, p2 - s2) + std::max<size_t>(s1, s2);
        else
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Unknown decimal binary operation");

        if (precision > DataTypeDecimal128::maxPrecision())
            return true;

        return false;
    }

    template <typename LeftDataType, typename RightDataType, typename ResultDataType>
    static ColumnPtr executeDecimal(
        const ColumnsWithTypeAndName & arguments,
        const LeftDataType & left_type,
        const RightDataType & right_type,
        const ResultDataType & result_type)
    {
        using LeftFieldType = typename LeftDataType::FieldType;
        using RightFieldType = typename RightDataType::FieldType;
        using ResultFieldType = typename ResultDataType::FieldType;
        using ColVecLeft = ColumnDecimal<LeftFieldType>;
        using ColVecRight = ColumnDecimal<RightFieldType>;

        ColumnPtr col_left = arguments[0].column;
        ColumnPtr col_right = arguments[1].column;

        const ColumnConst * col_left_const = checkAndGetColumnConst<ColVecLeft>(col_left.get());
        const ColumnConst * col_right_const = checkAndGetColumnConst<ColVecRight>(col_right.get());
        const ColVecLeft * col_left_vec = checkAndGetColumn<ColVecLeft>(col_left.get());
        const ColVecRight * col_right_vec = checkAndGetColumn<ColVecRight>(col_right.get());

        size_t rows = col_left->size();
        size_t max_scale = getMaxScaled(left_type.getScale(), right_type.getScale(), result_type.getScale());

        bool calculate_with_i256 = false;
        if constexpr (Mode != OpMode::Effect)
        {
            if (shouldPromoteTo256(left_type, right_type, result_type))
                calculate_with_i256 = true;

            if (is_division && max_scale - left_type.getScale() + max_scale > ResultDataType::maxPrecision())
                calculate_with_i256 = true;
        }

        auto p1 = left_type.getPrecision();
        auto p2 = right_type.getPrecision();
        if (DataTypeDecimal<LeftFieldType>::maxPrecision() < p1 + max_scale - left_type.getScale()
            || DataTypeDecimal<RightFieldType>::maxPrecision() < p2 + max_scale - right_type.getScale())
            calculate_with_i256 = true;

        if (calculate_with_i256)
        {
            /// Use Int256 for calculation
            return executeDecimalImpl<LeftDataType, RightDataType, ResultDataType, Int256>(
                left_type, right_type, col_left_const, col_right_const, col_left_vec, col_right_vec, rows, result_type);
        }
        else if constexpr (is_division)
        {
            /// Use Int128 for calculation
            return executeDecimalImpl<LeftDataType, RightDataType, ResultDataType, Int128>(
                left_type, right_type, col_left_const, col_right_const, col_left_vec, col_right_vec, rows, result_type);
        }
        else
        {
            /// Use ResultNativeType for calculation
            return executeDecimalImpl<LeftDataType, RightDataType, ResultDataType, NativeType<ResultFieldType>>(
                left_type, right_type, col_left_const, col_right_const, col_left_vec, col_right_vec, rows, result_type);
        }
    }

private:
    template <typename LeftDataType, typename RightDataType, typename ResultDataType, typename ScaledNativeType>
    static ColumnPtr executeDecimalImpl(
        const LeftDataType & left_type,
        const RightDataType & right_type,
        const ColumnConst * col_left_const,
        const ColumnConst * col_right_const,
        const ColumnDecimal<typename LeftDataType::FieldType> * col_left_vec,
        const ColumnDecimal<typename RightDataType::FieldType> * col_right_vec,
        size_t rows,
        const ResultDataType & result_type)
    {
        using LeftFieldType = typename LeftDataType::FieldType;
        using RightFieldType = typename RightDataType::FieldType;
        using ResultFieldType = typename ResultDataType::FieldType;
        using ColVecResult = ColumnVectorOrDecimal<ResultFieldType>;

        size_t max_scale = getMaxScaled(left_type.getScale(), right_type.getScale(), result_type.getScale());

        ScaledNativeType scale_left = [&]
        {
            if constexpr (is_multiply)
                return ScaledNativeType{1};

            auto diff = max_scale - left_type.getScale();
            if constexpr (is_division)
                return DecimalUtils::scaleMultiplier<ScaledNativeType>(diff + max_scale);
            else
                return DecimalUtils::scaleMultiplier<ScaledNativeType>(diff);
        }();

        ScaledNativeType scale_right = [&]
        {
            if constexpr (is_multiply)
                return ScaledNativeType{1};
            else
                return DecimalUtils::scaleMultiplier<ScaledNativeType>(max_scale - right_type.getScale());
        }();

        ScaledNativeType unscale_result = [&]
        {
            auto result_scale = result_type.getScale();
            auto diff = max_scale - result_scale;
            chassert(diff >= 0);
            return DecimalUtils::scaleMultiplier<ScaledNativeType>(diff);
        }();

        ScaledNativeType max_value = intExp10OfSize<ScaledNativeType>(result_type.getPrecision());

        auto res_vec = ColVecResult::create(rows, result_type.getScale());
        auto & res_vec_data = res_vec->getData();
        auto res_null_map = ColumnUInt8::create(rows, 0);
        auto & res_nullmap_data = res_null_map->getData();

        if (col_left_vec && col_right_vec)
        {
                process<OpCase::Vector>(
                    col_left_vec->getData().data(),
                    col_right_vec->getData().data(),
                    res_vec_data,
                    res_nullmap_data,
                    rows,
                    scale_left,
                    scale_right,
                    unscale_result,
                    max_value);
        }
        else if (col_left_const && col_right_vec)
        {
            LeftFieldType left_value = col_left_const->getValue<LeftFieldType>();
            process<OpCase::LeftConstant>(
                &left_value,
                col_right_vec->getData().data(),
                res_vec_data,
                res_nullmap_data,
                rows,
                scale_left,
                scale_right,
                unscale_result,
                max_value);
        }
        else if (col_left_vec && col_right_const)
        {
            RightFieldType right_value = col_right_const->getValue<RightFieldType>();
            process<OpCase::RightConstant>(
                col_left_vec->getData().data(),
                &right_value,
                res_vec_data,
                res_nullmap_data,
                rows,
                scale_left,
                scale_right,
                unscale_result,
                max_value);
        }
        else
            throw Exception(
                ErrorCodes::LOGICAL_ERROR,
                "Unexpected argument types {} {} {}",
                left_type.getName(),
                right_type.getName(),
                result_type.getName());

        return ColumnNullable::create(std::move(res_vec), std::move(res_null_map));
    }

        template <
            OpCase op_case,
            typename LeftFieldType,
            typename RightFieldType,
            typename ResultFieldType,
            typename ScaledNativeType>
        static void NO_INLINE process(
            const LeftFieldType * __restrict left_data, // maybe scalar or vector
            const RightFieldType * __restrict right_data, // maybe scalar or vector
            PaddedPODArray<ResultFieldType> & __restrict res_vec_data, // should be vector
            NullMap & res_nullmap_data,
            size_t rows,
            const ScaledNativeType & scale_left,
            const ScaledNativeType & scale_right,
            const ScaledNativeType & unscale_result,
            const ScaledNativeType & max_value)
        {
            using ResultNativeType = NativeType<ResultFieldType>;

            if constexpr (op_case == OpCase::Vector)
            {
                for (size_t i = 0; i < rows; ++i)
                    res_nullmap_data[i] = !calculate(
                        static_cast<ScaledNativeType>(unwrap<op_case == OpCase::LeftConstant>(left_data, i)),
                        static_cast<ScaledNativeType>(unwrap<op_case == OpCase::RightConstant>(right_data, i)),
                        scale_left,
                        scale_right,
                        unscale_result,
                        max_value,
                        res_vec_data[i].value);
            }
            else if constexpr (op_case == OpCase::LeftConstant)
            {
                ScaledNativeType scaled_left
                    = applyScaled(static_cast<ScaledNativeType>(unwrap<op_case == OpCase::LeftConstant>(left_data, 0)), scale_left);

                for (size_t i = 0; i < rows; ++i)
                    res_nullmap_data[i] = !calculate(
                        scaled_left,
                        static_cast<ScaledNativeType>(unwrap<op_case == OpCase::RightConstant>(right_data, i)),
                        static_cast<ScaledNativeType>(1),
                        scale_right,
                        unscale_result,
                        max_value,
                        res_vec_data[i].value);
            }
            else if constexpr (op_case == OpCase::RightConstant)
            {
                ScaledNativeType scaled_right
                    = applyScaled(static_cast<ScaledNativeType>(unwrap<op_case == OpCase::RightConstant>(right_data, 0)), scale_right);

                for (size_t i = 0; i < rows; ++i)
                    res_nullmap_data[i] = !calculate(
                        static_cast<ScaledNativeType>(unwrap<op_case == OpCase::LeftConstant>(left_data, i)),
                        scaled_right,
                        scale_left,
                        static_cast<ScaledNativeType>(1),
                        unscale_result,
                        max_value,
                        res_vec_data[i].value);
            }
    }

    template <
        typename ScaledNativeType,
        typename ResultNativeType>
    static ALWAYS_INLINE bool calculate(
        const ScaledNativeType & left,
        const ScaledNativeType & right,
        const ScaledNativeType & scale_left,
        const ScaledNativeType & scale_right,
        const ScaledNativeType & unscale_result,
        const ScaledNativeType & max_value,
        ResultNativeType & res)
    {
        auto scaled_left = scale_left > 1 ? applyScaled(left, scale_left) : left;
        auto scaled_right = scale_right > 1 ? applyScaled(right, scale_right) : right;

        ScaledNativeType c_res = 0;
        auto success = Operation::template apply<>(scaled_left, scaled_right, c_res);
        if (!success)
            return false;

        if (unscale_result > 1)
            c_res = applyUnscaled(c_res, unscale_result);

        res = static_cast<ResultNativeType>(c_res);

        if constexpr (std::is_same_v<ScaledNativeType, Int256> || is_division)
            return c_res > -max_value && c_res < max_value;
        else
            return true;
    }

    /// Unwrap underlying native type from decimal type
    template <bool is_scalar, typename E>
    static auto unwrap(const E * elem, size_t i)
    {
        if constexpr (is_scalar)
            return elem->value;
        else
            return elem[i].value;
    }


    template <typename T>
    static ALWAYS_INLINE T applyScaled(T n, T scale)
    {
        chassert(scale != 0);

        T res;
        DecimalMultiplyImpl::apply(n, scale, res);
        return res;
    }

    template <typename T>
    static ALWAYS_INLINE T applyUnscaled(T n, T scale)
    {
        chassert(scale != 0);

        T res;
        DecimalDivideImpl::apply(n, scale, res);
        return res;
    }
};

template <class Operation, typename Name, OpMode mode = OpMode::Default>
class SparkFunctionDecimalBinaryArithmetic final : public IFunction
{
    static constexpr bool is_plus = SparkIsOperation<Operation>::plus;
    static constexpr bool is_minus = SparkIsOperation<Operation>::minus;
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

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        if (arguments.size() != 3)
            throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, "Function '{}' expects 3 arguments", getName());

        if (!isDecimal(arguments[0]) || !isDecimal(arguments[1]) || !isDecimal(arguments[2]))
            throw Exception(
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "Illegal type {} {} {} of argument of function {}",
                arguments[0]->getName(),
                arguments[1]->getName(),
                arguments[2]->getName(),
                getName());

        return makeNullable(arguments[2]);
    }

    // executeImpl2
    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t) const override
    {
        const auto & left_argument = arguments[0];
        const auto & right_argument = arguments[1];

        const auto * left_generic = left_argument.type.get();
        const auto * right_generic = right_argument.type.get();

        ColumnPtr res;
        bool valid = castTripleTypes(
            left_generic,
            right_generic,
            removeNullable(arguments[2].type).get(),
            [&](const auto & left, const auto & right, const auto & result) {
                return (res = SparkDecimalBinaryOperation<Operation, mode>::template executeDecimal<>(arguments, left, right, result))
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

#if USE_EMBEDDED_COMPILER
    virtual ColumnNumbers getArgumentsThatDontParticipateInCompilation(const DataTypes & /*types*/) const { return {2}; }

    bool isCompilableImpl(const DataTypes & arguments, const DataTypePtr & result_type) const override
    {
        const auto & denull_left_type = arguments[0];
        const auto & denull_right_type = arguments[1];
        const auto & denull_result_type = removeNullable(result_type);
        if (!canBeNativeType(denull_left_type) || !canBeNativeType(denull_right_type) || !canBeNativeType(denull_result_type))
            return false;

        return castTripleTypes(
            denull_left_type.get(),
            denull_right_type.get(),
            denull_result_type.get(),
            [&](const auto & left_type, const auto & right_type, const auto & result_type)
            {
                using LeftDataType = std::decay_t<decltype(left_type)>;
                using RightDataType = std::decay_t<decltype(right_type)>;
                using ResultDataType = std::decay_t<decltype(result_type)>;
                using LeftFieldType = typename LeftDataType::FieldType;
                using RightFieldType = typename RightDataType::FieldType;
                using ResultFieldType = typename ResultDataType::FieldType;

                size_t max_scale = SparkDecimalBinaryOperation<Operation, mode>::getMaxScaled(
                    left_type.getScale(), right_type.getScale(), result_type.getScale());
                auto p1 = left_type.getPrecision();
                auto p2 = right_type.getPrecision();
                if (DataTypeDecimal<LeftFieldType>::maxPrecision() < p1 + max_scale - left_type.getScale()
                    || DataTypeDecimal<RightFieldType>::maxPrecision() < p2 + max_scale - right_type.getScale())
                    return false;

                if (SparkDecimalBinaryOperation<Operation, mode>::shouldPromoteTo256(left_type, right_type, result_type)
                    || (is_division && max_scale - left_type.getScale() + max_scale > ResultDataType::maxPrecision()))
                    return false;

                return true;
            });
    }

    llvm::Value *
    compileImpl(llvm::IRBuilderBase & builder, const ValuesWithType & arguments, const DataTypePtr & result_type) const override
    {
        const auto & denull_left_type = arguments[0].type;
        const auto & denull_right_type = arguments[1].type;
        const auto & denull_result_type = removeNullable(result_type);
        llvm::Value * nullable_result = nullptr;

        castTripleTypes(
            denull_left_type.get(),
            denull_right_type.get(),
            denull_result_type.get(),
            [&](const auto & left_type, const auto & right_type, const auto & result_type)
            {
                using LeftDataType = std::decay_t<decltype(left_type)>;
                using RightDataType = std::decay_t<decltype(right_type)>;
                using ResultDataType = std::decay_t<decltype(result_type)>;
                using LeftFieldType = typename LeftDataType::FieldType;
                using RightFieldType = typename RightDataType::FieldType;
                using ResultFieldType = typename ResultDataType::FieldType;
                using LeftNativeType = NativeType<LeftFieldType>;
                using RightNativeType = NativeType<RightFieldType>;
                using ResultNativeType = NativeType<ResultFieldType>;

                size_t max_scale = SparkDecimalBinaryOperation<Operation, mode>::getMaxScaled(
                    left_type.getScale(), right_type.getScale(), result_type.getScale());
                auto p1 = left_type.getPrecision();
                auto p2 = right_type.getPrecision();
                bool calculate_with_256 = false;
                if (DataTypeDecimal<LeftFieldType>::maxPrecision() < p1 + max_scale - left_type.getScale()
                    || DataTypeDecimal<RightFieldType>::maxPrecision() < p2 + max_scale - right_type.getScale())
                    calculate_with_256 = true;

                if (SparkDecimalBinaryOperation<Operation, mode>::shouldPromoteTo256(left_type, right_type, result_type)
                    || (is_division && max_scale - left_type.getScale() + max_scale > ResultDataType::maxPrecision()) || calculate_with_256)
                    nullable_result = compileHelper<Int256>(builder, arguments, left_type, right_type, result_type);
                    // nullable_result = compileHelper<Int128>(builder, arguments, left_type, right_type, result_type);
                else if (is_division)
                    nullable_result = compileHelper<Int128>(builder, arguments, left_type, right_type, result_type);
                else
                    nullable_result = compileHelper<ResultNativeType>(builder, arguments, left_type, right_type, result_type);

                return true;
            });

        return nullable_result;
    }

    template <typename CalculateType, typename LeftDataType, typename RightDataType, typename ResultDataType>
    static llvm::Value * compileHelper(
        llvm::IRBuilderBase & builder,
        const ValuesWithType & arguments,
        const LeftDataType & left_type,
        const RightDataType & right_type,
        const ResultDataType & result_type)
    {
        auto & b = static_cast<llvm::IRBuilder<> &>(builder);
        DataTypePtr calculate_type = std::make_shared<DataTypeNumber<CalculateType>>();

        auto * left = nativeCast(b, arguments[0], calculate_type);
        auto * right = nativeCast(b, arguments[1], calculate_type);

        size_t max_scale = SparkDecimalBinaryOperation<Operation, mode>::getMaxScaled(
            left_type.getScale(), right_type.getScale(), result_type.getScale());

        CalculateType scale_left = [&]
        {
            if constexpr (is_multiply)
                return CalculateType{1};

            auto diff = max_scale - left_type.getScale();
            if constexpr (is_division)
                return DecimalUtils::scaleMultiplier<CalculateType>(diff + max_scale);
            else
                return DecimalUtils::scaleMultiplier<CalculateType>(diff);
        }();

        CalculateType scale_right = [&]
        {
            if constexpr (is_multiply)
                return CalculateType{1};
            else
                return DecimalUtils::scaleMultiplier<CalculateType>(max_scale - right_type.getScale());
        }();

        auto * scaled_left = b.CreateMul(left, getNativeConstant(b, scale_left));
        auto * scaled_right = b.CreateMul(right, getNativeConstant(b, scale_right));

        llvm::Value * scaled_result = nullptr;
        llvm::Value * is_null = llvm::ConstantInt::getFalse(b.getContext());
        if constexpr (is_plus)
            scaled_result = b.CreateAdd(scaled_left, scaled_right);
        else if constexpr (is_minus)
            scaled_result = b.CreateSub(scaled_left, scaled_right);
        else if constexpr (is_multiply)
            scaled_result = b.CreateMul(scaled_left, scaled_right);
        else if constexpr (is_division)
        {
            auto * zero = getNativeConstant(b, static_cast<CalculateType>(0));
            auto * is_zero = b.CreateICmpEQ(scaled_right, zero);

            scaled_result = b.CreateSDiv(scaled_left, scaled_right);
            is_null = is_zero;
        }
        else if constexpr (is_modulo)
        {
            auto * zero = getNativeConstant(b, static_cast<CalculateType>(0));
            auto * is_zero = b.CreateICmpEQ(scaled_right, zero);

            scaled_result = b.CreateSRem(scaled_left, scaled_right);
            is_null = is_zero;
        }

        auto result_scale = result_type.getScale();
        auto scale_diff = max_scale - result_scale;
        auto * unscaled_result = scaled_result;
        if (scale_diff)
        {
            auto scaled_diff = DecimalUtils::scaleMultiplier<CalculateType>(scale_diff);
            unscaled_result = b.CreateSDiv(scaled_result, getNativeConstant(b, scaled_diff));
        }

        /// check overflow
        if constexpr (std::is_same_v<CalculateType, Int256> || is_division)
        {
            auto max_value = intExp10OfSize<CalculateType>(result_type.getPrecision());
            auto * max_value_const = getNativeConstant(b, max_value);
            auto * is_overflow = b.CreateOr(
                b.CreateICmpSGE(unscaled_result, max_value_const), b.CreateICmpSLE(unscaled_result, b.CreateNeg(max_value_const)));
            auto * overflow_result = getNativeConstant(b, static_cast<CalculateType>(0));
            is_null = b.CreateOr(is_null, is_overflow);
        }

        auto * result = nativeCast(b, calculate_type, unscaled_result, result_type.getPtr());
        auto * nullable_type = toNativeType(b, makeNullable(result_type.getPtr()));
        auto * nullable_result = llvm::Constant::getNullValue(nullable_type);
        auto * nullablel_result_with_value = b.CreateInsertValue(nullable_result, result, {0});
        return b.CreateInsertValue(nullablel_result_with_value, is_null, {1});
    }

    template <is_integer T>
    static llvm::Constant * getNativeConstant(llvm::IRBuilderBase & builder, T element)
    {
        auto * type = llvm::Type::getIntNTy(builder.getContext(), sizeof(T) * 8);
        if constexpr (std::is_integral_v<T>)
        {
            return llvm::ConstantInt::get(type, static_cast<uint64_t>(element), true);
        }
        else
        {
            llvm::APInt value(type->getIntegerBitWidth(), element.items);
            return llvm::ConstantInt::get(type, value);
        }
    }
#endif // USE_EMBEDDED_COMPILER

private:
    template <typename F>
    static bool castTripleTypes(const IDataType * left, const IDataType * right, const IDataType * result, F && f)
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

}
}
