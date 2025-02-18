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
#include <Columns/IColumn.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeNumberBase.h>
#include <Functions/FunctionBinaryArithmetic.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/castTypeToEither.h>


namespace DB
{
namespace ErrorCodes
{
extern const int ILLEGAL_TYPE_OF_ARGUMENT;
extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}
}

namespace local_engine
{
template <typename A, typename B>
struct SparkDivideFloatingImpl
{
    using ResultType = typename DB::NumberTraits::ResultOfFloatingPointDivision<A, B>::Type;
    static const constexpr bool allow_fixed_string = false;
    static const constexpr bool allow_string_integer = false;

    template <typename Result = ResultType>
    static inline NO_SANITIZE_UNDEFINED Result apply(A a, B b)
    {
        return static_cast<Result>(a) / b;
    }
};

class SparkFunctionDivide : public DB::IFunction
{
public:
    static constexpr auto name = "sparkDivide";
    static DB::FunctionPtr create(DB::ContextPtr) { return std::make_shared<SparkFunctionDivide>(); }

    SparkFunctionDivide() = default;
    ~SparkFunctionDivide() override = default;

    String getName() const override { return name; }
    size_t getNumberOfArguments() const override { return 2; }
    bool useDefaultImplementationForConstants() const override { return true; }
    bool isSuitableForShortCircuitArgumentsExecution(const DB::DataTypesWithConstInfo & /*arguments*/) const override { return true; }

    DB::DataTypePtr getReturnTypeImpl(const DB::DataTypes & arguments) const override
    {
        if (arguments.size() != 2)
            throw DB::Exception(DB::ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, "Function {}'s arguments number must be 2", name);

        /// Spark guarantees that the input arguments of sparkDivide are float64
        /// by transforming "ia/ib" to "cast(ia as float64)/cast(ib as float64)"
        DB::WhichDataType left(arguments[0]);
        DB::WhichDataType right(arguments[1]);
        if (!left.isFloat64() || !right.isFloat64())
            throw DB::Exception(DB::ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Function {}'s arguments type must be float64", name);

        return DB::makeNullable(std::make_shared<const DB::DataTypeFloat64>());
    }

    DB::ColumnPtr
    executeImpl(const DB::ColumnsWithTypeAndName & arguments, const DB::DataTypePtr & result_type, size_t input_rows_count) const override
    {
        using L = Float64;
        using R = Float64;
        using T = Float64;

        const DB::ColumnVector<L> * col_left = nullptr;
        const DB::ColumnVector<R> * col_right = nullptr;
        const DB::ColumnVector<L> * const_col_left = checkAndGetColumnConstData<DB::ColumnVector<L>>(arguments[0].column.get());
        const DB::ColumnVector<R> * const_col_right = checkAndGetColumnConstData<DB::ColumnVector<R>>(arguments[1].column.get());

        L left_const_val = 0;
        if (const_col_left)
            left_const_val = const_col_left->getElement(0);
        else
            col_left = assert_cast<const DB::ColumnVector<L> *>(arguments[0].column.get());

        R right_const_val = 0;
        if (const_col_right)
        {
            right_const_val = const_col_right->getElement(0);
            if (right_const_val == 0)
            {
                auto data_col = DB::ColumnVector<T>::create(1, 0);
                auto null_map_col = DB::ColumnVector<UInt8>::create(1, 1);
                return DB::ColumnConst::create(DB::ColumnNullable::create(std::move(data_col), std::move(null_map_col)), input_rows_count);
            }
        }
        else
            col_right = assert_cast<const DB::ColumnVector<R> *>(arguments[1].column.get());

        auto res_col = DB::ColumnVector<T>::create(input_rows_count, 0);
        auto res_null_map = DB::ColumnVector<UInt8>::create(input_rows_count, 0);
        DB::PaddedPODArray<T> & res_data = res_col->getData();
        DB::PaddedPODArray<UInt8> & res_null_map_data = res_null_map->getData();
        vector(col_left, col_right, left_const_val, right_const_val, res_data, res_null_map_data, input_rows_count);
        return DB::ColumnNullable::create(std::move(res_col), std::move(res_null_map));
    }

    MULTITARGET_FUNCTION_AVX2_SSE42(
        MULTITARGET_FUNCTION_HEADER(static void NO_SANITIZE_UNDEFINED NO_INLINE),
        vectorImpl,
        MULTITARGET_FUNCTION_BODY(
            (const DB::ColumnVector<Float64> * col_left,
             const DB::ColumnVector<Float64> * col_right,
             Float64 left_const_val,
             Float64 right_const_val,
             DB::PaddedPODArray<Float64> & res_data,
             DB::PaddedPODArray<UInt8> & res_null_map_data,
             size_t input_rows_count) /// NOLINT
            {
                if (col_left && col_right)
                {
                    const auto & ldata = col_left->getData();
                    const auto & rdata = col_right->getData();

                    for (size_t i = 0; i < input_rows_count; ++i)
                    {
                        auto l = ldata[i];
                        auto r = rdata[i];
                        res_data[i] = SparkDivideFloatingImpl<Float64, Float64>::apply(l, r ? r : 1);
                        res_null_map_data[i] = !rdata[i];
                    }
                }
                else if (col_left)
                {
                    Float64 r = right_const_val;
                    for (size_t i = 0; i < input_rows_count; ++i)
                    {
                        Float64 l = col_left->getData()[i];

                        /// r must not be zero because r = 0 is already processed in fast path
                        /// No need to assign null_map_data[i] = 0, because it is already 0
                        // res_null_map_data[i] = 0;
                        res_data[i] = SparkDivideFloatingImpl<Float64, Float64>::apply(l, r);
                    }
                }
                else if (col_right)
                {
                    Float64 l = left_const_val;
                    for (size_t i = 0; i < input_rows_count; ++i)
                    {
                        Float64 r = col_right->getData()[i];
                        res_null_map_data[i] = !r;
                        res_data[i] = SparkDivideFloatingImpl<Float64, Float64>::apply(l, r ? r : 1);
                    }
                }
            }))

    static void NO_INLINE vector(
        const DB::ColumnVector<Float64> * col_left,
        const DB::ColumnVector<Float64> * col_right,
        Float64 left_const_val,
        Float64 right_const_val,
        DB::PaddedPODArray<Float64> & res_data,
        DB::PaddedPODArray<UInt8> & res_null_map_data,
        size_t input_rows_count)
    {
#if USE_MULTITARGET_CODE
        if (isArchSupported(DB::TargetArch::AVX2))
        {
            vectorImplAVX2(col_left, col_right, left_const_val, right_const_val, res_data, res_null_map_data, input_rows_count);
            return;
        }

        if (isArchSupported(DB::TargetArch::SSE42))
        {
            vectorImplSSE42(col_left, col_right, left_const_val, right_const_val, res_data, res_null_map_data, input_rows_count);
            return;
        }
#endif

        vectorImpl(col_left, col_right, left_const_val, right_const_val, res_data, res_null_map_data, input_rows_count);
    }

#if USE_EMBEDDED_COMPILER
    bool isCompilableImpl(const DB::DataTypes & arguments, const DB::DataTypePtr & result_type) const override
    {
        if (2 != arguments.size())
            return false;

        if (!canBeNativeType(*arguments[0]) || !canBeNativeType(*arguments[1]) || !canBeNativeType(*result_type))
            return false;

        return true;
    }

    llvm::Value *
    compileImpl(llvm::IRBuilderBase & builder, const DB::ValuesWithType & arguments, const DB::DataTypePtr & result_type) const override
    {
        assert(2 == arguments.size());
        auto * left = arguments[0].value;
        auto * right = arguments[1].value;

        auto * zero = llvm::ConstantFP::get(right->getType(), 0.0);
        auto * neg_zero = llvm::ConstantFP::get(right->getType(), -0.0);
        auto * is_null = builder.CreateOr(builder.CreateFCmpOEQ(right, zero), builder.CreateFCmpOEQ(right, neg_zero));
        auto * result_value = builder.CreateFDiv(left, right);

        auto * nullable_structure_type = toNativeType(builder, makeNullable(result_type));
        auto * nullable_structure_value = llvm::Constant::getNullValue(nullable_structure_type);
        auto * nullable_structure_with_result_value = builder.CreateInsertValue(nullable_structure_value, result_value, 0);
        return builder.CreateInsertValue(nullable_structure_with_result_value, is_null, 1);
    }
#endif // USE_EMBEDDED_COMPILER
};
}
