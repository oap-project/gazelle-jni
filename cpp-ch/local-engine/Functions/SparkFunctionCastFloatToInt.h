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
#include <DataTypes/Native.h>
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

template <is_integer T, typename Name>
class SparkFunctionCastFloatToInt : public DB::IFunction
{
public:
    static constexpr auto name = Name::name;
    static DB::FunctionPtr create(DB::ContextPtr) { return std::make_shared<SparkFunctionCastFloatToInt>(); }

    SparkFunctionCastFloatToInt() = default;
    ~SparkFunctionCastFloatToInt() override = default;

    String getName() const override { return name; }
    size_t getNumberOfArguments() const override { return 1; }
    bool useDefaultImplementationForConstants() const override { return true; }
    bool isSuitableForShortCircuitArgumentsExecution(const DB::DataTypesWithConstInfo & /*arguments*/) const override { return true; }

    DB::DataTypePtr getReturnTypeImpl(const DB::DataTypes & arguments) const override
    {
        if (arguments.size() != 1)
            throw DB::Exception(DB::ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, "Function {}'s arguments number must be 1", name);

        if (!isFloat(removeNullable(arguments[0])))
            throw DB::Exception(DB::ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Function {}'s 1st argument must be float type", name);

        return makeNullable(std::make_shared<const DB::DataTypeNumber<T>>());
    }

    DB::ColumnPtr executeImpl(const DB::ColumnsWithTypeAndName & arguments, const DB::DataTypePtr & result_type, size_t input_rows_count) const override
    {
        DB::ColumnPtr src_col = arguments[0].column;
        size_t size = input_rows_count;

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

    MULTITARGET_FUNCTION_AVX2_SSE42(
        MULTITARGET_FUNCTION_HEADER(template <typename F> static void NO_SANITIZE_UNDEFINED NO_INLINE),
        vectorImpl,
        MULTITARGET_FUNCTION_BODY(
            (F int_min,
             F int_max,
             const DB::PaddedPODArray<F> & src_data,
             DB::PaddedPODArray<T> & data,
             DB::PaddedPODArray<UInt8> & null_map_data,
             size_t rows) /// NOLINT
            {
                for (size_t i = 0; i < rows; ++i)
                {
                    null_map_data[i] = !isFinite(src_data[i]);
                    data[i] = static_cast<T>(std::fmax(int_min, std::fmin(int_max, src_data[i])));
                }
            }))

    template <typename F>
    static void NO_INLINE vector(
        F int_min,
        F int_max,
        const DB::PaddedPODArray<F> & src_data,
        DB::PaddedPODArray<T> & data,
        DB::PaddedPODArray<UInt8> & null_map_data,
        size_t rows)
    {
#if USE_MULTITARGET_CODE
        if (isArchSupported(DB::TargetArch::AVX2))
        {
            vectorImplAVX2(int_min, int_max, src_data, data, null_map_data, rows);
            return;
        }

        if (isArchSupported(DB::TargetArch::SSE42))
        {
            vectorImplSSE42(int_min, int_max, src_data, data, null_map_data, rows);
            return;
        }
#endif

        vectorImpl(int_min, int_max, src_data, data, null_map_data, rows);
    }

    template <typename F>
    void executeInternal(const DB::ColumnPtr & src, DB::PaddedPODArray<T> & data, DB::PaddedPODArray<UInt8> & null_map_data) const
    {
        const DB::ColumnVector<F> * src_vec = assert_cast<const DB::ColumnVector<F> *>(src.get());

        size_t rows = src_vec->size();
        const auto & src_data = src_vec->getData();
        const auto int_min = static_cast<F>(std::numeric_limits<T>::min());
        const auto int_max = static_cast<F>(std::numeric_limits<T>::max());
        vector(int_min, int_max, src_data, data, null_map_data, rows);
    }

#if USE_EMBEDDED_COMPILER
    bool isCompilableImpl(const DB::DataTypes & types, const DB::DataTypePtr & result_type) const override
    {
        if (types.size() != 1)
            return false;

        if (!canBeNativeType(types[0]) || !canBeNativeType(result_type))
            return false;

        return true;
    }


    llvm::Value *
    compileImpl(llvm::IRBuilderBase & builder, const DB::ValuesWithType & arguments, const DB::DataTypePtr & result_type) const override
    {
        auto & b = static_cast<llvm::IRBuilder<> &>(builder);
        llvm::Value * src_value = arguments[0].value;

        auto * int_type = toNativeType(b, removeNullable(result_type));
        llvm::Type * float_type = src_value->getType();

        llvm::Value * is_nan = b.CreateFCmpUNO(src_value, src_value);
        llvm::Value * is_inf = b.CreateOr(
            b.CreateFCmpOEQ(src_value, llvm::ConstantFP::getInfinity(float_type, false)),
            b.CreateFCmpOEQ(src_value, llvm::ConstantFP::getInfinity(float_type, true)));

        bool is_signed = std::is_signed_v<T>;
        llvm::Value * result_value = is_signed_v<T> ? b.CreateFPToSI(src_value, int_type) : b.CreateFPToUI(src_value, int_type);
        llvm::Value * result_is_null = b.CreateOr(is_nan, is_inf);

        auto * nullable_structure_type = toNativeType(b, result_type);
        auto * nullable_structure_value = llvm::Constant::getNullValue(nullable_structure_type);
        auto * nullable_structure_with_result_value = b.CreateInsertValue(nullable_structure_value, result_value, {0});
        return b.CreateInsertValue(nullable_structure_with_result_value, result_is_null, {1});
    }
#endif // USE_EMBEDDED_COMPILER

};

}