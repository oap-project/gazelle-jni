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
    static inline NO_SANITIZE_UNDEFINED Result apply(A a [[maybe_unused]], B b [[maybe_unused]])
    {
        return static_cast<Result>(a) / b;
    }

#if USE_EMBEDDED_COMPILER
    static constexpr bool compilable = true;
    static inline llvm::Value * compile(llvm::IRBuilder<> & b, llvm::Value * left, llvm::Value * right, bool)
    {
        if (left->getType()->isIntegerTy())
            throw DB::Exception(DB::ErrorCodes::LOGICAL_ERROR, "SparkDivideFloatingImpl expected a floating-point type");
        return b.CreateFDiv(left, right);
    }
#endif
};

class SparkFunctionDivide : public DB::IFunction
{
public:
    size_t getNumberOfArguments() const override { return 2; }
    static constexpr auto name = "sparkDivide";
    static DB::FunctionPtr create(DB::ContextPtr) { return std::make_shared<SparkFunctionDivide>(); }
    SparkFunctionDivide() = default;
    ~SparkFunctionDivide() override = default;
    String getName() const override { return name; }
    bool useDefaultImplementationForConstants() const override { return true; }
    bool isSuitableForShortCircuitArgumentsExecution(const DB::DataTypesWithConstInfo & /*arguments*/) const override { return true; }

    DB::DataTypePtr getReturnTypeImpl(const DB::DataTypes &) const override
    {
        return DB::makeNullable(std::make_shared<const DB::DataTypeFloat64>());
    }

    DB::ColumnPtr executeImpl(const DB::ColumnsWithTypeAndName & arguments, const DB::DataTypePtr & result_type, size_t input_rows_count) const override
    {
        if (arguments.size() != 2)
            throw DB::Exception(DB::ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, "Function {}'s arguments number must be 2", name);
        if (!isNativeNumber(arguments[0].type) || !isNativeNumber(arguments[1].type))
        {
            throw DB::Exception(DB::ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Function {}'s arguments type must be native number", name);
        }
        using Types = TypeList<
            DB::DataTypeFloat32,
            DB::DataTypeFloat64,
            DB::DataTypeUInt8,
            DB::DataTypeUInt16,
            DB::DataTypeUInt32,
            DB::DataTypeUInt64,
            DB::DataTypeInt8,
            DB::DataTypeInt16,
            DB::DataTypeInt32,
            DB::DataTypeInt64>;
        
        DB::ColumnPtr result = nullptr;
        bool valid = castTypeToEither(Types{}, arguments[0].type.get(), [&](const auto & left_)
        {
            return castTypeToEither(Types{}, arguments[1].type.get(), [&](const auto & right_)
            {
                using L = typename std::decay_t<decltype(left_)>::FieldType;
                using R = typename std::decay_t<decltype(right_)>::FieldType;
                using T = typename DB::NumberTraits::ResultOfFloatingPointDivision<L, R>::Type;
                const DB::ColumnVector<L> * vec1 = nullptr;
                const DB::ColumnVector<R> * vec2 = nullptr;
                const DB::ColumnVector<L> * const_col_left = checkAndGetColumnConstData<DB::ColumnVector<L>>(arguments[0].column.get());
                const DB::ColumnVector<R> * const_col_right = checkAndGetColumnConstData<DB::ColumnVector<R>>(arguments[1].column.get());
                L left_const_val = 0;
                R right_const_val = 0;
                if (const_col_left)
                    left_const_val = const_col_left->getElement(0);
                else
                    vec1 = assert_cast<const DB::ColumnVector<L> *>(arguments[0].column.get());
                
                if (const_col_right)
                {
                    right_const_val = const_col_right->getElement(0);
                    if (right_const_val == 0)
                    {
                        auto data_col = DB::ColumnVector<T>::create(arguments[0].column->size(), 0);
                        auto null_map_col = DB::ColumnVector<UInt8>::create(arguments[0].column->size(), 1);
                        result = DB::ColumnNullable::create(std::move(data_col), std::move(null_map_col));
                        return true;
                    }
                }
                else
                    vec2 = assert_cast<const DB::ColumnVector<R> *>(arguments[1].column.get());

                auto vec3 = DB::ColumnVector<T>::create(input_rows_count, 0);
                auto null_map_col = DB::ColumnVector<UInt8>::create(input_rows_count, 0);
                DB::PaddedPODArray<T> & data = vec3->getData();
                DB::PaddedPODArray<UInt8> & null_map = null_map_col->getData();
                for (size_t i = 0; i < input_rows_count; ++i)
                {
                    L l = vec1 ? vec1->getElement(i) : left_const_val;
                    R r = vec2 ? vec2->getElement(i) : right_const_val;
                    if (r == 0)
                        null_map[i] = 1;
                    else
                        data[i] = SparkDivideFloatingImpl<L,R>::apply(l, r);
                }
                result = DB::ColumnNullable::create(std::move(vec3), std::move(null_map_col));
                return true;
            });
        });
        if (!valid)
            throw DB::Exception(DB::ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Function {}'s arguments type is not valid", name);
        return result;
    }
};
}
