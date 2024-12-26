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
#include "SparkFunctionCheckDecimalOverflow.h"

#include <Columns/ColumnDecimal.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnsNumber.h>
#include <Core/DecimalFunctions.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypesDecimal.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/IFunction.h>

namespace DB
{
namespace ErrorCodes
{
extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
extern const int ILLEGAL_TYPE_OF_ARGUMENT;
extern const int ILLEGAL_COLUMN;
extern const int TYPE_MISMATCH;
}
}


namespace local_engine
{
using namespace DB;

struct CheckDecimalOverflowSpark
{
    static constexpr auto name = "checkDecimalOverflowSpark";
};
struct CheckDecimalOverflowSparkOrNull
{
    static constexpr auto name = "checkDecimalOverflowSparkOrNull";
};

enum class CheckExceptionMode
{
    Throw, /// Throw exception if value cannot be parsed.
    Null /// Return ColumnNullable with NULLs when value cannot be parsed.
};

namespace
{
/// Returns received decimal value if and Decimal value has less digits then it's Precision allow, 0 otherwise.
/// Precision could be set as second argument or omitted. If omitted function uses Decimal precision of the first argument.
template <typename Name, CheckExceptionMode mode>
class FunctionCheckDecimalOverflow : public IFunction
{
public:
    static constexpr auto name = Name::name;
    static constexpr auto exception_mode = mode;

    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionCheckDecimalOverflow>(); }

    String getName() const override { return name; }
    size_t getNumberOfArguments() const override { return 3; }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return false; }
    bool useDefaultImplementationForConstants() const override { return true; }
    ColumnNumbers getArgumentsThatAreAlwaysConstant() const override { return {1, 2}; }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        if (!isDecimal(arguments[0].type) || !isInteger(arguments[1].type) || !isInteger(arguments[2].type))
            throw Exception(
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "Illegal type {} {} {} of argument of function {}",
                arguments[0].type->getName(),
                arguments[1].type->getName(),
                arguments[2].type->getName(),
                getName());

        UInt32 precision = extractArgument(arguments[1]);
        UInt32 scale = extractArgument(arguments[2]);

        auto return_type = createDecimal<DataTypeDecimal>(precision, scale);
        if constexpr (exception_mode == CheckExceptionMode::Null)
        {
            if (!arguments[0].type->isNullable())
                return std::make_shared<DataTypeNullable>(return_type);
        }

        return return_type;
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
    {
        const auto & src_column = arguments[0];
        UInt32 precision = extractArgument(arguments[1]);
        UInt32 scale = extractArgument(arguments[2]);

        ColumnPtr result_column;

        auto call = [&](const auto & types) -> bool
        {
            using Types = std::decay_t<decltype(types)>;
            using FromDataType = typename Types::LeftType;
            using ToDataType = typename Types::RightType;

            if constexpr (IsDataTypeDecimal<FromDataType>)
            {
                using FromFieldType = typename FromDataType::FieldType;
                using ColVecType = ColumnDecimal<FromFieldType>;

                if (const ColVecType * col_vec = checkAndGetColumn<ColVecType>(src_column.column.get()))
                {
                    executeInternal<FromFieldType, ToDataType>(*col_vec, result_column, input_rows_count, precision, scale);
                    return true;
                }
            }

            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Illegal column while execute function {}", getName());
        };

        if (precision <= DecimalUtils::max_precision<Decimal32>)
            callOnIndexAndDataType<DataTypeDecimal<Decimal32>>(src_column.type->getTypeId(), call);
        else if (precision <= DecimalUtils::max_precision<Decimal64>)
            callOnIndexAndDataType<DataTypeDecimal<Decimal64>>(src_column.type->getTypeId(), call);
        else if (precision <= DecimalUtils::max_precision<Decimal128>)
            callOnIndexAndDataType<DataTypeDecimal<Decimal128>>(src_column.type->getTypeId(), call);
        else
            callOnIndexAndDataType<DataTypeDecimal<Decimal256>>(src_column.type->getTypeId(), call);


        if (!result_column)
            throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Wrong call for {} with {}", getName(), src_column.type->getName());

        return result_column;
    }

private:
    template <typename T, typename ToDataType>
    static void executeInternal(
        const ColumnDecimal<T> & col_source, ColumnPtr & result_column, size_t input_rows_count, UInt32 precision, UInt32 scale_to)
    {
        using ToFieldType = typename ToDataType::FieldType;
        using ToColumnType = typename ToDataType::ColumnType;

        ColumnUInt8::MutablePtr col_null_map_to;
        ColumnUInt8::Container * vec_null_map_to [[maybe_unused]] = nullptr;
        auto scale_from = col_source.getScale();

        if constexpr (exception_mode == CheckExceptionMode::Null)
        {
            col_null_map_to = ColumnUInt8::create(input_rows_count, false);
            vec_null_map_to = &col_null_map_to->getData();
        }

        typename ToColumnType::MutablePtr col_to = ToColumnType::create(input_rows_count, scale_to);
        auto & vec_to = col_to->getData();
        vec_to.resize_exact(input_rows_count);

        auto & datas = col_source.getData();
        for (size_t i = 0; i < input_rows_count; ++i)
        {
            // bool overflow = outOfDigits<T>(datas[i], precision, scale_from, scale_to);
            ToFieldType result;
            bool success = convertToDecimalImpl<T, ToDataType>(datas[i], precision, scale_from, scale_to, result);

            if (success)
                vec_to[i] = static_cast<ToFieldType>(result);
            else
            {
                vec_to[i] = static_cast<ToFieldType>(0);
                if constexpr (exception_mode == CheckExceptionMode::Null)
                    (*vec_null_map_to)[i] = static_cast<UInt8>(1);
                else
                    throw Exception(ErrorCodes::DECIMAL_OVERFLOW, "Decimal value is overflow.");
            }
        }

        if constexpr (exception_mode == CheckExceptionMode::Null)
            result_column = ColumnNullable::create(std::move(col_to), std::move(col_null_map_to));
        else
            result_column = std::move(col_to);
    }

    template <is_decimal FromFieldType, typename ToDataType>
    requires(IsDataTypeDecimal<ToDataType>)
    static bool convertToDecimalImpl(
        const FromFieldType & decimal, UInt32 precision_to, UInt32 scale_from, UInt32 scale_to, typename ToDataType::FieldType & result)
    {
        if constexpr (std::is_same_v<FromFieldType, Decimal32>)
            return convertDecimalsImpl<DataTypeDecimal<Decimal32>, ToDataType>(decimal, precision_to, scale_from, scale_to, result);

        else if constexpr (std::is_same_v<FromFieldType, Decimal64>)
            return convertDecimalsImpl<DataTypeDecimal<Decimal64>, ToDataType>(decimal, precision_to, scale_from, scale_to, result);
        else if constexpr (std::is_same_v<FromFieldType, Decimal128>)
            return convertDecimalsImpl<DataTypeDecimal<Decimal128>, ToDataType>(decimal, precision_to, scale_from, scale_to, result);
        else
            return convertDecimalsImpl<DataTypeDecimal<Decimal256>, ToDataType>(decimal, precision_to, scale_from, scale_to, result);
    }

    template <typename FromDataType, typename ToDataType>
    requires(IsDataTypeDecimal<FromDataType> && IsDataTypeDecimal<ToDataType>)
    static bool convertDecimalsImpl(
        const typename FromDataType::FieldType & value,
        UInt32 precision_to,
        UInt32 scale_from,
        UInt32 scale_to,
        typename ToDataType::FieldType & result)
    {
        using FromFieldType = typename FromDataType::FieldType;
        using ToFieldType = typename ToDataType::FieldType;
        using MaxFieldType = std::conditional_t<(sizeof(FromFieldType) > sizeof(ToFieldType)), FromFieldType, ToFieldType>;
        using MaxNativeType = typename MaxFieldType::NativeType;


        auto false_value = []() -> bool
        {
            if constexpr (exception_mode == CheckExceptionMode::Null)
                return false;
            else
                throw Exception(ErrorCodes::DECIMAL_OVERFLOW, "Decimal value is overflow.");
        };

        MaxNativeType converted_value;
        if (scale_to > scale_from)
        {
            converted_value = DecimalUtils::scaleMultiplier<MaxNativeType>(scale_to - scale_from);
            if (common::mulOverflow(static_cast<MaxNativeType>(value.value), converted_value, converted_value))
                return false_value();
        }
        else if (scale_to == scale_from)
            converted_value = value.value;
        else
            converted_value = value.value / DecimalUtils::scaleMultiplier<MaxNativeType>(scale_from - scale_to);

        // if constexpr (sizeof(FromFieldType) > sizeof(ToFieldType))
        // {
        MaxNativeType pow10 = intExp10OfSize<MaxNativeType>(precision_to);
        if (converted_value <= -pow10 || converted_value >= pow10)
            return false_value();
        // }

        result = static_cast<typename ToFieldType::NativeType>(converted_value);
        return true;
    }
};

using FunctionCheckDecimalOverflowThrow = FunctionCheckDecimalOverflow<CheckDecimalOverflowSpark, CheckExceptionMode::Throw>;
using FunctionCheckDecimalOverflowOrNull = FunctionCheckDecimalOverflow<CheckDecimalOverflowSparkOrNull, CheckExceptionMode::Null>;
}

REGISTER_FUNCTION(CheckDecimalOverflowSpark)
{
    factory.registerFunction<FunctionCheckDecimalOverflowThrow>(FunctionDocumentation{.description = R"(
Check decimal precision is overflow. If overflow throws exception.
)"});

    factory.registerFunction<FunctionCheckDecimalOverflowOrNull>(FunctionDocumentation{.description = R"(
Check decimal precision is overflow. If overflow return `NULL`.
)"});
}
}
