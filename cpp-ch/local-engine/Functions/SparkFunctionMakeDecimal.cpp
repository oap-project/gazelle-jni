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
#include <Columns/ColumnNullable.h>
#include <Core/DecimalFunctions.h>
#include <Core/callOnTypeIndex.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypesDecimal.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/IFunction.h>
#include <Functions/SparkFunctionCheckDecimalOverflow.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int DECIMAL_OVERFLOW;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int ILLEGAL_COLUMN;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int TYPE_MISMATCH;
}
}


namespace local_engine
{
using namespace DB;

struct NameMakeDecimal
{
    static constexpr auto name = "makeDecimalSpark";
};
struct NameMakeDecimalOrNull
{
    static constexpr auto name = "makeDecimalSparkOrNull";
};

enum class ConvertExceptionMode
{
    Throw, /// Throw exception if value cannot be parsed.
    Null /// Return ColumnNullable with NULLs when value cannot be parsed.
};

namespace
{
    /// Create decimal with nested value, precision and scale. Required 3 arguments.
    /// If overflow, throw exceptions by default. Else use 'orNull' function will return null.
    template <typename Name, ConvertExceptionMode mode>
    class FunctionMakeDecimal : public IFunction
    {
    public:
        static constexpr auto name = Name::name;
        static constexpr auto exception_mode = mode;

        static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionMakeDecimal>(); }

        String getName() const override { return name; }
        size_t getNumberOfArguments() const override { return 3; }
        bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return false; }
        bool useDefaultImplementationForConstants() const override { return true; }
        ColumnNumbers getArgumentsThatAreAlwaysConstant() const override { return {1, 2}; }

        DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
        {
            if (!isInteger(arguments[0].type) || !isInteger(arguments[1].type) || !isInteger(arguments[2].type))
                throw Exception(
                    ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                    "Cannot format {} {} {} as decimal",
                    arguments[0].type->getName(),
                    arguments[1].type->getName(),
                    arguments[2].type->getName());

            DataTypePtr res = createDecimal<DataTypeDecimal>(extractArgument(arguments[1]), extractArgument(arguments[2]));
            if constexpr (exception_mode == ConvertExceptionMode::Null)
                return std::make_shared<DataTypeNullable>(res);
            else
                return res;
        }

        ColumnPtr
        executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t input_rows_count) const override
        {
            const auto & unscale_column = arguments[0];
            if (!unscale_column.column)
                throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Illegal column while execute function {}", getName());

            auto precision_value = extractArgument(arguments[1]);
            auto scale_value = extractArgument(arguments[2]);

            if (precision_value <= DecimalUtils::max_precision<Decimal32>)
                return executeInternal<DataTypeDecimal<Decimal32>>(arguments, result_type, input_rows_count, precision_value, scale_value);
            else if (precision_value <= DecimalUtils::max_precision<Decimal64>)
                return executeInternal<DataTypeDecimal<Decimal64>>(arguments, result_type, input_rows_count, precision_value, scale_value);
            else if (precision_value <= DecimalUtils::max_precision<Decimal128>)
                return executeInternal<DataTypeDecimal<Decimal128>>(arguments, result_type, input_rows_count, precision_value, scale_value);
            else
                return executeInternal<DataTypeDecimal<Decimal256>>(arguments, result_type, input_rows_count, precision_value, scale_value);
        }

    private:
        template <typename DataType>
            requires(IsDataTypeDecimal<DataType>)
        static ColumnPtr executeInternal(
            const ColumnsWithTypeAndName & arguments,
            const DataTypePtr & result_type,
            size_t input_rows_count,
            UInt32 precision_value,
            UInt32 scale)
        {
            auto src_column = arguments[0];
            ColumnPtr result_column;

            auto call = [&](const auto & types) -> bool //-V657
            {
                using Types = std::decay_t<decltype(types)>;
                using FromDataType = typename Types::LeftType;
                using ToDataType = typename Types::RightType;

                if constexpr (IsDataTypeNumber<FromDataType>)
                {
                    ColumnUInt8::MutablePtr col_null_map_to;
                    ColumnUInt8::Container * vec_null_map_to [[maybe_unused]] = nullptr;
                    if constexpr (exception_mode == ConvertExceptionMode::Null)
                    {
                        col_null_map_to = ColumnUInt8::create(input_rows_count, false);
                        vec_null_map_to = &col_null_map_to->getData();
                    }

                    using ToFieldType = typename ToDataType::FieldType;
                    using ToNativeType = typename ToFieldType::NativeType;
                    using ToColumnType = typename ToDataType::ColumnType;
                    using FromFieldType = typename FromDataType::FieldType;
                    typename ToColumnType::MutablePtr col_to = ToColumnType::create(input_rows_count, scale);

                    const auto & vector = typeid_cast<const ColumnVector<FromFieldType> *>(arguments[0].column.get());
                    auto & vec_to = col_to->getData();
                    auto & datas = vector->getData();
                    vec_to.resize_exact(input_rows_count);

                    for (size_t i = 0; i < input_rows_count; ++i)
                    {
                        ToNativeType result;
                        bool convert_result
                            = convertDecimalsFromIntegerImpl<FromFieldType, ToNativeType>(datas[i], result, precision_value);

                        if (convert_result)
                            vec_to[i] = static_cast<ToFieldType>(result);
                        else
                        {
                            if constexpr (exception_mode == ConvertExceptionMode::Null)
                            {
                                vec_to[i] = static_cast<ToFieldType>(0);
                                (*vec_null_map_to)[i] = 1;
                            }
                            else
                                throw Exception(
                                    ErrorCodes::ILLEGAL_COLUMN,
                                    "Cannot parse {} as {}",
                                    src_column.type->getName(),
                                    result_type->getName());
                        }
                    }

                    if constexpr (exception_mode == ConvertExceptionMode::Null)
                        result_column = ColumnNullable::create(std::move(col_to), std::move(col_null_map_to));
                    else
                        result_column = std::move(col_to);

                    return true;
                }
                else
                    return false;
            };

            bool r = callOnIndexAndDataType<DataType>(src_column.type->getTypeId(), call);

            if (!r)
                throw Exception(
                    ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Illegal type {} of argument of function {}", src_column.type->getName(), name);

            return result_column;
        }

        template <typename FromNativeType, typename ToNativeType>
        static bool convertDecimalsFromIntegerImpl(FromNativeType from, ToNativeType & result, UInt32 precision_value)
        {
            Field convert_to = convertNumericTypeImpl<FromNativeType, ToNativeType>(from);
            if (convert_to.isNull())
            {
                if constexpr (ConvertExceptionMode::Throw == exception_mode)
                    throw Exception(ErrorCodes::DECIMAL_OVERFLOW, "Convert overflow");
                else
                    return false;
            }
            result = static_cast<ToNativeType>(convert_to.safeGet<ToNativeType>());

            ToNativeType pow10 = intExp10OfSize<ToNativeType>(precision_value);
            if ((result < 0 && result <= -pow10) || result >= pow10)
            {
                if constexpr (ConvertExceptionMode::Throw == exception_mode)
                    throw Exception(ErrorCodes::DECIMAL_OVERFLOW, "Convert overflow");
                else
                    return false;
            }

            return true;
        }
    };

    using FunctionMakeDecimalThrow = FunctionMakeDecimal<NameMakeDecimal, ConvertExceptionMode::Throw>;
    using FunctionMakeDecimalOrNull = FunctionMakeDecimal<NameMakeDecimalOrNull, ConvertExceptionMode::Null>;
}

REGISTER_FUNCTION(MakeDecimalSpark)
{
    factory.registerFunction<FunctionMakeDecimalThrow>(FunctionDocumentation{.description = R"(
Create a decimal value by use nested type. If overflow throws exception.
)"});
    factory.registerFunction<FunctionMakeDecimalOrNull>(FunctionDocumentation{.description = R"(
Create a decimal value by use nested type. If overflow return `NULL`.
)"});
}
}
