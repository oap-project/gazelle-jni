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
#include <memory>
#include <Columns/ColumnMap.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnStringHelpers.h>
#include <Columns/ColumnsNumber.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Formats/FormatFactory.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <IO/WriteHelpers.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}
}

namespace local_eingine
{
class SparkFunctionMapToString : public DB::IFunction
{
public:
    static constexpr auto name = "sparkCastMapToString";
    static DB::FunctionPtr create(DB::ContextPtr context) { return std::make_shared<SparkFunctionMapToString>(context); }
    explicit SparkFunctionMapToString(DB::ContextPtr context_) : context(context_) {}
    ~SparkFunctionMapToString() override = default;
    String getName() const override { return name; }
    size_t getNumberOfArguments() const override { return 3; }
    bool isSuitableForShortCircuitArgumentsExecution(const DB::DataTypesWithConstInfo & /*arguments*/) const override { return true; }
    bool useDefaultImplementationForNulls() const override { return false; }
    bool useDefaultImplementationForLowCardinalityColumns() const override { return false; }

    DB::DataTypePtr getReturnTypeImpl(const DB::ColumnsWithTypeAndName & arguments) const override
    {
        if (arguments.size() != 3)
            throw DB::Exception(DB::ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, "Function {} argument size must be 3", name);

        auto arg_type = DB::removeNullable(arguments[0].type);
        if (!DB::WhichDataType(arg_type).isMap())
        {
            throw DB::Exception(
                DB::ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "Illegal type {} of argument[1] of function {}",
                arguments[0].type->getName(),
                getName());
        }

        auto key_type = DB::WhichDataType(removeNullable(arguments[1].type));
        auto value_type = DB::WhichDataType(removeNullable(arguments[2].type));
        // Not support complex types in key or value
        if (!key_type.isString() && !key_type.isNumber() && !value_type.isString() && !value_type.isNumber())
        {
            throw DB::Exception(
                DB::ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "Cast MapToString not support {}, {} as key value",
                arguments[1].type->getName(),
                arguments[2].type->getName());
        }

        if (arguments[0].type->isNullable())
            return makeNullable(std::make_shared<DB::DataTypeString>());
        else
            return std::make_shared<DB::DataTypeString>();
    }

    DB::ColumnPtr executeImpl(const DB::ColumnsWithTypeAndName & arguments, const DB::DataTypePtr & result_type, size_t /*input_rows*/) const override
    {
        DB::ColumnUInt8::MutablePtr null_map = nullptr;
        if (const auto * col_nullable = checkAndGetColumn<DB::ColumnNullable>(arguments[0].column.get()))
        {
            null_map = DB::ColumnUInt8::create();
            null_map->insertRangeFrom(col_nullable->getNullMapColumn(), 0, col_nullable->size());
        }

        const auto & col_with_type_and_name = columnGetNested(arguments[0]);
        const DB::IColumn & column = *col_with_type_and_name.column;
        const DB::IColumn & col_from = column.isConst() ? reinterpret_cast<const DB::ColumnConst &>(column).getDataColumn() : column;

        size_t size = col_from.size();
        auto col_to = removeNullable(result_type)->createColumn();

        {
            DB::FormatSettings format_settings = context ? DB::getFormatSettings(context) : DB::FormatSettings{};
            DB::ColumnStringHelpers::WriteHelper write_helper(
                    assert_cast<DB::ColumnString &>(*col_to),
                    size);

            auto & write_buffer = write_helper.getWriteBuffer();
            auto key_serializer = arguments[1].type->getDefaultSerialization();
            auto value_serializer = arguments[2].type->getDefaultSerialization();

            for (size_t row = 0; row < size; ++row)
            {
                serializeInSparkStyle(
                    col_from,
                    row,
                    write_buffer,
                    format_settings,
                    key_serializer,
                    value_serializer);
                write_helper.rowWritten();
            }

            write_helper.finalize();
        }

        if (result_type->isNullable() && null_map)
            return DB::ColumnNullable::create(std::move(col_to), std::move(null_map));
        return col_to;
    }

private:
    DB::ContextPtr context;

    void serializeInSparkStyle(
        const DB::IColumn & column,
        size_t row_num,
        DB::WriteBuffer & ostr,
        const DB::FormatSettings & settings,
        const DB::SerializationPtr & key_serializer,
        const DB::SerializationPtr & value_serializer) const
    {

        const auto & column_map = assert_cast<const DB::ColumnMap &>(column);

        const auto & nested_array = column_map.getNestedColumn();
        const auto & nested_tuple = column_map.getNestedData();
        const auto & offsets = nested_array.getOffsets();
        const auto& key_column = nested_tuple.getColumn(0);
        const auto& value_column = nested_tuple.getColumn(1);

        size_t offset = offsets[row_num - 1];
        size_t next_offset = offsets[row_num];

        writeChar('{', ostr);
        for (size_t i = offset; i < next_offset; ++i)
        {
            if (i != offset)
            {
                writeChar(',', ostr);
                writeChar(' ', ostr);
            }

            key_serializer->serializeText(key_column, i, ostr, settings);
            writeChar(' ', ostr);
            writeChar('-', ostr);
            writeChar('>', ostr);
            writeChar(' ', ostr);
            value_serializer->serializeText(value_column, i, ostr, settings);
        }
        writeChar('}', ostr);
    }
};

}
