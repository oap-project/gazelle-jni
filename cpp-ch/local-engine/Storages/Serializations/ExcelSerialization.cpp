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
#include "ExcelSerialization.h"
#include <Columns/ColumnsNumber.h>
#include <DataTypes/DataTypesDecimal.h>
#include <DataTypes/Serializations/SerializationBool.h>
#include <DataTypes/Serializations/SerializationDate32.h>
#include <DataTypes/Serializations/SerializationDateTime64.h>
#include <DataTypes/Serializations/SerializationDecimal.h>
#include <DataTypes/Serializations/SerializationNumber.h>
#include <DataTypes/Serializations/SerializationString.h>
#include <Storages/Serializations/ExcelBoolReader.h>
#include <Storages/Serializations/ExcelReadHelpers.h>
#include <Storages/Serializations/ExcelStringReader.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int INCORRECT_DATA;
}
}

namespace local_engine
{
using namespace DB;
void ExcelSerialization::deserializeTextCSV(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const
{
    if (typeid_cast<const DB::SerializationDate32 *>(nested_ptr.get()))
    {
        deserializeDate32TextCSV(column, istr, settings);
    }
    else if (const auto * datetime64 = typeid_cast<const DB::SerializationDateTime64 *>(nested_ptr.get()))
    {
        deserializeDatetimeTextCSV<DB::SerializationDateTime64::ColumnType>(
            column, istr, settings, datetime64->getTimeZone(), DateLUT::instance("UTC"));
    }
    else if (typeid_cast<const SerializationNumber<UInt8> *>(nested_ptr.get()))
    {
        deserializeNumberTextCSV<UInt8>(column, istr, settings);
    }
    else if (typeid_cast<const SerializationNumber<UInt16> *>(nested_ptr.get()))
    {
        deserializeNumberTextCSV<UInt16>(column, istr, settings);
    }
    else if (typeid_cast<const SerializationNumber<UInt32> *>(nested_ptr.get()))
    {
        deserializeNumberTextCSV<UInt32>(column, istr, settings);
    }
    else if (typeid_cast<const SerializationNumber<UInt64> *>(nested_ptr.get()))
    {
        deserializeNumberTextCSV<UInt64>(column, istr, settings);
    }
    else if (typeid_cast<const SerializationNumber<Int8> *>(nested_ptr.get()))
    {
        deserializeNumberTextCSV<Int8>(column, istr, settings);
    }
    else if (typeid_cast<const SerializationNumber<Int16> *>(nested_ptr.get()))
    {
        deserializeNumberTextCSV<Int16>(column, istr, settings);
    }
    else if (typeid_cast<const SerializationNumber<Int32> *>(nested_ptr.get()))
    {
        deserializeNumberTextCSV<Int32>(column, istr, settings);
    }
    else if (typeid_cast<const SerializationNumber<Int64> *>(nested_ptr.get()))
    {
        deserializeNumberTextCSV<Int64>(column, istr, settings);
    }
    else if (typeid_cast<const SerializationNumber<Float32> *>(nested_ptr.get()))
    {
        deserializeNumberTextCSV<Float32>(column, istr, settings);
    }
    else if (typeid_cast<const SerializationNumber<Float64> *>(nested_ptr.get()))
    {
        deserializeNumberTextCSV<Float64>(column, istr, settings);
    }
    else if (typeid_cast<const SerializationString *>(nested_ptr.get()))
    {
        deserializeExcelStringTextCSV(column, istr, settings, escape);
    }
    else if (typeid_cast<const SerializationBool *>(nested_ptr.get()))
    {
        deserializeExcelBoolTextCSV(column, istr, settings);
    }
    else
    {
        nested_ptr->deserializeTextCSV(column, istr, settings);
    }
}

template <typename T>
requires is_arithmetic_v<T>
void ExcelSerialization::deserializeNumberTextCSV(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const
{
    T x;
    bool result = local_engine::readCSV(x, istr, settings);

    if (result)
        assert_cast<ColumnVector<T> &>(column).getData().push_back(x);
}

void ExcelSerialization::deserializeDate32TextCSV(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const
{
    LocalDate value;
    bool result = local_engine::readCSV(value, istr, settings);

    if (result)
        assert_cast<ColumnInt32 &>(column).getData().push_back(value.getExtenedDayNum());
}

template <typename ColumnType>
void ExcelSerialization::deserializeDatetimeTextCSV(
    IColumn & column, ReadBuffer & istr, const FormatSettings & settings, const DateLUTImpl & time_zone, const DateLUTImpl & utc_time_zone)
    const
{
    DateTime64 x = 0;

    if (istr.eof())
        throwReadAfterEOF();

    char maybe_quote = *istr.position();
    bool quote = false;
    if (maybe_quote == '\'' || maybe_quote == '\"')
    {
        quote = true;
        ++istr.position();
    }

    if (!local_engine::readDateTime64Text(x, istr, settings, time_zone, utc_time_zone, quote))
        return;

    if (maybe_quote == '\'' || maybe_quote == '\"')
        assertChar(maybe_quote, istr);

    assert_cast<ColumnType &>(column).getData().push_back(x);
}
}
