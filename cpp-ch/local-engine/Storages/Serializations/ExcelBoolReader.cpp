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
#include "ExcelBoolReader.h"
#include <Columns/ColumnsNumber.h>
#include <IO/PeekableReadBuffer.h>
#include <IO/ReadHelpers.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int CANNOT_PARSE_BOOL;
    extern const int ILLEGAL_COLUMN;
}
}

namespace local_engine
{
using namespace DB;


DB::ColumnUInt8 * checkAndGetDeserializeColumnType(IColumn & column)
{
    auto * col = typeid_cast<DB::ColumnUInt8 *>(&column);
    if (!checkAndGetColumn<DB::ColumnUInt8>(&column))
        throw Exception(DB::ErrorCodes::ILLEGAL_COLUMN, "Bool type can only deserialize columns of type UInt8.{}", column.getName());
    return col;
}

bool tryDeserializeAllVariants(ColumnUInt8 * column, ReadBuffer & istr)
{
    if (checkCharCaseInsensitive('1', istr))
    {
        column->insert(true);
    }
    else if (checkCharCaseInsensitive('0', istr))
    {
        column->insert(false);
    }
    /// 'True' and 'T'
    else if (checkCharCaseInsensitive('t', istr))
    {
        /// Check if it's just short form `T` or full form `True`
        if (checkCharCaseInsensitive('r', istr))
        {
            if (!checkStringCaseInsensitive("ue", istr))
                return false;
        }
        column->insert(true);
    }
    /// 'False' and 'F'
    else if (checkCharCaseInsensitive('f', istr))
    {
        /// Check if it's just short form `F` or full form `False`
        if (checkCharCaseInsensitive('a', istr))
        {
            if (!checkStringCaseInsensitive("lse", istr))
                return false;
        }
        column->insert(false);
    }
    /// 'Yes' and 'Y'
    else if (checkCharCaseInsensitive('y', istr))
    {
        /// Check if it's just short form `Y` or full form `Yes`
        if (checkCharCaseInsensitive('e', istr))
        {
            if (!checkCharCaseInsensitive('s', istr))
                return false;
        }
        column->insert(true);
    }
    /// 'No' and 'N'
    else if (checkCharCaseInsensitive('n', istr))
    {
        /// Check if it's just short form `N` or full form `No`
        checkCharCaseInsensitive('o', istr);
        column->insert(false);
    }
    /// 'On' and 'Off'
    else if (checkCharCaseInsensitive('o', istr))
    {
        if (checkCharCaseInsensitive('n', istr))
            column->insert(true);
        else if (checkStringCaseInsensitive("ff", istr))
        {
            column->insert(false);
        }
        else
            return false;
    }
    /// 'Enable' and 'Enabled'
    else if (checkStringCaseInsensitive("enable", istr))
    {
        /// Check if it's 'enable' or 'enabled'
        checkCharCaseInsensitive('d', istr);
        column->insert(true);
    }
    /// 'Disable' and 'Disabled'
    else if (checkStringCaseInsensitive("disable", istr))
    {
        /// Check if it's 'disable' or 'disabled'
        checkCharCaseInsensitive('d', istr);
        column->insert(false);
    }
    else
    {
        return false;
    }

    return true;
}

void deserializeImpl(
    IColumn & column, ReadBuffer & istr, const FormatSettings & settings, std::function<bool(ReadBuffer &)> check_end_of_value)
{
    DB::ColumnUInt8 * col = checkAndGetDeserializeColumnType(column);

    DB::PeekableReadBuffer buf(istr);
    buf.setCheckpoint();
    if (checkString(settings.bool_true_representation, buf) && check_end_of_value(buf))
    {
        col->insert(true);
        return;
    }

    buf.rollbackToCheckpoint();
    if (checkString(settings.bool_false_representation, buf) && check_end_of_value(buf))
    {
        col->insert(false);
        buf.dropCheckpoint();
        if (buf.hasUnreadData())
            throw Exception(
                ErrorCodes::CANNOT_PARSE_BOOL,
                "Cannot continue parsing after parsed bool value because it will result in the loss of some data. It may happen if "
                "bool_true_representation or bool_false_representation contains some delimiters of input format");
        return;
    }

    buf.rollbackToCheckpoint();
    if (tryDeserializeAllVariants(col, buf) && check_end_of_value(buf))
    {
        buf.dropCheckpoint();
        if (buf.hasUnreadData())
            throw Exception(
                ErrorCodes::CANNOT_PARSE_BOOL,
                "Cannot continue parsing after parsed bool value because it will result in the loss of some data. It may happen if "
                "bool_true_representation or bool_false_representation contains some delimiters of input format");
        return;
    }

    buf.makeContinuousMemoryFromCheckpointToPos();
    buf.rollbackToCheckpoint();
    throw Exception(
        ErrorCodes::CANNOT_PARSE_BOOL,
        "Cannot parse boolean value here: '{}', should be '{}' or '{}' controlled by setting bool_true_representation and "
        "bool_false_representation or one of "
        "True/False/T/F/Y/N/Yes/No/On/Off/Enable/Disable/Enabled/Disabled/1/0",
        String(buf.position(), std::min(10lu, buf.available())),
        settings.bool_true_representation,
        settings.bool_false_representation);
}


void deserializeExcelBoolTextCSV(DB::IColumn & column, DB::ReadBuffer & istr, const DB::FormatSettings & settings)
{
    if (istr.eof())
        throw DB::Exception(DB::ErrorCodes::CANNOT_PARSE_BOOL, "Expected boolean value but get EOF.");

    deserializeImpl(
        column,
        istr,
        settings,
        [&](DB::ReadBuffer & buf)
        {
            /// skip all chars before quote/delimiter exclude line delimiter
            while (!buf.eof() && *buf.position() == ' ')
                ++buf.position();

            return buf.eof() || *buf.position() == settings.csv.delimiter || *buf.position() == '\n' || *buf.position() == '\r';
        });
}

}
