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

#include "Common/assert_cast.h"


#include <Columns/ColumnDecimal.h>
#include <IO/ReadHelpers.h>
#include <IO/readDecimalText.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int CANNOT_PARSE_NUMBER;
}
}

namespace local_engine
{

template <typename T>
bool readExcelCSVDecimalText(DB::ReadBuffer & buf, T & x, uint32_t precision, uint32_t & scale, const DB::FormatSettings & format_settings)
{
    char maybe_quote = *buf.position();
    bool has_quote = false;
    if (maybe_quote == '\'' || maybe_quote == '\"')
    {
        ++buf.position();
        has_quote = true;
    }

    /// deal empty string ""
    if ((has_quote && !buf.eof() && *buf.position() == maybe_quote)
        || (!has_quote && !buf.eof()
            && (*buf.position() == format_settings.csv.delimiter || *buf.position() == '\n' || *buf.position() == '\r')))
        return false;

    bool result = readDecimalText<T, bool>(buf, x, precision, scale, false);

    if (!result)
        return false;

    if (has_quote)
        assertChar(maybe_quote, buf);

    if (!buf.eof() && (*buf.position() != format_settings.csv.delimiter && *buf.position() != '\n' && *buf.position() != '\r'))
        return false;

    return true;
}

template <typename T>
void deserializeExcelDecimalText(
    DB::IColumn & column, DB::ReadBuffer & istr, UInt32 precision, UInt32 scale, const DB::FormatSettings & formatSettings)
{
    if (istr.eof())
        DB::throwReadAfterEOF();

    T x;
    UInt32 unread_scale = scale;
    bool result = readExcelCSVDecimalText(istr, x, precision, unread_scale, formatSettings);
    if (result && !common::mulOverflow(x.value, DB::DecimalUtils::scaleMultiplier<T>(unread_scale), x.value))
        assert_cast<DB::ColumnDecimal<T> &>(column).getData().push_back(x);
}
}
