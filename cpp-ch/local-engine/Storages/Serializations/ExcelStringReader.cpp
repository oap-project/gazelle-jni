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
#include "ExcelStringReader.h"

#include <bit>
#include <IO/Operators.h>
#include <IO/PeekableReadBuffer.h>
#include <Common/PODArray.h>

#ifdef __SSE2__
#    include <emmintrin.h>
#endif

#if defined(__aarch64__) && defined(__ARM_NEON)
#    include <arm_neon.h>
#    pragma clang diagnostic ignored "-Wreserved-identifier"
#endif


namespace local_engine
{
using namespace DB;

template <typename T>
static void appendToStringOrVector(T & s, ReadBuffer & rb, const char * end)
{
    s.append(rb.position(), end - rb.position());
}

template <>
inline void appendToStringOrVector(PaddedPODArray<UInt8> & s, ReadBuffer & rb, const char * end)
{
    if (rb.isPadded())
        s.insertSmallAllowReadWriteOverflow15(rb.position(), end);
    else
        s.insert(rb.position(), end);
}

template <typename Vector, bool include_quotes>
void readExcelCSVQuoteString(Vector & s, ReadBuffer & buf, const char delimiter, const String & escape_value, const char & quote)
{
    if constexpr (include_quotes)
        s.push_back(quote);

    /// The quoted case. We are looking for the next quotation mark.
    while (!buf.eof())
    {
        char * next_pos = buf.position();

        [&]()
        {
#ifdef __SSE2__
            auto qe = _mm_set1_epi8(quote);
            for (; next_pos + 15 < buf.buffer().end(); next_pos += 16)
            {
                __m128i bytes = _mm_loadu_si128(reinterpret_cast<const __m128i *>(next_pos));
                auto eq = _mm_cmpeq_epi8(bytes, qe);
                if (!escape_value.empty())
                {
                    eq = _mm_or_si128(eq, _mm_cmpeq_epi8(bytes, _mm_set1_epi8(escape_value[0])));
                }

                uint16_t bit_mask = _mm_movemask_epi8(eq);
                if (bit_mask)
                {
                    next_pos += std::countr_zero(bit_mask);
                    return;
                }
            }
//#elif defined(__aarch64__) && defined(__ARM_NEON)
//                auto rc = vdupq_n_u8('\r');
//                auto nc = vdupq_n_u8('\n');
//                auto dc = vdupq_n_u8(delimiter);
//                /// Returns a 64 bit mask of nibbles (4 bits for each byte).
//                auto get_nibble_mask = [](uint8x16_t input) -> uint64_t
//                { return vget_lane_u64(vreinterpret_u64_u8(vshrn_n_u16(vreinterpretq_u16_u8(input), 4)), 0); };
//                for (; next_pos + 15 < buf.buffer().end(); next_pos += 16)
//                {
//                    uint8x16_t bytes = vld1q_u8(reinterpret_cast<const uint8_t *>(next_pos));
//                    auto eq = vorrq_u8(vorrq_u8(vceqq_u8(bytes, rc), vceqq_u8(bytes, nc)), vceqq_u8(bytes, dc));
//                    uint64_t bit_mask = get_nibble_mask(eq);
//                    if (bit_mask)
//                    {
//                        next_pos += std::countr_zero(bit_mask) >> 2;
//                        return;
//                    }
//                }
#endif
            while (next_pos < buf.buffer().end() && *next_pos != quote)
            {
                if (!escape_value.empty() && *next_pos == escape_value[0])
                    break;

                ++next_pos;
            }
        }();

        if (buf.position() != next_pos)
        {
            appendToStringOrVector(s, buf, next_pos);
        }

        if (!escape_value.empty() && escape_value[0] == *next_pos)
        {
            next_pos++;

            if (next_pos != buf.buffer().end())
            {
                if (*next_pos == quote || *next_pos == escape_value[0])
                    s.push_back(*next_pos);
                else
                {
                    s.push_back(escape_value[0]);
                    s.push_back(*next_pos);
                }
                next_pos++;
                buf.position() = next_pos;
                continue;
            }
        }

        buf.position() = next_pos;
        if (!buf.hasPendingData())
            continue;

        if (!buf.eof())
        {
            auto end_char = *buf.position();
            if (end_char == quote)
                buf.position()++;

            if (!buf.eof() && *buf.position() != delimiter && *buf.position() != '\r' && *buf.position() != '\n')
            {
                s.push_back(end_char);
                continue;
            }
        }

        if constexpr (include_quotes)
            s.push_back(quote);

        return;
    }
}

template <typename Vector>
void readExcelCSVStringInto(Vector & s, ReadBuffer & buf, const FormatSettings::CSV & settings, const String & escape_value)
{
    /// Empty string
    if (buf.eof())
        return;

    const char delimiter = settings.delimiter;
    const char maybe_quote = *buf.position();
    const String & custom_delimiter = settings.custom_delimiter;

    /// Emptiness and not even in quotation marks.
    if (custom_delimiter.empty() && maybe_quote == delimiter)
        return;

    if ((settings.allow_single_quotes && maybe_quote == '\'') || (settings.allow_double_quotes && maybe_quote == '"'))
    {
        ++buf.position();
        if (!buf.eof() && *buf.position() == '{' && *(buf.position() + 1) == maybe_quote)
            readExcelCSVQuoteString<Vector, true>(s, buf, delimiter, escape_value, maybe_quote);
        else
            readExcelCSVQuoteString(s, buf, delimiter, escape_value, maybe_quote);
    }
    else
    {
        /// If custom_delimiter is specified, we should read until first occurrences of
        /// custom_delimiter in buffer.
        if (!custom_delimiter.empty())
        {
            PeekableReadBuffer * peekable_buf = dynamic_cast<PeekableReadBuffer *>(&buf);
            if (!peekable_buf)
                throw Exception(
                    ErrorCodes::LOGICAL_ERROR, "Reading CSV string with custom delimiter is allowed only when using PeekableReadBuffer");

            while (true)
            {
                if (peekable_buf->eof())
                    throw Exception(
                        ErrorCodes::INCORRECT_DATA,
                        "Unexpected EOF while reading CSV string, expected custom delimiter \"{}\"",
                        custom_delimiter);

                char * next_pos
                    = reinterpret_cast<char *>(memchr(peekable_buf->position(), custom_delimiter[0], peekable_buf->available()));
                if (!next_pos)
                    next_pos = peekable_buf->buffer().end();

                appendToStringOrVector(s, *peekable_buf, next_pos);
                peekable_buf->position() = next_pos;

                if (!buf.hasPendingData())
                    continue;

                {
                    PeekableReadBufferCheckpoint checkpoint{*peekable_buf, true};
                    if (checkString(custom_delimiter, *peekable_buf))
                        return;
                }

                s.push_back(*peekable_buf->position());
                ++peekable_buf->position();
            }

            return;
        }

        /// Unquoted case. Look for delimiter or \r or \n.
        while (!buf.eof())
        {
            char * next_pos = buf.position();

            [&]()
            {
#ifdef __SSE2__
                auto rc = _mm_set1_epi8('\r');
                auto nc = _mm_set1_epi8('\n');
                auto dc = _mm_set1_epi8(delimiter);
                for (; next_pos + 15 < buf.buffer().end(); next_pos += 16)
                {
                    __m128i bytes = _mm_loadu_si128(reinterpret_cast<const __m128i *>(next_pos));
                    auto eq = _mm_or_si128(_mm_or_si128(_mm_cmpeq_epi8(bytes, rc), _mm_cmpeq_epi8(bytes, nc)), _mm_cmpeq_epi8(bytes, dc));
                    uint16_t bit_mask = _mm_movemask_epi8(eq);
                    if (bit_mask)
                    {
                        next_pos += std::countr_zero(bit_mask);
                        return;
                    }
                }
#elif defined(__aarch64__) && defined(__ARM_NEON)
                auto rc = vdupq_n_u8('\r');
                auto nc = vdupq_n_u8('\n');
                auto dc = vdupq_n_u8(delimiter);
                /// Returns a 64 bit mask of nibbles (4 bits for each byte).
                auto get_nibble_mask = [](uint8x16_t input) -> uint64_t
                { return vget_lane_u64(vreinterpret_u64_u8(vshrn_n_u16(vreinterpretq_u16_u8(input), 4)), 0); };
                for (; next_pos + 15 < buf.buffer().end(); next_pos += 16)
                {
                    uint8x16_t bytes = vld1q_u8(reinterpret_cast<const uint8_t *>(next_pos));
                    auto eq = vorrq_u8(vorrq_u8(vceqq_u8(bytes, rc), vceqq_u8(bytes, nc)), vceqq_u8(bytes, dc));
                    uint64_t bit_mask = get_nibble_mask(eq);
                    if (bit_mask)
                    {
                        next_pos += std::countr_zero(bit_mask) >> 2;
                        return;
                    }
                }
#endif
                while (next_pos < buf.buffer().end() && *next_pos != delimiter && *next_pos != '\r' && *next_pos != '\n')
                    ++next_pos;
            }();

            appendToStringOrVector(s, buf, next_pos);
            buf.position() = next_pos;

            if (!buf.hasPendingData())
                continue;

            return;
        }
    }
}

void deserializeExcelStringTextCSV(IColumn & column, ReadBuffer & istr, const FormatSettings & settings, const String & escape_value)
{
    excelRead(column, [&](ColumnString::Chars & data) { readExcelCSVStringInto(data, istr, settings.csv, escape_value); });
}

}
