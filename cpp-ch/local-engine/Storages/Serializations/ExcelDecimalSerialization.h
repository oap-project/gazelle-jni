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

#include <DataTypes/Serializations/SerializationNumber.h>
#include <Common/DateLUTImpl.h>

namespace DB
{
namespace ErrorCodes
{
extern const int NOT_IMPLEMENTED;
}
}

namespace local_engine
{

template <DB::is_decimal T>
class ExcelDecimalSerialization final : public DB::ISerialization
{
public:
    explicit ExcelDecimalSerialization(const DB::SerializationPtr & nested_, UInt32 precision_, UInt32 scale_)
        : nested_ptr(nested_), precision(precision_), scale(scale_){}

    void serializeBinary(const DB::Field &, DB::WriteBuffer &, const DB::FormatSettings &) const override
    {
        throw DB::Exception(DB::ErrorCodes::NOT_IMPLEMENTED, "Not implemented for excel serialization");
    }
    void deserializeBinary(DB::Field &, DB::ReadBuffer &, const DB::FormatSettings &) const override
    {
        throw DB::Exception(DB::ErrorCodes::NOT_IMPLEMENTED, "Not implemented for excel serialization");
    }
    void serializeBinary(const DB::IColumn &, size_t, DB::WriteBuffer &, const DB::FormatSettings &) const override
    {
        throw DB::Exception(DB::ErrorCodes::NOT_IMPLEMENTED, "Not implemented for excel serialization");
    }
    void deserializeBinary(DB::IColumn &, DB::ReadBuffer &, const DB::FormatSettings &) const override
    {
        throw DB::Exception(DB::ErrorCodes::NOT_IMPLEMENTED, "Not implemented for excel serialization");
    }
    void serializeText(const DB::IColumn &, size_t, DB::WriteBuffer &, const DB::FormatSettings &) const override
    {
        throw DB::Exception(DB::ErrorCodes::NOT_IMPLEMENTED, "Not implemented for excel serialization");
    }
    void deserializeWholeText(DB::IColumn &, DB::ReadBuffer &, const DB::FormatSettings &) const override
    {
        throw DB::Exception(DB::ErrorCodes::NOT_IMPLEMENTED, "Not implemented for excel serialization");
    }
    void serializeTextEscaped(const DB::IColumn &, size_t, DB::WriteBuffer &, const DB::FormatSettings &) const override
    {
        throw DB::Exception(DB::ErrorCodes::NOT_IMPLEMENTED, "Not implemented for excel serialization");
    }
    void deserializeTextEscaped(DB::IColumn &, DB::ReadBuffer &, const DB::FormatSettings &) const override
    {
        throw DB::Exception(DB::ErrorCodes::NOT_IMPLEMENTED, "Not implemented for excel serialization");
    }
    void serializeTextQuoted(const DB::IColumn &, size_t, DB::WriteBuffer &, const DB::FormatSettings &) const override
    {
        throw DB::Exception(DB::ErrorCodes::NOT_IMPLEMENTED, "Not implemented for excel serialization");
    }
    void deserializeTextQuoted(DB::IColumn &, DB::ReadBuffer &, const DB::FormatSettings &) const override
    {
        throw DB::Exception(DB::ErrorCodes::NOT_IMPLEMENTED, "Not implemented for excel serialization");
    }
    void serializeTextJSON(const DB::IColumn &, size_t, DB::WriteBuffer &, const DB::FormatSettings &) const override
    {
        throw DB::Exception(DB::ErrorCodes::NOT_IMPLEMENTED, "Not implemented for excel serialization");
    }
    void deserializeTextJSON(DB::IColumn &, DB::ReadBuffer &, const DB::FormatSettings &) const override
    {
        throw DB::Exception(DB::ErrorCodes::NOT_IMPLEMENTED, "Not implemented for excel serialization");
    }
    void serializeTextCSV(const DB::IColumn &, size_t, DB::WriteBuffer &, const DB::FormatSettings &) const override
    {
        throw DB::Exception(DB::ErrorCodes::NOT_IMPLEMENTED, "Not implemented for excel serialization");
    }

    void deserializeTextCSV(DB::IColumn & column, DB::ReadBuffer & istr, const DB::FormatSettings &) const override;


private:
    DB::SerializationPtr nested_ptr;
    const UInt32 precision;
    const UInt32 scale;
};
}
