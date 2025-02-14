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
#include <Core/AccurateComparison.h>
#include <Core/ColumnWithTypeAndName.h>
#include <Core/Field.h>
#include <DataTypes/IDataType.h>


namespace DB
{
namespace ErrorCodes
{
    extern const int TYPE_MISMATCH;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int DECIMAL_OVERFLOW;

}
}

namespace local_engine
{

template <typename From, typename To>
DB::Field convertNumericTypeImpl(From from)
{
    To result;
    if (!accurate::convertNumeric(from, result))
        return {};
    return result;
}

template <typename To>
DB::Field convertNumericType(const DB::Field & from)
{
    if (from.getType() == DB::Field::Types::UInt64)
        return convertNumericTypeImpl<UInt64, To>(from.safeGet<UInt64>());
    if (from.getType() == DB::Field::Types::Int64)
        return convertNumericTypeImpl<Int64, To>(from.safeGet<Int64>());
    if (from.getType() == DB::Field::Types::UInt128)
        return convertNumericTypeImpl<UInt128, To>(from.safeGet<UInt128>());
    if (from.getType() == DB::Field::Types::Int128)
        return convertNumericTypeImpl<Int128, To>(from.safeGet<Int128>());
    if (from.getType() == DB::Field::Types::UInt256)
        return convertNumericTypeImpl<UInt256, To>(from.safeGet<UInt256>());
    if (from.getType() == DB::Field::Types::Int256)
        return convertNumericTypeImpl<Int256, To>(from.safeGet<Int256>());

    throw DB::Exception(DB::ErrorCodes::TYPE_MISMATCH, "Type mismatch. Expected: Integer. Got: {}", from.getType());
}

inline UInt32 extractArgument(const DB::ColumnWithTypeAndName & named_column)
{
    if (!isColumnConst(*named_column.column.get()))
    {
        throw DB::Exception(
            DB::ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Illegal type {} of argument, column type must be const", named_column.type->getName());
    }

    DB::Field from;
    named_column.column->get(0, from);
    DB::Field to = convertNumericType<UInt32>(from);
    if (to.isNull())
    {
        throw DB::Exception(
            DB::ErrorCodes::DECIMAL_OVERFLOW, "{} convert overflow, precision/scale value must in UInt32", named_column.type->getName());
    }
    return static_cast<UInt32>(to.safeGet<UInt32>());
}

}
