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
#include "ExcelDecimalSerialization.h"
#include <DataTypes/DataTypesDecimal.h>
#include <DataTypes/Serializations/SerializationString.h>
#include <Storages/Serializations/ExcelDecimalReader.h>
#include <Storages/Serializations/ExcelStringReader.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
    extern const int INCORRECT_DATA;
}
}

namespace local_engine
{
using namespace DB;
template <is_decimal T>
void ExcelDecimalSerialization<T>::deserializeTextCSV(IColumn & column, ReadBuffer & istr, const FormatSettings & formatSettings) const
{
    deserializeExcelDecimalText<T>(column, istr, precision, scale, formatSettings);
}

template class ExcelDecimalSerialization<Decimal32>;
template class ExcelDecimalSerialization<Decimal64>;
template class ExcelDecimalSerialization<Decimal128>;
template class ExcelDecimalSerialization<Decimal256>;
}
