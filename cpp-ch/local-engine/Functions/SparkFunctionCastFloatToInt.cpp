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

#include <limits.h>
#include <base/types.h>
#include <base/wide_integer.h>
#include <base/wide_integer_impl.h>
#include <Functions/SparkFunctionCastFloatToInt.h>

using namespace DB;

namespace local_engine
{

struct NameToInt8 { static constexpr auto name = "sparkCastFloatToInt8"; };
struct NameToInt16 { static constexpr auto name = "sparkCastFloatToInt16"; };
struct NameToInt32 { static constexpr auto name = "sparkCastFloatToInt32"; };
struct NameToInt64 { static constexpr auto name = "sparkCastFloatToInt64"; };

using SparkFunctionCastFloatToInt8 = local_engine::SparkFunctionCastFloatToInt<Int8, NameToInt8>;
using SparkFunctionCastFloatToInt16 = local_engine::SparkFunctionCastFloatToInt<Int16, NameToInt16>;
using SparkFunctionCastFloatToInt32 = local_engine::SparkFunctionCastFloatToInt<Int32, NameToInt32>;
using SparkFunctionCastFloatToInt64 = local_engine::SparkFunctionCastFloatToInt<Int64, NameToInt64>;

REGISTER_FUNCTION(SparkFunctionCastToInt)
{
    factory.registerFunction<SparkFunctionCastFloatToInt8>();
    factory.registerFunction<SparkFunctionCastFloatToInt16>();
    factory.registerFunction<SparkFunctionCastFloatToInt32>();
    factory.registerFunction<SparkFunctionCastFloatToInt64>();
}
}
