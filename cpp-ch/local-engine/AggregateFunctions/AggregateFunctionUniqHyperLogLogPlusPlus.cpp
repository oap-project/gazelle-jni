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
#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/AggregateFunctionUniqHyperLogLogPlusPlus.h>
#include <AggregateFunctions/Helpers.h>
#include <DataTypes/DataTypeDate.h>
#include <DataTypes/DataTypeDate32.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeUUID.h>
#include <DataTypes/DataTypeIPv4andIPv6.h>

namespace DB
{

namespace
{

AggregateFunctionPtr createAggregateFunctionUniqHyperLogLogPlusPlus(
    const std::string & , const DataTypes & argument_types, const Array & params, const Settings *)
{
    return std::make_shared<AggregateFunctionUniqHyperLogLogPlusPlus>(argument_types, params);
}

}

void registerAggregateFunctionUniqHyperLogLogPlusPlus(AggregateFunctionFactory & factory)
{
    AggregateFunctionProperties properties = {.returns_default_when_only_null = true, .is_order_dependent = false};
    factory.registerFunction("uniqHLLPP", {createAggregateFunctionUniqHyperLogLogPlusPlus, properties});
}

}
