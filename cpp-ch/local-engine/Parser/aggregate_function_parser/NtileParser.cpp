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
#include "NtileParser.h"
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypesNumber.h>
#include <Interpreters/ActionsDAG.h>

namespace local_engine
{
using namespace DB;
DB::ActionsDAG::NodeRawConstPtrs
NtileParser::parseFunctionArguments(const CommonFunctionInfo & func_info, DB::ActionsDAG & actions_dag) const
{
    if (func_info.arguments.size() != 1)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Function ntile takes exactly one argument");
    DB::ActionsDAG::NodeRawConstPtrs args;

    const auto & arg0 = func_info.arguments[0].value();
    auto [data_type, field] = parseLiteral(arg0.literal());
    if (!(DB::WhichDataType(data_type).isInt32()))
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "ntile's argument must be i32");
    Int32 field_index = static_cast<Int32>(field.safeGet<Int32>());
    // For CH, the data type of the args[0] must be the UInt32
    const auto * index_node = addColumnToActionsDAG(actions_dag, std::make_shared<DataTypeUInt32>(), field_index);
    args.emplace_back(index_node);
    return args;
}
AggregateFunctionParserRegister<NtileParser> ntile_register;
}
