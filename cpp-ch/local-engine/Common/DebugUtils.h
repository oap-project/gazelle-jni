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

#include <Core/Block.h>

namespace google::protobuf
{
class Message;
}
namespace DB
{
class QueryPlan;
}
namespace debug
{

void dumpMemoryUsage(const char * type);
void dumpPlan(DB::QueryPlan & plan, const char * type = "clickhouse plan", bool force = false, LoggerPtr = nullptr);
void dumpMessage(const google::protobuf::Message & message, const char * type, bool force = false, LoggerPtr = nullptr);

void headBlock(const DB::Block & block, size_t count = 10);
void headColumn(const DB::ColumnPtr & column, size_t count = 10);
void printBlockHeader(const DB::Block & block, const std::string & prefix = "");
std::string showString(const DB::Block & block, size_t numRows = 20, size_t truncate = 20, bool vertical = false);
inline std::string verticalShowString(const DB::Block & block, size_t numRows = 20, size_t truncate = 20)
{
    return showString(block, numRows, truncate, true);
}

}
