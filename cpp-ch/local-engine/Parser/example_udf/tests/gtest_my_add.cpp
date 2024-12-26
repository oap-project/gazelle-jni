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
#include <iostream>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypesNumber.h>
#include <Parser/FunctionExecutor.h>
#include <Parser/FunctionParser.h>
#include <gtest/gtest.h>
#include <Common/QueryContext.h>

using namespace DB;
using namespace local_engine;

TEST(MyAdd, Common)
{
    auto context = local_engine::QueryContext::globalContext();
    auto type = std::make_shared<DataTypeNullable>(std::make_shared<DataTypeInt64>());
    FunctionExecutor executor("my_add", {type, type}, type, context);

    std::vector<FunctionExecutor::TestCase> cases = {
        {{Int64(1), Int64(2)}, Int64(3)},
        {{{}, Int64(2)}, {}},
        {{Int64(1), {}}, {}},
    };

    auto ok = executor.executeAndCompare(cases);
    ASSERT_TRUE(ok);
}
