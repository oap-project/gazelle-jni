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

#include <AggregateFunctions/AggregateFunctionUniqHyperLogLogPlusPlus.h>
#include <IO/ReadBufferFromString.h>
#include <gtest/gtest.h>

using namespace DB;

static std::vector<UInt64> random_uint64s
    = {17956993516945311251ULL,
       4306050051188505054ULL,
       14289061765075743502ULL,
       16763375724458316157ULL,
       6144297519955185930ULL,
       18446472757487308114ULL,
       16923578592198257123ULL,
       13557354668567515845ULL,
       15328387702200001967ULL,
       15878166530370497646ULL};

static void initSmallHLL(HyperLogLogPlusPlusData & hll)
{
    for (auto x : random_uint64s)
        hll.add(x);
}

static void initLargeHLL(HyperLogLogPlusPlusData & hll)
{
    for (auto x : random_uint64s)
    {
        for (size_t i = 0; i < 100; ++i)
            hll.add(x * (i+1));
    }
}

TEST(HyperLogLogPlusPlusDataTest, Small)
{
    HyperLogLogPlusPlusData hll;
    initSmallHLL(hll);
    EXPECT_EQ(hll.query(), 10);
}

TEST(HyperLogLogPlusPlusDataTest, Large)
{
    HyperLogLogPlusPlusData hll;
    initLargeHLL(hll);
    EXPECT_EQ(hll.query(), 821);
}

TEST(HyperLogLogPlusPlusDataTest, Merge) {
    HyperLogLogPlusPlusData hll1;
    initSmallHLL(hll1);

    HyperLogLogPlusPlusData hll2;
    initLargeHLL(hll2);

    hll1.merge(hll2);
    EXPECT_EQ(hll1.query(), 821);
}

TEST(HyperLogLogPlusPlusDataTest, SerializeAndDeserialize) {
    HyperLogLogPlusPlusData hll1;
    initLargeHLL(hll1);

    WriteBufferFromOwnString write_buffer;
    hll1.serialize(write_buffer);

    ReadBufferFromString read_buffer(write_buffer.str());
    HyperLogLogPlusPlusData hll2;
    hll2.deserialize(read_buffer);

    EXPECT_EQ(hll2.query(), 821);
}
