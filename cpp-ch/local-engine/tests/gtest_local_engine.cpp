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
#include <fstream>
#include <iostream>
#include <gluten_test_util.h>
#include <incbin.h>
#include <Builder/SerializedPlanBuilder.h>
#include <Disks/DiskLocal.h>
#include <Formats/FormatFactory.h>
#include <Interpreters/Context.h>
#include <Parser/CHColumnToSparkRow.h>
#include <Parser/SparkRowToCHColumn.h>
#include <Parser/SubstraitParserUtils.h>
#include <Processors/Executors/PipelineExecutor.h>
#include <Processors/QueryPlan/Optimizations/QueryPlanOptimizationSettings.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <Storages/MergeTree/SparkStorageMergeTree.h>
#include <gtest/gtest.h>
#include <config.pb.h>
#include <Common/CHUtil.h>
#include <Common/GlutenConfig.h>

using namespace local_engine;
using namespace DB;

TEST(TESTUtil, TestByteToLong)
{
    Int64 expected = 0xf085460ccf7f0000l;
    char arr[8];
    arr[0] = -16;
    arr[1] = -123;
    arr[2] = 70;
    arr[3] = 12;
    arr[4] = -49;
    arr[5] = 127;
    arr[6] = 0;
    arr[7] = 0;
    std::reverse(arr, arr + 8);
    Int64 result = reinterpret_cast<Int64 *>(arr)[0];
    std::cout << std::to_string(result);

    ASSERT_EQ(expected, result);
}

TEST(ReadBufferFromFile, seekBackwards)
{
    static constexpr size_t N = 256;
    static constexpr size_t BUF_SIZE = 64;

    auto tmp_file = createTemporaryFile("/tmp/");

    {
        WriteBufferFromFile out(tmp_file->path());
        for (size_t i = 0; i < N; ++i)
            writeIntBinary(i, out);
        out.finalize();
    }

    ReadBufferFromFile in(tmp_file->path(), BUF_SIZE);
    size_t x;

    /// Read something to initialize the buffer.
    in.seek(BUF_SIZE * 10, SEEK_SET);
    readIntBinary(x, in);

    /// Check 2 consecutive  seek calls without reading.
    in.seek(BUF_SIZE * 2, SEEK_SET);
    //    readIntBinary(x, in);
    in.seek(BUF_SIZE, SEEK_SET);

    readIntBinary(x, in);
    ASSERT_EQ(x, 8);
}

INCBIN(_config_json, SOURCE_DIR "/utils/extern-local-engine/tests/json/gtest_local_engine_config.json");

namespace DB
{
void registerOutputFormatParquet(DB::FormatFactory & factory);
}

int main(int argc, char ** argv)
{
    SparkConfigs::update(
        local_engine::JsonStringToBinary<gluten::ConfigMap>(EMBEDDED_PLAN(_config_json)),
        [&](const SparkConfigs::ConfigMap & spark_conf_map) { BackendInitializerUtil::initBackend(spark_conf_map); },
        true);

    auto & factory = FormatFactory::instance();
    DB::registerOutputFormatParquet(factory);

    SCOPE_EXIT({ BackendFinalizerUtil::finalizeGlobally(); });

    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}