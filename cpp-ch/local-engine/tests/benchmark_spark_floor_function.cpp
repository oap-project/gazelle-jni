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

#if USE_MULTITARGET_CODE
#include <immintrin.h>
#endif

#include <Columns/IColumn.h>
#include <Core/Block.h>
#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/IDataType.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionsRound.h>
#include <Functions/SparkFunctionFloor.h>
#include <benchmark/benchmark.h>
#include <Common/QueryContext.h>
#include <Common/TargetSpecific.h>

using namespace DB;

static Block createDataBlock(String type_str, size_t rows)
{
    auto type = DataTypeFactory::instance().get(type_str);
    auto column = type->createColumn();
    for (size_t i = 0; i < rows; ++i)
    {
        if (i % 100)
        {
            column->insertDefault();
        }
        else if (isInt(type))
        {
            column->insert(i);
        }
        else if (isFloat(type))
        {
            double d = i * 1.0;
            column->insert(d);
        }
    }
    Block block;
    block.insert(ColumnWithTypeAndName(std::move(column), type, "d"));
    return std::move(block);
}

static void BM_CHFloorFunction_For_Int64(benchmark::State & state)
{
    using namespace DB;
    auto & factory = FunctionFactory::instance();
    auto function = factory.get("floor", local_engine::QueryContext::globalContext());
    Block int64_block = createDataBlock("Nullable(Int64)", 65536);
    auto executable = function->build(int64_block.getColumnsWithTypeAndName());
    for (auto _ : state)
    {
        auto result = executable->execute(int64_block.getColumnsWithTypeAndName(), executable->getResultType(), int64_block.rows(), false);
        benchmark::DoNotOptimize(result);
    }
}

static void BM_CHFloorFunction_For_Float64(benchmark::State & state)
{
    using namespace DB;
    auto & factory = FunctionFactory::instance();
    auto function = factory.get("floor", local_engine::QueryContext::globalContext());
    Block float64_block = createDataBlock("Nullable(Float64)", 65536);
    auto executable = function->build(float64_block.getColumnsWithTypeAndName());
    for (auto _ : state)
    {
        auto result = executable->execute(float64_block.getColumnsWithTypeAndName(), executable->getResultType(), float64_block.rows(), false);
        benchmark::DoNotOptimize(result);
    }
}

static void BM_SparkFloorFunction_For_Int64(benchmark::State & state)
{
    using namespace DB;
    auto & factory = FunctionFactory::instance();
    auto function = factory.get("sparkFloor", local_engine::QueryContext::globalContext());
    Block int64_block = createDataBlock("Nullable(Int64)", 65536);
    auto executable = function->build(int64_block.getColumnsWithTypeAndName());
    for (auto _ : state)
    {
        auto result = executable->execute(int64_block.getColumnsWithTypeAndName(), executable->getResultType(), int64_block.rows(), false);
        benchmark::DoNotOptimize(result);
    }
}

static void BM_SparkFloorFunction_For_Float64(benchmark::State & state)
{
    using namespace DB;
    auto & factory = FunctionFactory::instance();
    auto function = factory.get("sparkFloor", local_engine::QueryContext::globalContext());
    Block float64_block = createDataBlock("Nullable(Float64)", 65536);
    auto executable = function->build(float64_block.getColumnsWithTypeAndName());
    for (auto _ : state)
    {
        auto result = executable->execute(float64_block.getColumnsWithTypeAndName(), executable->getResultType(), float64_block.rows(), false);
        benchmark::DoNotOptimize(result);
    }
}

static void nanInfToNullAutoOpt(float * data, uint8_t * null_map, size_t size)
{
    for (size_t i = 0; i < size; ++i)
    {
        uint8_t is_nan = (data[i] != data[i]);
        uint8_t is_inf
            = ((*reinterpret_cast<const uint32_t *>(&data[i]) & 0b01111111111111111111111111111111) == 0b01111111100000000000000000000000);
        uint8_t null_flag = is_nan | is_inf;
        null_map[i] = null_flag;

        UInt32 * uint_data = reinterpret_cast<UInt32 *>(&data[i]);
        *uint_data &= ~(-null_flag);
    }
}

static void BMNanInfToNullAutoOpt(benchmark::State & state)
{
    constexpr size_t size = 8192;
    float data[size];
    uint8_t null_map[size] = {0};
    for (size_t i = 0; i < size; ++i)
        data[i] = static_cast<float>(rand()) / rand();

    for (auto _ : state)
    {
        nanInfToNullAutoOpt(data, null_map, size);
        benchmark::DoNotOptimize(null_map);
    }
}
BENCHMARK(BMNanInfToNullAutoOpt);

DECLARE_AVX2_SPECIFIC_CODE(

    void nanInfToNullSIMD(float * data, uint8_t * null_map, size_t size) {
        const __m256 inf = _mm256_set1_ps(INFINITY);
        const __m256 neg_inf = _mm256_set1_ps(-INFINITY);
        const __m256 zero = _mm256_set1_ps(0.0f);

        size_t i = 0;
        for (; i + 7 < size; i += 8)
        {
            __m256 values = _mm256_loadu_ps(&data[i]);

            __m256 is_inf = _mm256_cmp_ps(values, inf, _CMP_EQ_OQ);
            __m256 is_neg_inf = _mm256_cmp_ps(values, neg_inf, _CMP_EQ_OQ);
            __m256 is_nan = _mm256_cmp_ps(values, values, _CMP_NEQ_UQ);
            __m256 is_null = _mm256_or_ps(_mm256_or_ps(is_inf, is_neg_inf), is_nan);
            __m256 new_values = _mm256_blendv_ps(values, zero, is_null);

            _mm256_storeu_ps(&data[i], new_values);

            UInt32 mask = static_cast<UInt32>(_mm256_movemask_ps(is_null));
            for (size_t j = 0; j < 8; ++j)
            {
                UInt8 null_flag = (mask & 1U);
                null_map[i + j] = null_flag;
                mask >>= 1;
            }
        }
    })

static void BMNanInfToNullAVX2(benchmark::State & state)
{
    constexpr size_t size = 8192;
    float data[size];
    uint8_t null_map[size] = {0};
    for (size_t i = 0; i < size; ++i)
        data[i] = static_cast<float>(rand()) / rand();

    for (auto _ : state)
    {
        ::TargetSpecific::AVX2::nanInfToNullSIMD(data, null_map, size);
        benchmark::DoNotOptimize(null_map);
    }
}
BENCHMARK(BMNanInfToNullAVX2);

static void nanInfToNull(float * data, uint8_t * null_map, size_t size)
{
    for (size_t i = 0; i < size; ++i)
    {
        if (data[i] != data[i])
            null_map[i] = 1;
        else if ((*reinterpret_cast<const uint32_t *>(&data[i]) & 0b01111111111111111111111111111111) == 0b01111111100000000000000000000000)
            null_map[i] = 1;
        else
            null_map[i] = 0;

        if (null_map[i])
            data[i] = 0.0;
    }
}

static void BMNanInfToNull(benchmark::State & state)
{
    constexpr size_t size = 8192;
    float data[size];
    uint8_t null_map[size] = {0};
    for (size_t i = 0; i < size; ++i)
        data[i] = static_cast<float>(rand()) / rand();

    for (auto _ : state)
    {
        nanInfToNull(data, null_map, size);
        benchmark::DoNotOptimize(null_map);
    }
}
BENCHMARK(BMNanInfToNull);


BENCHMARK(BM_CHFloorFunction_For_Int64);
BENCHMARK(BM_CHFloorFunction_For_Float64);
BENCHMARK(BM_SparkFloorFunction_For_Int64);
BENCHMARK(BM_SparkFloorFunction_For_Float64);