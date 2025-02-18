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

#if defined(__x86_64__)

#include <cstddef>
#include <Columns/ColumnsCommon.h>
#include <Columns/IColumn.h>
#include <Core/Block.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/IDataType.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionsRound.h>
#include <Functions/SparkFunctionFloor.h>
#include <Parser/SerializedPlanParser.h>
#include <base/types.h>
#include <benchmark/benchmark.h>
#include <Common/QueryContext.h>
#include <Common/TargetSpecific.h>
#include <DataTypes/DataTypeNullable.h>

#if USE_MULTITARGET_CODE
#include <immintrin.h>
#endif

using namespace DB;

static IColumn::Offsets createOffsets(size_t rows)
{
    IColumn::Offsets offsets(rows, 0);
    for (size_t i = 0; i < rows; ++i)
        offsets[i] = offsets[i-1] + (rand() % 10);
    return offsets;
}

static ColumnPtr createColumn(const DataTypePtr & type, size_t rows)
{
    const auto * type_array = typeid_cast<const DataTypeArray *>(type.get());
    if (type_array)
    {
        auto data_col = createColumn(type_array->getNestedType(), rows);
        auto offset_col = ColumnArray::ColumnOffsets::create(rows, 0);
        auto & offsets = offset_col->getData();
        for (size_t i = 0; i < data_col->size(); ++i)
            offsets[i] = offsets[i - 1] + (rand() % 10);
        auto new_data_col = data_col->replicate(offsets);

        return ColumnArray::create(std::move(new_data_col), std::move(offset_col));
    }

    auto type_not_nullable = removeNullable(type);
    auto column = type->createColumn();
    for (size_t i = 0; i < rows; ++i)
    {
        if (i % 100)
        {
            column->insertDefault();
        }
        else if (isInt(type_not_nullable))
        {
            column->insert(i);
        }
        else if (isFloat(type_not_nullable))
        {
            double d = i * 1.0;
            column->insert(d);
        }
        else if (isDecimal(type_not_nullable))
        {
            Decimal128 d = Decimal128(i * i);
            column->insert(d);
        }
        else if (isString(type_not_nullable))
        {
            String s = "helloworld";
            column->insert(s);
        }
        else
        {
            column->insertDefault();
        }
    }
    return std::move(column);
}

static Block createBlock(const String & type_str, size_t rows)
{
    auto type = DataTypeFactory::instance().get(type_str);
    auto column = createColumn(type, rows);

    Block block;
    block.insert(ColumnWithTypeAndName(std::move(column), type, "d"));
    return std::move(block);
}

static void BM_CHFloorFunction_For_Int64(benchmark::State & state)
{
    using namespace DB;
    auto & factory = FunctionFactory::instance();
    auto function = factory.get("floor", local_engine::QueryContext::globalContext());
    Block int64_block = createBlock("Nullable(Int64)", 65536);
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
    Block float64_block = createBlock("Nullable(Float64)", 65536);
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
    Block int64_block = createBlock("Nullable(Int64)", 65536);
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
    Block float64_block = createBlock("Nullable(Float64)", 65536);
    auto executable = function->build(float64_block.getColumnsWithTypeAndName());
    for (auto _ : state)
    {
        auto result = executable->execute(float64_block.getColumnsWithTypeAndName(), executable->getResultType(), float64_block.rows(), false);
        benchmark::DoNotOptimize(result);
    }
}

BENCHMARK(BM_CHFloorFunction_For_Int64);
BENCHMARK(BM_CHFloorFunction_For_Float64);
BENCHMARK(BM_SparkFloorFunction_For_Int64);
BENCHMARK(BM_SparkFloorFunction_For_Float64);

static void BM_OptSparkDivide_VectorVector(benchmark::State & state)
{
    using namespace DB;
    auto & factory = FunctionFactory::instance();
    auto function = factory.get("sparkDivide", local_engine::QueryContext::globalContext());
    auto type = DataTypeFactory::instance().get("Nullable(Float64)");
    auto left = createColumn(type, 65536);
    auto right = createColumn(type, 65536);
    auto block = Block({ColumnWithTypeAndName(left, type, "left"), ColumnWithTypeAndName(right, type, "right")});
    auto executable = function->build(block.getColumnsWithTypeAndName());
    for (auto _ : state)
    {
        auto result = executable->execute(block.getColumnsWithTypeAndName(), executable->getResultType(), block.rows(), false);
        benchmark::DoNotOptimize(result);
    }
}

static void BM_OptSparkDivide_VectorConstant(benchmark::State & state)
{
    using namespace DB;
    auto & factory = FunctionFactory::instance();
    auto function = factory.get("sparkDivide", local_engine::QueryContext::globalContext());
    auto type = DataTypeFactory::instance().get("Nullable(Float64)");
    auto left = createColumn(type, 65536);
    auto right = createColumn(type, 1);
    auto const_right = ColumnConst::create(std::move(right), 65536);
    auto block = Block({ColumnWithTypeAndName(left, type, "left"), ColumnWithTypeAndName(std::move(const_right), type, "right")});
    auto executable = function->build(block.getColumnsWithTypeAndName());
    for (auto _ : state)
    {
        auto result = executable->execute(block.getColumnsWithTypeAndName(), executable->getResultType(), block.rows(), false);
        benchmark::DoNotOptimize(result);
    }
}

static void BM_OptSparkDivide_ConstantVector(benchmark::State & state)
{
    using namespace DB;
    auto & factory = FunctionFactory::instance();
    auto function = factory.get("sparkDivide", local_engine::QueryContext::globalContext());
    auto type = DataTypeFactory::instance().get("Nullable(Float64)");
    auto left = createColumn(type, 1);
    auto const_left = ColumnConst::create(std::move(left), 65536);
    auto right = createColumn(type, 65536);
    auto block = Block({ColumnWithTypeAndName(std::move(const_left), type, "left"), ColumnWithTypeAndName(std::move(right), type, "right")});
    auto executable = function->build(block.getColumnsWithTypeAndName());
    for (auto _ : state)
    {
        auto result = executable->execute(block.getColumnsWithTypeAndName(), executable->getResultType(), block.rows(), false);
        benchmark::DoNotOptimize(result);
    }
}

BENCHMARK(BM_OptSparkDivide_VectorVector);
BENCHMARK(BM_OptSparkDivide_VectorConstant);
BENCHMARK(BM_OptSparkDivide_ConstantVector);

static void BM_OptSparkCastFloatToInt(benchmark::State & state)
{
    using namespace DB;
    auto & factory = FunctionFactory::instance();
    auto function = factory.get("sparkCastFloatToInt32", local_engine::QueryContext::globalContext());
    auto type = DataTypeFactory::instance().get("Nullable(Float64)");
    auto input = createColumn(type, 65536);
    auto block = Block({ColumnWithTypeAndName(std::move(input), type, "input")});
    auto executable = function->build(block.getColumnsWithTypeAndName());
    for (auto _ : state)
    {
        auto result = executable->execute(block.getColumnsWithTypeAndName(), executable->getResultType(), block.rows(), false);
        benchmark::DoNotOptimize(result);
    }
}

BENCHMARK(BM_OptSparkCastFloatToInt);

/// decimal to decimal, scale up
static void BM_OptCheckDecimalOverflowSparkFromDecimal1(benchmark::State & state)
{
    using namespace DB;
    auto & factory = FunctionFactory::instance();
    auto function = factory.get("checkDecimalOverflowSparkOrNull", local_engine::QueryContext::globalContext());
    auto type = DataTypeFactory::instance().get("Nullable(Decimal128(10))");

    auto input = createColumn(type, 65536);
    auto precision = ColumnConst::create(ColumnUInt32::create(1, 38), 65536);
    auto scale = ColumnConst::create(ColumnUInt32::create(1, 5), 65536);

    auto block = Block(
        {ColumnWithTypeAndName(std::move(input), type, "input"),
         ColumnWithTypeAndName(std::move(precision), std::make_shared<DataTypeUInt32>(), "precision"),
         ColumnWithTypeAndName(std::move(scale), std::make_shared<DataTypeUInt32>(), "scale")});
    auto executable = function->build(block.getColumnsWithTypeAndName());
    for (auto _ : state)
    {
        auto result = executable->execute(block.getColumnsWithTypeAndName(), executable->getResultType(), block.rows(), false);
        benchmark::DoNotOptimize(result);
    }
}

/// decimal to decimal, scale down
static void BM_OptCheckDecimalOverflowSparkFromDecimal2(benchmark::State & state)
{
    using namespace DB;
    auto & factory = FunctionFactory::instance();
    auto function = factory.get("checkDecimalOverflowSparkOrNull", local_engine::QueryContext::globalContext());
    auto type = DataTypeFactory::instance().get("Nullable(Decimal128(10))");

    auto input = createColumn(type, 65536);
    auto precision = ColumnConst::create(ColumnUInt32::create(1, 38), 65536);
    auto scale = ColumnConst::create(ColumnUInt32::create(1, 15), 65536);

    auto block = Block(
        {ColumnWithTypeAndName(std::move(input), type, "input"),
         ColumnWithTypeAndName(std::move(precision), std::make_shared<DataTypeUInt32>(), "precision"),
         ColumnWithTypeAndName(std::move(scale), std::make_shared<DataTypeUInt32>(), "scale")});
    auto executable = function->build(block.getColumnsWithTypeAndName());
    for (auto _ : state)
    {
        auto result = executable->execute(block.getColumnsWithTypeAndName(), executable->getResultType(), block.rows(), false);
        benchmark::DoNotOptimize(result);
    }
}

/// decimal to decimal, scale doesn't change
static void BM_OptCheckDecimalOverflowSparkFromDecimal3(benchmark::State & state)
{
    using namespace DB;
    auto & factory = FunctionFactory::instance();
    auto function = factory.get("checkDecimalOverflowSparkOrNull", local_engine::QueryContext::globalContext());
    auto type = DataTypeFactory::instance().get("Nullable(Decimal(38, 10))");

    auto input = createColumn(type, 65536);
    auto precision = ColumnConst::create(ColumnUInt32::create(1, 38), 65536);
    auto scale = ColumnConst::create(ColumnUInt32::create(1, 10), 65536);

    auto block = Block(
        {ColumnWithTypeAndName(std::move(input), type, "input"),
         ColumnWithTypeAndName(std::move(precision), std::make_shared<DataTypeUInt32>(), "precision"),
         ColumnWithTypeAndName(std::move(scale), std::make_shared<DataTypeUInt32>(), "scale")});
    auto executable = function->build(block.getColumnsWithTypeAndName());
    for (auto _ : state)
    {
        auto result = executable->execute(block.getColumnsWithTypeAndName(), executable->getResultType(), block.rows(), false);
        benchmark::DoNotOptimize(result);
    }
}

/// int to decimal
static void BM_OptCheckDecimalOverflowSparkFromInt(benchmark::State & state)
{
    using namespace DB;
    auto & factory = FunctionFactory::instance();
    auto function = factory.get("checkDecimalOverflowSparkOrNull", local_engine::QueryContext::globalContext());
    auto type = DataTypeFactory::instance().get("Nullable(Int64)");

    auto input = createColumn(type, 65536);
    auto precision = ColumnConst::create(ColumnUInt32::create(1, 38), 65536);
    auto scale = ColumnConst::create(ColumnUInt32::create(1, 10), 65536);

    auto block = Block(
        {ColumnWithTypeAndName(std::move(input), type, "input"),
         ColumnWithTypeAndName(std::move(precision), std::make_shared<DataTypeUInt32>(), "precision"),
         ColumnWithTypeAndName(std::move(scale), std::make_shared<DataTypeUInt32>(), "scale")});
    auto executable = function->build(block.getColumnsWithTypeAndName());
    for (auto _ : state)
    {
        auto result = executable->execute(block.getColumnsWithTypeAndName(), executable->getResultType(), block.rows(), false);
        benchmark::DoNotOptimize(result);
    }
}

/// float to decimal
static void BM_OptCheckDecimalOverflowSparkFromFloat(benchmark::State & state)
{
    using namespace DB;
    auto & factory = FunctionFactory::instance();
    auto function = factory.get("checkDecimalOverflowSparkOrNull", local_engine::QueryContext::globalContext());
    auto type = DataTypeFactory::instance().get("Nullable(Float64)");

    auto input = createColumn(type, 65536);
    auto precision = ColumnConst::create(ColumnUInt32::create(1, 38), 65536);
    auto scale = ColumnConst::create(ColumnUInt32::create(1, 10), 65536);

    auto block = Block(
        {ColumnWithTypeAndName(std::move(input), type, "input"),
         ColumnWithTypeAndName(std::move(precision), std::make_shared<DataTypeUInt32>(), "precision"),
         ColumnWithTypeAndName(std::move(scale), std::make_shared<DataTypeUInt32>(), "scale")});
    auto executable = function->build(block.getColumnsWithTypeAndName());
    for (auto _ : state)
    {
        auto result = executable->execute(block.getColumnsWithTypeAndName(), executable->getResultType(), block.rows(), false);
        benchmark::DoNotOptimize(result);
    }
}

BENCHMARK(BM_OptCheckDecimalOverflowSparkFromDecimal1);
BENCHMARK(BM_OptCheckDecimalOverflowSparkFromDecimal2);
BENCHMARK(BM_OptCheckDecimalOverflowSparkFromDecimal3);
BENCHMARK(BM_OptCheckDecimalOverflowSparkFromInt);
BENCHMARK(BM_OptCheckDecimalOverflowSparkFromFloat);

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



/*
/// TO run in https://quick-bench.com/q/h-2qGgqxM8ksp57VD0w7JdKKN-I
using UInt8 = unsigned char;
using UInt64 = unsigned long long;
using Int64 = signed long long;
template<typename T>
using PaddedPODArray = std::vector<T>;
*/


/*
Test performance of fillConstantConstant*
Benchmark when BranchType is Int64
-------------------------------------------------------------------
Benchmark                         Time             CPU   Iterations
-------------------------------------------------------------------
BM_fillConstantConstant1      31360 ns        31359 ns        22249
BM_fillConstantConstant2      31369 ns        31368 ns        22288
BM_fillConstantConstant3      31583 ns        31581 ns        22254
*/

/*
Test performance of fillVectorVector*
Benchmark when BranchType is Float64
---------------------------------------------------------------
Benchmark                     Time             CPU   Iterations
---------------------------------------------------------------
BM_fillVectorVector1     414177 ns       414161 ns         1687
BM_fillVectorVector2      96669 ns        96665 ns         7432
BM_fillVectorVector3      78439 ns        78436 ns         8812

Benchmark when BranchType is Int64
---------------------------------------------------------------
Benchmark                     Time             CPU   Iterations
---------------------------------------------------------------
BM_fillVectorVector1      80645 ns        80643 ns         8101
BM_fillVectorVector2      73841 ns        73838 ns         9484
BM_fillVectorVector3      73883 ns        73881 ns         9485

Benchmark when BranchType is Decimal64
---------------------------------------------------------------
Benchmark                     Time             CPU   Iterations
---------------------------------------------------------------
BM_fillVectorVector1      82413 ns        82408 ns         8635
BM_fillVectorVector2      76289 ns        76287 ns         9213
BM_fillVectorVector3      76262 ns        76260 ns         9244

Benchmark when BranchType is Int256
---------------------------------------------------------------
Benchmark                     Time             CPU   Iterations
---------------------------------------------------------------
BM_fillVectorVector1     307741 ns       307726 ns         2263
BM_fillVectorVector2    2184999 ns      2184903 ns          321
BM_fillVectorVector3     318616 ns       318605 ns         2209

Benchmark when BranchType is Decimal256
---------------------------------------------------------------
Benchmark                     Time             CPU   Iterations
---------------------------------------------------------------
BM_fillVectorVector1     303179 ns       303164 ns         2311
BM_fillVectorVector3     305023 ns       305010 ns         2266
*/

/*
Some commands that would be helpful

# run benchmark
./build_gcc/utils/extern-local-engine/tests/benchmark_local_engine --benchmark_filter="BM_fillVectorVector*"

# get full symbol name
objdump -t  ./build_gcc/utils/extern-local-engine/tests/benchmark_local_engine   |  c++filt   | grep "fillVectorVector1"

# get assembly code mixed with source code by symbol name
gdb -batch -ex "disassemble/rs 'void fillVectorVector3<double, double>(DB::PODArray<char8_t, 4096ul, Allocator<false, false>, 63ul, 64ul> const&, DB::PODArray<double, 4096ul, Allocator<false, false>, 63ul, 64ul> const&, DB::PODArray<double, 4096ul, Allocator<false, false>, 63ul, 64ul> const&, DB::PODArray<double, 4096ul, Allocator<false, false>, 63ul, 64ul>&)'" ./build_gcc/utils/extern-local-engine/tests/benchmark_local_engine    | c++filt  > 3.S
*/

using ResultType = Float64;

template <typename T>
static NO_INLINE void fillConstantConstant1(const PaddedPODArray<UInt8> & cond, T a, T b, PaddedPODArray<T> & res)
{
    size_t rows = cond.size();
    for (size_t i = 0; i < rows; ++i)
    {
        res[i] = cond[i] ? static_cast<T>(a) : static_cast<T>(b);
    }
}

template <typename T>
static NO_INLINE void
fillConstantConstant3(const PaddedPODArray<UInt8> & cond, T a, T b, PaddedPODArray<T> & res)
{
    size_t rows = cond.size();
    T new_a = static_cast<T>(a);
    T new_b = static_cast<T>(b);
    alignas(64) const T ab[2] = {new_a, new_b};
    for (size_t i = 0; i < rows; ++i)
    {
        if constexpr (std::is_integral_v<T> && sizeof(T) == 1)
        {
            /// auto opt: cmove and simd is used for integral types
            // res[i] = cond[i] ? new_a : new_b;
            res[i] = ab[!cond[i]];
        }
        else if constexpr (std::is_floating_point_v<T>)
        {
            /// auto opt: cmove not used but simd is used for floating point types
            res[i] = cond[i] ? new_a : new_b;
        }
        else if constexpr (is_decimal<T> && sizeof(T) <= 8)
        {
            /// auto opt: simd is used for decimal types
            res[i] = cond[i] ? new_a : new_b;
        }
        else if constexpr (is_decimal<T> && sizeof(T) == 32)
        {
            /// avoid branch mispredict
            res[i] = ab[!cond[i]];
        }
        else if constexpr (is_decimal<T> && sizeof(T) == 16)
        {
            /// auto opt: cmove and loop unrolling
            // res[i] = cond[i] ? static_cast<T>(a) : static_cast<T>(b);
            res[i] = ab[!cond[i]];
        }
        else if constexpr (is_big_int_v<T> && sizeof(T) == 32)
        {
            res[i] = ab[!cond[i]];
        }
        else if constexpr (is_big_int_v<T> && sizeof(T) == 16)
        {
            // res[i] = cond[i] ? static_cast<T>(a) : static_cast<T>(b);
            res[i] = ab[!cond[i]];
        }
        else
        {
            res[i] = cond[i] ? static_cast<T>(a) : static_cast<T>(b);
        }
    }
}

template <typename Branch1Type = ResultType, typename Branch2Type = ResultType>
static NO_INLINE void fillVectorVector1(
    const PaddedPODArray<UInt8> & cond,
    const PaddedPODArray<Branch1Type> & a,
    const PaddedPODArray<Branch2Type> & b,
    PaddedPODArray<ResultType> & res)
{
    size_t rows = cond.size();
    for (size_t i = 0; i < rows; ++i)
    {
        res[i] = cond[i] ? static_cast<ResultType>(a[i]) : static_cast<ResultType>(b[i]);
    }
}

template <typename Branch1Type = ResultType, typename Branch2Type = ResultType>
static NO_INLINE void fillVectorVector2(
    const PaddedPODArray<UInt8> & cond,
    const PaddedPODArray<Branch1Type> & a,
    const PaddedPODArray<Branch2Type> & b,
    PaddedPODArray<ResultType> & res)
{
    size_t rows = cond.size();
    for (size_t i = 0; i < rows; ++i)
    {
        // res[i] = (!!cond[i]) * static_cast<ResultType>(a[i]) + (!cond[i]) * static_cast<ResultType>(b[i]);
    }
}

template <typename Branch1Type = ResultType, typename Branch2Type = ResultType>
static NO_INLINE void fillVectorVector3(
    const PaddedPODArray<UInt8> & cond,
    const PaddedPODArray<Branch1Type> & a,
    const PaddedPODArray<Branch2Type> & b,
    PaddedPODArray<ResultType> & res)
{
    size_t rows = cond.size();
    for (size_t i = 0; i < rows; ++i)
    {
        if constexpr (std::is_integral_v<ResultType> || (is_decimal<ResultType> && sizeof(ResultType) <= 8))
        {
            // res[i] = (!!cond[i]) * static_cast<ResultType>(a[i]) + (!cond[i]) * static_cast<ResultType>(b[i]);
        }
        else if constexpr (std::is_floating_point_v<ResultType>)
        {
            using UIntType = std::conditional_t<sizeof(ResultType) == 8, UInt64, UInt32>;
            using IntType = std::conditional_t<sizeof(ResultType) == 8, Int64, Int32>;
            auto mask = static_cast<UIntType>(static_cast<IntType>(cond[i]) - 1);
            auto new_a = static_cast<ResultType>(a[i]);
            auto new_b = static_cast<ResultType>(b[i]);
            UIntType uint_a;
            std::memcpy(&uint_a, &new_a, sizeof(UIntType));
            UIntType uint_b;
            std::memcpy(&uint_b, &new_b, sizeof(UIntType));
            UIntType tmp = (~mask & uint_a) | (mask & uint_b);
            // auto tmp = (~mask & (*reinterpret_cast<const UIntType *>(&new_a))) | (mask & (*reinterpret_cast<const UIntType *>(&new_b)));
            res[i] = *(reinterpret_cast<ResultType *>(&tmp));
        }
        else
        {
            res[i] = cond[i] ? static_cast<ResultType>(a[i]) : static_cast<ResultType>(b[i]);
        }
    }
}

static constexpr size_t ROWS = 65536;
static void initCondition(PaddedPODArray<UInt8> & cond)
{
    cond.resize(ROWS);
    for (size_t i = 0; i < ROWS; ++i)
    {
        cond[i] = std::rand() % 2;
    }
}

template <typename T>
static void initBranch(PaddedPODArray<T> & branch)
{
    branch.resize(ROWS);
    for (size_t i = 0; i < ROWS; ++i)
    {
        branch[i] = static_cast<T>(std::rand());
    }
}

template <typename T = ResultType>
static void BM_fillConstantConstant1(benchmark::State & state)
{
    PaddedPODArray<UInt8> cond;
    T a(std::rand());
    T b(std::rand());
    PaddedPODArray<T> res(ROWS);
    initCondition(cond);

    for (auto _ : state)
    {
        fillConstantConstant1(cond, a, b, res);
        benchmark::DoNotOptimize(res);
    }
}

template <typename T = ResultType>
static void BM_fillConstantConstant3(benchmark::State & state)
{
    PaddedPODArray<UInt8> cond;
    T a(std::rand());
    T b(std::rand());
    PaddedPODArray<T> res(ROWS);
    initCondition(cond);

    for (auto _ : state)
    {
        fillConstantConstant3(cond, a, b, res);
        benchmark::DoNotOptimize(res);
    }
}

static void BM_fillVectorVector1(benchmark::State & state)
{
    PaddedPODArray<UInt8> cond;
    PaddedPODArray<ResultType> a;
    PaddedPODArray<ResultType> b;
    PaddedPODArray<ResultType> res(ROWS);
    initCondition(cond);
    initBranch(a);
    initBranch(b);

    for (auto _ : state)
    {
        fillVectorVector1(cond, a, b, res);
        benchmark::DoNotOptimize(res);
    }
}

static void BM_fillVectorVector2(benchmark::State & state)
{
    PaddedPODArray<UInt8> cond;
    PaddedPODArray<ResultType> a;
    PaddedPODArray<ResultType> b;
    PaddedPODArray<ResultType> res(ROWS);
    initCondition(cond);
    initBranch(a);
    initBranch(b);

    for (auto _ : state)
    {
        fillVectorVector2(cond, a, b, res);
        benchmark::DoNotOptimize(res);
    }
}

static void BM_fillVectorVector3(benchmark::State & state)
{
    PaddedPODArray<UInt8> cond;
    PaddedPODArray<ResultType> a;
    PaddedPODArray<ResultType> b;
    PaddedPODArray<ResultType> res(ROWS);
    initCondition(cond);
    initBranch(a);
    initBranch(b);

    for (auto _ : state)
    {
        fillVectorVector3(cond, a, b, res);
        benchmark::DoNotOptimize(res);
    }
}

/*
-------------------------------------------------------------------------------
Benchmark                                     Time             CPU   Iterations
-------------------------------------------------------------------------------
BM_fillConstantConstant1<Int8>           492635 ns       492619 ns         1415
BM_fillConstantConstant3<Int8>            80339 ns        80336 ns         8803
BM_fillConstantConstant1<Int16>            7899 ns         7899 ns        88745
BM_fillConstantConstant3<Int16>            7903 ns         7903 ns        88738
BM_fillConstantConstant1<Int32>           15704 ns        15703 ns        44615
BM_fillConstantConstant3<Int32>           15849 ns        15848 ns        44592
BM_fillConstantConstant1<Int64>           31443 ns        31442 ns        22226
BM_fillConstantConstant3<Int64>           31407 ns        31406 ns        22304
BM_fillConstantConstant1<Int128>          95711 ns        95709 ns         7317
BM_fillConstantConstant3<Int128>          91466 ns        91463 ns         7657
BM_fillConstantConstant1<Int256>         565219 ns       565201 ns         1233
BM_fillConstantConstant3<Int256>         131145 ns       131140 ns         5350
BM_fillConstantConstant1<Float32>         15768 ns        15768 ns        44554
BM_fillConstantConstant3<Float32>         15685 ns        15684 ns        44597
BM_fillConstantConstant1<Float64>         31377 ns        31376 ns        22281
BM_fillConstantConstant3<Float64>         31367 ns        31366 ns        22307
BM_fillConstantConstant1<Decimal32>       65185 ns        65182 ns        10912
BM_fillConstantConstant3<Decimal32>       15703 ns        15702 ns        44490
BM_fillConstantConstant1<Decimal64>       64509 ns        64507 ns        10875
BM_fillConstantConstant3<Decimal64>       31839 ns        31838 ns        22305
BM_fillConstantConstant1<Decimal128>      95602 ns        95600 ns         7325
BM_fillConstantConstant3<Decimal128>      91615 ns        91612 ns         7646
BM_fillConstantConstant1<Decimal256>     572220 ns       572208 ns         1234
BM_fillConstantConstant3<Decimal256>     130326 ns       130323 ns         5375
BM_fillConstantConstant1<DateTime64>      64597 ns        64596 ns        10844
BM_fillConstantConstant3<DateTime64>      64964 ns        64963 ns        10885
*/
BENCHMARK_TEMPLATE(BM_fillConstantConstant1, UInt8);
BENCHMARK_TEMPLATE(BM_fillConstantConstant3, UInt8);
BENCHMARK_TEMPLATE(BM_fillConstantConstant1, Int8);
BENCHMARK_TEMPLATE(BM_fillConstantConstant3, Int8);
BENCHMARK_TEMPLATE(BM_fillConstantConstant1, UInt16);
BENCHMARK_TEMPLATE(BM_fillConstantConstant3, UInt16);
BENCHMARK_TEMPLATE(BM_fillConstantConstant1, Int16);
BENCHMARK_TEMPLATE(BM_fillConstantConstant3, Int16);
BENCHMARK_TEMPLATE(BM_fillConstantConstant1, UInt32);
BENCHMARK_TEMPLATE(BM_fillConstantConstant3, UInt32);
BENCHMARK_TEMPLATE(BM_fillConstantConstant1, Int32);
BENCHMARK_TEMPLATE(BM_fillConstantConstant3, Int32);
BENCHMARK_TEMPLATE(BM_fillConstantConstant1, UInt64);
BENCHMARK_TEMPLATE(BM_fillConstantConstant3, UInt64);
BENCHMARK_TEMPLATE(BM_fillConstantConstant1, Int64);
BENCHMARK_TEMPLATE(BM_fillConstantConstant3, Int64);
BENCHMARK_TEMPLATE(BM_fillConstantConstant1, UInt128);
BENCHMARK_TEMPLATE(BM_fillConstantConstant3, UInt128);
BENCHMARK_TEMPLATE(BM_fillConstantConstant1, Int128);
BENCHMARK_TEMPLATE(BM_fillConstantConstant3, Int128);
BENCHMARK_TEMPLATE(BM_fillConstantConstant1, UInt256);
BENCHMARK_TEMPLATE(BM_fillConstantConstant3, UInt256);
BENCHMARK_TEMPLATE(BM_fillConstantConstant1, Int256);
BENCHMARK_TEMPLATE(BM_fillConstantConstant3, Int256);
BENCHMARK_TEMPLATE(BM_fillConstantConstant1, Float32);
BENCHMARK_TEMPLATE(BM_fillConstantConstant3, Float32);
BENCHMARK_TEMPLATE(BM_fillConstantConstant1, Float64);
BENCHMARK_TEMPLATE(BM_fillConstantConstant3, Float64);
BENCHMARK_TEMPLATE(BM_fillConstantConstant1, Decimal32);
BENCHMARK_TEMPLATE(BM_fillConstantConstant3, Decimal32);
BENCHMARK_TEMPLATE(BM_fillConstantConstant1, Decimal64);
BENCHMARK_TEMPLATE(BM_fillConstantConstant3, Decimal64);
BENCHMARK_TEMPLATE(BM_fillConstantConstant1, Decimal128);
BENCHMARK_TEMPLATE(BM_fillConstantConstant3, Decimal128);
BENCHMARK_TEMPLATE(BM_fillConstantConstant1, Decimal256);
BENCHMARK_TEMPLATE(BM_fillConstantConstant3, Decimal256);
BENCHMARK_TEMPLATE(BM_fillConstantConstant1, DateTime64);
BENCHMARK_TEMPLATE(BM_fillConstantConstant3, DateTime64);

BENCHMARK(BM_fillVectorVector1);
BENCHMARK(BM_fillVectorVector2);
BENCHMARK(BM_fillVectorVector3);


template <typename T>
struct slice
{
    T * data;
};

template <typename T>
NO_INLINE auto BitOrProcess(slice<T> & d)
{
    for (auto i = 0u; i < 65536; ++i)
        d.data[i] |= T(0xaa);
}

template <typename T>
void initSlice(slice<T> & d)
{
    d.data = new T[65536];
    for (auto i = 0u; i < 65536; ++i)
        d.data[i] = T(std::rand());
}

template <typename T>
void finalizeSlice(slice<T> & d)
{
    delete[] d.data;
}

template <typename T>
void BM_BitOrProcess(benchmark::State & state)
{
    slice<T> d;
    initSlice(d);

    for (auto _ : state)
    {
        BitOrProcess(d);
        benchmark::DoNotOptimize(d);
    }
}

BENCHMARK_TEMPLATE(BM_BitOrProcess, char8_t);
BENCHMARK_TEMPLATE(BM_BitOrProcess, int8_t);
BENCHMARK_TEMPLATE(BM_BitOrProcess, uint8_t);

MULTITARGET_FUNCTION_AVX512BW_AVX512F_AVX2_SSE42(
MULTITARGET_FUNCTION_HEADER(static void NO_INLINE), isNotNull, MULTITARGET_FUNCTION_BODY((const PaddedPODArray<UInt8> & null_map, PaddedPODArray<UInt8> & res) /// NOLINT
{
    for (size_t i = 0; i < 65536; ++i)
        res[i] = !null_map[i];
}))



#define BENCHMARK_ISNOTNULL_TEMPLATE(TARGET) \
static void BM_isNotNull##TARGET(benchmark::State & state) \
{ \
    PaddedPODArray<UInt8> null_map; \
    initCondition(null_map); \
    for (auto _ : state) \
    { \
        PaddedPODArray<UInt8> res(ROWS); \
        isNotNull##TARGET(null_map, res); \
        benchmark::DoNotOptimize(res); \
    } \
} \
BENCHMARK(BM_isNotNull##TARGET);

BENCHMARK_ISNOTNULL_TEMPLATE()
BENCHMARK_ISNOTNULL_TEMPLATE(SSE42)
BENCHMARK_ISNOTNULL_TEMPLATE(AVX2)
BENCHMARK_ISNOTNULL_TEMPLATE(AVX512F)
BENCHMARK_ISNOTNULL_TEMPLATE(AVX512BW)

MULTITARGET_FUNCTION_AVX512BW_AVX512F_AVX2_SSE42(
MULTITARGET_FUNCTION_HEADER(static void NO_INLINE), isNotNullTest, MULTITARGET_FUNCTION_BODY((const PaddedPODArray<UInt8> & null_map, PaddedPODArray<UInt8> & res) /// NOLINT
{
    res.reserve(ROWS);
    for (size_t i = 0; i < ROWS; ++i)
        res.push_back(!null_map[i]);
}))


#define BENCHMARK_ISNOTNULLTEST_TEMPLATE(TARGET) \
static void BM_isNotNullTest##TARGET(benchmark::State & state) \
{ \
    PaddedPODArray<UInt8> null_map; \
    initCondition(null_map); \
    for (auto _ : state) \
    { \
        PaddedPODArray<UInt8> res; \
        isNotNullTest##TARGET(null_map, res); \
        benchmark::DoNotOptimize(res); \
    } \
} \
BENCHMARK(BM_isNotNullTest##TARGET);

BENCHMARK_ISNOTNULLTEST_TEMPLATE()
BENCHMARK_ISNOTNULLTEST_TEMPLATE(SSE42)
BENCHMARK_ISNOTNULLTEST_TEMPLATE(AVX2)
BENCHMARK_ISNOTNULLTEST_TEMPLATE(AVX512F)
BENCHMARK_ISNOTNULLTEST_TEMPLATE(AVX512BW)

/*
Run on (32 X 2100 MHz CPU s)
CPU Caches:
  L1 Data 32 KiB (x16)
  L1 Instruction 32 KiB (x16)
  L2 Unified 1024 KiB (x16)
  L3 Unified 11264 KiB (x2)
Load Average: 4.47, 4.43, 4.74
-------------------------------------------------------------------
Benchmark                         Time             CPU   Iterations
-------------------------------------------------------------------
BM_isNotNull                   3854 ns         3854 ns       181463
BM_isNotNullSSE42              3859 ns         3859 ns       181243
BM_isNotNullAVX2               3037 ns         3037 ns       232898
BM_isNotNullAVX512F            2859 ns         2859 ns       245235
BM_isNotNullAVX512BW           2880 ns         2880 ns       244221
BM_isNotNullTest              95141 ns        95139 ns         7342
BM_isNotNullTestSSE42         95201 ns        95199 ns         7322
BM_isNotNullTestAVX2          95107 ns        95105 ns         7362
BM_isNotNullTestAVX512F       95151 ns        95147 ns         7370
BM_isNotNullTestAVX512BW      95150 ns        95148 ns         7348
*/


static NO_INLINE void insertManyFrom(IColumn & dst, const IColumn & src)
{
    size_t size = src.size();
    dst.insertManyFrom(src, size/2, size);
}

/*
static NO_INLINE void insertManyFromV1(IColumn & dst, const IColumn & src)
{
    size_t size = src.size();
    ColumnNullable * dst_nullable = typeid_cast<ColumnNullable *>(&dst);
    ColumnVector<Int64> * dst_nested = typeid_cast<ColumnVector<Int64> *>(&dst_nullable->getNestedColumn());
    auto & dst_data = dst_nested->getData();
    auto & dst_null_map = dst_nullable->getNullMapData();

    auto src_field = src[size/2];
    if (src_field.isNull())
    {
        dst_data.resize_fill(size, 0);
        dst_null_map.resize_fill(size, 1);
    }
    else
    {
        auto src_value = src_field.get<Int64>();
        dst_data.resize_fill(size, src_value);
        dst_null_map.resize_fill(size, 0);
    }
}
*/

template <const std::string & str_type>
static void BM_insertManyFrom(benchmark::State & state)
{
    auto type = DataTypeFactory::instance().get(str_type);
    auto src = createColumn(type, ROWS);

    for (auto _ : state)
    {
        state.PauseTiming();
        auto dst = type->createColumn();
        dst->reserve(ROWS);
        state.ResumeTiming();

        insertManyFrom(*dst, *src);
        benchmark::DoNotOptimize(dst);
    }
}

static const String type_int64 = "Int64";
static const String type_nullable_int64 = "Nullable(Int64)";
static const String type_string = "String";
static const String type_nullable_string = "Nullable(String)";
static const String type_decimal = "Decimal128(3)";
static const String type_nullable_decimal = "Nullable(Decimal128(3))";

static const String type_array_int64 = "Array(Int64)";
static const String type_array_nullable_int64 = "Array(Nullable(Int64))";
static const String type_array_string = "Array(String)";
static const String type_array_nullable_string = "Array(Nullable(String))";

BENCHMARK_TEMPLATE(BM_insertManyFrom, type_int64);
BENCHMARK_TEMPLATE(BM_insertManyFrom, type_nullable_int64);
BENCHMARK_TEMPLATE(BM_insertManyFrom, type_string);
BENCHMARK_TEMPLATE(BM_insertManyFrom, type_nullable_string);
BENCHMARK_TEMPLATE(BM_insertManyFrom, type_decimal);
BENCHMARK_TEMPLATE(BM_insertManyFrom, type_nullable_decimal);

BENCHMARK_TEMPLATE(BM_insertManyFrom, type_array_int64);
BENCHMARK_TEMPLATE(BM_insertManyFrom, type_array_nullable_int64);
BENCHMARK_TEMPLATE(BM_insertManyFrom, type_array_string);
BENCHMARK_TEMPLATE(BM_insertManyFrom, type_array_nullable_string);

/// Benchmark result: https://github.com/ClickHouse/ClickHouse/pull/60846

template <const std::string & str_type>
static void BM_replicate(benchmark::State & state)
{
    auto type = DataTypeFactory::instance().get(str_type);
    auto col = createColumn(type, ROWS);
    auto offsets = createOffsets(ROWS);
    for (auto _ : state)
    {
        auto new_col = col->replicate(offsets);
        benchmark::DoNotOptimize(new_col);
    }
}

BENCHMARK_TEMPLATE(BM_replicate, type_string);
BENCHMARK_TEMPLATE(BM_replicate, type_nullable_string);


IColumn::Filter mockFilter(size_t rows)
{
    IColumn::Filter result(rows);
    for (size_t i = 0; i < rows; ++i)
        result[i] = rand() % 2 ? 0 : 1;
    return std::move(result);
}

ColumnPtr mockIndexes(size_t rows)
{
    auto filter = mockFilter(rows);
    auto result = ColumnUInt64::create();
    auto & data = result->getData();
    for (size_t i = 0; i < rows; ++i)
    {
        if (filter[i])
            data.push_back(i);
    }
    return std::move(result);
}

template <const std::string & str_type>
static void BM_filter(benchmark::State & state)
{
    auto type = DataTypeFactory::instance().get(str_type);
    auto src = createColumn(type, ROWS);
    auto filter = mockFilter(ROWS);
    auto dst_size_hint = countBytesInFilter(filter);
    for (auto _ : state)
    {
        auto dst = src->filter(filter, dst_size_hint);
        benchmark::DoNotOptimize(dst);
    }
}

template <const std::string & str_type>
static void BM_index(benchmark::State & state)
{
    auto type = DataTypeFactory::instance().get(str_type);
    auto src = createColumn(type, ROWS);
    auto indexes = mockIndexes(ROWS);
    for (auto _ : state)
    {
        auto dst = src->index(*indexes, 0);
        benchmark::DoNotOptimize(dst);
    }
}


/*
template <const std::string & str_type>
static void BM_filterInPlace(benchmark::State & state)
{
    auto type = DataTypeFactory::instance().get(str_type);
    auto indexes_col = mockIndexes(ROWS);
    const auto & indexes = assert_cast<const ColumnUInt64 &>(*indexes_col).getData();
    for (auto _ : state)
    {
        state.PauseTiming();
        auto src = createColumn(type, ROWS);
        auto mutable_src = src->assumeMutable();
        state.ResumeTiming();

        mutable_src->filterInPlace(indexes, 0);
        benchmark::DoNotOptimize(src);
    }
}
*/


/*
10%
---------------------------------------------------------------------------------
Benchmark                                       Time             CPU   Iterations
---------------------------------------------------------------------------------
BM_filter<type_int64>                       28485 ns        28484 ns        24242
BM_index<type_int64>                         7925 ns         7925 ns        88317
BM_filterInPlace<type_int64>                 8553 ns         8520 ns        84726
BM_filter<type_nullable_int64>              64513 ns        64512 ns        10885
BM_index<type_nullable_int64>               14209 ns        14209 ns        49289
BM_filterInPlace<type_nullable_int64>       17854 ns        17706 ns        41712
BM_filter<type_string>                      82993 ns        82990 ns         8313
BM_index<type_string>                       35828 ns        35827 ns        19943
BM_filterInPlace<type_string>               28748 ns        28621 ns        23922
BM_filter<type_nullable_string>            115106 ns       115099 ns         6057
BM_index<type_nullable_string>              42771 ns        42767 ns        17238
BM_filterInPlace<type_nullable_string>      35743 ns        35625 ns



99%
---------------------------------------------------------------------------------
Benchmark                                       Time             CPU   Iterations
---------------------------------------------------------------------------------
BM_filter<type_int64>                       81142 ns        81139 ns         8908
BM_index<type_int64>                        75867 ns        75865 ns         9197
BM_filterInPlace<type_int64>                57672 ns        57635 ns        12098
BM_filter<type_nullable_int64>             150746 ns       150740 ns         4716
BM_index<type_nullable_int64>              129113 ns       129108 ns         5440
BM_filterInPlace<type_nullable_int64>      111840 ns       111763 ns         6190
BM_filter<type_string>                     367415 ns       367092 ns         1917
BM_index<type_string>                      261185 ns       261174 ns         2682
BM_filterInPlace<type_string>              215903 ns       215809 ns         3180
BM_filter<type_nullable_string>            435441 ns       435426 ns         1577
BM_index<type_nullable_string>             313108 ns       313097 ns         2229
BM_filterInPlace<type_nullable_string>     271534 ns       271424 ns
*/

BENCHMARK_TEMPLATE(BM_filter, type_int64);
BENCHMARK_TEMPLATE(BM_index, type_int64);
// BENCHMARK_TEMPLATE(BM_filterInPlace, type_int64);

BENCHMARK_TEMPLATE(BM_filter, type_nullable_int64);
BENCHMARK_TEMPLATE(BM_index, type_nullable_int64);
// BENCHMARK_TEMPLATE(BM_filterInPlace, type_nullable_int64);

BENCHMARK_TEMPLATE(BM_filter, type_string);
BENCHMARK_TEMPLATE(BM_index, type_string);
// BENCHMARK_TEMPLATE(BM_filterInPlace, type_string);

BENCHMARK_TEMPLATE(BM_filter, type_nullable_string);
BENCHMARK_TEMPLATE(BM_index, type_nullable_string);
// BENCHMARK_TEMPLATE(BM_filterInPlace, type_nullable_string);



/// If mask is a number of this kind: [0]*[1]* function returns the length of the cluster of 1s.
/// Otherwise it returns the special value: 0xFF.
static inline uint8_t prefixToCopy(UInt64 mask)
{
    if (mask == 0)
        return 0;
    if (mask == static_cast<UInt64>(-1))
        return 64;
    /// Row with index 0 correspond to the least significant bit.
    /// So the length of the prefix to copy is 64 - #(leading zeroes).
    const UInt64 leading_zeroes = __builtin_clzll(mask);
    if (mask == ((static_cast<UInt64>(-1) << leading_zeroes) >> leading_zeroes))
        return 64 - leading_zeroes;
    else
        return 0xFF;
}

static inline uint8_t suffixToCopy(UInt64 mask)
{
    const auto prefix_to_copy = prefixToCopy(~mask);
    return prefix_to_copy >= 64 ? prefix_to_copy : 64 - prefix_to_copy;
}


static inline UInt64 blsr(UInt64 mask)
{
#ifdef __BMI__
    return _blsr_u64(mask);
#else
    return mask & (mask-1);
#endif
}

DECLARE_DEFAULT_CODE(

template <typename T>
void myFilterToIndices(const UInt8 * filt, size_t start, size_t end, PaddedPODArray<T> & indices)
{
    size_t pos = 0;
    for (; start + 64 <= end; start += 64)
    {
        UInt64 mask = bytes64MaskToBits64Mask(filt + start);
        const uint8_t prefix_to_copy = prefixToCopy(mask);

        if (0xFF != prefix_to_copy)
        {
            for (size_t i = 0; i < prefix_to_copy; ++i)
                indices[pos++] = start + i;
        }
        else
        {
            const uint8_t suffix_to_copy = suffixToCopy(mask);
            if (0xFF != suffix_to_copy)
            {
                for (size_t i = 64 - suffix_to_copy; i < 64; ++i)
                    indices[pos++] = start + i;
            }
            else
            {
                while (mask)
                {
                    size_t index = std::countr_zero(mask);
                    indices[pos++] = start + index;
                    mask = blsr(mask);
                }
            }
        }
    }

    for (; start != end; ++start)
        if (filt[start])
            indices[pos++] = start;
}
)

DECLARE_AVX512F_SPECIFIC_CODE(

template <typename T>
void myFilterToIndices(const UInt8 * filt, size_t start, size_t end, PaddedPODArray<T> & indices)
{
    static constexpr size_t LOOPS_PER_MASK = sizeof(T);
    static constexpr size_t MASK_BITS_PER_LOOP = 64 / LOOPS_PER_MASK;
    static constexpr UInt64 MASK_IN_LOOP = (1ULL << MASK_BITS_PER_LOOP) - 1;

    __m512i index_vec;
    __m512i increment_vec;
    if constexpr (std::is_same_v<T, UInt64>)
    {
        index_vec
            = _mm512_set_epi64(start + 7, start + 6, start + 5, start + 4, start + 3, start + 2, start + 1, start); // Initial index vector
        increment_vec = _mm512_set1_epi64(8); // Increment vector
    }
    else if constexpr (std::is_same_v<T, UInt32>)
    {
        index_vec = _mm512_set_epi32(
            start + 15,
            start + 14,
            start + 13,
            start + 12,
            start + 11,
            start + 10,
            start + 9,
            start + 8,
            start + 7,
            start + 6,
            start + 5,
            start + 4,
            start + 3,
            start + 2,
            start + 1,
            start); // Initial index vector
        increment_vec = _mm512_set1_epi64(16); // Increment vector
    }
    /*
    else if constexpr (std::is_same_v<T, UInt16>)
    {
        index_vec = _mm512_set_epi16(
            start + 31,
            start + 30,
            start + 29,
            start + 28,
            start + 27,
            start + 26,
            start + 25,
            start + 24,
            start + 23,
            start + 22,
            start + 21,
            start + 20,
            start + 19,
            start + 18,
            start + 17,
            start + 16,
            start + 15,
            start + 14,
            start + 13,
            start + 12,
            start + 11,
            start + 10,
            start + 9,
            start + 8,
            start + 7,
            start + 6,
            start + 5,
            start + 4,
            start + 3,
            start + 2,
            start + 1,
            start); // Initial index vector
        increment_vec = _mm512_set1_epi64(32); // Increment vector
    }
    */

    size_t pos = 0;
    for (; start + 64 <= end; start += 64)
    {
        UInt64 mask64 = bytes64MaskToBits64Mask(filt + start);

        for (size_t i = 0; i < LOOPS_PER_MASK; ++i)
        {
            auto offset = std::popcount(mask64 & MASK_IN_LOOP);
            if (offset)
            {
                if constexpr (std::is_same_v<T, UInt64>)
                {
                    __m512i compressed_indices = _mm512_maskz_compress_epi64(mask64 & MASK_IN_LOOP, index_vec); // Compress indices
                    _mm512_storeu_si512(&indices[pos], compressed_indices); // Store compressed indices
                }
                else if constexpr (std::is_same_v<T, UInt32>)
                {
                    __m512i compressed_indices = _mm512_maskz_compress_epi32(mask64 & MASK_IN_LOOP, index_vec); // Compress indices
                    _mm512_storeu_si512(&indices[pos], compressed_indices); // Store compressed indices
                }
                /*
                else if constexpr (std::is_same_v<T, UInt16>)
                {
                    __m512i compressed_indices = _mm512_maskz_compress_epi16(mask64 & MASK_IN_LOOP, index_vec); // Compress indices
                    _mm512_storeu_si512(&indices[pos], compressed_indices); // Store compressed indices
                }
                */

                pos += offset;
            }


            if constexpr (std::is_same_v<T, UInt64>)
                index_vec = _mm512_add_epi64(index_vec, increment_vec); // Increment the index vector
            else if constexpr (std::is_same_v<T, UInt32>)
                index_vec = _mm512_add_epi32(index_vec, increment_vec); // Increment the index vector
            /*
            else if constexpr (std::is_same_v<T, UInt16>)
                index_vec = _mm512_add_epi16(index_vec, increment_vec); // Increment the index vector
            */

            mask64 >>= MASK_BITS_PER_LOOP;
        }
    }

    for (; start != end; ++start)
    {
        if (filt[start])
            indices[pos++] = start;
    }
})

template <typename T>
static NO_INLINE size_t myFilterToIndicesAVX512(const IColumn::Filter & filt, PaddedPODArray<T> & indices)
{
    static constexpr size_t PADDING_BYTES = 64/sizeof(T) - 1;
    if (filt.empty())
        return 0;

    size_t start = 0;
    size_t end = filt.size();

    size_t size = countBytesInFilter(filt.data(), start, end);
    indices.resize_exact(size + PADDING_BYTES);
    ::TargetSpecific::AVX512F::myFilterToIndices(filt.data(), start, end, indices);
    indices.resize_exact(size);
    return start;
}

template <typename T>
static NO_INLINE size_t myFilterToIndicesDefault(const IColumn::Filter & filt, PaddedPODArray<T> & indices)
{
    if (filt.empty())
        return 0;

    size_t start = 0;
    size_t end = filt.size();
    size_t size = countBytesInFilter(filt.data(), start, end);
    indices.resize_exact(size);
    ::TargetSpecific::Default::myFilterToIndices(filt.data(), start, end, indices);
    return start;
}

template <typename T>
static void BM_myFilterToIndicesDefault(benchmark::State & state)
{
    for (auto _ : state)
    {
        state.PauseTiming();
        auto filter = mockFilter(ROWS);
        state.ResumeTiming();

        PaddedPODArray<T> indices;
        auto start = myFilterToIndicesDefault(filter, indices);
        benchmark::DoNotOptimize(start);
        benchmark::DoNotOptimize(indices);
    }
}

template <typename T>
static void BM_myFilterToIndicesAVX512(benchmark::State & state)
{
    for (auto _ : state)
    {
        state.PauseTiming();
        auto filter = mockFilter(ROWS);
        state.ResumeTiming();

        PaddedPODArray<T> indices;
        auto start = myFilterToIndicesAVX512(filter, indices);
        benchmark::DoNotOptimize(start);
        benchmark::DoNotOptimize(indices);
    }
}

BENCHMARK_TEMPLATE(BM_myFilterToIndicesDefault, UInt16);

/*
BENCHMARK_TEMPLATE(BM_myFilterToIndicesDefault, UInt32);
BENCHMARK_TEMPLATE(BM_myFilterToIndicesDefault, UInt64);

// BENCHMARK_TEMPLATE(BM_myFilterToIndicesAVX512, UInt16);
BENCHMARK_TEMPLATE(BM_myFilterToIndicesAVX512, UInt32);
BENCHMARK_TEMPLATE(BM_myFilterToIndicesAVX512, UInt64);
*/


#endif
