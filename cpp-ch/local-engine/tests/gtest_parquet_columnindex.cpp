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
#include "config.h"
#if USE_PARQUET
#include <charconv>
#include <ranges>
#include <string>
#include <Columns/ColumnString.h>
#include <IO/ReadBufferFromFile.h>
#include <Interpreters/ActionsVisitor.h>
#include <Interpreters/Context.h>
#include <Interpreters/ExpressionActions.h>
#include <Parsers/ExpressionListParsers.h>
#include <Storages/Parquet/ArrowUtils.h>
#include <Storages/Parquet/ColumnIndexFilter.h>
#include <Storages/Parquet/ParquetConverter.h>
#include <Storages/Parquet/RowRanges.h>
#include <Storages/Parquet/VectorizedParquetRecordReader.h>
#include <Storages/Parquet/VirtualColumnRowIndexReader.h>
#include <boost/iterator/counting_iterator.hpp>
#include <gtest/gtest.h>
#include <parquet/page_index.h>
#include <parquet/schema.h>
#include <parquet/statistics.h>
#include <tests/gluten_test_util.h>
#include <Common/BlockTypeUtils.h>
#include <Common/QueryContext.h>

#define ASSERT_DURATION_LE(secs, stmt) \
    { \
        std::promise<bool> completed; \
        auto stmt_future = completed.get_future(); \
        std::thread( \
            [&](std::promise<bool> & completed) \
            { \
                stmt; \
                completed.set_value(true); \
            }, \
            std::ref(completed)) \
            .detach(); \
        if (stmt_future.wait_for(std::chrono::seconds(secs)) == std::future_status::timeout) \
            GTEST_FATAL_FAILURE_("       timed out (> " #secs " seconds). Check code for infinite loops"); \
    }


namespace DB::ErrorCodes
{
extern const int LOGICAL_ERROR;
}

namespace parquet
{
using ColumnIndexPtr = std::unique_ptr<ColumnIndex>;
using OffsetIndexPtr = std::unique_ptr<OffsetIndex>;
using OffsetIndexBuilderPtr = std::unique_ptr<OffsetIndexBuilder>;
}

using namespace DB;

namespace test_utils
{
class PrimitiveNodeBuilder
{
    parquet::Repetition::type repetition_ = parquet::Repetition::UNDEFINED;
    parquet::ConvertedType::type converted_type_ = parquet::ConvertedType::NONE;
    parquet::Type::type physical_type_ = parquet::Type::UNDEFINED;
    int length_ = -1;
    int precision_ = -1;
    int scale_ = -1;

public:
    PrimitiveNodeBuilder & as(parquet::ConvertedType::type converted_type)
    {
        converted_type_ = converted_type;
        return *this;
    }

    PrimitiveNodeBuilder & with_length(int length)
    {
        length_ = length;
        return *this;
    }
    PrimitiveNodeBuilder & asDecimal(int precision, int scale)
    {
        converted_type_ = parquet::ConvertedType::DECIMAL;
        precision_ = precision;
        scale_ = scale;
        return *this;
    }
    parquet::schema::NodePtr named(const std::string & name) const
    {
        assert(!name.empty());
        if (physical_type_ == parquet::Type::UNDEFINED)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Unsupported physical type");
        return parquet::schema::PrimitiveNode::Make(
            name, repetition_, physical_type_, converted_type_, length_, precision_, scale_, /*field_id*/ -1);
    }
    parquet::ColumnDescriptor descriptor(const std::string & name) const { return {named(name), /*max_definition_level=*/1, 0}; }
    static PrimitiveNodeBuilder optional(parquet::Type::type physical_type)
    {
        PrimitiveNodeBuilder builder;
        builder.repetition_ = parquet::Repetition::OPTIONAL;
        builder.physical_type_ = physical_type;
        return builder;
    }
    static PrimitiveNodeBuilder required(parquet::Type::type physical_type)
    {
        PrimitiveNodeBuilder builder;
        builder.repetition_ = parquet::Repetition::REQUIRED;
        builder.physical_type_ = physical_type;
        return builder;
    }
};

class CIBuilder
{
    const parquet::schema::NodePtr node_;
    const parquet::schema::PrimitiveNode * primitive_node_;
    std::vector<parquet::EncodedStatistics> page_stats_;

public:
    explicit CIBuilder(const parquet::schema::NodePtr & node)
        : node_(node), primitive_node_(static_cast<parquet::schema::PrimitiveNode *>(node.get()))
    {
    }

    CIBuilder & addNullPage(int64_t nullCount)
    {
        page_stats_.emplace_back();
        parquet::EncodedStatistics & stats = page_stats_.back();
        stats.all_null_value = true;
        stats.set_null_count(nullCount);
        return *this;
    }
    CIBuilder & addPage(int64_t nullCount, int64_t min, int64_t max)
    {
        assert(primitive_node_ && primitive_node_->physical_type() == parquet::Type::INT64);
        auto encode = [=](int64_t value) { return std::string(reinterpret_cast<const char *>(&value), sizeof(int64_t)); };
        addPage(nullCount, encode(min), encode(max));
        return *this;
    }

    CIBuilder & addPage(int64_t nullCount, Int32 min, Int32 max)
    {
        assert(primitive_node_ && primitive_node_->physical_type() == parquet::Type::INT32);
        auto encode = [=](Int32 value) { return std::string(reinterpret_cast<const char *>(&value), sizeof(Int32)); };
        addPage(nullCount, encode(min), encode(max));
        return *this;
    }
    CIBuilder & addPage(int64_t nullCount, double min, double max)
    {
        auto encode = [=](double value) { return std::string(reinterpret_cast<const char *>(&value), sizeof(double)); };
        addPage(nullCount, encode(min), encode(max));
        return *this;
    }

    CIBuilder & addPage(int64_t nullCount, const std::string & min, const std::string & max)
    {
        page_stats_.emplace_back();
        parquet::EncodedStatistics & stats = page_stats_.back();
        stats.set_null_count(nullCount);
        stats.set_min(min);
        stats.set_max(max);
        return *this;
    }

    CIBuilder & addSamePages(size_t num, int64_t nullCount, const std::string & min, const std::string & max)
    {
        for (size_t i = 0; i < num; ++i)
            addPage(nullCount, min, max);
        return *this;
    }

    parquet::ColumnIndexPtr build() const
    {
        const parquet::ColumnDescriptor descr(node_, /*max_definition_level=*/1, 0);
        const auto builder = parquet::ColumnIndexBuilder::Make(&descr);
        std::ranges::for_each(page_stats_, [&](const auto & stats) { builder->AddPage(stats); });
        builder->Finish();
        return builder->Build();
    }

    parquet::ColumnDescriptor descr() const { return {node_, /*max_definition_level=*/1, 0}; }
};

class OIBuilder
{
    size_t previous_count_ = 0;
    std::vector<size_t> row_index_;

public:
    OIBuilder & addPage(size_t row_count)
    {
        row_index_.push_back(previous_count_);
        previous_count_ += row_count;
        return *this;
    }

    OIBuilder & addSamePages(size_t num, size_t row_count)
    {
        for (size_t i = 0; i < num; ++i)
            addPage(row_count);
        return *this;
    }

    parquet::OffsetIndexPtr build() const
    {
        parquet::OffsetIndexBuilderPtr builder = parquet::OffsetIndexBuilder::Make();
        // we don't care about the offset and compressed_page_size.
        std::ranges::for_each(row_index_, [&](const auto & row_index) { builder->AddPage(1, 1, row_index); });
        constexpr int64_t final_position = 4096;
        builder->Finish(final_position);
        return builder->Build();
    }
};

/***
   * <pre>
   * row     column1        column2        column3        column4        column5
   *                                                 (no column index)
   *      ------0------  ------0------  ------0------  ------0------  ------0------
   * 0.   1              Zulu           2.03                          null
   *      ------1------  ------1------  ------1------  ------1------  ------1------
   * 1.   2              Yankee         4.67                          null
   * 2.   3              Xray           3.42                          null
   * 3.   4              Whiskey        8.71                          null
   *                     ------2------                 ------2------
   * 4.   5              Victor         0.56                          null
   * 5.   6              Uniform        4.30                          null
   *                                    ------2------  ------3------
   * 6.   null           null           null                          null
   *      ------2------                                ------4------
   * 7.   7              Tango          3.50                          null
   *                     ------3------
   * 8.   7              null           3.14                          null
   *      ------3------
   * 9.   7              null           null                          null
   *                                    ------3------
   * 10.  null           null           9.99                          null
   *                     ------4------
   * 11.  8              Sierra         8.78                          null
   *                                                   ------5------
   * 12.  9              Romeo          9.56                          null
   * 13.  10             Quebec         2.71                          null
   *      ------4------
   * 14.  11             Papa           5.71                          null
   * 15.  12             Oscar          4.09                          null
   *                     ------5------  ------4------  ------6------
   * 16.  13             November       null                          null
   * 17.  14             Mike           null                          null
   * 18.  15             Lima           0.36                          null
   * 19.  16             Kilo           2.94                          null
   * 20.  17             Juliett        4.23                          null
   *      ------5------  ------6------                 ------7------
   * 21.  18             India          null                          null
   * 22.  19             Hotel          5.32                          null
   *                                    ------5------
   * 23.  20             Golf           4.17                          null
   * 24.  21             Foxtrot        7.92                          null
   * 25.  22             Echo           7.95                          null
   *                                   ------6------
   * 26.  23             Delta          null                          null
   *      ------6------
   * 27.  24             Charlie        null                          null
   *                                                   ------8------
   * 28.  25             Bravo          null                          null
   *                     ------7------
   * 29.  26             Alfa           null                          null
   * </pre>
   */
constexpr int64_t TOTALSIZE = 30;
using PNB = PrimitiveNodeBuilder;
static const CIBuilder c1 = CIBuilder(PNB::optional(parquet::Type::INT32).named("column1"))
                                .addPage(0, Int32{1}, 1)
                                .addPage(1, Int32{2}, 6)
                                .addPage(0, Int32{7}, 7)
                                .addPage(1, Int32{7}, 10)
                                .addPage(0, Int32{11}, 17)
                                .addPage(0, Int32{18}, 23)
                                .addPage(0, Int32{24}, 26);
static const OIBuilder o1 = OIBuilder().addPage(1).addPage(6).addPage(2).addPage(5).addPage(7).addPage(6).addPage(3);
static const parquet::ColumnDescriptor d1 = c1.descr();

static const CIBuilder c2 = CIBuilder(PNB::optional(parquet::Type::BYTE_ARRAY).as(parquet::ConvertedType::UTF8).named("column2"))
                                .addPage(0, "Zulu", "Zulu")
                                .addPage(0, "Whiskey", "Yankee")
                                .addPage(1, "Tango", "Victor")
                                .addNullPage(3)
                                .addPage(0, "Oscar", "Sierra")
                                .addPage(0, "Juliett", "November")
                                .addPage(0, "Bravo", "India")
                                .addPage(0, "Alfa", "Alfa");
static const OIBuilder o2 = OIBuilder().addPage(1).addPage(3).addPage(4).addPage(3).addPage(5).addPage(5).addPage(8).addPage(1);
static const parquet::ColumnDescriptor d2 = c2.descr();

// UNORDERED
static const CIBuilder c3 = CIBuilder(PNB::optional(parquet::Type::DOUBLE).named("column3"))
                                .addPage(0, 2.03, 2.03)
                                .addPage(0, 0.56, 8.71)
                                .addPage(2, 3.14, 3.50)
                                .addPage(0, 2.71, 9.99)
                                .addPage(3, 0.36, 5.32)
                                .addPage(0, 4.17, 7.95)
                                .addNullPage(4);
static const OIBuilder o3 = OIBuilder().addPage(1).addPage(5).addPage(4).addPage(6).addPage(7).addPage(3).addPage(4);
static const parquet::ColumnDescriptor d3 = c3.descr();

// static const CIBuilder c4; // no column index
static const OIBuilder o4 = OIBuilder().addPage(1).addPage(3).addPage(2).addPage(1).addPage(5).addPage(4).addPage(5).addPage(7).addPage(2);
static const parquet::ColumnDescriptor d4{PNB::optional(parquet::Type::BYTE_ARRAY).as(parquet::ConvertedType::UTF8).named("column4"), 1, 0};

static const CIBuilder c5 = CIBuilder(PNB::optional(parquet::Type::INT64).named("column5")).addNullPage(1).addNullPage(29);
static const OIBuilder o5 = OIBuilder().addPage(1).addPage(29);
static const parquet::ColumnDescriptor d5 = c5.descr();

// GLUTEN-7179 - test customer.c_mktsegment = 'BUILDING'
static const CIBuilder c6 = CIBuilder(PNB::optional(parquet::Type::BYTE_ARRAY).as(parquet::ConvertedType::UTF8).named("c_mktsegment"))
                                .addSamePages(75, 0, "AUTOMOBILE", "MACHINERY")
                                .addPage(0, "AUTOMOBILE", "FURNITURE");
static const OIBuilder o6 = OIBuilder().addSamePages(77, 10);
static const parquet::ColumnDescriptor d6 = c6.descr();

local_engine::ColumnIndexStore buildTestColumnIndexStore()
{
    local_engine::ColumnIndexStore result;
    result[d1.name()] = std::move(local_engine::ColumnIndex::create(&d1, c1.build(), o1.build()));
    result[d2.name()] = std::move(local_engine::ColumnIndex::create(&d2, c2.build(), o2.build()));
    result[d3.name()] = std::move(local_engine::ColumnIndex::create(&d3, c3.build(), o3.build()));
    result[d4.name()] = std::move(local_engine::ColumnIndex::create(&d4, nullptr, o3.build()));
    result[d5.name()] = std::move(local_engine::ColumnIndex::create(&d5, c5.build(), o5.build()));
    result[d6.name()] = std::move(local_engine::ColumnIndex::create(&d6, c6.build(), o6.build()));
    return result;
}

AnotherRowType buildTestRowType()
{
    AnotherRowType result;
    result.emplace_back(toAnotherFieldType(d1));
    result.emplace_back(toAnotherFieldType(d2));
    result.emplace_back(toAnotherFieldType(d3));
    result.emplace_back(toAnotherFieldType(d4));
    result.emplace_back(toAnotherFieldType(d5));
    result.emplace_back(toAnotherFieldType(d6));
    return result;
}

local_engine::RowRanges buildTestRowRanges(const std::vector<Int32> & rowIndexes)
{
    if (rowIndexes.empty())
        return {};

    assert(rowIndexes.size() % 2 == 0);
    local_engine::RowRanges result;
    const parquet::OffsetIndexBuilderPtr builder = parquet::OffsetIndexBuilder::Make();

    local_engine::PageIndexs pageIndexes;
    for (Int32 i = 0, n = rowIndexes.size(); i < n; i += 2)
    {
        const int64_t from = rowIndexes[i];
        const int64_t to = rowIndexes[i + 1];
        builder->AddPage(0, 0, from);
        builder->AddPage(0, 0, to + 1);
        pageIndexes.push_back(i);
    }
    constexpr int64_t final_position = 4096;
    builder->Finish(final_position);
    const auto offset_index = builder->Build();

    const Int32 rgCount = rowIndexes.back() - 1;
    return local_engine::RowRangesBuilder(rgCount, offset_index->page_locations()).toRowRanges(pageIndexes);
}

void assertRows(const local_engine::RowRanges & ranges, const std::vector<size_t> & expectedRows)
{
    std::vector<size_t> actualRows;
    for (const auto & range : ranges.getRanges())
        for (size_t row = range.from; row <= range.to; ++row)
            actualRows.push_back(row);
    ASSERT_EQ(actualRows, expectedRows);
}

local_engine::RowRanges calculateRowRangesForTest(const std::string & exp)
{
    static const AnotherRowType name_and_types = buildTestRowType();
    static const local_engine::ColumnIndexStore column_index_store = buildTestColumnIndexStore();
    const local_engine::ColumnIndexFilter filter(
        local_engine::test::parseFilter(exp, name_and_types).value(), local_engine::QueryContext::globalContext());
    return filter.calculateRowRanges(column_index_store, TOTALSIZE);
}

void testCondition(const std::string & exp, const std::vector<size_t> & expectedRows)
{
    assertRows(calculateRowRangesForTest(exp), expectedRows);
}

void testCondition(const std::string & exp, size_t rowCount)
{
    testCondition(exp, std::vector(boost::counting_iterator<size_t>(0), boost::counting_iterator(rowCount)));
}
}

using namespace test_utils;

TEST(RowRanges, Create)
{
    using local_engine::RowRanges;
    auto ranges = buildTestRowRanges({1, 2, 3, 4, 6, 7, 7, 10, 15, 17});
    assertRows(ranges, {1, 2, 3, 4, 6, 7, 8, 9, 10, 15, 16, 17});
    ASSERT_EQ(ranges.rowCount(), 12);
    ASSERT_TRUE(ranges.isOverlapping(4, 5));
    ASSERT_FALSE(ranges.isOverlapping(5, 5));
    ASSERT_TRUE(ranges.isOverlapping(10, 14));
    ASSERT_FALSE(ranges.isOverlapping(11, 14));
    ASSERT_FALSE(ranges.isOverlapping(18, std::numeric_limits<size_t>::max()));

    ranges = RowRanges::createSingle(5);
    assertRows(ranges, {0, 1, 2, 3, 4});
    ASSERT_EQ(5, ranges.rowCount());
    ASSERT_TRUE(ranges.isOverlapping(0, 100));
    ASSERT_FALSE(ranges.isOverlapping(5, std::numeric_limits<size_t>::max()));

    const RowRanges empty;
    assertRows(empty, {});
    ASSERT_EQ(0, empty.rowCount());
    ASSERT_FALSE(empty.isOverlapping(0, std::numeric_limits<size_t>::max()));
}

TEST(RowRanges, Union)
{
    using local_engine::RowRanges;
    const RowRanges ranges1 = buildTestRowRanges({2, 5, 7, 9, 14, 14, 20, 24});
    const RowRanges ranges2 = buildTestRowRanges({1, 2, 4, 5, 11, 12, 14, 15, 21, 22});
    const RowRanges empty = buildTestRowRanges({});

    assertRows(RowRanges::unionRanges(ranges1, ranges2), {1, 2, 3, 4, 5, 7, 8, 9, 11, 12, 14, 15, 20, 21, 22, 23, 24});
    assertRows(RowRanges::unionRanges(ranges2, ranges1), {1, 2, 3, 4, 5, 7, 8, 9, 11, 12, 14, 15, 20, 21, 22, 23, 24});
    assertRows(RowRanges::unionRanges(ranges1, ranges1), {2, 3, 4, 5, 7, 8, 9, 14, 20, 21, 22, 23, 24});
    assertRows(RowRanges::unionRanges(ranges1, empty), {2, 3, 4, 5, 7, 8, 9, 14, 20, 21, 22, 23, 24});
    assertRows(RowRanges::unionRanges(empty, ranges1), {2, 3, 4, 5, 7, 8, 9, 14, 20, 21, 22, 23, 24});
    assertRows(RowRanges::unionRanges(ranges2, ranges2), {1, 2, 4, 5, 11, 12, 14, 15, 21, 22});
    assertRows(RowRanges::unionRanges(ranges2, empty), {1, 2, 4, 5, 11, 12, 14, 15, 21, 22});
    assertRows(RowRanges::unionRanges(empty, ranges2), {1, 2, 4, 5, 11, 12, 14, 15, 21, 22});
    assertRows(RowRanges::unionRanges(empty, empty), {});
}

TEST(RowRanges, Intersection)
{
    using local_engine::RowRanges;
    const RowRanges ranges1 = buildTestRowRanges({2, 5, 7, 9, 14, 14, 20, 24});
    const RowRanges ranges2 = buildTestRowRanges({1, 2, 6, 7, 9, 9, 11, 12, 14, 15, 21, 22});
    const RowRanges empty = buildTestRowRanges({});
    assertRows(RowRanges::intersection(ranges1, ranges2), {2, 7, 9, 14, 21, 22});
    assertRows(RowRanges::intersection(ranges2, ranges1), {2, 7, 9, 14, 21, 22});
    assertRows(RowRanges::intersection(ranges1, ranges1), {2, 3, 4, 5, 7, 8, 9, 14, 20, 21, 22, 23, 24});
    assertRows(RowRanges::intersection(ranges1, empty), {});
    assertRows(RowRanges::intersection(empty, ranges1), {});
    assertRows(RowRanges::intersection(ranges2, ranges2), {1, 2, 6, 7, 9, 11, 12, 14, 15, 21, 22});
    assertRows(RowRanges::intersection(ranges2, empty), {});
    assertRows(RowRanges::intersection(empty, ranges2), {});
    assertRows(RowRanges::intersection(empty, empty), {});
}

TEST(ColumnIndex, Filtering)
{
    using namespace test_utils;
    testCondition("column1 in (7)", {7, 8, 9, 10, 11, 12, 13});
    testCondition("column1 in (1, 7)", {0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13});

    testCondition(R"(column2 in ('Zulu','Alfa'))", {0,  1,  2,  3,  4,  5,  6,  7,  11, 12, 13, 14, 15, 16,
                                                    17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29});

    testCondition("column3 in (2.03)", {0, 1, 2, 3, 4, 5, 16, 17, 18, 19, 20, 21, 22});
    testCondition(
        "column3 in (2.03, 9.98)", {0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25});

    testCondition(
        "column4 >= 'XYZ'", {0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29});

    testCondition("column5 in (7, 20)", {});

    testCondition("column1 is null and column2 is null and column3 is null and column4 is null", {6, 9});
    testCondition(
        "column1 is not null and column2 is not null and column3 is not null and column4 is not null",
        {0, 1, 2, 3, 4, 5, 6, 7, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25});
    testCondition(
        "(column1 < 20 and column2 >= 'Quebec') or (column3 > 5.32 and column4 <= 'XYZ')",
        {0, 1, 2, 3, 4, 5, 6, 7, 10, 11, 12, 13, 14, 15, 23, 24, 25});

    testCondition("column1 >= 7 and column2 > 'India' and column3 is null and column4 is not null", {7, 16, 17, 18, 19, 20});

    testCondition("column1 >= 7 and column1 < 11 and column2 > 'Romeo' and column2 <= 'Tango'", {7, 11, 12, 13});
}


TEST(RowIndex, VirtualColumnRowIndexReader)
{
    local_engine::RowGroupInformation rg_info{
        .index = 0,
        .num_rows = 30,
        .rowStartIndexOffset = 0,
        .columnIndexStore = nullptr,
        .rowRanges{calculateRowRangesForTest("column1 in (7)")}};
    std::vector<local_engine::RowGroupInformation> rowGroups;
    rowGroups.push_back(std::move(rg_info));

    local_engine::ColumnIndexRowRangesProvider provider({std::move(rowGroups)});
    local_engine::VirtualColumnRowIndexReader reader(provider, local_engine::BIGINT());

    DB::ColumnPtr col = reader.readBatch(TOTALSIZE);
    const auto & col_str = typeid_cast<const ColumnInt64 &>(*col);
    std::vector<size_t> result;
    std::ranges::for_each(col_str.getData(), [&](const auto & val) { result.push_back(val); });
    ASSERT_EQ(result, std::vector<size_t>({7, 8, 9, 10, 11, 12, 13}));
}

TEST(ColumnIndex, FilteringWithAllNullPages)
{
    using namespace test_utils;
    testCondition("column5 != 1234567", TOTALSIZE);
    testCondition("column1 >= 10 or column5 != 1234567", TOTALSIZE);
    // testCondition("column5 == 1234567", TOTALSIZE);
    // testCondition("column5 >= 1234567", TOTALSIZE);
}

TEST(ColumnIndex, GLUTEN_7179_INFINTE_LOOP)
{
    using namespace test_utils;
    ASSERT_DURATION_LE(10, { testCondition("c_mktsegment = 'BUILDING'", 760); })
}

TEST(ColumnIndex, FilteringWithNotFoundColumnName)
{
    using namespace test_utils;
    using namespace local_engine;
    const local_engine::ColumnIndexStore column_index_store = buildTestColumnIndexStore();

    {
        // COLUMN5 is not found in the column_index_store,
        const AnotherRowType upper_name_and_types{{"COLUMN5", BIGINT()}};
        const local_engine::ColumnIndexFilter filter_upper(
            local_engine::test::parseFilter("COLUMN5 in (7, 20)", upper_name_and_types).value(),
            local_engine::QueryContext::globalContext());
        assertRows(
            filter_upper.calculateRowRanges(column_index_store, TOTALSIZE),
            std::vector(boost::counting_iterator<size_t>(0), boost::counting_iterator<size_t>(TOTALSIZE)));
    }

    {
        const AnotherRowType lower_name_and_types{{"column5", BIGINT()}};
        const local_engine::ColumnIndexFilter filter_lower(
            local_engine::test::parseFilter("column5 in (7, 20)", lower_name_and_types).value(),
            local_engine::QueryContext::globalContext());
        assertRows(filter_lower.calculateRowRanges(column_index_store, TOTALSIZE), {});
    }
}

using ParquetValue = std::variant<
    parquet::BooleanType::c_type,
    parquet::Int32Type::c_type,
    parquet::Int64Type::c_type,
    parquet::FloatType::c_type,
    parquet::DoubleType::c_type,
    parquet::ByteArrayType::c_type>;

template <typename PhysicalType>
void doCompare(
    const parquet::ColumnDescriptor & descriptor, const DB::Field & value, const std::function<void(const ParquetValue &)> & compare)
{
    local_engine::ToParquet<PhysicalType> to_parquet;
    compare({to_parquet.as(value, descriptor)});
}

void with_actual(const DB::Field & value, const parquet::ColumnDescriptor & desc, const std::function<void(const ParquetValue &)> & compare)
{
    using namespace local_engine;
    switch (desc.physical_type())
    {
        case parquet::Type::BOOLEAN:
            doCompare<parquet::BooleanType>(desc, value, compare);
            return;
        case parquet::Type::INT32: {
            switch (desc.converted_type())
            {
                case parquet::ConvertedType::UINT_8:
                case parquet::ConvertedType::UINT_16:
                case parquet::ConvertedType::UINT_32:
                case parquet::ConvertedType::INT_8:
                case parquet::ConvertedType::INT_16:
                case parquet::ConvertedType::INT_32:
                case parquet::ConvertedType::NONE:
                    doCompare<parquet::Int32Type>(desc, value, compare);
                    return;
                default:
                    break;
            }
        }
        break;
        case parquet::Type::INT64:
            switch (desc.converted_type())
            {
                case parquet::ConvertedType::INT_64:
                case parquet::ConvertedType::UINT_64:
                case parquet::ConvertedType::NONE:
                    doCompare<parquet::Int64Type>(desc, value, compare);
                    return;
                default:
                    break;
            }
            break;
        case parquet::Type::INT96:
            // doCompare<parquet::Int96Type>(desc, value, compare);
            break;
        case parquet::Type::FLOAT:
            doCompare<parquet::FloatType>(desc, value, compare);
            return;
        case parquet::Type::DOUBLE:
            doCompare<parquet::DoubleType>(desc, value, compare);
            return;
        case parquet::Type::BYTE_ARRAY:
            switch (desc.converted_type())
            {
                case parquet::ConvertedType::UTF8:
                    doCompare<parquet::ByteArrayType>(desc, value, compare);
                    return;
                default:
                    break;
            }
            break;
        case parquet::Type::FIXED_LEN_BYTE_ARRAY:
            // doCompare<parquet::FLBAType>(desc, value, compare);
            break;
        case parquet::Type::UNDEFINED:
            break;
    }
    ASSERT_TRUE(false) << "Unsupported physical type: [" << TypeToString(desc.physical_type()) << "] with logical type: ["
                       << desc.logical_type()->ToString() << "] with converted type: [" << ConvertedTypeToString(desc.converted_type())
                       << "]";
}

// for gtest
namespace parquet
{
void PrintTo(const ByteArray & val, std::ostream * os)
{
    *os << '[' << std::hex;

    for (size_t i = 0; i < val.len; ++i)
    {
        *os << std::setw(2) << std::setfill('0') << static_cast<int>(val.ptr[i]);
        if (i != val.len - 1)
            *os << ", ";
    }
    *os << ']';
}
}
TEST(ColumnIndex, DecimalField)
{
    // we can't define `operator==` for parquet::FLBAType
    Field value = DecimalField<Decimal128>(Int128(300000000), 4);
    local_engine::ToParquet<parquet::FLBAType> to_parquet;
    const parquet::ColumnDescriptor desc
        = PNB::optional(parquet::Type::FIXED_LEN_BYTE_ARRAY).asDecimal(38, 4).with_length(13).descriptor("column1");
    uint8_t expected_a[13]{0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x11, 0xE1, 0xA3, 0x0};
    const parquet::ByteArray expected{13, expected_a};
    const parquet::ByteArray actual{13, to_parquet.as(value, desc).ptr};
    ASSERT_EQ(actual, expected);


    /// Exception test, only in release node
#ifdef NDEBUG
    Field unsupport = DecimalField<Decimal256>(Int256(300000000), 4);
    EXPECT_THROW(to_parquet.as(unsupport, desc), DB::Exception);

    const parquet::ColumnDescriptor error
        = PNB::optional(parquet::Type::FIXED_LEN_BYTE_ARRAY).asDecimal(38, 4).with_length(18).descriptor("column1");
    EXPECT_THROW(to_parquet.as(value, error), DB::Exception);
#endif
}


TEST(ColumnIndex, Field)
{
    std::string s_tmp = "hello world";

    using TESTDATA = std::tuple<
        std::string, // name
        DB::Field, //value
        parquet::ColumnDescriptor, //desc
        ParquetValue //expected value
        >;
    const std::vector<TESTDATA> datas{
        {"int32_UINT_8",
         static_cast<UInt8>(1),
         PNB::optional(parquet::Type::INT32).as(parquet::ConvertedType::UINT_8).descriptor("column1"),
         ParquetValue{int{1}}},
        {"int32_INT_8",
         static_cast<Int8>(-1),
         PNB::optional(parquet::Type::INT32).as(parquet::ConvertedType::INT_8).descriptor("column1"),
         ParquetValue{int{-1}}},
        {"int32_INT32", static_cast<Int32>(-1), PNB::optional(parquet::Type::INT32).descriptor("column1"), ParquetValue{int{-1}}},
        {"int32_UINT32",
         static_cast<UInt32>(-1),
         PNB::optional(parquet::Type::INT32).as(parquet::ConvertedType::UINT_32).descriptor("column1"),
         ParquetValue{int{-1}}},
        {"string_UTF8",
         s_tmp,
         PNB::optional(parquet::Type::BYTE_ARRAY).as(parquet::ConvertedType::UTF8).descriptor("column1"),
         ParquetValue{ByteArrayFromString(s_tmp)}}};

    std::ranges::for_each(
        datas,
        [](const auto & data)
        {
            const auto & name = std::get<0>(data);
            const auto & value = std::get<1>(data);
            const auto & desc = std::get<2>(data);
            const auto & expected = std::get<3>(data);
            with_actual(value, desc, [&](const ParquetValue & actual) { ASSERT_EQ(actual, expected) << name; });
        });

    const std::vector<std::pair<String, Field>> primitive_fields{
        {"f_bool", static_cast<UInt8>(1)},
        {"f_byte", static_cast<Int8>(1)},
        {"f_short", static_cast<Int16>(2)},
        {"f_int", static_cast<Int32>(3)},
        {"f_long", static_cast<Int64>(4)},
        {"f_float", static_cast<Float32>(5.5)},
        {"f_double", Float64{6.6}},
        {"f_string", "hello world"},
        {"f_binary", "hello world"},
        {"f_decimal", DecimalField<Decimal64>(777, 2)},
        {"f_date", static_cast<Int32>(18262)},
        {"f_timestamp", DecimalField<DateTime64>(1666162060000000L, 6)}}; // 2022-09-01 12:34:20.000000

    std::ranges::for_each(
        primitive_fields,
        [](const auto & pair)
        {
            const auto & name = pair.first;
            const auto & field = pair.second;
            std::cout << fmt::format("{:>11}:{:<10} = {}\n", name, field.getTypeName(), field);
        });
}

struct ReadStatesParam
{
    ReadStatesParam() = default;

    ReadStatesParam(local_engine::RowRanges ranges, std::shared_ptr<local_engine::ColumnReadState> states)
        : row_ranges(std::move(ranges)), read_states(std::move(states)) { };

    local_engine::RowRanges row_ranges;
    std::shared_ptr<local_engine::ColumnReadState> read_states;
};

/// for gtest
void PrintTo(const ReadStatesParam & infos, std::ostream * os)
{
    const std::vector<local_engine::Range> & ranges = infos.row_ranges.getRanges();
    *os << "[";
    for (size_t i = 0; i < ranges.size(); ++i)
    {
        *os << "[" << ranges[i].from << "," << ranges[i].to << "]";
        if (i != ranges.size() - 1)
            *os << ",";
    }
    *os << "]";
}
namespace arrow::io
{
void PrintTo(const ReadRange & infos, std::ostream * os)
{
    *os << "[" << infos.offset << "," << infos.length << "]";
}
}

class TestBuildPageReadStates : public ::testing::TestWithParam<ReadStatesParam>
{
protected:
    void SetUp() override
    {
        // clang-format off
        //
        // Column chunk data layout:
        //  |-----------------------------------------------------------------------------------------|
        //  | dict_page | data_page0 | data_page1 | data_page2 | data_page3 | data_page4 | data_page5 |
        //      -            215K         300K         512K         268K         435K         355K
        //      -           [0,439]    [440,997]    [998,1345]   [1346,1834]  [1835,2176]  [2177,2703]
        //
        // clang-format on
        page_locations_.push_back(parquet::PageLocation{1048626, 220160, 0}); // page0
        page_locations_.push_back(parquet::PageLocation{1268786, 307200, 440}); // page1
        page_locations_.push_back(parquet::PageLocation{1575986, 524288, 998}); // page2
        page_locations_.push_back(parquet::PageLocation{2100274, 274432, 1346}); // page3
        page_locations_.push_back(parquet::PageLocation{2374706, 445440, 1835}); // page4
        page_locations_.push_back(parquet::PageLocation{2820146, 363520, 2177}); // page5

        param_ = GetParam();
    }
    void BuildAndVerifyPageReadStates() const
    {
        const auto [actaul_read_ranges, actaul_read_infos] = buildRead(num_rows_, chunk_range_, page_locations_, param_.row_ranges);
        const auto & expected_read_ranges = param_.read_states->first;
        const auto & expected_read_info = param_.read_states->second;
        ASSERT_EQ(expected_read_ranges, actaul_read_ranges);
        ASSERT_EQ(expected_read_info, actaul_read_infos);
    }

private:
    const int64_t num_rows_ = 2704;
    const ::arrow::io::ReadRange chunk_range_{50, 3183666};
    std::vector<parquet::PageLocation> page_locations_;

    ReadStatesParam param_;
};

std::vector<ReadStatesParam> GenerateTestCases()
{
    using namespace local_engine;
    std::vector<ReadStatesParam> params;
    // clang-format off
    //
    //  0         350
    //  |<--read-->|
    //  |-----------------------------------------------------------------------------|
    //  | data_page0 | data_page1 | data_page2 | data_page3 | data_page4 | data_page5 |
    //       215K         300K         512K         268K         435K         355K
    //      [0,439]    [440,997]    [998,1345]   [1346,1834]  [1835,2176]  [2177,2703]
    //
    // clang-format on
    {
        RowRanges ranges{{{0, 350}}};
        ReadRanges read_ranges{{{50, 1268736}}}; /*dict page + data_page0*/

        auto read_states = std::make_shared<ColumnReadState>(read_ranges, ReadSequence{351, -89});
        params.emplace_back(ranges, std::move(read_states));
    }

    // clang-format off
    //
    //  0           439
    //  |<-- read -->|
    //  |-----------------------------------------------------------------------------|
    //  | data_page0 | data_page1 | data_page2 | data_page3 | data_page4 | data_page5 |
    //       215K         300K         512K         268K         435K         355K
    //      [0,439]    [440,997]    [998,1345]   [1346,1834]  [1835,2176]  [2177,2703]
    //
    // clang-format on
    {
        RowRanges ranges{{{0, 439}}};
        ReadRanges read_ranges{{{50, 1268736}}}; /*dict page + data_page0*/

        auto read_states = std::make_shared<ColumnReadState>(read_ranges, ReadSequence{440});
        params.emplace_back(ranges, std::move(read_states));
    }

    // clang-format off
    //
    //  0             503
    //  |<--- read --->|
    //  |-----------------------------------------------------------------------------|
    //  | data_page0 | data_page1 | data_page2 | data_page3 | data_page4 | data_page5 |
    //       215K         300K         512K         268K         435K         355K
    //      [0,439]    [440,997]    [998,1345]   [1346,1834]  [1835,2176]  [2177,2703]
    //
    // clang-format on
    {
        RowRanges ranges{{{0, 503}}};
        ReadRanges read_ranges{{{50, 1575936}}} /*dict page + data_page0 + data_page1*/;

        auto read_states = std::make_shared<ColumnReadState>(read_ranges, ReadSequence{504, -494});

        params.emplace_back(ranges, std::move(read_states));
    }

    // clang-format off
    //
    //  0                        997
    //  |<--------- read -------->|
    //  |-----------------------------------------------------------------------------|
    //  | data_page0 | data_page1 | data_page2 | data_page3 | data_page4 | data_page5 |
    //       215K         300K         512K         268K         435K         355K
    //      [0,439]    [440,997]    [998,1345]   [1346,1834]  [1835,2176]  [2177,2703]
    //
    // clang-format on
    {
        RowRanges ranges{{{0, 997}}};
        ReadRanges read_ranges{{{50, 1575936}}} /*dict page + data_page0 + data_page1*/;

        auto read_states = std::make_shared<ColumnReadState>(read_ranges, ReadSequence{998});
        params.emplace_back(ranges, std::move(read_states));
    }

    // clang-format off
    //
    //  0                          1105
    //  |<---------- read ---------->|
    //  |-----------------------------------------------------------------------------|
    //  | data_page0 | data_page1 | data_page2 | data_page3 | data_page4 | data_page5 |
    //       215K         300K         512K         268K         435K         355K
    //      [0,439]    [440,997]    [998,1345]   [1346,1834]  [1835,2176]  [2177,2703]
    //
    // clang-format on
    {
        RowRanges ranges{{{0, 1105}}};
        ReadRanges read_ranges{{{50, 2100224}}} /*dict page + data_page0 + data_page1 + data_page2*/;

        auto read_states = std::make_shared<ColumnReadState>(read_ranges, ReadSequence{1106, -240});
        params.emplace_back(ranges, std::move(read_states));
    }

    // clang-format off
    //
    //   57       402
    //    |<-read->|
    //  |-----------------------------------------------------------------------------|
    //  | data_page0 | data_page1 | data_page2 | data_page3 | data_page4 | data_page5 |
    //       215K         300K         512K         268K         435K         355K
    //      [0,439]    [440,997]    [998,1345]   [1346,1834]  [1835,2176]  [2177,2703]
    //
    // clang-format on
    {
        RowRanges ranges{{{57, 402}}};
        ReadRanges read_ranges{{{50, 1268736}}}; /*dict page + data_page0*/

        auto read_states = std::make_shared<ColumnReadState>(read_ranges, ReadSequence{-57, 346, -37});
        params.emplace_back(ranges, std::move(read_states));
    }

    // clang-format off
    //
    //   57         439
    //    |<--read-->|
    //  |-----------------------------------------------------------------------------|
    //  | data_page0 | data_page1 | data_page2 | data_page3 | data_page4 | data_page5 |
    //       215K         300K         512K         268K         435K         355K
    //      [0,439]    [440,997]    [998,1345]   [1346,1834]  [1835,2176]  [2177,2703]
    //
    // clang-format on
    {
        RowRanges ranges{{{57, 439}}};
        ReadRanges read_ranges{{{50, 1268736}}}; /*dict page + data_page0*/

        auto read_states = std::make_shared<ColumnReadState>(read_ranges, ReadSequence{-57, 383});
        params.emplace_back(ranges, std::move(read_states));
    }

    // clang-format off
    //
    //   57               637
    //    |<---- read ---->|
    //  |-----------------------------------------------------------------------------|
    //  | data_page0 | data_page1 | data_page2 | data_page3 | data_page4 | data_page5 |
    //       215K         300K         512K         268K         435K         355K
    //      [0,439]    [440,997]    [998,1345]   [1346,1834]  [1835,2176]  [2177,2703]
    //
    // clang-format on
    {
        RowRanges ranges{{{57, 637}}};
        ReadRanges read_ranges{{{50, 1575936}}} /*dict page + data_page0 + data_page1*/;

        auto read_states = std::make_shared<ColumnReadState>(read_ranges, ReadSequence{-57, 581, -360});
        params.emplace_back(ranges, std::move(read_states));
    }

    // clang-format off
    //
    //   57                      997
    //    |<-------- read ------->|
    //  |-----------------------------------------------------------------------------|
    //  | data_page0 | data_page1 | data_page2 | data_page3 | data_page4 | data_page5 |
    //       215K         300K         512K         268K         435K         355K
    //      [0,439]    [440,997]    [998,1345]   [1346,1834]  [1835,2176]  [2177,2703]
    //
    // clang-format on
    {
        RowRanges ranges{{{57, 997}}};
        ReadRanges read_ranges{{{50, 1575936}}} /*dict page + data_page0 + data_page1*/;

        auto read_states = std::make_shared<ColumnReadState>(read_ranges, ReadSequence{-57, 941});
        params.emplace_back(ranges, std::move(read_states));
    }

    // clang-format off
    //
    //   57                        1104
    //    |<--------- read --------->|
    //  |-----------------------------------------------------------------------------|
    //  | data_page0 | data_page1 | data_page2 | data_page3 | data_page4 | data_page5 |
    //       215K         300K         512K         268K         435K         355K
    //      [0,439]    [440,997]    [998,1345]   [1346,1834]  [1835,2176]  [2177,2703]
    //
    // clang-format on
    {
        RowRanges ranges{{{57, 1104}}};
        ReadRanges read_ranges{{{50, 2100224}}} /*dict page + data_page0 + data_page1 + data_page2*/;

        auto read_states = std::make_shared<ColumnReadState>(read_ranges, ReadSequence{-57, 1048, -241});
        params.emplace_back(ranges, std::move(read_states));
    }

    // clang-format off
    //                                1197 1246
    //   57                       1104 |   | 1311 1450
    //    |<--------- read --------->| |<->| |<-->|
    //  |-----------------------------------------------------------------------------|
    //  | data_page0 | data_page1 | data_page2 | data_page3 | data_page4 | data_page5 |
    //       215K         300K         512K         268K         435K         355K
    //      [0,439]    [440,997]    [998,1345]   [1346,1834]  [1835,2176]  [2177,2703]
    //
    // clang-format on
    {
        RowRanges ranges{{{57, 1104}, {1197, 1246}, {1311, 1450}}};
        ReadRanges read_ranges{{{50, 2374656}}} /*dict page + data_page0 + data_page1 + data_page2 + data_page3*/;

        auto read_states = std::make_shared<ColumnReadState>(read_ranges, ReadSequence{-57, 1048, -92, 50, -64, 140, -384});
        params.emplace_back(ranges, std::move(read_states));
    }

    // clang-format off
    //
    //   57       403 475 763 859        1259  1403    1679
    //    |<-read->|   |<-->|  |<---read-->|     |<----->|
    //  |-----------------------------------------------------------------------------|
    //  | data_page0 | data_page1 | data_page2 | data_page3 | data_page4 | data_page5 |
    //       215K         300K         512K         268K         435K         355K
    //      [0,439]    [440,997]    [998,1345]   [1346,1834]  [1835,2176]  [2177,2703]
    //
    // clang-format on
    {
        RowRanges ranges{{{57, 403}, {475, 763}, {859, 1259}, {1403, 1679}}};
        ReadRanges read_ranges{{{50, 2374656}}} /*dict page + data_page0 + data_page1 + data_page2 + data_page3*/;

        auto read_states = std::make_shared<ColumnReadState>(read_ranges, ReadSequence{-57, 347, -71, 289, -95, 401, -143, 277, -155});
        params.emplace_back(ranges, std::move(read_states));
    }

    // clang-format off
    //
    //       257         666     1004  1117 1289   1599      2001           2433
    //        |           |        |<--->|  |<------>|         |              |
    //  |-----------------------------------------------------------------------------|
    //  | data_page0 | data_page1 | data_page2 | data_page3 | data_page4 | data_page5 |
    //       215K         300K         512K         268K         435K         355K
    //      [0,439]    [440,997]    [998,1345]   [1346,1834]  [1835,2176]  [2177,2703]
    //
    // clang-format on
    {
        RowRanges ranges{{{257, 257}, {666, 666}, {1004, 1117}, {1289, 1599}, {2001, 2001}, {2433, 2433}}};
        /*dict page + data_page0 + data_page1 + data_page2 + data_page3 + data_page4 + data_page5*/
        ReadRanges read_ranges{{{50, 3183616}}};

        auto read_states
            = std::make_shared<ColumnReadState>(read_ranges, ReadSequence{-257, 1, -408, 1, -337, 114, -171, 311, -401, 1, -431, 1, -270});
        params.emplace_back(ranges, std::move(read_states));
    }

    // clang-format off
    //
    //                                                       1900            2300
    //                                                         |<---- read --->|
    //  |-----------------------------------------------------------------------------|
    //  | data_page0 | data_page1 | data_page2 | data_page3 | data_page4 | data_page5 |
    //       215K         300K         512K         268K         435K         355K
    //      [0,439]    [440,997]    [998,1345]   [1346,1834]  [1835,2176]  [2177,2703]
    //
    // clang-format on
    {
        RowRanges ranges{{{1900, 2300}}};
        ReadRanges read_ranges{{{50, 1048576} /*dict page*/, {2374706, 808960} /*data_page4 + data_page5*/}};

        auto read_states = std::make_shared<ColumnReadState>(read_ranges, ReadSequence{-65, 401, -403});
        params.emplace_back(ranges, std::move(read_states));
    }

    // clang-format off
    //
    //  0                                                                           2703
    //  |<--------------------------------------------------------------------------->|
    //  |-----------------------------------------------------------------------------|
    //  | data_page0 | data_page1 | data_page2 | data_page3 | data_page4 | data_page5 |
    //       215K         300K         512K         268K         435K         355K
    //      [0,439]    [440,997]    [998,1345]   [1346,1834]  [1835,2176]  [2177,2703]
    //
    // clang-format on
    {
        RowRanges ranges{{{0, 2703}}};
        ReadRanges read_ranges{{{50, 3183666}}};
        auto read_states = std::make_shared<ColumnReadState>(read_ranges, ReadSequence{2704});
        params.emplace_back(ranges, std::move(read_states));
    }
    return params;
}

INSTANTIATE_TEST_SUITE_P(BuildPageReadStates, TestBuildPageReadStates, ::testing::ValuesIn(GenerateTestCases()));

TEST_P(TestBuildPageReadStates, BuildPageReadStates)
{
    BuildAndVerifyPageReadStates();
}

TEST(ColumnIndex, VectorizedParquetRecordReader)
{
    using namespace local_engine;

    //TODO: move test parquet to s3 and download to CI machine.
    const std::string filename
        = "/home/chang/test/tpch/parquet/Index/60001/part-00000-76ef9b89-f292-495f-9d0d-98325f3d8956-c000.snappy.parquet";

    const FormatSettings format_settings{};


    static const AnotherRowType name_and_types{{"11", BIGINT()}};
    const auto filterAction = test::parseFilter("`11` = 10 or `11` = 50", name_and_types);
    auto column_index_filter = std::make_shared<ColumnIndexFilter>(filterAction.value(), local_engine::QueryContext::globalContext());

    Block blockHeader({{BIGINT(), "11"}, {STRING(), "18"}});

    ReadBufferFromFilePRead in(filename);

    ParquetMetaBuilder metaBuilder{.collectPageIndex = true};
    metaBuilder.build(&in, &blockHeader, column_index_filter.get(), [](UInt64 /*midpoint_offset*/) -> bool { return true; });
    ColumnIndexRowRangesProvider provider{metaBuilder};

    VectorizedParquetRecordReader recordReader(blockHeader, format_settings);
    auto arrow_file = test::asArrowFileForParquet(in, format_settings);
    recordReader.initialize(arrow_file, provider);

    auto chunk{recordReader.nextBatch()};
    ASSERT_EQ(chunk.getNumColumns(), 2);
    ASSERT_EQ(chunk.getNumRows(), format_settings.parquet.max_block_size);

    do
    {
        const auto & col_a = *(chunk.getColumns()[0]);
        /// TODO: nullable
        /// ROW 1 => pageindex [9, 10]..[10,10]..[10,11]
        /// ROW 1 => pageindex [49, 50]..[50,50]
        for (size_t i = 0; i < chunk.getNumRows(); i++)
        {
            bool row1 = col_a.get64(i) >= 9 || col_a.get64(i) <= 11;
            bool row2 = col_a.get64(i) == 49 || col_a.get64(i) == 50;
            EXPECT_TRUE(row1 || row2);
        }
        chunk = recordReader.nextBatch();
    } while (chunk.getNumRows() > 0);
}

#endif //USE_PARQUET