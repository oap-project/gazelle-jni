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
#include "SelectorBuilder.h"
#include <limits>
#include <memory>
#include <Columns/ColumnConst.h>
#include <Columns/ColumnMap.h>
#include <Columns/ColumnNullable.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypesDecimal.h>
#include <Functions/FunctionFactory.h>
#include <Parser/ExpressionParser.h>
#include <Parser/ParserContext.h>
#include <Parser/SerializedPlanParser.h>
#include <Parser/TypeParser.h>
#include <Poco/JSON/Parser.h>
#include <Poco/MemoryStream.h>
#include <Common/CHUtil.h>
#include <Common/Exception.h>
#include <Common/QueryContext.h>

namespace DB
{
namespace ErrorCodes
{
extern const int LOGICAL_ERROR;
}
}
namespace local_engine
{
using namespace DB;
PartitionInfo PartitionInfo::fromSelector(DB::IColumn::Selector selector, size_t partition_num, bool use_external_sort_shuffle)
{
    if (use_external_sort_shuffle)
    {
        return PartitionInfo{.src_partition_num = std::move(selector), .partition_num = partition_num};
    }
    else
    {
        auto rows = selector.size();
        std::vector<size_t> partition_row_idx_start_points(partition_num + 1, 0);
        IColumn::Selector partition_selector(rows, 0);
        for (size_t i = 0; i < rows; ++i)
            partition_row_idx_start_points[selector[i]]++;

        for (size_t i = 1; i <= partition_num; ++i)
            partition_row_idx_start_points[i] += partition_row_idx_start_points[i - 1];
        for (size_t i = rows; i-- > 0;)
        {
            partition_selector[partition_row_idx_start_points[selector[i]] - 1] = i;
            partition_row_idx_start_points[selector[i]]--;
        }
        return PartitionInfo{
            .partition_selector = std::move(partition_selector),
            .partition_start_points = partition_row_idx_start_points,
            .src_partition_num = std::move(selector),
            .partition_num = partition_num};
    }
}

PartitionInfo RoundRobinSelectorBuilder::build(DB::Block & block)
{
    DB::IColumn::Selector result;
    result.resize_fill(block.rows(), 0);
    for (auto & pid : result)
    {
        pid = pid_selection;
        pid_selection = (pid_selection + 1) % parts_num;
    }
    return PartitionInfo::fromSelector(std::move(result), parts_num, use_sort_shuffle);
}

HashSelectorBuilder::HashSelectorBuilder(
    UInt32 parts_num_, const std::vector<size_t> & exprs_index_, const std::string & hash_function_name_, bool use_external_sort_shuffle)
    : SelectorBuilder(use_external_sort_shuffle), parts_num(parts_num_), exprs_index(exprs_index_), hash_function_name(hash_function_name_)
{
}

PartitionInfo HashSelectorBuilder::build(DB::Block & block)
{
    ColumnsWithTypeAndName args;
    for (size_t i = 0; i < exprs_index.size(); i++)
        args.emplace_back(block.safeGetByPosition(exprs_index.at(i)));
    auto flatten_block = BlockUtil::flattenBlock(DB::Block(args), BlockUtil::FLAT_STRUCT_FORCE | BlockUtil::FLAT_NESTED_TABLE, true);
    args = flatten_block.getColumnsWithTypeAndName();

    if (!hash_function) [[unlikely]]
    {
        auto & factory = DB::FunctionFactory::instance();
        auto function = factory.get(hash_function_name, QueryContext::globalContext());

        hash_function = function->build(args);
    }

    auto rows = block.rows();
    DB::IColumn::Selector partition_ids;
    partition_ids.reserve(rows);
    auto result_type = hash_function->getResultType();
    auto hash_column = hash_function->execute(args, result_type, rows, false);

    if (isNothing(removeNullable(result_type)))
    {
        /// TODO: implement new hash function sparkCityHash64 like sparkXxHash64 to process null literal as column more gracefully.
        /// Current implementation may cause partition skew.
        for (size_t i = 0; i < rows; i++)
            partition_ids.emplace_back(0);
    }
    else
    {
        if (hash_function_name == "sparkMurmurHash3_32")
        {
            /// sparkMurmurHash3_32 returns are all not null.
            auto parts_num_int32 = static_cast<Int32>(parts_num);
            for (size_t i = 0; i < rows; i++)
            {
                // cast to int32 to be the same as the data type of the vanilla spark
                auto hash_int32 = static_cast<Int32>(hash_column->get64(i));
                auto res = hash_int32 % parts_num_int32;
                if (res < 0)
                {
                    res += parts_num_int32;
                }
                partition_ids.emplace_back(static_cast<UInt64>(res));
            }
        }
        else
        {
            if (hash_column->isNullable())
            {
                const auto * null_col = typeid_cast<const ColumnNullable *>(hash_column->getPtr().get());
                auto & null_map = null_col->getNullMapData();
                for (size_t i = 0; i < rows; ++i)
                {
                    auto hash_value = static_cast<UInt64>(hash_column->get64(i)) & static_cast<UInt64>(static_cast<Int64>(null_map[i]) - 1);
                    partition_ids.emplace_back(static_cast<UInt64>(hash_value % parts_num));
                }
            }
            else
            {
                for (size_t i = 0; i < rows; i++)
                    partition_ids.emplace_back(static_cast<UInt64>(hash_column->get64(i) % parts_num));
            }
        }
    }
    return PartitionInfo::fromSelector(std::move(partition_ids), parts_num, use_sort_shuffle);
}


static std::map<int, std::pair<int, int>> direction_map = {{1, {1, -1}}, {2, {1, 1}}, {3, {-1, 1}}, {4, {-1, -1}}};

RangeSelectorBuilder::RangeSelectorBuilder(const std::string & option, const size_t partition_num_, bool use_external_sort_shuffle)
    : SelectorBuilder(use_external_sort_shuffle)
{
    Poco::JSON::Parser parser;
    auto info = parser.parse(option).extract<Poco::JSON::Object::Ptr>();
    auto ordering_infos = info->get("ordering").extract<Poco::JSON::Array::Ptr>();
    initSortInformation(ordering_infos);
    initRangeBlock(info->get("range_bounds").extract<Poco::JSON::Array::Ptr>());
    partition_num = partition_num_;
}

PartitionInfo RangeSelectorBuilder::build(DB::Block & block)
{
    DB::IColumn::Selector result;
    computePartitionIdByBinarySearch(block, result);
    return PartitionInfo::fromSelector(std::move(result), partition_num, use_sort_shuffle);
}

void RangeSelectorBuilder::initSortInformation(Poco::JSON::Array::Ptr orderings)
{
    for (uint32_t i = 0; i < orderings->size(); ++i)
    {
        auto ordering = orderings->get(i).extract<Poco::JSON::Object::Ptr>();
        auto col_pos = ordering->get("column_ref").convert<Int32>();
        auto col_name = ordering->get("column_name").convert<String>();

        auto sort_direction = ordering->get("direction").convert<int>();
        auto d_iter = direction_map.find(sort_direction);
        if (d_iter == direction_map.end())
            throw DB::Exception(DB::ErrorCodes::LOGICAL_ERROR, "Unsupported sorting direction:{}", sort_direction);
        DB::SortColumnDescription ch_col_sort_descr(col_name, d_iter->second.first, d_iter->second.second);
        sort_descriptions.emplace_back(ch_col_sort_descr);

        auto type_name = ordering->get("data_type").convert<std::string>();
        auto type = TypeParser::getCHTypeByName(type_name);
        SortFieldTypeInfo info;
        info.inner_type = type;
        info.is_nullable = ordering->get("is_nullable").convert<bool>();
        sort_field_types.emplace_back(info);
        sorting_key_columns.emplace_back(col_pos);
    }
}

template <typename T>
void RangeSelectorBuilder::safeInsertFloatValue(const Poco::Dynamic::Var & field_value, DB::MutableColumnPtr & col)
{
    try
    {
        col->insert(field_value.convert<T>());
    }
    catch (const std::exception &)
    {
        String val = Poco::toLower(field_value.convert<std::string>());
        T res;
        if (val == "nan")
            res = std::numeric_limits<T>::quiet_NaN();
        else if (val == "infinity")
            res = std::numeric_limits<T>::infinity();
        else if (val == "-infinity")
            res = -std::numeric_limits<T>::infinity();
        else
            throw DB::Exception(DB::ErrorCodes::LOGICAL_ERROR, "Unsupported value: {}", val);
        col->insert(res);
    }
}

void RangeSelectorBuilder::initRangeBlock(Poco::JSON::Array::Ptr range_bounds)
{
    DB::ColumnsWithTypeAndName columns;
    for (uint32_t i = 0; i < sort_field_types.size(); ++i)
    {
        auto & type_info = sort_field_types[i];
        auto inner_col = type_info.inner_type->createColumn();
        auto data_type = type_info.inner_type;
        DB::MutableColumnPtr col = std::move(inner_col);
        if (type_info.is_nullable)
        {
            col = ColumnNullable::create(std::move(col), DB::ColumnUInt8::create(0, 0));
            data_type = std::make_shared<DB::DataTypeNullable>(data_type);
        }
        for (uint32_t r = 0; r < range_bounds->size(); ++r)
        {
            auto row = range_bounds->get(r).extract<Poco::JSON::Array::Ptr>();
            auto field_info = row->get(i).extract<Poco::JSON::Object::Ptr>();
            if (field_info->get("is_null").convert<bool>())
            {
                col->insertData(nullptr, 0);
            }
            else
            {
                const auto & type_name = type_info.inner_type->getName();
                const auto & field_value = field_info->get("value");
                if (type_name == "UInt8")
                {
                    col->insert(static_cast<UInt8>(field_value.convert<Int16>()));
                }
                else if (type_name == "Int8")
                {
                    col->insert(field_value.convert<Poco::Int8>());
                }
                else if (type_name == "Int16")
                {
                    col->insert(field_value.convert<Int16>());
                }
                else if (type_name == "Int32")
                {
                    col->insert(field_value.convert<Int32>());
                }
                else if (type_name == "Int64")
                {
                    col->insert(field_value.convert<Int64>());
                }
                else if (type_name == "Float32")
                {
                    safeInsertFloatValue<Float32>(field_value, col);
                }
                else if (type_name == "Float64")
                {
                    safeInsertFloatValue<Float64>(field_value, col);
                }
                else if (type_name == "String")
                {
                    col->insert(field_value.convert<std::string>());
                }
                else if (type_name == "Date32")
                {
                    int val = field_value.convert<Int32>();
                    col->insert(val);
                }
                else if (const auto * timestamp = dynamic_cast<const DB::DataTypeDateTime64 *>(type_info.inner_type.get()))
                {
                    auto value = field_value.convert<Int64>();
                    col->insert(DecimalField<DateTime64>(value, 6));
                }
                else if (const auto * decimal32 = dynamic_cast<const DB::DataTypeDecimal<DB::Decimal32> *>(type_info.inner_type.get()))
                {
                    auto value = decimal32->parseFromString(field_value.convert<std::string>());
                    col->insert(DB::DecimalField<DB::Decimal32>(value, decimal32->getScale()));
                }
                else if (const auto * decimal64 = dynamic_cast<const DB::DataTypeDecimal<DB::Decimal64> *>(type_info.inner_type.get()))
                {
                    auto value = decimal64->parseFromString(field_value.convert<std::string>());
                    col->insert(DB::DecimalField<DB::Decimal64>(value, decimal64->getScale()));
                }
                else if (const auto * decimal128 = dynamic_cast<const DB::DataTypeDecimal<DB::Decimal128> *>(type_info.inner_type.get()))
                {
                    auto value = decimal128->parseFromString(field_value.convert<std::string>());
                    col->insert(DB::DecimalField<DB::Decimal128>(value, decimal128->getScale()));
                }
                else
                {
                    throw DB::Exception(DB::ErrorCodes::LOGICAL_ERROR, "Unsupported data type: {}", type_info.inner_type->getName());
                }
            }
        }
        auto col_name = "sort_col_" + std::to_string(i);
        columns.emplace_back(std::move(col), data_type, col_name);
    }
    range_bounds_block = DB::Block(columns);
}

void RangeSelectorBuilder::initActionsDAG(const DB::Block & block)
{
    std::lock_guard lock(actions_dag_mutex);
    if (has_init_actions_dag)
        return;

    auto parser_context = ParserContext::build(QueryContext::globalContext(), *projection_plan_pb);
    ExpressionParser expression_parser(parser_context);

    const auto & expressions = projection_plan_pb->relations().at(0).root().input().project().expressions();
    std::vector<substrait::Expression> exprs;
    exprs.reserve(expressions.size());
    for (const auto & expression : expressions)
        exprs.emplace_back(expression);

    auto projection_actions_dag = expression_parser.expressionsToActionsDAG(exprs, block);
    projection_expression_actions = std::make_unique<DB::ExpressionActions>(std::move(projection_actions_dag));
    has_init_actions_dag = true;
}

void RangeSelectorBuilder::computePartitionIdByBinarySearch(DB::Block & block, DB::IColumn::Selector & selector)
{
    Chunks chunks;
    Chunk chunk(block.getColumns(), block.rows());
    chunks.emplace_back(std::move(chunk));
    selector.clear();
    selector.reserve(block.rows());
    auto input_columns = block.getColumns();
    auto total_rows = block.rows();
    const auto & bounds_columns = range_bounds_block.getColumns();
    auto max_part = bounds_columns[0]->size();
    for (size_t i = 0; i < bounds_columns.size(); i++)
        if (bounds_columns[i]->isNullable() && !input_columns[sorting_key_columns[i]]->isNullable())
            input_columns[sorting_key_columns[i]] = makeNullable(input_columns[sorting_key_columns[i]]);
    for (size_t r = 0; r < total_rows; ++r)
    {
        size_t selected_partition = 0;
        auto ret = binarySearchBound(bounds_columns, 0, max_part - 1, input_columns, sorting_key_columns, r);
        if (ret >= 0)
            selected_partition = ret;
        else
            selected_partition = max_part;
        selector.emplace_back(selected_partition);
    }
}
namespace
{
int doCompareAt(const ColumnPtr & lhs, size_t n, size_t m, const IColumn & rhs, int nan_direction_hint)
{
    if (const auto * l_const = typeid_cast<const ColumnConst *>(lhs.get()))
    {
        // we know rhs never be Const
        chassert(l_const->getDataType() == rhs.getDataType());
        return l_const->getDataColumn().compareAt(0, m, rhs, nan_direction_hint);
    }
    return lhs->compareAt(n, m, rhs, nan_direction_hint);
}
}

int RangeSelectorBuilder::compareRow(
    const DB::Columns & columns,
    const std::vector<size_t> & required_columns,
    size_t row,
    const DB::Columns & bound_columns,
    size_t bound_row) const
{
    for (size_t i = 0, n = required_columns.size(); i < n; ++i)
    {
        auto lpos = required_columns[i];
        auto rpos = i;
        auto res = doCompareAt(columns[lpos], row, bound_row, *bound_columns[rpos], sort_descriptions[i].nulls_direction)
            * sort_descriptions[i].direction;
        if (res != 0)
            return res;
    }
    return 0;
}

// If there were elements in range[l,r] that are larger then the row
// the return the min element's index. otherwise return -1
int RangeSelectorBuilder::binarySearchBound(
    const DB::Columns & bound_columns, Int64 l, Int64 r, const DB::Columns & columns, const std::vector<size_t> & used_cols, size_t row)
{
    if (l > r)
        return -1;
    auto m = (l + r) >> 1;
    auto cmp_ret = compareRow(columns, used_cols, row, bound_columns, m);
    if (l == r)
    {
        if (cmp_ret <= 0)
            return static_cast<int>(m);
        else
            return -1;
    }

    if (cmp_ret == 0)
        return static_cast<int>(m);
    if (cmp_ret < 0)
    {
        cmp_ret = binarySearchBound(bound_columns, l, m - 1, columns, used_cols, row);
        if (cmp_ret < 0)
        {
            // m is the upper bound
            return static_cast<int>(m);
        }
        return cmp_ret;
    }
    else
    {
        cmp_ret = binarySearchBound(bound_columns, m + 1, r, columns, used_cols, row);
        if (cmp_ret < 0)
            return -1;
        else
            return cmp_ret;
    }
    __builtin_unreachable();
}
}
