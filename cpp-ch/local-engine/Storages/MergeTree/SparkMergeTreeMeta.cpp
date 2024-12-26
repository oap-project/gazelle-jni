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
#include "SparkMergeTreeMeta.h"

#include <IO/ReadBufferFromString.h>
#include <IO/ReadHelpers.h>
#include <Parser/SubstraitParserUtils.h>
#include <Parser/TypeParser.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTSelectQuery.h>
#include <Storages/MergeTree/IMergeTreeDataPart.h>
#include <Storages/MergeTree/MetaDataHelper.h>
#include <Storages/MergeTree/StorageMergeTreeFactory.h>
#include <google/protobuf/util/json_util.h>
#include <rapidjson/document.h>
#include <write_optimization.pb.h>
#include <Poco/StringTokenizer.h>
#include <Common/DebugUtils.h>

using namespace DB;
using namespace local_engine;
namespace
{
// set skip index for each column if specified
void setSecondaryIndex(
    const DB::NamesAndTypesList & columns,
    const ContextPtr & context,
    const MergeTreeTable & table,
    std::shared_ptr<DB::StorageInMemoryMetadata> metadata)
{
    std::unordered_set<std::string> minmax_index_cols;
    std::unordered_set<std::string> bf_index_cols;
    std::unordered_set<std::string> set_index_cols;
    {
        Poco::StringTokenizer tokenizer(table.minmax_index_key, ",");
        for (const auto & token : tokenizer)
            minmax_index_cols.insert(token);
    }
    {
        Poco::StringTokenizer tokenizer(table.bf_index_key, ",");
        for (const auto & token : tokenizer)
            bf_index_cols.insert(token);
    }
    {
        Poco::StringTokenizer tokenizer(table.set_index_key, ",");
        for (const auto & token : tokenizer)
            set_index_cols.insert(token);
    }

    std::stringstream ss;
    bool first = true;
    for (const auto & column : columns)
    {
        if (minmax_index_cols.contains(column.name))
        {
            if (!first)
                ss << ", ";
            else
                first = false;
            ss << "_minmax_" << column.name << " " << column.name << " TYPE minmax GRANULARITY 1";
        }

        if (bf_index_cols.contains(column.name))
        {
            if (!first)
                ss << ", ";
            else
                first = false;
            ss << "_bloomfilter_" << column.name << " " << column.name << " TYPE bloom_filter GRANULARITY 1";
        }

        if (set_index_cols.contains(column.name))
        {
            if (!first)
                ss << ", ";
            else
                first = false;
            ss << "_set_" << column.name << " " << column.name << " TYPE set(0) GRANULARITY 1";
        }
    }
    metadata->setSecondaryIndices(IndicesDescription::parse(ss.str(), metadata->getColumns(), context));
}

void parseTableConfig(MergeTreeTableSettings & settings, const String & config_json)
{
    rapidjson::Document doc;
    doc.Parse(config_json.c_str());
    if (doc.HasMember("storage_policy"))
        settings.storage_policy = doc["storage_policy"].GetString();
}

std::shared_ptr<DB::StorageInMemoryMetadata>
doBuildMetadata(const DB::NamesAndTypesList & columns, const ContextPtr & context, const MergeTreeTable & table)
{
    std::shared_ptr<DB::StorageInMemoryMetadata> metadata = std::make_shared<DB::StorageInMemoryMetadata>();
    ColumnsDescription columns_description;
    for (const auto & item : columns)
        columns_description.add(ColumnDescription(item.name, item.type));
    metadata->setColumns(std::move(columns_description));

    setSecondaryIndex(columns, context, table, metadata);

    metadata->partition_key.expression_list_ast = std::make_shared<ASTExpressionList>();
    metadata->sorting_key = KeyDescription::parse(table.order_by_key, metadata->getColumns(), context, true);
    if (table.primary_key.empty())
        if (table.order_by_key != MergeTreeTable::TUPLE)
            metadata->primary_key = KeyDescription::parse(table.order_by_key, metadata->getColumns(), context, true);
        else
            metadata->primary_key.expression = std::make_shared<ExpressionActions>(ActionsDAG{});
    else
        metadata->primary_key = KeyDescription::parse(table.primary_key, metadata->getColumns(), context, true);
    return metadata;
}

void doParseMergeTreeTableString(MergeTreeTable & table, ReadBufferFromString & in)
{
    assertString("MergeTree;", in);
    readString(table.database, in);
    assertChar('\n', in);
    readString(table.table, in);
    assertChar('\n', in);
    readString(table.snapshot_id, in);
    assertChar('\n', in);
    String schema;
    readString(schema, in);
    table.schema = JsonStringToMessage<substrait::NamedStruct>(schema);
    assertChar('\n', in);
    readString(table.order_by_key, in);
    assertChar('\n', in);
    readString(table.primary_key, in);
    assertChar('\n', in);
    readString(table.low_card_key, in);
    assertChar('\n', in);
    readString(table.minmax_index_key, in);
    assertChar('\n', in);
    readString(table.bf_index_key, in);
    assertChar('\n', in);
    readString(table.set_index_key, in);
    assertChar('\n', in);
    readString(table.relative_path, in);
    assertChar('\n', in);
    readString(table.absolute_path, in);
    assertChar('\n', in);
    String json;
    readString(json, in);
    parseTableConfig(table.table_configs, json);
    assertChar('\n', in);
}

}
namespace local_engine
{

SparkStorageMergeTreePtr MergeTreeTable::getStorage(ContextMutablePtr context) const
{
    const DB::Block header = TypeParser::buildBlockFromNamedStruct(schema, low_card_key);
    const auto metadata = buildMetaData(header, context);

    return StorageMergeTreeFactory::getStorage(
        StorageID(database, table),
        snapshot_id,
        *this,
        [&]() -> SparkStorageMergeTreePtr
        {
            auto custom_storage_merge_tree = std::make_shared<SparkWriteStorageMergeTree>(*this, *metadata, context);
            return custom_storage_merge_tree;
        });
}

SparkStorageMergeTreePtr MergeTreeTable::copyToDefaultPolicyStorage(const ContextMutablePtr & context) const
{
    MergeTreeTable merge_tree_table{*this};
    auto temp_uuid = UUIDHelpers::generateV4();
    String temp_uuid_str = toString(temp_uuid);
    merge_tree_table.table = merge_tree_table.table + "_" + temp_uuid_str;
    merge_tree_table.snapshot_id = "";
    merge_tree_table.table_configs.storage_policy = "";
    merge_tree_table.relative_path = merge_tree_table.relative_path + "_" + temp_uuid_str;
    return merge_tree_table.getStorage(context);
}

SparkStorageMergeTreePtr MergeTreeTable::copyToVirtualStorage(const ContextMutablePtr & context) const
{
    MergeTreeTable merge_tree_table{*this};
    auto temp_uuid = UUIDHelpers::generateV4();
    String temp_uuid_str = toString(temp_uuid);
    merge_tree_table.table = merge_tree_table.table + "_" + temp_uuid_str;
    merge_tree_table.snapshot_id = "";
    return merge_tree_table.getStorage(context);
}

MergeTreeTableInstance::MergeTreeTableInstance(const std::string & info) : MergeTreeTable()
{
    ReadBufferFromString in(info);
    doParseMergeTreeTableString(*this, in);

    while (!in.eof())
    {
        MergeTreePart part;
        readString(part.name, in);
        assertChar('\n', in);
        readIntText(part.begin, in);
        assertChar('\n', in);
        readIntText(part.end, in);
        assertChar('\n', in);
        parts.emplace_back(part);
    }
}

MergeTreeTableInstance::MergeTreeTableInstance(const google::protobuf::Any & any) : MergeTreeTableInstance(toString(any))
{
}

MergeTreeTableInstance::MergeTreeTableInstance(const substrait::ReadRel::ExtensionTable & extension_table)
    : MergeTreeTableInstance(extension_table.detail())
{
    debug::dumpMessage(extension_table, "merge_tree_table");
}

SparkStorageMergeTreePtr MergeTreeTableInstance::restoreStorage(const ContextMutablePtr & context) const
{
    auto result = getStorage(context);
    restoreMetaData(result, *this, *context);
    return result;
}

std::shared_ptr<DB::StorageInMemoryMetadata> MergeTreeTable::buildMetaData(const DB::Block & header, const ContextPtr & context) const
{
    return doBuildMetadata(header.getNamesAndTypesList(), context, *this);
}

MergeTreeTable::MergeTreeTable(const local_engine::Write & write, const substrait::NamedStruct & table_schema)
{
    assert(write.has_mergetree());
    const Write_MergeTreeWrite & merge_tree_write = write.mergetree();
    database = merge_tree_write.database();
    table = merge_tree_write.table();
    snapshot_id = merge_tree_write.snapshot_id();
    schema = table_schema;
    order_by_key = merge_tree_write.order_by_key();
    low_card_key = merge_tree_write.low_card_key();
    minmax_index_key = merge_tree_write.minmax_index_key();
    bf_index_key = merge_tree_write.bf_index_key();
    set_index_key = merge_tree_write.set_index_key();
    primary_key = merge_tree_write.primary_key();
    relative_path = merge_tree_write.relative_path();
    absolute_path = merge_tree_write.absolute_path(); // always empty, see createNativeWrite in java
    table_configs.storage_policy = merge_tree_write.storage_policy();
}

std::unique_ptr<SelectQueryInfo> buildQueryInfo(NamesAndTypesList & names_and_types_list)
{
    std::unique_ptr<SelectQueryInfo> query_info = std::make_unique<SelectQueryInfo>();
    query_info->query = std::make_shared<ASTSelectQuery>();
    auto syntax_analyzer_result = std::make_shared<TreeRewriterResult>(names_and_types_list);
    syntax_analyzer_result->analyzed_join = std::make_shared<TableJoin>();
    query_info->syntax_analyzer_result = syntax_analyzer_result;
    return query_info;
}

std::unordered_set<String> MergeTreeTableInstance::getPartNames() const
{
    std::unordered_set<String> names;
    for (const auto & part : parts)
        names.emplace(part.name);
    return names;
}

RangesInDataParts MergeTreeTableInstance::extractRange(DataPartsVector parts_vector) const
{
    std::unordered_map<String, DataPartPtr> name_index;
    std::ranges::for_each(parts_vector, [&](const DataPartPtr & part) { name_index.emplace(part->name, part); });
    RangesInDataParts ranges_in_data_parts;

    std::ranges::transform(
        parts,
        std::inserter(ranges_in_data_parts, ranges_in_data_parts.end()),
        [&](const MergeTreePart & part)
        {
            RangesInDataPart ranges_in_data_part;
            ranges_in_data_part.data_part = name_index.at(part.name);
            ranges_in_data_part.part_index_in_query = 0;
            ranges_in_data_part.ranges.emplace_back(MarkRange(part.begin, part.end));
            return ranges_in_data_part;
        });
    return ranges_in_data_parts;
}

bool sameColumns(const substrait::NamedStruct & left, const substrait::NamedStruct & right)
{
    if (left.names_size() != right.names_size())
        return false;
    std::unordered_map<String, substrait::Type::KindCase> map;
    for (size_t i = 0; i < left.names_size(); i++)
        map.emplace(left.names(i), left.struct_().types(i).kind_case());
    for (size_t i = 0; i < right.names_size(); i++)
        if (!map.contains(right.names(i)) || map[right.names(i)] != right.struct_().types(i).kind_case())
            return false;
    return true;
}

bool MergeTreeTable::sameTable(const MergeTreeTable & other) const
{
    return database == other.database && table == other.table && snapshot_id == other.snapshot_id && sameColumns(schema, other.schema)
        && order_by_key == other.order_by_key && low_card_key == other.low_card_key && minmax_index_key == other.minmax_index_key
        && bf_index_key == other.bf_index_key && set_index_key == other.set_index_key && primary_key == other.primary_key
        && relative_path == other.relative_path && absolute_path == other.absolute_path
        && table_configs.storage_policy == other.table_configs.storage_policy;
}
}