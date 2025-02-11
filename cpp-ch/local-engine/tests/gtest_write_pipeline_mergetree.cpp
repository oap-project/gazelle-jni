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

#include <gluten_test_util.h>
#include <incbin.h>
#include <testConfig.h>
#include <Core/Settings.h>
#include <DataTypes/DataTypeNullable.h>
#include <Interpreters/Context.h>
#include <Interpreters/InterpreterCreateQuery.h>
#include <Parser/LocalExecutor.h>
#include <Parser/RelParsers/WriteRelParser.h>
#include <Parser/SubstraitParserUtils.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ParserQuery.h>
#include <Parsers/parseQuery.h>
#include <Processors/Transforms/DeduplicationTokenTransforms.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <Storages/MergeTree/MergeTreeSink.h>
#include <Storages/MergeTree/SparkMergeTreeMeta.h>
#include <Storages/MergeTree/SparkMergeTreeWriteSettings.h>
#include <Storages/MergeTree/SparkMergeTreeWriter.h>
#include <Storages/StorageMergeTree.h>
#include <gtest/gtest.h>
#include <substrait/algebra.pb.h>
#include <Common/BlockTypeUtils.h>
#include <Common/DebugUtils.h>
#include <Common/QueryContext.h>
#include <Common/ThreadStatus.h>

namespace DB::Setting
{
extern const SettingsUInt64 max_parser_depth;
extern const SettingsUInt64 max_parser_backtracks;
extern const SettingsBool allow_settings_after_format_in_insert;
extern const SettingsUInt64 max_query_size;
extern const SettingsUInt64 min_insert_block_size_rows;
extern const SettingsUInt64 min_insert_block_size_bytes;
}

using namespace local_engine;
using namespace DB;

namespace
{
void do_remove(const std::string & folder)
{
    namespace fs = std::filesystem;
    if (const std::filesystem::path ph(folder); fs::exists(ph))
        fs::remove_all(ph);
}

Chunk person_chunk()
{
    auto id = INT()->createColumn();
    id->insert(100);
    id->insert(200);
    id->insert(300);
    id->insert(400);
    id->insert(500);
    id->insert(600);
    id->insert(700);

    auto name = STRING()->createColumn();
    name->insert("Joe");
    name->insert("Marry");
    name->insert("Mike");
    name->insert("Fred");
    name->insert("Albert");
    name->insert("Michelle");
    name->insert("Dan");

    auto age = makeNullable(INT())->createColumn();
    Field null_field;
    age->insert(30);
    age->insert(null_field);
    age->insert(18);
    age->insert(50);
    age->insert(null_field);
    age->insert(30);
    age->insert(50);


    MutableColumns x;
    x.push_back(std::move(id));
    x.push_back(std::move(name));
    x.push_back(std::move(age));
    return {std::move(x), 7};
}
}

TEST(MergeTree, ClickhouseMergeTree)
{
    ThreadStatus thread_status;

    const auto context = DB::Context::createCopy(QueryContext::globalContext());
    context->setPath("./");
    const Settings & settings = context->getSettingsRef();

    const std::string query
        = R"(create table if not exists person (id Int32, Name String, Age Nullable(Int32)) engine = MergeTree() ORDER BY id)";

    const char * begin = query.data();
    const char * end = query.data() + query.size();
    ParserQuery parser(end, settings[Setting::allow_settings_after_format_in_insert]);

    ASTPtr ast = parseQuery(
        parser,
        begin,
        end,
        "",
        settings[Setting::max_query_size],
        settings[Setting::max_parser_depth],
        settings[Setting::max_parser_backtracks]);

    EXPECT_TRUE(ast->as<ASTCreateQuery>());
    auto & create = ast->as<ASTCreateQuery &>();

    ColumnsDescription column_descriptions
        = InterpreterCreateQuery::getColumnsDescription(*create.columns_list->columns, context, LoadingStrictnessLevel::CREATE);

    StorageInMemoryMetadata metadata;
    metadata.setColumns(column_descriptions);
    metadata.setComment("args.comment");
    ASTPtr partition_by_key;
    metadata.partition_key = KeyDescription::getKeyFromAST(partition_by_key, metadata.columns, context);

    MergeTreeData::MergingParams merging_params;
    merging_params.mode = MergeTreeData::MergingParams::Ordinary;


    /// This merging param maybe used as part of sorting key
    std::optional<String> merging_param_key_arg;
    /// Get sorting key from engine arguments.
    ///
    /// NOTE: store merging_param_key_arg as additional key column. We do it
    /// before storage creation. After that storage will just copy this
    /// column if sorting key will be changed.
    metadata.sorting_key
        = KeyDescription::getSortingKeyFromAST(create.storage->order_by->ptr(), metadata.columns, context, merging_param_key_arg);

    std::unique_ptr<MergeTreeSettings> storage_settings = std::make_unique<MergeTreeSettings>(context->getMergeTreeSettings());

    UUID uuid;
    UUIDHelpers::getHighBytes(uuid) = 0xffffffffffff0fffull | 0x0000000000004000ull;
    UUIDHelpers::getLowBytes(uuid) = 0x3fffffffffffffffull | 0x8000000000000000ull;

    SCOPE_EXIT({ do_remove("WritePipeline_MergeTree"); });

    auto merge_tree = std::make_shared<StorageMergeTree>(
        StorageID("", "", uuid),
        "WritePipeline_MergeTree",
        metadata,
        LoadingStrictnessLevel::CREATE,
        context,
        "",
        merging_params,
        std::move(storage_settings));

    Block header{{INT(), "id"}, {STRING(), "Name"}, {makeNullable(INT()), "Age"}};
    DB::Squashing squashing(header, settings[Setting::min_insert_block_size_rows], settings[Setting::min_insert_block_size_bytes]);
    squashing.add(person_chunk());
    auto x = Squashing::squash(squashing.flush());
    x.getChunkInfos().add(std::make_shared<DeduplicationToken::TokenInfo>());

    ASSERT_EQ(7, x.getNumRows());
    ASSERT_EQ(3, x.getNumColumns());


    auto metadata_snapshot = std::make_shared<const StorageInMemoryMetadata>(metadata);
    ASTPtr none;
    auto sink = std::static_pointer_cast<MergeTreeSink>(merge_tree->write(none, metadata_snapshot, context, false));

    sink->consume(x);
    sink->onFinish();
}

INCBIN(_1_mergetree_, SOURCE_DIR "/utils/extern-local-engine/tests/json/mergetree/1_mergetree.json");
INCBIN(_1_mergetree_hdfs_, SOURCE_DIR "/utils/extern-local-engine/tests/json/mergetree/1_mergetree_hdfs.json");
INCBIN(_1_read_, SOURCE_DIR "/utils/extern-local-engine/tests/json/mergetree/1_plan.json");

TEST(MergeTree, SparkMergeTree)
{
    ThreadStatus thread_status;

    const auto context = DB::Context::createCopy(QueryContext::globalContext());
    context->setPath("./");
    const Settings & settings = context->getSettingsRef();

    const auto extension_table = local_engine::JsonStringToMessage<substrait::ReadRel::ExtensionTable>(EMBEDDED_PLAN(_1_mergetree_));
    MergeTreeTableInstance merge_tree_table(extension_table);

    EXPECT_EQ(merge_tree_table.database, "default");
    EXPECT_EQ(merge_tree_table.table, "lineitem_mergetree");
    EXPECT_EQ(merge_tree_table.relative_path, "lineitem_mergetree");
    EXPECT_EQ(merge_tree_table.table_configs.storage_policy, "default");

    do_remove(merge_tree_table.relative_path);

    const auto dest_storage = merge_tree_table.getStorage(QueryContext::globalMutableContext());
    EXPECT_TRUE(dest_storage);
    EXPECT_FALSE(dest_storage->getStoragePolicy()->getAnyDisk()->isRemote());
    DB::StorageMetadataPtr metadata_snapshot = dest_storage->getInMemoryMetadataPtr();
    Block header = metadata_snapshot->getSampleBlock();

    constexpr std::string_view split_template
        = R"({"items":[{"uriFile":"{replace_local_files}","length":"19230111","parquet":{},"schema":{},"metadataColumns":[{}],"properties":{"fileSize":"19230111","modificationTime":"1722330598029"}}]})";
    constexpr std::string_view file{GLUTEN_SOURCE_TPCH_URI("lineitem/part-00000-d08071cb-0dfa-42dc-9198-83cb334ccda3-c000.snappy.parquet")};

    SparkMergeTreeWritePartitionSettings gm_write_settings{
        .part_name_prefix{"this_is_prefix"},
    };
    gm_write_settings.set(context);

    auto writer = local_engine::SparkMergeTreeWriter::create(merge_tree_table, context, SparkMergeTreeWriter::CPP_UT_JOB_ID);
    SparkMergeTreeWriter & spark_merge_tree_writer = *writer;

    auto [_, local_executor] = test::create_plan_and_executor(EMBEDDED_PLAN(_1_read_), split_template, file);
    EXPECT_TRUE(local_executor->hasNext());

    do
    {
        spark_merge_tree_writer.write(*local_executor->nextColumnar());
    } while (local_executor->hasNext());

    spark_merge_tree_writer.close();

    ///
    {
        const auto extension_table_hdfs
            = local_engine::JsonStringToMessage<substrait::ReadRel::ExtensionTable>(EMBEDDED_PLAN(_1_mergetree_hdfs_));
        MergeTreeTableInstance merge_tree_table_hdfs(extension_table_hdfs);
        EXPECT_EQ(merge_tree_table_hdfs.database, "default");
        EXPECT_EQ(merge_tree_table_hdfs.table, "lineitem_mergetree_hdfs");
        EXPECT_EQ(merge_tree_table_hdfs.relative_path, "3.5/test/lineitem_mergetree_hdfs");
        EXPECT_EQ(merge_tree_table_hdfs.table_configs.storage_policy, "__hdfs_main");

        const auto dest_storage_hdfs = merge_tree_table_hdfs.getStorage(QueryContext::globalMutableContext());
        EXPECT_TRUE(dest_storage_hdfs);
        EXPECT_TRUE(dest_storage_hdfs->getStoragePolicy()->getAnyDisk()->isRemote());
    }
}

INCBIN(_3_mergetree_plan_input_, SOURCE_DIR "/utils/extern-local-engine/tests/json/mergetree/lineitem_parquet_input.json");
namespace
{
void writeMerge(
    std::string_view json_plan,
    const std::string & outputPath,
    const TestSettings & test_settings,
    const std::function<void(const DB::Block &)> & callback,
    std::optional<std::string> input = std::nullopt)
{
    auto query_id = QueryContext::instance().initializeQuery("gtest_mergetree");
    SCOPE_EXIT({ QueryContext::instance().finalizeQuery(query_id); });
    const auto context = QueryContext::instance().currentQueryContext();

    for (const auto & x : test_settings)
        context->setSetting(x.first, x.second);
    GlutenWriteSettings settings{.task_write_tmp_dir = outputPath};
    settings.set(context);
    SparkMergeTreeWritePartitionSettings partition_settings{.part_name_prefix = "_1"};
    partition_settings.set(context);

    auto input_json = input.value_or(replaceLocalFilesWithTPCH(EMBEDDED_PLAN(_3_mergetree_plan_input_)));
    auto [_, local_executor] = test::create_plan_and_executor(json_plan, input_json, context);

    while (local_executor->hasNext())
        callback(*local_executor->nextColumnar());
}
}
INCBIN(_3_mergetree_plan_, SOURCE_DIR "/utils/extern-local-engine/tests/json/mergetree/3_one_pipeline.json");
INCBIN(_4_mergetree_plan_, SOURCE_DIR "/utils/extern-local-engine/tests/json/mergetree/4_one_pipeline.json");
INCBIN(_lowcard_plan_, SOURCE_DIR "/utils/extern-local-engine/tests/json/mergetree/lowcard.json");
INCBIN(_case_sensitive_plan_, SOURCE_DIR "/utils/extern-local-engine/tests/json/mergetree/case_sensitive.json");
TEST(MergeTree, Pipeline)
{
    // context->setSetting("mergetree.max_num_part_per_merge_task", 1);
    writeMerge(
        EMBEDDED_PLAN(_3_mergetree_plan_),
        "tmp/lineitem_mergetree",
        {{"min_insert_block_size_rows", 100000} /*, {"optimize.minFileSize", 1024 * 1024 * 10}*/},
        [&](const DB::Block & block)
        {
            EXPECT_EQ(1, block.rows());
            std::cerr << debug::verticalShowString(block, 10, 50) << std::endl;
        });
}

TEST(MergeTree, PipelineWithPartition)
{
    writeMerge(
        EMBEDDED_PLAN(_4_mergetree_plan_),
        "tmp/lineitem_mergetree_p",
        {},
        [&](const DB::Block & block)
        {
            EXPECT_EQ(3815, block.rows());
            std::cerr << debug::showString(block, 50, 50) << std::endl;
        });
}

TEST(MergeTree, lowcard)
{
    writeMerge(
        EMBEDDED_PLAN(_lowcard_plan_),
        "tmp/lineitem_mergetre_lowcard",
        {},
        [&](const DB::Block & block)
        {
            EXPECT_EQ(1, block.rows());
            std::cerr << debug::verticalShowString(block, 10, 50) << std::endl;
        });
}

TEST(MergeTree, case_sensitive)
{
    //TODO: case_sensitive
    GTEST_SKIP();
    writeMerge(
        EMBEDDED_PLAN(_case_sensitive_plan_),
        "tmp/LINEITEM_MERGETREE_CASE_SENSITIVE",
        {},
        [&](const DB::Block & block)
        {
            EXPECT_EQ(1, block.rows());
            std::cerr << debug::showString(block, 20, 50) << std::endl;
        });
}