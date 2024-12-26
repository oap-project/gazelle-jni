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
#include "BroadCastJoinBuilder.h"

#include <Compression/CompressedReadBuffer.h>
#include <Interpreters/TableJoin.h>
#include <Join/StorageJoinFromReadBuffer.h>
#include <Parser/RelParsers/JoinRelParser.h>
#include <Parser/TypeParser.h>
#include <QueryPipeline/ProfileInfo.h>
#include <Shuffle/ShuffleReader.h>
#include <jni/SharedPointerWrapper.h>
#include <jni/jni_common.h>
#include <Poco/StringTokenizer.h>
#include <Common/CHUtil.h>
#include <Common/JNIUtils.h>
#include <Common/logger_useful.h>

namespace DB
{
namespace ErrorCodes
{
extern const int UNKNOWN_TYPE;
}
}

namespace local_engine
{
namespace BroadCastJoinBuilder
{
using namespace DB;
static jclass Java_CHBroadcastBuildSideCache = nullptr;
static jmethodID Java_get = nullptr;
jlong callJavaGet(const std::string & id)
{
    GET_JNIENV(env)
    const jstring s = charTojstring(env, id.c_str());
    const auto result = safeCallStaticLongMethod(env, Java_CHBroadcastBuildSideCache, Java_get, s);
    CLEAN_JNIENV

    return result;
}

DB::Block resetBuildTableBlockName(Block & block, bool only_one = false)
{
    DB::ColumnsWithTypeAndName new_cols;
    std::set<std::string> names;
    int32_t seq = 0;
    for (const auto & col : block)
    {
      // Add a prefix to avoid column name conflicts with left table.
      std::stringstream new_name;
      // add a sequence to avoid duplicate name in some rare cases
      if (names.find(col.name) == names.end())
      {
         new_name << BlockUtil::RIHGT_COLUMN_PREFIX << col.name;
         names.insert(col.name);
      }
      else
      {
        new_name << BlockUtil::RIHGT_COLUMN_PREFIX  << (seq++) << "_" << col.name;
      }
      new_cols.emplace_back(col.column, col.type, new_name.str());

      if (only_one)
        break;
    }
    return DB::Block(new_cols);
}

void cleanBuildHashTable(const std::string & hash_table_id, jlong instance)
{
    auto clean_join = [&]
    {
        SharedPointerWrapper<StorageJoinFromReadBuffer>::dispose(instance);
    };
    /// Record memory usage in Total Memory Tracker
    ThreadFromGlobalPoolNoTracingContextPropagation thread(clean_join);
    thread.join();
    LOG_DEBUG(&Poco::Logger::get("BroadCastJoinBuilder"), "Broadcast hash table {} is cleaned", hash_table_id);
}

std::shared_ptr<StorageJoinFromReadBuffer> getJoin(const std::string & key)
{
    const jlong result = callJavaGet(key);

    if (unlikely(result == 0))
        throw Exception(ErrorCodes::LOGICAL_ERROR, "broadcast table {} not found in cache.", key);

    auto wrapper = SharedPointerWrapper<StorageJoinFromReadBuffer>::sharedPtr(result);
    if (unlikely(!wrapper))
        throw Exception(ErrorCodes::LOGICAL_ERROR, "broadcast table {} not found, cache value is invalidated.", key);

    return wrapper;
}

std::shared_ptr<StorageJoinFromReadBuffer> buildJoin(
    const std::string & key,
    DB::ReadBuffer & input,
    jlong row_count,
    const std::string & join_keys,
    jint join_type,
    bool has_mixed_join_condition,
    bool is_existence_join,
    const std::string & named_struct,
    bool is_null_aware_anti_join,
    bool has_null_key_values)
{
    auto join_key_list = Poco::StringTokenizer(join_keys, ",");
    Names key_names;
    for (const auto & key_name : join_key_list)
        key_names.emplace_back(BlockUtil::RIHGT_COLUMN_PREFIX + key_name);

    DB::JoinKind kind;
    DB::JoinStrictness strictness;

    if (key.starts_with("BuiltBNLJBroadcastTable-"))
        std::tie(kind, strictness) = JoinUtil::getCrossJoinKindAndStrictness(static_cast<substrait::CrossRel_JoinType>(join_type));
    else
        std::tie(kind, strictness) = JoinUtil::getJoinKindAndStrictness(static_cast<substrait::JoinRel_JoinType>(join_type), is_existence_join);


    substrait::NamedStruct substrait_struct;
    substrait_struct.ParseFromString(named_struct);
    Block header = TypeParser::buildBlockFromNamedStruct(substrait_struct);
    header = resetBuildTableBlockName(header);

    Blocks data;
    auto collect_data = [&]
    {
        bool only_one_column = header.getNamesAndTypesList().empty();
        if (only_one_column)
            header = BlockUtil::buildRowCountBlock(0).getColumnsWithTypeAndName();

        NativeReader block_stream(input);
        ProfileInfo info;
        while (Block block = block_stream.read())
        {
            DB::ColumnsWithTypeAndName columns;
            for (size_t i = 0; i < block.columns(); ++i)
            {
                const auto & column = block.getByPosition(i);
                if (only_one_column)
                {
                    auto virtual_block = BlockUtil::buildRowCountBlock(column.column->size()).getColumnsWithTypeAndName();
                    header = virtual_block;
                    columns.emplace_back(virtual_block.back());
                    break;
                }

                columns.emplace_back(BlockUtil::convertColumnAsNecessary(column, header.getByPosition(i)));
            }

            DB::Block final_block(columns);
            info.update(final_block);
            data.emplace_back(std::move(final_block));
        }
    };
    /// Record memory usage in Total Memory Tracker
    ThreadFromGlobalPoolNoTracingContextPropagation thread(collect_data);
    thread.join();

    ColumnsDescription columns_description(header.getNamesAndTypesList());

    return make_shared<StorageJoinFromReadBuffer>(
        data,
        row_count,
        key_names,
        true,
        kind,
        strictness,
        has_mixed_join_condition,
        columns_description,
        ConstraintsDescription(),
        key,
        true,
        is_null_aware_anti_join,
        has_null_key_values);
}

void init(JNIEnv * env)
{
    /**
     * Scala object will be compiled into two classes, one is with '$' suffix which is normal class,
     * and one is utility class which only has static method.
     *
     * Here, we use utility class.
     */

    const char * classSig = "Lorg/apache/gluten/execution/CHBroadcastBuildSideCache;";
    Java_CHBroadcastBuildSideCache = CreateGlobalClassReference(env, classSig);
    Java_get = GetStaticMethodID(env, Java_CHBroadcastBuildSideCache, "get", "(Ljava/lang/String;)J");
}

void destroy(JNIEnv * env)
{
    env->DeleteGlobalRef(Java_CHBroadcastBuildSideCache);
}

}
}
