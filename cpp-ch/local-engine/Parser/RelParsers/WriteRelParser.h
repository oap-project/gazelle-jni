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
#pragma once

#include <map>
#include <string>
#include <Core/Block.h>
#include <Core/Names.h>
#include <Interpreters/Context_fwd.h>
#include <google/protobuf/repeated_field.h>
#include <Common/GlutenSettings.h>

namespace substrait
{
class WriteRel;
class NamedStruct;
}

namespace DB
{
class QueryPipelineBuilder;
using QueryPipelineBuilderPtr = std::unique_ptr<QueryPipelineBuilder>;
}

namespace local_engine
{

using PartitionIndexes = google::protobuf::RepeatedField<::int32_t>;

void addSinkTransform(const DB::ContextPtr & context, const substrait::WriteRel & write_rel, const DB::QueryPipelineBuilderPtr & builder);

DB::Names collect_partition_cols(const DB::Block & header, const substrait::NamedStruct & struct_, const PartitionIndexes & partition_by);

#define WRITE_RELATED_SETTINGS(M, ALIAS) \
    M(String, task_write_tmp_dir, , "The temporary directory for writing data") \
    M(String, task_write_filename_pattern, , "The pattern to generate file name for writing delta parquet in spark 3.5")

DECLARE_GLUTEN_SETTINGS(GlutenWriteSettings, WRITE_RELATED_SETTINGS)

}
