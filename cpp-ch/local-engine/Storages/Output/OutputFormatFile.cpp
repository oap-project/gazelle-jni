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

#include "OutputFormatFile.h"
#include <DataTypes/IDataType.h>
#include <Processors/Port.h>
#include <Storages/Output/ORCOutputFormatFile.h>
#include <Storages/Output/ParquetOutputFormatFile.h>
#include <boost/algorithm/string/case_conv.hpp>

namespace DB
{
namespace ErrorCodes
{
extern const int NOT_IMPLEMENTED;
extern const int BAD_ARGUMENTS;
}

}
namespace local_engine
{

using namespace DB;

OutputFormatFile::OutputFormatFile(
    DB::ContextPtr context_,
    const std::string & file_uri_,
    WriteBufferBuilderPtr write_buffer_builder_,
    const DB::Block & preferred_schema_)
    : context(context_), file_uri(file_uri_), write_buffer_builder(write_buffer_builder_), preferred_schema(preferred_schema_)
{
}

Block OutputFormatFile::createHeaderWithPreferredSchema(const Block & header)
{
    if (!preferred_schema)
        throw DB::Exception(DB::ErrorCodes::LOGICAL_ERROR, "preferred_schema is empty");

    /// Create a new header with the preferred column name and type
    DB::ColumnsWithTypeAndName columns;
    columns.reserve(preferred_schema.columns());
    size_t index = 0;
    for (const auto & name_type : header.getNamesAndTypesList())
    {
        if (name_type.name.starts_with("__bucket_value__"))
            continue;

        const auto & preferred_column = preferred_schema.getByPosition(index++);
        ColumnWithTypeAndName column(preferred_column.type->createColumn(), preferred_column.type, preferred_column.name);
        columns.emplace_back(std::move(column));
    }
    return {std::move(columns)};
}

OutputFormatFilePtr OutputFormatFileUtil::createFile(
    DB::ContextPtr context,
    local_engine::WriteBufferBuilderPtr write_buffer_builder,
    const std::string & file_uri,
    const DB::Block & preferred_schema,
    const std::string & format_hint)
{
#if USE_PARQUET
    if (boost::to_lower_copy(file_uri).ends_with(".parquet") || "parquet" == boost::to_lower_copy(format_hint))
        return std::make_shared<ParquetOutputFormatFile>(context, file_uri, write_buffer_builder, preferred_schema);
#endif

#if USE_ORC
    if (boost::to_lower_copy(file_uri).ends_with(".orc") || "orc" == boost::to_lower_copy(format_hint))
        return std::make_shared<ORCOutputFormatFile>(context, file_uri, write_buffer_builder, preferred_schema);
#endif


    throw DB::Exception(DB::ErrorCodes::NOT_IMPLEMENTED, "Format not supported for file :{}", file_uri);
}
}
