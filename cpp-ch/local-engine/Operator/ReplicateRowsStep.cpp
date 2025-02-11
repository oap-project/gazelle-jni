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
#include "ReplicateRowsStep.h"

#include <Columns/IColumn.h>
#include <QueryPipeline/QueryPipelineBuilder.h>

namespace DB
{
namespace ErrorCodes
{
extern const int LOGICAL_ERROR;
}
}

namespace local_engine
{
static DB::ITransformingStep::Traits getTraits()
{
    return DB::ITransformingStep::Traits{
        {
            .preserves_number_of_streams = true,
            .preserves_sorting = false,
        },
        {
            .preserves_number_of_rows = false,
        }};
}

ReplicateRowsStep::ReplicateRowsStep(const DB::Block& input_header)
    : ITransformingStep(input_header, transformHeader(input_header), getTraits())
{
}

DB::Block ReplicateRowsStep::transformHeader(const DB::Block & input)
{
    DB::Block output;
    for (int i = 1; i < input.columns(); i++)
    {
        output.insert(input.getByPosition(i));
    }
    return output;
}

void ReplicateRowsStep::transformPipeline(DB::QueryPipelineBuilder & pipeline, const DB::BuildQueryPipelineSettings & /*settings*/)
{
    pipeline.addSimpleTransform([&](const DB::Block & header) { return std::make_shared<ReplicateRowsTransform>(header); });
}

void ReplicateRowsStep::updateOutputHeader()
{
    output_header = transformHeader(input_headers.front());
}

ReplicateRowsTransform::ReplicateRowsTransform(const DB::Block & input_header_)
    : ISimpleTransform(input_header_, ReplicateRowsStep::transformHeader(input_header_), true)
{
}

void ReplicateRowsTransform::transform(DB::Chunk & chunk)
{
    auto replica_column = chunk.getColumns().front();
    size_t total_rows = 0;
    for (int i = 0; i < replica_column->size(); i++)
    {
        total_rows += replica_column->get64(i);
    }

    auto columns = chunk.detachColumns();
    DB::MutableColumns mutable_columns;
    for (int i = 1; i < columns.size(); i++)
    {
        mutable_columns.push_back(columns[i]->cloneEmpty());
        mutable_columns.back()->reserve(total_rows);
        DB::ColumnPtr src_col = columns[i];
        DB::MutableColumnPtr & cur = mutable_columns.back();
        for (int j = 0; j < replica_column->size(); j++)
        {
            cur->insertManyFrom(*src_col, j, replica_column->getUInt(j));
        }
    }

    chunk.setColumns(std::move(mutable_columns), total_rows);
}
}
