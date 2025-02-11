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

#include "InputFileNameParser.h"

#include <ranges>

#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Processors/ISimpleTransform.h>
#include <Processors/QueryPlan/QueryPlan.h>
#include <QueryPipeline/QueryPipelineBuilder.h>


namespace local_engine
{
static DB::ITransformingStep::Traits getTraits()
{
    return DB::ITransformingStep::Traits{
        {
            .returns_single_stream = false,
            .preserves_number_of_streams = true,
            .preserves_sorting = true,
        },
        {
            .preserves_number_of_rows = true,
        }};
}

static DB::Block createOutputHeader(
    const DB::Block & header,
    const std::optional<String> & file_name,
    const std::optional<Int64> & block_start,
    const std::optional<Int64> & block_length)
{
    DB::Block output_header{header};
    if (file_name.has_value())
        output_header.insert(DB::ColumnWithTypeAndName{std::make_shared<DB::DataTypeString>(), FileMetaColumns::INPUT_FILE_NAME});
    if (block_start.has_value())
        output_header.insert(DB::ColumnWithTypeAndName{std::make_shared<DB::DataTypeInt64>(), FileMetaColumns::INPUT_FILE_BLOCK_START});
    if (block_length.has_value())
        output_header.insert(DB::ColumnWithTypeAndName{std::make_shared<DB::DataTypeInt64>(), FileMetaColumns::INPUT_FILE_BLOCK_LENGTH});
    return output_header;
}

class InputFileExprProjectTransform : public DB::ISimpleTransform
{
public:
    InputFileExprProjectTransform(
        const DB::Block & input_header_,
        const DB::Block & output_header_,
        const std::optional<String> & file_name,
        const std::optional<Int64> & block_start,
        const std::optional<Int64> & block_length)
        : ISimpleTransform(input_header_, output_header_, true), file_name(file_name), block_start(block_start), block_length(block_length)
    {
    }

    String getName() const override { return "InputFileExprProjectTransform"; }

    void transform(DB::Chunk & chunk) override
    {
        InputFileNameParser::addInputFileColumnsToChunk(output.getHeader(), chunk, file_name, block_start, block_length);
    }

private:
    std::optional<String> file_name;
    std::optional<Int64> block_start;
    std::optional<Int64> block_length;
};

class InputFileExprProjectStep : public DB::ITransformingStep
{
public:
    InputFileExprProjectStep(
        const DB::Block & input_header,
        const std::optional<String> & file_name,
        const std::optional<Int64> & block_start,
        const std::optional<Int64> & block_length)
        : ITransformingStep(input_header, createOutputHeader(input_header, file_name, block_start, block_length), getTraits(), true)
        , file_name(file_name)
        , block_start(block_start)
        , block_length(block_length)
    {
    }

    String getName() const override { return "InputFileExprProjectStep"; }

    void transformPipeline(DB::QueryPipelineBuilder & pipeline, const DB::BuildQueryPipelineSettings & /*settings*/) override
    {
        pipeline.addSimpleTransform(
            [&](const DB::Block & header)
            { return std::make_shared<InputFileExprProjectTransform>(header, *output_header, file_name, block_start, block_length); });
    }

protected:
    void updateOutputHeader() override
    {
        // do nothing
    }

private:
    std::optional<String> file_name;
    std::optional<Int64> block_start;
    std::optional<Int64> block_length;
};

bool InputFileNameParser::hasInputFileNameColumn(const DB::Block & block)
{
    return block.findByName(FileMetaColumns::INPUT_FILE_NAME) != nullptr;
}

bool InputFileNameParser::hasInputFileBlockStartColumn(const DB::Block & block)
{
    return block.findByName(FileMetaColumns::INPUT_FILE_BLOCK_START) != nullptr;
}

bool InputFileNameParser::hasInputFileBlockLengthColumn(const DB::Block & block)
{
    return block.findByName(FileMetaColumns::INPUT_FILE_BLOCK_LENGTH) != nullptr;
}

void InputFileNameParser::addInputFileColumnsToChunk(
    const DB::Block & header,
    DB::Chunk & chunk,
    const std::optional<String> & file_name,
    const std::optional<Int64> & block_start,
    const std::optional<Int64> & block_length)
{
    auto output_columns = chunk.getColumns();
    for (size_t i = 0; i < header.columns(); ++i)
    {
        const auto & column = header.getByPosition(i);
        if (column.name == FileMetaColumns::INPUT_FILE_NAME)
        {
            if (!file_name.has_value())
                throw DB::Exception(DB::ErrorCodes::LOGICAL_ERROR, "Input file name is not set");
            auto type_string = std::make_shared<DB::DataTypeString>();
            auto file_name_column = type_string->createColumnConst(chunk.getNumRows(), file_name.value());
            output_columns.insert(output_columns.begin() + i, std::move(file_name_column));
        }
        else if (column.name == FileMetaColumns::INPUT_FILE_BLOCK_START)
        {
            if (!block_start.has_value())
                throw DB::Exception(DB::ErrorCodes::LOGICAL_ERROR, "block_start is not set");
            auto type_int64 = std::make_shared<DB::DataTypeInt64>();
            auto block_start_column = type_int64->createColumnConst(chunk.getNumRows(), block_start.value());
            output_columns.insert(output_columns.begin() + i, std::move(block_start_column));
        }
        else if (column.name == FileMetaColumns::INPUT_FILE_BLOCK_LENGTH)
        {
            if (!block_length.has_value())
                throw DB::Exception(DB::ErrorCodes::LOGICAL_ERROR, "block_length is not set");
            auto type_int64 = std::make_shared<DB::DataTypeInt64>();
            auto block_length_column = type_int64->createColumnConst(chunk.getNumRows(), block_length.value());
            output_columns.insert(output_columns.begin() + i, std::move(block_length_column));
        }
    }
    chunk.setColumns(output_columns, chunk.getNumRows());
}

bool InputFileNameParser::containsInputFileColumns(const DB::Block & block)
{
    return FileMetaColumns::hasVirtualColumns(block);
}

DB::Block InputFileNameParser::removeInputFileColumn(const DB::Block & block)
{
    return FileMetaColumns::removeVirtualColumns(block);
}

std::optional<DB::IQueryPlanStep *> InputFileNameParser::addInputFileProjectStep(DB::QueryPlan & plan)
{
    if (!file_name.has_value() && !block_start.has_value() && !block_length.has_value())
        return std::nullopt;
    auto step = std::make_unique<InputFileExprProjectStep>(plan.getCurrentHeader(), file_name, block_start, block_length);
    step->setStepDescription("Input file expression project");
    std::optional<DB::IQueryPlanStep *> result = step.get();
    plan.addStep(std::move(step));
    return result;
}

}
