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

#include "LocalExecutor.h"

#include <Core/Settings.h>
#include <Interpreters/Context.h>
#include <Processors/Executors/PipelineExecutor.h>
#include <QueryPipeline/printPipeline.h>
#include <Common/QueryContext.h>
#include <Common/logger_useful.h>

namespace DB::Setting
{
extern const SettingsMaxThreads max_threads;
}
using namespace DB;
namespace local_engine
{

LocalExecutor::~LocalExecutor()
{
    if (dump_pipeline)
        LOG_INFO(&Poco::Logger::get("LocalExecutor"), "Dump pipeline:\n{}", dumpPipeline());

    if (spark_buffer)
    {
        ch_column_to_spark_row->freeMem(spark_buffer->address, spark_buffer->size);
        spark_buffer.reset();
    }
}

std::unique_ptr<SparkRowInfo> LocalExecutor::writeBlockToSparkRow(const Block & block) const
{
    return ch_column_to_spark_row->convertCHColumnToSparkRow(block);
}

void LocalExecutor::initPullingPipelineExecutor()
{
    if (!executor)
    {
        query_pipeline = QueryPipelineBuilder::getPipeline(std::move(*query_pipeline_builder));
        executor = std::make_unique<PullingPipelineExecutor>(query_pipeline);
    }
}

bool LocalExecutor::hasNext()
{
    initPullingPipelineExecutor();
    size_t columns = currentBlock().columns();
    if (columns == 0 || isConsumed())
    {
        auto empty_block = header.cloneEmpty();
        setCurrentBlock(empty_block);
        bool has_next = executor->pull(currentBlock());
        produce();
        return has_next;
    }
    return true;
}

bool LocalExecutor::fallbackMode() const
{
    return executor.get() || fallback_mode;
}

SparkRowInfoPtr LocalExecutor::next()
{
    checkNextValid();
    SparkRowInfoPtr row_info = writeBlockToSparkRow(currentBlock());
    consume();
    if (spark_buffer)
    {
        ch_column_to_spark_row->freeMem(spark_buffer->address, spark_buffer->size);
        spark_buffer.reset();
    }
    spark_buffer = std::make_unique<SparkBuffer>();
    spark_buffer->address = row_info->getBufferAddress();
    spark_buffer->size = row_info->getTotalBytes();
    return row_info;
}
Block * LocalExecutor::nextColumnar()
{
    checkNextValid();
    Block * columnar_batch;
    if (currentBlock().columns() > 0)
    {
        columnar_batch = &currentBlock();
    }
    else
    {
        auto empty_block = header.cloneEmpty();
        setCurrentBlock(empty_block);
        columnar_batch = &currentBlock();
    }
    consume();
    return columnar_batch;
}

void LocalExecutor::cancel() const
{
    if (executor)
        executor->cancel();
    if (push_executor)
        push_executor->cancel();
}

void LocalExecutor::setSinks(const std::function<void(DB::QueryPipelineBuilder &)> & setter) const
{
    setter(*query_pipeline_builder);
}

void LocalExecutor::execute()
{
    chassert(query_pipeline_builder);
    push_executor = query_pipeline_builder->execute();
    push_executor->execute(QueryContext::instance().currentQueryContext()->getSettingsRef()[Setting::max_threads], false);
}

Block LocalExecutor::getHeader()
{
    return header;
}

LocalExecutor::LocalExecutor(QueryPlanPtr query_plan, QueryPipelineBuilderPtr pipeline_builder, bool dump_pipeline_)
    : query_pipeline_builder(std::move(pipeline_builder))
    , header(query_pipeline_builder->getHeader().cloneEmpty())
    , dump_pipeline(dump_pipeline_)
    , ch_column_to_spark_row(std::make_unique<CHColumnToSparkRow>())
    , current_query_plan(std::move(query_plan))
{
    if (current_executor)
        fallback_mode = true;
    // only need record last executor
    current_executor = this;
}
thread_local LocalExecutor * LocalExecutor::current_executor = nullptr;
std::string LocalExecutor::dumpPipeline() const
{
    const DB::Processors * processors_ref = nullptr;
    if (push_executor)
    {
        processors_ref = &(push_executor->getProcessors());
    }
    else
    {
        processors_ref = &(query_pipeline.getProcessors());
    }
    const auto & processors = *processors_ref;
    for (auto & processor : processors)
    {
        WriteBufferFromOwnString buffer;
        auto data_stats = processor->getProcessorDataStats();
        buffer << "(";
        buffer << "\nexecution time: " << processor->getElapsedNs() / 1000U << " us.";
        buffer << "\ninput wait time: " << processor->getInputWaitElapsedNs() / 1000U << " us.";
        buffer << "\noutput wait time: " << processor->getOutputWaitElapsedNs() / 1000U << " us.";
        buffer << "\ninput rows: " << data_stats.input_rows;
        buffer << "\ninput bytes: " << data_stats.input_bytes;
        buffer << "\noutput rows: " << data_stats.output_rows;
        buffer << "\noutput bytes: " << data_stats.output_bytes;
        buffer << ")";
        processor->setDescription(buffer.str());
    }
    WriteBufferFromOwnString out;
    DB::printPipeline(processors, out);
    return out.str();
}
}
