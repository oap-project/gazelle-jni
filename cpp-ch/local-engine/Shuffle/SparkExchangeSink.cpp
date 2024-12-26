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
#include "SparkExchangeSink.h"

#include <Processors/Sinks/NullSink.h>
#include <Processors/Transforms/AggregatingTransform.h>
#include <QueryPipeline/QueryPipelineBuilder.h>
#include <Shuffle/PartitionWriter.h>
#include <Storages/IO/AggregateSerializationUtils.h>
#include <boost/algorithm/string/case_conv.hpp>
#include <jni/CelebornClient.h>
#include <jni/jni_common.h>
#include <Poco/StringTokenizer.h>


namespace DB
{
namespace ErrorCodes
{
extern const int BAD_ARGUMENTS;
}
}

using namespace DB;

namespace local_engine
{
void SparkExchangeSink::consume(Chunk chunk)
{
    Stopwatch wall_time;
    if (chunk.getNumRows() == 0)
        return;
    split_result.total_blocks += 1;
    split_result.total_rows += chunk.getNumRows();
    auto aggregate_info = chunk.getChunkInfos().get<AggregatedChunkInfo>();
    auto input = inputs.front().getHeader().cloneWithColumns(chunk.detachColumns());
    Stopwatch split_time_watch;
    if (!sort_writer)
        input = convertAggregateStateInBlock(input);
    split_result.total_split_time += split_time_watch.elapsedNanoseconds();

    Stopwatch compute_pid_time_watch;
    PartitionInfo partition_info = partitioner->build(input);
    split_result.total_compute_pid_time += compute_pid_time_watch.elapsedNanoseconds();

    Block out_block;
    for (size_t col_i = 0; col_i < output_header.columns(); ++col_i)
    {
        out_block.insert(input.getByPosition(output_columns_indicies[col_i]));
    }
    if (aggregate_info)
    {
        out_block.info.is_overflows = aggregate_info->is_overflows;
        out_block.info.bucket_num = aggregate_info->bucket_num;
    }
    partition_writer->write(partition_info, out_block);
    split_result.wall_time += wall_time.elapsedNanoseconds();
}

void SparkExchangeSink::onFinish()
{
    Stopwatch wall_time;
    if (!dynamic_cast<LocalPartitionWriter *>(partition_writer.get()))
    {
        partition_writer->evictPartitions();
    }
    split_result.wall_time += wall_time.elapsedNanoseconds();
}

void SparkExchangeSink::initOutputHeader(const Block & block)
{
    if (!output_header)
    {
        if (output_columns_indicies.empty())
        {
            output_header = block.cloneEmpty();
            for (size_t i = 0; i < block.columns(); ++i)
                output_columns_indicies.push_back(i);
        }
        else
        {
            ColumnsWithTypeAndName cols;
            for (const auto & index : output_columns_indicies)
                cols.push_back(block.getByPosition(index));

            output_header = Block(std::move(cols));
        }
    }
}

SparkExchangeManager::SparkExchangeManager(const Block& header, const String & short_name, const SplitOptions & options_, jobject rss_pusher): input_header(materializeBlock(header)), options(options_)
{
    if (rss_pusher)
    {
        GET_JNIENV(env)
        jclass celeborn_partition_pusher_class =
            CreateGlobalClassReference(env, "Lorg/apache/spark/shuffle/CelebornPartitionPusher;");
        jmethodID celeborn_push_partition_data_method =
            GetMethodID(env, celeborn_partition_pusher_class, "pushPartitionData", "(I[BI)I");
        CLEAN_JNIENV
        celeborn_client = std::make_unique<CelebornClient>(rss_pusher, celeborn_push_partition_data_method);
        use_rss = true;
    }
    if (!partitioner_creators.contains(short_name))
    {
        throw DB::Exception(DB::ErrorCodes::BAD_ARGUMENTS, "unsupported splitter {}", short_name);
    }
    partitioner_creator = partitioner_creators[short_name];
    Poco::StringTokenizer output_column_tokenizer(options_.out_exprs, ",");
    for (const auto & iter : output_column_tokenizer)
        output_columns_indicies.push_back(std::stoi(iter));
    auto overhead_memory = header.columns() * 16 * options.split_size * options.partition_num;
    use_sort_shuffle = overhead_memory > options.spill_threshold * 0.5 || options.partition_num >= 300 || options.force_memory_sort;

    split_result.partition_lengths.resize(options.partition_num, 0);
    split_result.raw_partition_lengths.resize(options.partition_num, 0);
}

static std::shared_ptr<PartitionWriter> createPartitionWriter(const SplitOptions& options, bool use_sort_shuffle, std::unique_ptr<CelebornClient> celeborn_client)
{
    if (celeborn_client)
    {
        if (use_sort_shuffle)
            return std::make_shared<MemorySortCelebornPartitionWriter>(options, std::move(celeborn_client));
        return std::make_shared<CelebornPartitionWriter>(options, std::move(celeborn_client));
    }
    if (use_sort_shuffle)
        return std::make_shared<MemorySortLocalPartitionWriter>(options);
    return std::make_shared<LocalPartitionWriter>(options);
}

void SparkExchangeManager::initSinks(size_t num)
{
    if (num > 1 && celeborn_client)
    {
        throw Exception(DB::ErrorCodes::BAD_ARGUMENTS, "CelebornClient can't be used with multiple sinks");
    }
    sinks.resize(num);
    partition_writers.resize(num);
    for (size_t i = 0; i < num; ++i)
    {
        partition_writers[i] = createPartitionWriter(options, use_sort_shuffle, std::move(celeborn_client));
        sinks[i] = std::make_shared<SparkExchangeSink>(input_header, partitioner_creator(options), partition_writers[i], output_columns_indicies, use_sort_shuffle);
    }
}

void SparkExchangeManager::setSinksToPipeline(DB::QueryPipelineBuilder & pipeline) const
{
    size_t count = 0;
    DB::Pipe::ProcessorGetterWithStreamKind getter = [&](const Block & header, Pipe::StreamType stream_type) -> ProcessorPtr
    {
        if (stream_type == Pipe::StreamType::Main)
        {
            return std::dynamic_pointer_cast<IProcessor>(sinks[count++]);
        }
        return std::make_shared<NullSink>(header);
    };
    chassert(pipeline.getNumStreams() == sinks.size());
    pipeline.resize(sinks.size());
    pipeline.setSinks(getter);
}

void SparkExchangeManager::pushBlock(const DB::Block & block)
{
    if (sinks.size() != 1)
    {
        throw DB::Exception(ErrorCodes::LOGICAL_ERROR, "only support push block to single sink");
    }

    sinks.front()->consume({block.getColumns(), block.rows()});
}

SelectBuilderPtr SparkExchangeManager::createRoundRobinSelectorBuilder(const SplitOptions & options_)
{
    return std::make_unique<RoundRobinSelectorBuilder>(options_.partition_num);
}

SelectBuilderPtr SparkExchangeManager::createHashSelectorBuilder(const SplitOptions & options_)
{
    Poco::StringTokenizer expr_list(options_.hash_exprs, ",");
    std::vector<size_t> hash_fields;
    for (const auto & expr : expr_list)
    {
        hash_fields.push_back(std::stoi(expr));
    }
    return std::make_unique<HashSelectorBuilder>(options_.partition_num, hash_fields, options_.hash_algorithm);
}

SelectBuilderPtr SparkExchangeManager::createSingleSelectorBuilder(const SplitOptions & options_)
{
    chassert(options_.partition_num == 1);
    return std::make_unique<RoundRobinSelectorBuilder>(options_.partition_num);
}

SelectBuilderPtr SparkExchangeManager::createRangeSelectorBuilder(const SplitOptions & options_)
{
    return std::make_unique<RangeSelectorBuilder>(options_.hash_exprs, options_.partition_num);
}

void SparkExchangeManager::finish()
{
    Stopwatch wall_time;

    mergeSplitResult();
    if (!use_rss)
    {
        auto infos = gatherAllSpillInfo();
        std::vector<Spillable::ExtraData> extra_datas;
        for (const auto & writer : partition_writers)
        {
            if (LocalPartitionWriter * local_partition_writer = dynamic_cast<LocalPartitionWriter *>(writer.get()))
            {
                extra_datas.emplace_back(local_partition_writer->getExtraData());
            }
        }
        if (!extra_datas.empty())
            chassert(extra_datas.size() == partition_writers.size());
        WriteBufferFromFile output(options.data_file, options.io_buffer_size);
        split_result.partition_lengths = mergeSpills(output, infos, extra_datas);
        output.finalize();
    }

    split_result.wall_time += wall_time.elapsedNanoseconds();
}

void checkPartitionLengths(const std::vector<UInt64> & partition_length, size_t partition_num)
{
    if (partition_num != partition_length.size())
    {
        throw Exception(DB::ErrorCodes::LOGICAL_ERROR, "except partition_lengths size is {}, but got {}", partition_num, partition_length.size());
    }
}

void SparkExchangeManager::mergeSplitResult()
{
    if (use_rss)
    {
        this->split_result.partition_lengths.resize(options.partition_num, 0);
        this->split_result.raw_partition_lengths.resize(options.partition_num, 0);
    }
    for (const auto & sink : sinks)
    {
        sink->onFinish();
        auto split_result = sink->getSplitResult();
        this->split_result.total_bytes_written += split_result.total_bytes_written;
        this->split_result.total_bytes_spilled += split_result.total_bytes_spilled;
        this->split_result.total_compress_time += split_result.total_compress_time;
        this->split_result.total_spill_time += split_result.total_spill_time;
        this->split_result.total_write_time += split_result.total_write_time;
        this->split_result.total_compute_pid_time += split_result.total_compute_pid_time;
        this->split_result.total_split_time += split_result.total_split_time;
        this->split_result.total_io_time += split_result.total_io_time;
        this->split_result.total_serialize_time += split_result.total_serialize_time;
        this->split_result.total_rows += split_result.total_rows;
        this->split_result.total_blocks += split_result.total_blocks;
        this->split_result.wall_time += split_result.wall_time;
        if (use_rss)
        {
            checkPartitionLengths(split_result.partition_lengths, options.partition_num);
            checkPartitionLengths(split_result.raw_partition_lengths, options.partition_num);
            for (size_t i = 0; i < options.partition_num; ++i)
            {
                this->split_result.partition_lengths[i] += split_result.partition_lengths[i];
                this->split_result.raw_partition_lengths[i] += split_result.raw_partition_lengths[i];
            }
        }
    }
}

std::vector<SpillInfo> SparkExchangeManager::gatherAllSpillInfo() const
{
    std::vector<SpillInfo> res;
    for (const auto & writer : partition_writers)
    {
        if (Spillable * spillable = dynamic_cast<Spillable *>(writer.get()))
        {
            for (const auto & info : spillable->getSpillInfos())
                res.emplace_back(info);
        }
    }
    return res;
}

std::vector<UInt64> SparkExchangeManager::mergeSpills(DB::WriteBuffer & data_file, const std::vector<SpillInfo>& spill_infos, const std::vector<Spillable::ExtraData> & extra_datas)
{
    if (sinks.empty()) return {};
    auto codec = DB::CompressionCodecFactory::instance().get(boost::to_upper_copy(options.compress_method), options.compress_level);

    CompressedWriteBuffer compressed_output(data_file, codec, options.io_buffer_size);
    NativeWriter writer(compressed_output, sinks.front()->getOutputHeader());

    std::vector<UInt64> partition_length(options.partition_num, 0);

    std::vector<std::shared_ptr<ReadBufferFromFile>> spill_inputs;

    spill_inputs.reserve(spill_infos.size());
    for (const auto & spill : spill_infos)
    {
        // only use readBig
        spill_inputs.emplace_back(std::make_shared<ReadBufferFromFilePRead>(spill.spilled_file, 0));
    }

    Stopwatch write_time_watch;
    Stopwatch io_time_watch;
    Stopwatch serialization_time_watch;
    size_t merge_io_time = 0;
    String buffer;
    for (size_t partition_id = 0; partition_id < options.partition_num; ++partition_id)
    {
        auto size_before = data_file.count();

        io_time_watch.restart();
        for (size_t i = 0; i < spill_infos.size(); ++i)
        {
            if (!spill_infos[i].partition_spill_infos.contains(partition_id))
            {
                continue;
            }
            size_t size = spill_infos[i].partition_spill_infos.at(partition_id).second;
            size_t offset = spill_infos[i].partition_spill_infos.at(partition_id).first;
            if (!size)
            {
                continue;
            }
            buffer.resize(size);
            auto count = spill_inputs[i]->readBigAt(buffer.data(), size, offset, nullptr);

            chassert(count == size);
            data_file.write(buffer.data(), count);
        }
        merge_io_time += io_time_watch.elapsedNanoseconds();

        serialization_time_watch.restart();
        if (!extra_datas.empty())
        {
            for (const auto & extra_data : extra_datas)
            {
                if (!extra_data.partition_block_buffer.empty() && !extra_data.partition_block_buffer[partition_id]->empty())
                {
                    Block block = extra_data.partition_block_buffer[partition_id]->releaseColumns();
                    if (block.rows() > 0)
                        extra_data.partition_buffer[partition_id]->addBlock(std::move(block));
                }
                if (!extra_data.partition_buffer.empty())
                {
                    size_t raw_size = extra_data.partition_buffer[partition_id]->spill(writer);
                    split_result.raw_partition_lengths[partition_id] += raw_size;
                }
            }
        }

        compressed_output.sync();
        partition_length[partition_id] = data_file.count() - size_before;
        split_result.total_serialize_time += serialization_time_watch.elapsedNanoseconds();
        split_result.total_bytes_written += partition_length[partition_id];
    }
    split_result.total_write_time += write_time_watch.elapsedNanoseconds();
    split_result.total_compress_time += compressed_output.getCompressTime();
    split_result.total_io_time += compressed_output.getWriteTime();
    split_result.total_serialize_time = split_result.total_serialize_time
        - split_result.total_io_time - split_result.total_compress_time;
    split_result.total_io_time += merge_io_time;

    for (const auto & spill : spill_infos)
        std::filesystem::remove(spill.spilled_file);
    return partition_length;
}

std::unordered_map<String, SelectBuilderCreator> SparkExchangeManager::partitioner_creators = {
    {"rr", createRoundRobinSelectorBuilder},
    {"hash", createHashSelectorBuilder},
    {"single", createSingleSelectorBuilder},
    {"range", createRangeSelectorBuilder},
};
}
