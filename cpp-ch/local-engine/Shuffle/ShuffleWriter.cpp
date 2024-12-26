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
#include "ShuffleWriter.h"
#include <Compression/CompressedWriteBuffer.h>
#include <Compression/CompressionFactory.h>
#include <Shuffle/WriteBufferFromJavaOutputStream.h>
#include <boost/algorithm/string/case_conv.hpp>

using namespace DB;

namespace local_engine
{
ShuffleWriter::ShuffleWriter(
    jobject output_stream, jbyteArray buffer, const std::string & codecStr, jint level, bool enable_compression, size_t customize_buffer_size)
{
    compression_enable = enable_compression;
    write_buffer = std::make_unique<WriteBufferFromJavaOutputStream>(output_stream, buffer, customize_buffer_size);
    if (compression_enable)
    {
        auto codec = DB::CompressionCodecFactory::instance().get(boost::to_upper_copy(codecStr), level < 0 ? std::nullopt : std::optional<int>(level));
        compressed_out = std::make_unique<CompressedWriteBuffer>(*write_buffer, codec);
    }
}
void ShuffleWriter::write(const Block & block)
{
    if (!native_writer)
    {
        if (compression_enable)
        {
            native_writer = std::make_unique<NativeWriter>(*compressed_out, block.cloneEmpty());
        }
        else
        {
            native_writer = std::make_unique<NativeWriter>(*write_buffer, block.cloneEmpty());
        }
    }
    if (block.rows() > 0)
    {
        native_writer->write(block);
    }
}
void ShuffleWriter::flush() const
{
    if (native_writer)
        native_writer->flush();
}

ShuffleWriter::~ShuffleWriter()
{
    if (native_writer)
        native_writer->flush();

    if (compression_enable)
        compressed_out->finalize();

    write_buffer->finalize();
}
}
