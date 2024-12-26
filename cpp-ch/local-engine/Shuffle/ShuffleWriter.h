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
#include <jni.h>
#include <Storages/IO/NativeWriter.h>

namespace local_engine
{
class ShuffleWriter
{
public:
    ShuffleWriter(
        jobject output_stream, jbyteArray buffer, const std::string & codecStr, jint level, bool enable_compression, size_t customize_buffer_size);
    virtual ~ShuffleWriter();
    void write(const DB::Block & block);
    void flush() const;

private:
    std::unique_ptr<DB::WriteBuffer> compressed_out;
    std::unique_ptr<DB::WriteBuffer> write_buffer;
    std::unique_ptr<NativeWriter> native_writer;
    bool compression_enable;
};
}
