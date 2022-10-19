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

#include "reader.h"
#include "arrow/ipc/reader.h"

#include <utility>

namespace gluten {
namespace shuffle {

ReaderOptions ReaderOptions::Defaults() {
  return {};
}

Reader::Reader(
    std::shared_ptr<arrow::io::InputStream> in,
    std::shared_ptr<arrow::Schema> schema,
    gluten::shuffle::ReaderOptions options)
    : in_(std::move(in)),
      schema_(std::move(schema)),
      options_(std::move(options)) {
  GLUTEN_ASSIGN_OR_THROW(first_message_, arrow::ipc::ReadMessage(in_.get()))
  if (first_message_->type() == arrow::ipc::MessageType::SCHEMA) {
    GLUTEN_ASSIGN_OR_THROW(
        schema_, arrow::ipc::ReadSchema(*first_message_, nullptr))
    first_message_consumed_ = true;
  }
}

arrow::Result<std::shared_ptr<gluten::memory::GlutenColumnarBatch>>
Reader::Next() {
  std::shared_ptr<arrow::RecordBatch> arrow_batch;
  if (!first_message_consumed_) {
    GLUTEN_ASSIGN_OR_THROW(
        arrow_batch,
        arrow::ipc::ReadRecordBatch(
            *first_message_, schema_, nullptr, options_.ipc_read_options))
    first_message_consumed_ = true;
  } else {
    GLUTEN_ASSIGN_OR_THROW(
        arrow_batch,
        arrow::ipc::ReadRecordBatch(
            schema_, nullptr, options_.ipc_read_options, in_.get()))
  }
  if (arrow_batch == nullptr) {
    return nullptr;
  }
  std::shared_ptr<gluten::memory::GlutenColumnarBatch> gluten_batch =
      std::make_shared<gluten::memory::GlutenArrowColumnarBatch>(arrow_batch);
  return gluten_batch;
}

arrow::Status Reader::Close() {}
} // namespace shuffle
} // namespace gluten