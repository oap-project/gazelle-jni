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
#include "ChunkBuffer.h"

#include <Columns/IColumn.h>

namespace local_engine
{
void ChunkBuffer::add(DB::Chunk & columns, int start, int end)
{
    if (accumulated_columns.empty())
    {
        auto num_cols = columns.getNumColumns();
        accumulated_columns.reserve(num_cols);
        for (size_t i = 0; i < num_cols; i++)
        {
            accumulated_columns.emplace_back(columns.getColumns()[i]->cloneEmpty());
        }
    }

    for (size_t i = 0; i < columns.getNumColumns(); ++i)
        accumulated_columns[i]->insertRangeFrom(*columns.getColumns()[i], start, end - start);
}
size_t ChunkBuffer::size() const
{
    if (accumulated_columns.empty())
        return 0;
    return accumulated_columns.at(0)->size();
}
DB::Chunk ChunkBuffer::releaseColumns()
{
    auto rows = size();
    DB::Columns res(std::make_move_iterator(accumulated_columns.begin()), std::make_move_iterator(accumulated_columns.end()));
    accumulated_columns.clear();
    return DB::Chunk(res, rows);
}

}
