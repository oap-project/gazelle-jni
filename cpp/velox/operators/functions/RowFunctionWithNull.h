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

#include "velox/expression/VectorFunction.h"
#include "velox/type/Type.h"

namespace gluten {

///@tparam allNull If true, set struct as null when all of arguments are all, else will
/// set it null when one of its arguments is null.
template <bool allNull>
class RowFunctionWithNull final : public facebook::velox::exec::VectorFunction {
 public:
  void apply(
      const facebook::velox::SelectivityVector& rows,
      std::vector<facebook::velox::VectorPtr>& args,
      const facebook::velox::TypePtr& outputType,
      facebook::velox::exec::EvalCtx& context,
      facebook::velox::VectorPtr& result) const override {
    auto argsCopy = args;

    facebook::velox::BufferPtr nulls =
        facebook::velox::AlignedBuffer::allocate<char>(facebook::velox::bits::nbytes(rows.size()), context.pool(), 1);
    auto* nullsPtr = nulls->asMutable<uint64_t>();
    auto cntNull = 0;
    rows.applyToSelected([&](facebook::velox::vector_size_t i) {
      facebook::velox::bits::clearNull(nullsPtr, i);
      if (!facebook::velox::bits::isBitNull(nullsPtr, i)) {
        int argsNullCnt = 0;
        for (size_t c = 0; c < argsCopy.size(); c++) {
          auto arg = argsCopy[c].get();
          if (arg->mayHaveNulls() && arg->isNullAt(i)) {
            // For row_constructor_with_null, if any argument of the struct is null,
            // set the struct as null.
            if constexpr (!allNull) {
              facebook::velox::bits::setNull(nullsPtr, i, true);
              cntNull++;
              break;
            } else {
              argsNullCnt++;
            }
          }
        }
        // For row_constructor_with_all_null, set the struct to be null when all arguments are all
        if constexpr (allNull) {
          if (argsNullCnt == argsCopy.size()) {
            facebook::velox::bits::setNull(nullsPtr, i, true);
            cntNull++;
          }
        }
      }
    });

    facebook::velox::RowVectorPtr localResult = std::make_shared<facebook::velox::RowVector>(
        context.pool(), outputType, nulls, rows.size(), std::move(argsCopy), cntNull /*nullCount*/);
    context.moveOrCopyResult(localResult, rows, result);
  }

  static facebook::velox::exec::VectorFunctionMetadata metadata() {
    return facebook::velox::exec::VectorFunctionMetadataBuilder().defaultNullBehavior(false).build();
  }
};

} // namespace gluten
