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

#include "RowConstructorWithNull.h"
#include "velox/expression/VectorFunction.h"

namespace gluten {

facebook::velox::TypePtr RowConstructorWithNullCallToSpecialForm::resolveType(
    const std::vector<facebook::velox::TypePtr>& argTypes) {
  auto numInput = argTypes.size();
  std::vector<std::string> names(numInput);
  std::vector<facebook::velox::TypePtr> types(numInput);
  for (auto i = 0; i < numInput; i++) {
    types[i] = argTypes[i];
    names[i] = fmt::format("c{}", i + 1);
  }
  return facebook::velox::ROW(std::move(names), std::move(types));
}

facebook::velox::exec::ExprPtr RowConstructorWithNullCallToSpecialForm::constructSpecialForm(
    const facebook::velox::TypePtr& type,
    std::vector<facebook::velox::exec::ExprPtr>&& compiledChildren,
    bool trackCpuUsage,
    const facebook::velox::core::QueryConfig& config) {
  auto name = this->rowFunctionName;
  auto [function, metadata] = facebook::velox::exec::vectorFunctionFactories().withRLock(
      [&config, &name](auto& functionMap) -> std::pair<
                                              std::shared_ptr<facebook::velox::exec::VectorFunction>,
                                              facebook::velox::exec::VectorFunctionMetadata> {
        auto functionIterator = functionMap.find(name);
        if (functionIterator != functionMap.end()) {
          return {functionIterator->second.factory(name, {}, config), functionIterator->second.metadata};
        } else {
          VELOX_FAIL("Function {} is not registered.", name);
        }
      });

  return std::make_shared<facebook::velox::exec::Expr>(
      type, std::move(compiledChildren), function, metadata, name, trackCpuUsage);
}

} // namespace gluten
