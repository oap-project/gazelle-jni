/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#include "velox/expression/VectorFunction.h"

namespace gluten {
std::vector<std::shared_ptr<facebook::velox::exec::FunctionSignature>> makeDecimalSignatures();

std::shared_ptr<facebook::velox::exec::VectorFunction> makeMakeDecimal(
    const std::string& name,
    const std::vector<facebook::velox::exec::VectorFunctionArg>& inputArgs);

std::vector<std::shared_ptr<facebook::velox::exec::FunctionSignature>> checkOverflowSignatures();

std::shared_ptr<facebook::velox::exec::VectorFunction> makeCheckOverflow(
    const std::string& name,
    const std::vector<facebook::velox::exec::VectorFunctionArg>& inputArgs);

std::vector<std::shared_ptr<facebook::velox::exec::FunctionSignature>> unscaledValueSignatures();

std::shared_ptr<facebook::velox::exec::VectorFunction> makeUnscaledValue(
    const std::string& name,
    const std::vector<facebook::velox::exec::VectorFunctionArg>& inputArgs);
} // namespace gluten
