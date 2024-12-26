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

#include "FilePathGenerator.h"
#include "JsonToProtoConverter.h"

#include "memory/VeloxMemoryManager.h"
#include "substrait/SubstraitToVeloxPlan.h"
#include "substrait/SubstraitToVeloxPlanValidator.h"
#include "velox/common/base/tests/GTestUtils.h"
#include "velox/dwio/common/tests/utils/DataFiles.h"
#include "velox/exec/tests/utils/AssertQueryBuilder.h"
#include "velox/exec/tests/utils/HiveConnectorTestBase.h"
#include "velox/exec/tests/utils/PlanBuilder.h"
#include "velox/exec/tests/utils/TempDirectoryPath.h"
#include "velox/type/Type.h"

using namespace facebook::velox;
using namespace facebook::velox::test;
using namespace facebook::velox::connector::hive;
using namespace facebook::velox::exec;

namespace gluten {

class Substrait2VeloxPlanValidatorTest : public exec::test::HiveConnectorTestBase {
 protected:
  bool validatePlan(std::string file) {
    std::string subPlanPath = FilePathGenerator::getDataFilePath(file);

    ::substrait::Plan substraitPlan;
    JsonToProtoConverter::readFromFile(subPlanPath, substraitPlan);
    return validatePlan(substraitPlan);
  }

  bool validatePlan(::substrait::Plan& plan) {
    auto queryCtx = core::QueryCtx::create();

    // An execution context used for function validation.
    std::unique_ptr<core::ExecCtx> execCtx = std::make_unique<core::ExecCtx>(pool_.get(), queryCtx.get());

    auto planValidator = std::make_shared<SubstraitToVeloxPlanValidator>(pool_.get(), execCtx.get());
    return planValidator->validate(plan);
  }
};

TEST_F(Substrait2VeloxPlanValidatorTest, group) {
  std::string subPlanPath = FilePathGenerator::getDataFilePath("group.json");

  ::substrait::Plan substraitPlan;
  JsonToProtoConverter::readFromFile(subPlanPath, substraitPlan);

  ASSERT_FALSE(validatePlan(substraitPlan));
}

} // namespace gluten
