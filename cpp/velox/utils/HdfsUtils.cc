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

#include "HdfsUtils.h"
#include <hdfs/hdfs.h>
#include "config/GlutenConfig.h"
#include "utils/Exception.h"

namespace gluten {

namespace {

struct Credential {
  const std::string userName;
  const std::string allTokens;

  bool operator==(const Credential& rhs) const {
    return userName == rhs.userName && allTokens == rhs.allTokens;
  }
  bool operator!=(const Credential& rhs) const {
    return !(rhs == *this);
  }
};

} // namespace

void updateHdfsTokens(const facebook::velox::config::ConfigBase* veloxCfg) {
  static std::mutex mtx;
  std::lock_guard lock{mtx};

  static std::optional<Credential> activeCredential{std::nullopt};

  const auto& newUserName = veloxCfg->get<std::string>(gluten::kUGIUserName);
  const auto& newAllTokens = veloxCfg->get<std::string>(gluten::kUGITokens);

  if (!newUserName.hasValue() || !newAllTokens.hasValue()) {
    return;
  }

  Credential newCredential{newUserName.value(), newAllTokens.value()};

  if (activeCredential.has_value() && activeCredential.value() == newCredential) {
    // Do nothing if the credential is the same with before.
    return;
  }

  hdfsSetDefautUserName(newCredential.userName.c_str());
  std::vector<folly::StringPiece> tokens;
  folly::split('\0', newCredential.allTokens, tokens);
  for (auto& token : tokens)
    hdfsSetTokenForDefaultUser(token.data());
  activeCredential.emplace(newCredential);
}

} // namespace gluten
