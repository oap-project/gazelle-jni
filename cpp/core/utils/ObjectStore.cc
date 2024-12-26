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

#include "ObjectStore.h"
#include <glog/logging.h>
#include <iostream>

// static
gluten::ResourceMap<gluten::ObjectStore*>& gluten::ObjectStore::stores() {
  static gluten::ResourceMap<gluten::ObjectStore*> stores;
  return stores;
}

gluten::ObjectStore::~ObjectStore() {
  // destructing in reversed order (the last added object destructed first)
  const std::lock_guard<std::mutex> lock(mtx_);
  for (auto itr = aliveObjects_.rbegin(); itr != aliveObjects_.rend(); itr++) {
    ResourceHandle handle = *itr;
    store_.erase(handle);
  }
  stores().erase(storeId_);
}

gluten::ObjectHandle gluten::ObjectStore::save(std::shared_ptr<void> obj) {
  const std::lock_guard<std::mutex> lock(mtx_);
  ResourceHandle handle = store_.insert(std::move(obj));
  aliveObjects_.insert(handle);
  return toObjHandle(handle);
}

void gluten::ObjectStore::releaseInternal(gluten::ResourceHandle handle) {
  const std::lock_guard<std::mutex> lock(mtx_);
  store_.erase(handle);
  aliveObjects_.erase(handle);
}
