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

#include "JniCommon.h"

gluten::JniCommonState::~JniCommonState() {
  JNIEnv* env;
  attachCurrentThreadAsDaemonOrThrow(vm_, &env);
  env->DeleteGlobalRef(executionResourceClass_);
}

void gluten::JniCommonState::ensureInitialized(JNIEnv* env) {
  std::lock_guard<std::mutex> lockGuard(mtx_);
  if (initialized_) {
    return;
  }
  initialize(env);
  initialized_ = true;
}

void gluten::JniCommonState::assertInitialized() {
  if (!initialized_) {
    throw gluten::GlutenException("Fatal: JniCommonState::Initialize(...) was not called before using the utility");
  }
}

jclass gluten::JniCommonState::executionResourceClass() {
  assertInitialized();
  return executionResourceClass_;
}

jmethodID gluten::JniCommonState::executionResourceCtxHandle() {
  assertInitialized();
  return executionResourceCtxHandle_;
}

jmethodID gluten::JniCommonState::executionResourceHandle() {
  assertInitialized();
  return executionResourceHandle_;
}

void gluten::JniCommonState::initialize(JNIEnv* env) {
  executionResourceClass_ = createGlobalClassReference(env, "Lio/glutenproject/exec/ExecutionResource;");
  executionResourceCtxHandle_ = getMethodIdOrError(env, executionResourceClass_, "ctxHandle", "()J");
  executionResourceHandle_ = getMethodIdOrError(env, executionResourceClass_, "handle", "()J");
  JavaVM* vm;
  if (env->GetJavaVM(&vm) != JNI_OK) {
    throw gluten::GlutenException("Unable to get JavaVM instance");
  }
  vm_ = vm;
}

gluten::ExecutionResource gluten::getExecutionResource(JNIEnv* env, jobject resourceObj) {
  int64_t ctxHandle = env->CallLongMethod(resourceObj, getJniCommonState()->executionResourceCtxHandle());
  checkException(env);
  auto ctx = reinterpret_cast<ExecutionCtx*>(ctxHandle);
  GLUTEN_CHECK(ctx != nullptr, "FATAL: resource instance should not be null.");
  int64_t handle = env->CallLongMethod(resourceObj, getJniCommonState()->executionResourceHandle());
  checkException(env);
  return ExecutionResource{ctx, handle};
}

std::vector<gluten::ExecutionResource> gluten::getExecutionResources(JNIEnv* env, jobjectArray resourceObjs) {
  std::vector<ExecutionResource> resources;
  jsize count = env->GetArrayLength(resourceObjs);
  for (jsize i = 0; i < count; i++) {
    jobject resource = env->GetObjectArrayElement(resourceObjs, i);
    resources.push_back(getExecutionResource(env, resource));
  }
  return resources;
}
