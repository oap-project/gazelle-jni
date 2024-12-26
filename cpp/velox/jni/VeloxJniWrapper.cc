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

#include <jni.h>

#include <glog/logging.h>
#include <jni/JniCommon.h>

#include <exception>
#include "JniUdf.h"
#include "compute/Runtime.h"
#include "compute/VeloxBackend.h"
#include "compute/VeloxRuntime.h"
#include "config/GlutenConfig.h"
#include "jni/JniError.h"
#include "jni/JniFileSystem.h"
#include "memory/VeloxColumnarBatch.h"
#include "memory/VeloxMemoryManager.h"
#include "substrait/SubstraitToVeloxPlanValidator.h"
#include "utils/ObjectStore.h"
#include "utils/VeloxBatchResizer.h"
#include "velox/common/base/BloomFilter.h"
#include "velox/common/file/FileSystems.h"

#include <iostream>

using namespace gluten;
using namespace facebook;

namespace {
jclass blockStripesClass;
jmethodID blockStripesConstructor;
} // namespace

#ifdef __cplusplus
extern "C" {
#endif

jint JNI_OnLoad(JavaVM* vm, void*) {
  JNIEnv* env;
  if (vm->GetEnv(reinterpret_cast<void**>(&env), jniVersion) != JNI_OK) {
    return JNI_ERR;
  }

  getJniCommonState()->ensureInitialized(env);
  getJniErrorState()->ensureInitialized(env);
  initVeloxJniFileSystem(env);
  initVeloxJniUDF(env);

  blockStripesClass =
      createGlobalClassReferenceOrError(env, "Lorg/apache/spark/sql/execution/datasources/BlockStripes;");
  blockStripesConstructor = env->GetMethodID(blockStripesClass, "<init>", "(J[J[II[B)V");

  DLOG(INFO) << "Loaded Velox backend.";

  return jniVersion;
}

void JNI_OnUnload(JavaVM* vm, void*) {
  JNIEnv* env;
  vm->GetEnv(reinterpret_cast<void**>(&env), jniVersion);

  env->DeleteGlobalRef(blockStripesClass);

  finalizeVeloxJniUDF(env);
  finalizeVeloxJniFileSystem(env);
  getJniErrorState()->close();
  getJniCommonState()->close();
  google::ShutdownGoogleLogging();
}

JNIEXPORT void JNICALL Java_org_apache_gluten_init_NativeBackendInitializer_initialize( // NOLINT
    JNIEnv* env,
    jclass,
    jbyteArray conf) {
  JNI_METHOD_START
  auto safeArray = getByteArrayElementsSafe(env, conf);
  auto sparkConf = parseConfMap(env, safeArray.elems(), safeArray.length());
  VeloxBackend::create(sparkConf);
  JNI_METHOD_END()
}

JNIEXPORT void JNICALL Java_org_apache_gluten_init_NativeBackendInitializer_shutdown( // NOLINT
    JNIEnv* env,
    jclass) {
  JNI_METHOD_START
  VeloxBackend::get()->tearDown();
  JNI_METHOD_END()
}

JNIEXPORT void JNICALL Java_org_apache_gluten_udf_UdfJniWrapper_registerFunctionSignatures( // NOLINT
    JNIEnv* env,
    jclass) {
  JNI_METHOD_START
  jniRegisterFunctionSignatures(env);
  JNI_METHOD_END()
}

JNIEXPORT jobject JNICALL
Java_org_apache_gluten_vectorized_PlanEvaluatorJniWrapper_nativeValidateWithFailureReason( // NOLINT
    JNIEnv* env,
    jobject wrapper,
    jbyteArray planArray) {
  JNI_METHOD_START
  auto ctx = getRuntime(env, wrapper);
  auto safeArray = getByteArrayElementsSafe(env, planArray);
  auto planData = safeArray.elems();
  auto planSize = env->GetArrayLength(planArray);
  auto runtime = dynamic_cast<VeloxRuntime*>(ctx);
  if (runtime->debugModeEnabled()) {
    try {
      auto jsonPlan = substraitFromPbToJson("Plan", planData, planSize, std::nullopt);
      LOG(INFO) << std::string(50, '#') << " received substrait::Plan: for validation";
      LOG(INFO) << jsonPlan;
    } catch (const std::exception& e) {
      LOG(WARNING) << "Error converting Substrait plan for validation to JSON: " << e.what();
    }
  }

  ::substrait::Plan subPlan;
  parseProtobuf(planData, planSize, &subPlan);

  // A query context with dummy configs. Used for function validation.
  std::unordered_map<std::string, std::string> configs{
      {velox::core::QueryConfig::kSparkPartitionId, "0"}, {velox::core::QueryConfig::kSessionTimezone, "GMT"}};
  auto queryCtx = velox::core::QueryCtx::create(nullptr, velox::core::QueryConfig(configs));
  auto pool = defaultLeafVeloxMemoryPool().get();
  // An execution context used for function validation.
  velox::core::ExecCtx execCtx(pool, queryCtx.get());

  SubstraitToVeloxPlanValidator planValidator(pool, &execCtx);
  jclass infoCls = env->FindClass("Lorg/apache/gluten/validate/NativePlanValidationInfo;");
  if (infoCls == nullptr) {
    std::string errorMessage = "Unable to CreateGlobalClassReferenceOrError for NativePlanValidationInfo";
    throw GlutenException(errorMessage);
  }
  jmethodID method = env->GetMethodID(infoCls, "<init>", "(ILjava/lang/String;)V");
  try {
    auto isSupported = planValidator.validate(subPlan);
    auto logs = planValidator.getValidateLog();
    std::string concatLog;
    for (int i = 0; i < logs.size(); i++) {
      concatLog += logs[i] + "@";
    }
    return env->NewObject(infoCls, method, isSupported, env->NewStringUTF(concatLog.c_str()));
  } catch (std::invalid_argument& e) {
    LOG(INFO) << "Failed to validate substrait plan because " << e.what();
    // return false;
    auto isSupported = false;
    return env->NewObject(infoCls, method, isSupported, env->NewStringUTF(""));
  }
  JNI_METHOD_END(nullptr)
}

JNIEXPORT jlong JNICALL Java_org_apache_gluten_columnarbatch_VeloxColumnarBatchJniWrapper_from( // NOLINT
    JNIEnv* env,
    jobject wrapper,
    jlong handle) {
  JNI_METHOD_START
  auto ctx = getRuntime(env, wrapper);
  auto runtime = dynamic_cast<VeloxRuntime*>(ctx);

  auto batch = ObjectStore::retrieve<ColumnarBatch>(handle);
  auto newBatch = VeloxColumnarBatch::from(runtime->memoryManager()->getLeafMemoryPool().get(), batch);
  return ctx->saveObject(newBatch);
  JNI_METHOD_END(kInvalidObjectHandle)
}

JNIEXPORT jlong JNICALL Java_org_apache_gluten_columnarbatch_VeloxColumnarBatchJniWrapper_compose( // NOLINT
    JNIEnv* env,
    jobject wrapper,
    jlongArray batchHandles) {
  JNI_METHOD_START
  auto ctx = getRuntime(env, wrapper);
  auto runtime = dynamic_cast<VeloxRuntime*>(ctx);

  int handleCount = env->GetArrayLength(batchHandles);
  auto safeArray = getLongArrayElementsSafe(env, batchHandles);

  std::vector<std::shared_ptr<ColumnarBatch>> batches;
  for (int i = 0; i < handleCount; ++i) {
    int64_t handle = safeArray.elems()[i];
    auto batch = ObjectStore::retrieve<ColumnarBatch>(handle);
    batches.push_back(batch);
  }
  auto newBatch = VeloxColumnarBatch::compose(runtime->memoryManager()->getLeafMemoryPool().get(), std::move(batches));
  return ctx->saveObject(newBatch);
  JNI_METHOD_END(kInvalidObjectHandle)
}

JNIEXPORT jlong JNICALL Java_org_apache_gluten_utils_VeloxBloomFilterJniWrapper_empty( // NOLINT
    JNIEnv* env,
    jobject wrapper,
    jint capacity) {
  JNI_METHOD_START
  auto ctx = getRuntime(env, wrapper);
  auto filter = std::make_shared<velox::BloomFilter<std::allocator<uint64_t>>>();
  filter->reset(capacity);
  GLUTEN_CHECK(filter->isSet(), "Bloom-filter is not initialized");
  return ctx->saveObject(filter);
  JNI_METHOD_END(kInvalidObjectHandle)
}

JNIEXPORT jlong JNICALL Java_org_apache_gluten_utils_VeloxBloomFilterJniWrapper_init( // NOLINT
    JNIEnv* env,
    jobject wrapper,
    jbyteArray data) {
  JNI_METHOD_START
  auto safeArray = getByteArrayElementsSafe(env, data);
  auto ctx = getRuntime(env, wrapper);
  auto filter = std::make_shared<velox::BloomFilter<std::allocator<uint64_t>>>();
  uint8_t* serialized = safeArray.elems();
  filter->merge(reinterpret_cast<char*>(serialized));
  return ctx->saveObject(filter);
  JNI_METHOD_END(kInvalidObjectHandle)
}

JNIEXPORT void JNICALL Java_org_apache_gluten_utils_VeloxBloomFilterJniWrapper_insertLong( // NOLINT
    JNIEnv* env,
    jobject wrapper,
    jlong handle,
    jlong item) {
  JNI_METHOD_START
  auto filter = ObjectStore::retrieve<velox::BloomFilter<std::allocator<uint64_t>>>(handle);
  GLUTEN_CHECK(filter->isSet(), "Bloom-filter is not initialized");
  filter->insert(folly::hasher<int64_t>()(item));
  JNI_METHOD_END()
}

JNIEXPORT jboolean JNICALL Java_org_apache_gluten_utils_VeloxBloomFilterJniWrapper_mightContainLong( // NOLINT
    JNIEnv* env,
    jobject wrapper,
    jlong handle,
    jlong item) {
  JNI_METHOD_START
  auto filter = ObjectStore::retrieve<velox::BloomFilter<std::allocator<uint64_t>>>(handle);
  GLUTEN_CHECK(filter->isSet(), "Bloom-filter is not initialized");
  bool out = filter->mayContain(folly::hasher<int64_t>()(item));
  return out;
  JNI_METHOD_END(false)
}

namespace {
static std::vector<char> serialize(BloomFilter<std::allocator<uint64_t>>* bf) {
  uint32_t size = bf->serializedSize();
  std::vector<char> buffer;
  buffer.reserve(size);
  char* data = buffer.data();
  bf->serialize(data);
  return buffer;
}
} // namespace

JNIEXPORT void JNICALL Java_org_apache_gluten_utils_VeloxBloomFilterJniWrapper_mergeFrom( // NOLINT
    JNIEnv* env,
    jobject wrapper,
    jlong handle,
    jlong other) {
  JNI_METHOD_START
  auto to = ObjectStore::retrieve<velox::BloomFilter<std::allocator<uint64_t>>>(handle);
  auto from = ObjectStore::retrieve<velox::BloomFilter<std::allocator<uint64_t>>>(other);
  GLUTEN_CHECK(to->isSet(), "Bloom-filter is not initialized");
  GLUTEN_CHECK(from->isSet(), "Bloom-filter is not initialized");
  std::vector<char> serialized = serialize(from.get());
  to->merge(serialized.data());
  JNI_METHOD_END()
}

JNIEXPORT jbyteArray JNICALL Java_org_apache_gluten_utils_VeloxBloomFilterJniWrapper_serialize( // NOLINT
    JNIEnv* env,
    jobject wrapper,
    jlong handle) {
  JNI_METHOD_START
  auto filter = ObjectStore::retrieve<velox::BloomFilter<std::allocator<uint64_t>>>(handle);
  GLUTEN_CHECK(filter->isSet(), "Bloom-filter is not initialized");
  std::vector<char> buffer = serialize(filter.get());
  auto size = buffer.capacity();
  jbyteArray out = env->NewByteArray(size);
  env->SetByteArrayRegion(out, 0, size, reinterpret_cast<jbyte*>(buffer.data()));
  return out;
  JNI_METHOD_END(nullptr)
}

JNIEXPORT jlong JNICALL Java_org_apache_gluten_utils_VeloxBatchResizerJniWrapper_create( // NOLINT
    JNIEnv* env,
    jobject wrapper,
    jint minOutputBatchSize,
    jint maxOutputBatchSize,
    jobject jIter) {
  JNI_METHOD_START
  auto ctx = getRuntime(env, wrapper);
  auto pool = dynamic_cast<VeloxMemoryManager*>(ctx->memoryManager())->getLeafMemoryPool();
  auto iter = makeJniColumnarBatchIterator(env, jIter, ctx, nullptr);
  auto appender = std::make_shared<ResultIterator>(
      std::make_unique<VeloxBatchResizer>(pool.get(), minOutputBatchSize, maxOutputBatchSize, std::move(iter)));
  return ctx->saveObject(appender);
  JNI_METHOD_END(kInvalidObjectHandle)
}

JNIEXPORT jboolean JNICALL
Java_org_apache_gluten_utils_VeloxFileSystemValidationJniWrapper_allSupportedByRegisteredFileSystems( // NOLINT
    JNIEnv* env,
    jclass,
    jobjectArray stringArray) {
  JNI_METHOD_START
  int size = env->GetArrayLength(stringArray);
  for (int i = 0; i < size; i++) {
    jstring string = (jstring)(env->GetObjectArrayElement(stringArray, i));
    std::string path = jStringToCString(env, string);
    if (!velox::filesystems::isPathSupportedByRegisteredFileSystems(path)) {
      return false;
    }
  }
  return true;
  JNI_METHOD_END(false)
}

JNIEXPORT jlong JNICALL Java_org_apache_gluten_datasource_VeloxDataSourceJniWrapper_init( // NOLINT
    JNIEnv* env,
    jobject wrapper,
    jstring filePath,
    jlong cSchema,
    jbyteArray options) {
  JNI_METHOD_START
  auto ctx = gluten::getRuntime(env, wrapper);
  auto runtime = dynamic_cast<VeloxRuntime*>(ctx);

  ObjectHandle handle = kInvalidObjectHandle;

  if (cSchema == -1) {
    // Only inspect the schema and not write
    handle = ctx->saveObject(runtime->createDataSource(jStringToCString(env, filePath), nullptr));
  } else {
    auto safeArray = gluten::getByteArrayElementsSafe(env, options);
    auto datasourceOptions = gluten::parseConfMap(env, safeArray.elems(), safeArray.length());
    auto& sparkConf = ctx->getConfMap();
    datasourceOptions.insert(sparkConf.begin(), sparkConf.end());
    auto schema = gluten::arrowGetOrThrow(arrow::ImportSchema(reinterpret_cast<struct ArrowSchema*>(cSchema)));
    handle = ctx->saveObject(runtime->createDataSource(jStringToCString(env, filePath), schema));
    auto datasource = ObjectStore::retrieve<VeloxDataSource>(handle);
    datasource->init(datasourceOptions);
  }

  return handle;
  JNI_METHOD_END(kInvalidObjectHandle)
}

JNIEXPORT void JNICALL Java_org_apache_gluten_datasource_VeloxDataSourceJniWrapper_inspectSchema( // NOLINT
    JNIEnv* env,
    jobject wrapper,
    jlong dsHandle,
    jlong cSchema) {
  JNI_METHOD_START
  auto datasource = ObjectStore::retrieve<VeloxDataSource>(dsHandle);
  datasource->inspectSchema(reinterpret_cast<struct ArrowSchema*>(cSchema));
  JNI_METHOD_END()
}

JNIEXPORT void JNICALL Java_org_apache_gluten_datasource_VeloxDataSourceJniWrapper_close( // NOLINT
    JNIEnv* env,
    jobject wrapper,
    jlong dsHandle) {
  JNI_METHOD_START
  auto datasource = ObjectStore::retrieve<VeloxDataSource>(dsHandle);
  datasource->close();
  ObjectStore::release(dsHandle);
  JNI_METHOD_END()
}

JNIEXPORT void JNICALL Java_org_apache_gluten_datasource_VeloxDataSourceJniWrapper_writeBatch( // NOLINT
    JNIEnv* env,
    jobject wrapper,
    jlong dsHandle,
    jlong batchHandle) {
  JNI_METHOD_START
  auto datasource = ObjectStore::retrieve<VeloxDataSource>(dsHandle);
  auto batch = ObjectStore::retrieve<ColumnarBatch>(batchHandle);
  datasource->write(batch);
  JNI_METHOD_END()
}

JNIEXPORT jobject JNICALL
Java_org_apache_gluten_datasource_VeloxDataSourceJniWrapper_splitBlockByPartitionAndBucket( // NOLINT
    JNIEnv* env,
    jobject wrapper,
    jlong batchHandle,
    jintArray partitionColIndice,
    jboolean hasBucket,
    jlong memoryManagerId) {
  JNI_METHOD_START
  auto ctx = gluten::getRuntime(env, wrapper);
  auto batch = ObjectStore::retrieve<ColumnarBatch>(batchHandle);
  auto safeArray = gluten::getIntArrayElementsSafe(env, partitionColIndice);
  int size = env->GetArrayLength(partitionColIndice);
  std::vector<int32_t> partitionColIndiceVec;
  for (int i = 0; i < size; ++i) {
    partitionColIndiceVec.push_back(safeArray.elems()[i]);
  }

  auto result = batch->toUnsafeRow(0);
  auto rowBytes = result.data();
  auto newBatchHandle = ctx->saveObject(ctx->select(batch, partitionColIndiceVec));

  auto bytesSize = result.size();
  jbyteArray bytesArray = env->NewByteArray(bytesSize);
  env->SetByteArrayRegion(bytesArray, 0, bytesSize, reinterpret_cast<jbyte*>(rowBytes));

  jlongArray batchArray = env->NewLongArray(1);
  long* cBatchArray = new long[1];
  cBatchArray[0] = newBatchHandle;
  env->SetLongArrayRegion(batchArray, 0, 1, cBatchArray);
  delete[] cBatchArray;

  jobject blockStripes = env->NewObject(
      blockStripesClass, blockStripesConstructor, batchHandle, batchArray, nullptr, batch->numColumns(), bytesArray);
  return blockStripes;
  JNI_METHOD_END(nullptr)
}

#ifdef __cplusplus
}
#endif
