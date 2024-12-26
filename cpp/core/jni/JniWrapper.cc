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
#include <algorithm>
#include <cstdint>
#include <filesystem>

#include "compute/Runtime.h"
#include "config/GlutenConfig.h"
#include "jni/JniCommon.h"
#include "jni/JniError.h"

#include <arrow/c/bridge.h>
#include <optional>
#include <string>
#include "memory/AllocationListener.h"
#include "operators/serializer/ColumnarBatchSerializer.h"
#include "shuffle/LocalPartitionWriter.h"
#include "shuffle/Partitioning.h"
#include "shuffle/ShuffleReader.h"
#include "shuffle/ShuffleWriter.h"
#include "shuffle/Utils.h"
#include "shuffle/rss/RssPartitionWriter.h"
#include "utils/ArrowStatus.h"
#include "utils/StringUtil.h"

using namespace gluten;

namespace {
jclass javaReservationListenerClass;

jmethodID reserveMemoryMethod;
jmethodID unreserveMemoryMethod;

jclass byteArrayClass;

jclass jniByteInputStreamClass;
jmethodID jniByteInputStreamRead;
jmethodID jniByteInputStreamTell;
jmethodID jniByteInputStreamClose;

jclass splitResultClass;
jmethodID splitResultConstructor;

jclass columnarBatchSerializeResultClass;
jmethodID columnarBatchSerializeResultConstructor;

jclass metricsBuilderClass;
jmethodID metricsBuilderConstructor;
jclass nativeColumnarToRowInfoClass;
jmethodID nativeColumnarToRowInfoConstructor;

jclass shuffleReaderMetricsClass;
jmethodID shuffleReaderMetricsSetDecompressTime;
jmethodID shuffleReaderMetricsSetDeserializeTime;

class JavaInputStreamAdaptor final : public arrow::io::InputStream {
 public:
  JavaInputStreamAdaptor(JNIEnv* env, arrow::MemoryPool* pool, jobject jniIn) : pool_(pool) {
    // IMPORTANT: DO NOT USE LOCAL REF IN DIFFERENT THREAD
    if (env->GetJavaVM(&vm_) != JNI_OK) {
      std::string errorMessage = "Unable to get JavaVM instance";
      throw GlutenException(errorMessage);
    }
    jniIn_ = env->NewGlobalRef(jniIn);
  }

  ~JavaInputStreamAdaptor() override {
    try {
      auto status = JavaInputStreamAdaptor::Close();
      if (!status.ok()) {
        LOG(WARNING) << __func__ << " call JavaInputStreamAdaptor::Close() failed, status:" << status.ToString();
      }
    } catch (std::exception& e) {
      LOG(WARNING) << __func__ << " call JavaInputStreamAdaptor::Close() got exception:" << e.what();
    }
  }

  // not thread safe
  arrow::Status Close() override {
    if (closed_) {
      return arrow::Status::OK();
    }
    JNIEnv* env;
    attachCurrentThreadAsDaemonOrThrow(vm_, &env);
    env->CallVoidMethod(jniIn_, jniByteInputStreamClose);
    checkException(env);
    env->DeleteGlobalRef(jniIn_);
    vm_->DetachCurrentThread();
    closed_ = true;
    return arrow::Status::OK();
  }

  arrow::Result<int64_t> Tell() const override {
    JNIEnv* env;
    attachCurrentThreadAsDaemonOrThrow(vm_, &env);
    jlong told = env->CallLongMethod(jniIn_, jniByteInputStreamTell);
    checkException(env);
    return told;
  }

  bool closed() const override {
    return closed_;
  }

  arrow::Result<int64_t> Read(int64_t nbytes, void* out) override {
    JNIEnv* env;
    attachCurrentThreadAsDaemonOrThrow(vm_, &env);
    jlong read = env->CallLongMethod(jniIn_, jniByteInputStreamRead, reinterpret_cast<jlong>(out), nbytes);
    checkException(env);
    return read;
  }

  arrow::Result<std::shared_ptr<arrow::Buffer>> Read(int64_t nbytes) override {
    GLUTEN_ASSIGN_OR_THROW(auto buffer, arrow::AllocateResizableBuffer(nbytes, pool_))
    GLUTEN_ASSIGN_OR_THROW(int64_t bytes_read, Read(nbytes, buffer->mutable_data()))
    GLUTEN_THROW_NOT_OK(buffer->Resize(bytes_read, false));
    buffer->ZeroPadding();
    return std::move(buffer);
  }

 private:
  arrow::MemoryPool* pool_;
  JavaVM* vm_;
  jobject jniIn_;
  bool closed_ = false;
};

/// Internal backend consists of empty implementations of Runtime API and MemoryManager API.
/// The backend is used for saving contextual objects only.
///
/// It's also possible to extend the implementation for handling Arrow-based requests either in the future.
inline static const std::string kInternalBackendKind{"internal"};

class InternalMemoryManager : public MemoryManager {
 public:
  InternalMemoryManager(const std::string& kind) : MemoryManager(kind) {}

  arrow::MemoryPool* getArrowMemoryPool() override {
    throw GlutenException("Not implemented");
  }

  const MemoryUsageStats collectMemoryUsageStats() const override {
    return MemoryUsageStats();
  }

  const int64_t shrink(int64_t size) override {
    return 0;
  }

  void hold() override {}
};

class InternalRuntime : public Runtime {
 public:
  InternalRuntime(
      const std::string& kind,
      MemoryManager* memoryManager,
      const std::unordered_map<std::string, std::string>& confMap)
      : Runtime(kind, memoryManager, confMap) {}
};

MemoryManager* internalMemoryManagerFactory(const std::string& kind, std::unique_ptr<AllocationListener> listener) {
  return new InternalMemoryManager(kind);
}

void internalMemoryManagerReleaser(MemoryManager* memoryManager) {
  delete memoryManager;
}

Runtime* internalRuntimeFactory(
    const std::string& kind,
    MemoryManager* memoryManager,
    const std::unordered_map<std::string, std::string>& sessionConf) {
  return new InternalRuntime(kind, memoryManager, sessionConf);
}

void internalRuntimeReleaser(Runtime* runtime) {
  delete runtime;
}

} // namespace

#ifdef __cplusplus
extern "C" {
#endif

jint JNI_OnLoad(JavaVM* vm, void* reserved) {
  JNIEnv* env;
  if (vm->GetEnv(reinterpret_cast<void**>(&env), jniVersion) != JNI_OK) {
    return JNI_ERR;
  }
  getJniCommonState()->ensureInitialized(env);
  getJniErrorState()->ensureInitialized(env);

  MemoryManager::registerFactory(kInternalBackendKind, internalMemoryManagerFactory, internalMemoryManagerReleaser);
  Runtime::registerFactory(kInternalBackendKind, internalRuntimeFactory, internalRuntimeReleaser);

  byteArrayClass = createGlobalClassReferenceOrError(env, "[B");

  jniByteInputStreamClass = createGlobalClassReferenceOrError(env, "Lorg/apache/gluten/vectorized/JniByteInputStream;");
  jniByteInputStreamRead = getMethodIdOrError(env, jniByteInputStreamClass, "read", "(JJ)J");
  jniByteInputStreamTell = getMethodIdOrError(env, jniByteInputStreamClass, "tell", "()J");
  jniByteInputStreamClose = getMethodIdOrError(env, jniByteInputStreamClass, "close", "()V");

  splitResultClass = createGlobalClassReferenceOrError(env, "Lorg/apache/gluten/vectorized/GlutenSplitResult;");
  splitResultConstructor = getMethodIdOrError(env, splitResultClass, "<init>", "(JJJJJJJJJJ[J[J)V");

  columnarBatchSerializeResultClass =
      createGlobalClassReferenceOrError(env, "Lorg/apache/gluten/vectorized/ColumnarBatchSerializeResult;");
  columnarBatchSerializeResultConstructor =
      getMethodIdOrError(env, columnarBatchSerializeResultClass, "<init>", "(J[B)V");

  metricsBuilderClass = createGlobalClassReferenceOrError(env, "Lorg/apache/gluten/metrics/Metrics;");

  metricsBuilderConstructor = getMethodIdOrError(
      env,
      metricsBuilderClass,
      "<init>",
      "([J[J[J[J[J[J[J[J[J[JJ[J[J[J[J[J[J[J[J[J[J[J[J[J[J[J[J[J[J[J[J[J[J[J[J[J[J)V");

  nativeColumnarToRowInfoClass =
      createGlobalClassReferenceOrError(env, "Lorg/apache/gluten/vectorized/NativeColumnarToRowInfo;");
  nativeColumnarToRowInfoConstructor = getMethodIdOrError(env, nativeColumnarToRowInfoClass, "<init>", "([I[IJ)V");

  javaReservationListenerClass = createGlobalClassReference(
      env,
      "Lorg/apache/gluten/memory/listener/"
      "ReservationListener;");

  reserveMemoryMethod = getMethodIdOrError(env, javaReservationListenerClass, "reserve", "(J)J");
  unreserveMemoryMethod = getMethodIdOrError(env, javaReservationListenerClass, "unreserve", "(J)J");

  shuffleReaderMetricsClass =
      createGlobalClassReferenceOrError(env, "Lorg/apache/gluten/vectorized/ShuffleReaderMetrics;");
  shuffleReaderMetricsSetDecompressTime =
      getMethodIdOrError(env, shuffleReaderMetricsClass, "setDecompressTime", "(J)V");
  shuffleReaderMetricsSetDeserializeTime =
      getMethodIdOrError(env, shuffleReaderMetricsClass, "setDeserializeTime", "(J)V");

  return jniVersion;
}

void JNI_OnUnload(JavaVM* vm, void* reserved) {
  JNIEnv* env;
  vm->GetEnv(reinterpret_cast<void**>(&env), jniVersion);
  env->DeleteGlobalRef(jniByteInputStreamClass);
  env->DeleteGlobalRef(splitResultClass);
  env->DeleteGlobalRef(columnarBatchSerializeResultClass);
  env->DeleteGlobalRef(nativeColumnarToRowInfoClass);
  env->DeleteGlobalRef(byteArrayClass);
  env->DeleteGlobalRef(shuffleReaderMetricsClass);

  getJniErrorState()->close();
  getJniCommonState()->close();
}

JNIEXPORT jlong JNICALL Java_org_apache_gluten_runtime_RuntimeJniWrapper_createRuntime( // NOLINT
    JNIEnv* env,
    jclass,
    jstring jBackendType,
    jlong nmmHandle,
    jbyteArray sessionConf) {
  JNI_METHOD_START
  MemoryManager* memoryManager = jniCastOrThrow<MemoryManager>(nmmHandle);
  auto safeArray = getByteArrayElementsSafe(env, sessionConf);
  auto sparkConf = parseConfMap(env, safeArray.elems(), safeArray.length());
  auto backendType = jStringToCString(env, jBackendType);

  auto runtime = Runtime::create(backendType, memoryManager, sparkConf);
  return reinterpret_cast<jlong>(runtime);
  JNI_METHOD_END(kInvalidObjectHandle)
}

JNIEXPORT void JNICALL Java_org_apache_gluten_runtime_RuntimeJniWrapper_releaseRuntime( // NOLINT
    JNIEnv* env,
    jclass,
    jlong ctxHandle) {
  JNI_METHOD_START
  auto runtime = jniCastOrThrow<Runtime>(ctxHandle);

  Runtime::release(runtime);
  JNI_METHOD_END()
}

namespace {
const std::string kBacktraceAllocation = "spark.gluten.memory.backtrace.allocation";
}

JNIEXPORT jlong JNICALL Java_org_apache_gluten_memory_NativeMemoryManagerJniWrapper_create( // NOLINT
    JNIEnv* env,
    jclass,
    jstring jBackendType,
    jobject jListener,
    jbyteArray sessionConf) {
  JNI_METHOD_START
  JavaVM* vm;
  if (env->GetJavaVM(&vm) != JNI_OK) {
    throw GlutenException("Unable to get JavaVM instance");
  }
  auto backendType = jStringToCString(env, jBackendType);
  auto safeArray = getByteArrayElementsSafe(env, sessionConf);
  auto sparkConf = parseConfMap(env, safeArray.elems(), safeArray.length());
  std::unique_ptr<AllocationListener> listener =
      std::make_unique<SparkAllocationListener>(vm, jListener, reserveMemoryMethod, unreserveMemoryMethod);
  bool backtrace = sparkConf.at(kBacktraceAllocation) == "true";
  if (backtrace) {
    listener = std::make_unique<BacktraceAllocationListener>(std::move(listener));
  }
  MemoryManager* mm = MemoryManager::create(backendType, std::move(listener));
  return reinterpret_cast<jlong>(mm);
  JNI_METHOD_END(-1L)
}

JNIEXPORT jbyteArray JNICALL Java_org_apache_gluten_memory_NativeMemoryManagerJniWrapper_collectUsage( // NOLINT
    JNIEnv* env,
    jclass,
    jlong nmmHandle) {
  JNI_METHOD_START
  auto* memoryManager = jniCastOrThrow<MemoryManager>(nmmHandle);

  const MemoryUsageStats& stats = memoryManager->collectMemoryUsageStats();
  auto size = stats.ByteSizeLong();
  jbyteArray out = env->NewByteArray(size);
  std::vector<uint8_t> buffer(size);
  GLUTEN_CHECK(
      stats.SerializeToArray(reinterpret_cast<void*>(buffer.data()), size),
      "Serialization failed when collecting memory usage stats");
  env->SetByteArrayRegion(out, 0, size, reinterpret_cast<jbyte*>(buffer.data()));
  return out;
  JNI_METHOD_END(nullptr)
}

JNIEXPORT jlong JNICALL Java_org_apache_gluten_memory_NativeMemoryManagerJniWrapper_shrink( // NOLINT
    JNIEnv* env,
    jclass,
    jlong nmmHandle,
    jlong size) {
  JNI_METHOD_START
  auto* memoryManager = jniCastOrThrow<MemoryManager>(nmmHandle);
  return memoryManager->shrink(static_cast<int64_t>(size));
  JNI_METHOD_END(kInvalidObjectHandle)
}

JNIEXPORT void JNICALL Java_org_apache_gluten_memory_NativeMemoryManagerJniWrapper_hold( // NOLINT
    JNIEnv* env,
    jclass,
    jlong nmmHandle) {
  JNI_METHOD_START
  auto* memoryManager = jniCastOrThrow<MemoryManager>(nmmHandle);
  memoryManager->hold();
  JNI_METHOD_END()
}

JNIEXPORT void JNICALL Java_org_apache_gluten_memory_NativeMemoryManagerJniWrapper_release( // NOLINT
    JNIEnv* env,
    jclass,
    jlong nmmHandle) {
  JNI_METHOD_START
  auto* memoryManager = jniCastOrThrow<MemoryManager>(nmmHandle);
  MemoryManager::release(memoryManager);
  JNI_METHOD_END()
}

JNIEXPORT jstring JNICALL Java_org_apache_gluten_vectorized_PlanEvaluatorJniWrapper_nativePlanString( // NOLINT
    JNIEnv* env,
    jobject wrapper,
    jbyteArray planArray,
    jboolean details) {
  JNI_METHOD_START

  auto safeArray = getByteArrayElementsSafe(env, planArray);
  auto planData = safeArray.elems();
  auto planSize = env->GetArrayLength(planArray);
  auto ctx = getRuntime(env, wrapper);
  ctx->parsePlan(planData, planSize, std::nullopt);
  auto& conf = ctx->getConfMap();
  auto planString = ctx->planString(details, conf);
  return env->NewStringUTF(planString.c_str());

  JNI_METHOD_END(nullptr)
}

JNIEXPORT void JNICALL Java_org_apache_gluten_vectorized_PlanEvaluatorJniWrapper_injectWriteFilesTempPath( // NOLINT
    JNIEnv* env,
    jclass,
    jbyteArray path) {
  JNI_METHOD_START
  auto len = env->GetArrayLength(path);
  auto safeArray = getByteArrayElementsSafe(env, path);
  std::string pathStr(reinterpret_cast<char*>(safeArray.elems()), len);
  *Runtime::localWriteFilesTempPath() = pathStr;
  JNI_METHOD_END()
}

JNIEXPORT jlong JNICALL
Java_org_apache_gluten_vectorized_PlanEvaluatorJniWrapper_nativeCreateKernelWithIterator( // NOLINT
    JNIEnv* env,
    jobject wrapper,
    jbyteArray planArr,
    jobjectArray splitInfosArr,
    jobjectArray iterArr,
    jint stageId,
    jint partitionId,
    jlong taskId,
    jboolean saveInput,
    jstring spillDir) {
  JNI_METHOD_START

  auto ctx = getRuntime(env, wrapper);
  auto& conf = ctx->getConfMap();

  ctx->setSparkTaskInfo({stageId, partitionId, taskId});

  std::string saveDir{};
  std::string fileIdentifier = "_" + std::to_string(stageId) + "_" + std::to_string(partitionId);
  if (saveInput) {
    if (conf.find(kGlutenSaveDir) == conf.end()) {
      throw GlutenException(kGlutenSaveDir + " is not configured.");
    }
    saveDir = conf.at(kGlutenSaveDir);
    std::filesystem::path f{saveDir};
    if (std::filesystem::exists(f)) {
      if (!std::filesystem::is_directory(f)) {
        throw GlutenException("Invalid path for " + kGlutenSaveDir + ": " + saveDir);
      }
    } else {
      std::error_code ec;
      std::filesystem::create_directory(f, ec);
      if (ec) {
        throw GlutenException("Failed to create directory: " + saveDir + ", error message: " + ec.message());
      }
    }
    ctx->dumpConf(saveDir + "/conf" + fileIdentifier + ".ini");
  }

  auto spillDirStr = jStringToCString(env, spillDir);

  auto safePlanArray = getByteArrayElementsSafe(env, planArr);
  auto planSize = env->GetArrayLength(planArr);
  ctx->parsePlan(
      safePlanArray.elems(),
      planSize,
      saveInput ? std::optional<std::string>(saveDir + "/plan" + fileIdentifier + ".json") : std::nullopt);

  for (jsize i = 0, splitInfoArraySize = env->GetArrayLength(splitInfosArr); i < splitInfoArraySize; i++) {
    jbyteArray splitInfoArray = static_cast<jbyteArray>(env->GetObjectArrayElement(splitInfosArr, i));
    jsize splitInfoSize = env->GetArrayLength(splitInfoArray);
    auto safeSplitArray = getByteArrayElementsSafe(env, splitInfoArray);
    auto splitInfoData = safeSplitArray.elems();
    ctx->parseSplitInfo(
        splitInfoData,
        splitInfoSize,
        saveInput ? std::optional<std::string>(saveDir + "/split" + fileIdentifier + "_" + std::to_string(i) + ".json")
                  : std::nullopt);
  }

  // Handle the Java iters
  jsize itersLen = env->GetArrayLength(iterArr);
  std::vector<std::shared_ptr<ResultIterator>> inputIters;
  for (int idx = 0; idx < itersLen; idx++) {
    std::shared_ptr<ArrowWriter> writer = nullptr;
    if (saveInput) {
      auto file = saveDir + "/data" + fileIdentifier + "_" + std::to_string(idx) + ".parquet";
      writer = ctx->createArrowWriter(file);
    }
    jobject iter = env->GetObjectArrayElement(iterArr, idx);
    auto arrayIter = makeJniColumnarBatchIterator(env, iter, ctx, writer);
    auto resultIter = std::make_shared<ResultIterator>(std::move(arrayIter));
    inputIters.push_back(std::move(resultIter));
  }

  return ctx->saveObject(ctx->createResultIterator(spillDirStr, inputIters, conf));
  JNI_METHOD_END(kInvalidObjectHandle)
}

JNIEXPORT jboolean JNICALL Java_org_apache_gluten_vectorized_ColumnarBatchOutIterator_nativeHasNext( // NOLINT
    JNIEnv* env,
    jobject wrapper,
    jlong iterHandle) {
  JNI_METHOD_START
  auto iter = ObjectStore::retrieve<ResultIterator>(iterHandle);
  if (iter == nullptr) {
    std::string errorMessage =
        "When hasNext() is called on a closed iterator, an exception is thrown. To prevent this, consider using the protectInvocationFlow() method when creating the iterator in scala side. This will allow the hasNext() method to be called multiple times without issue.";
    throw GlutenException(errorMessage);
  }
  return iter->hasNext();
  JNI_METHOD_END(false)
}

JNIEXPORT jlong JNICALL Java_org_apache_gluten_vectorized_ColumnarBatchOutIterator_nativeNext( // NOLINT
    JNIEnv* env,
    jobject wrapper,
    jlong iterHandle) {
  JNI_METHOD_START
  auto ctx = getRuntime(env, wrapper);

  auto iter = ObjectStore::retrieve<ResultIterator>(iterHandle);
  if (!iter->hasNext()) {
    return kInvalidObjectHandle;
  }

  std::shared_ptr<ColumnarBatch> batch = iter->next();
  auto batchHandle = ctx->saveObject(batch);

  iter->setExportNanos(batch->getExportNanos());
  return batchHandle;
  JNI_METHOD_END(kInvalidObjectHandle)
}

JNIEXPORT jobject JNICALL Java_org_apache_gluten_metrics_IteratorMetricsJniWrapper_nativeFetchMetrics( // NOLINT
    JNIEnv* env,
    jobject wrapper,
    jlong iterHandle) {
  JNI_METHOD_START
  auto iter = ObjectStore::retrieve<ResultIterator>(iterHandle);
  auto metrics = iter->getMetrics();
  unsigned int numMetrics = 0;
  if (metrics) {
    numMetrics = metrics->numMetrics;
  }

  jlongArray longArray[Metrics::kNum];
  for (auto i = static_cast<int>(Metrics::kBegin); i != static_cast<int>(Metrics::kEnd); ++i) {
    longArray[i] = env->NewLongArray(numMetrics);
    if (metrics) {
      env->SetLongArrayRegion(longArray[i], 0, numMetrics, metrics->get((Metrics::TYPE)i));
    }
  }

  return env->NewObject(
      metricsBuilderClass,
      metricsBuilderConstructor,
      longArray[Metrics::kInputRows],
      longArray[Metrics::kInputVectors],
      longArray[Metrics::kInputBytes],
      longArray[Metrics::kRawInputRows],
      longArray[Metrics::kRawInputBytes],
      longArray[Metrics::kOutputRows],
      longArray[Metrics::kOutputVectors],
      longArray[Metrics::kOutputBytes],
      longArray[Metrics::kCpuCount],
      longArray[Metrics::kWallNanos],
      metrics ? metrics->veloxToArrow : -1,
      longArray[Metrics::kPeakMemoryBytes],
      longArray[Metrics::kNumMemoryAllocations],
      longArray[Metrics::kSpilledInputBytes],
      longArray[Metrics::kSpilledBytes],
      longArray[Metrics::kSpilledRows],
      longArray[Metrics::kSpilledPartitions],
      longArray[Metrics::kSpilledFiles],
      longArray[Metrics::kNumDynamicFiltersProduced],
      longArray[Metrics::kNumDynamicFiltersAccepted],
      longArray[Metrics::kNumReplacedWithDynamicFilterRows],
      longArray[Metrics::kFlushRowCount],
      longArray[Metrics::kLoadedToValueHook],
      longArray[Metrics::kScanTime],
      longArray[Metrics::kSkippedSplits],
      longArray[Metrics::kProcessedSplits],
      longArray[Metrics::kSkippedStrides],
      longArray[Metrics::kProcessedStrides],
      longArray[Metrics::kRemainingFilterTime],
      longArray[Metrics::kIoWaitTime],
      longArray[Metrics::kStorageReadBytes],
      longArray[Metrics::kLocalReadBytes],
      longArray[Metrics::kRamReadBytes],
      longArray[Metrics::kPreloadSplits],
      longArray[Metrics::kPhysicalWrittenBytes],
      longArray[Metrics::kWriteIOTime],
      longArray[Metrics::kNumWrittenFiles]);

  JNI_METHOD_END(nullptr)
}

JNIEXPORT jlong JNICALL Java_org_apache_gluten_vectorized_ColumnarBatchOutIterator_nativeSpill( // NOLINT
    JNIEnv* env,
    jobject wrapper,
    jlong iterHandle,
    jlong size) {
  JNI_METHOD_START
  auto it = ObjectStore::retrieve<ResultIterator>(iterHandle);
  if (it == nullptr) {
    std::string errorMessage = "Invalid result iter handle " + std::to_string(iterHandle);
    throw GlutenException(errorMessage);
  }
  return it->spillFixedSize(size);
  JNI_METHOD_END(kInvalidObjectHandle)
}

JNIEXPORT void JNICALL Java_org_apache_gluten_vectorized_ColumnarBatchOutIterator_nativeClose( // NOLINT
    JNIEnv* env,
    jobject wrapper,
    jlong iterHandle) {
  JNI_METHOD_START
  ObjectStore::release(iterHandle);
  JNI_METHOD_END()
}

JNIEXPORT jlong JNICALL
Java_org_apache_gluten_vectorized_NativeColumnarToRowJniWrapper_nativeColumnarToRowInit( // NOLINT
    JNIEnv* env,
    jobject wrapper) {
  JNI_METHOD_START
  auto ctx = getRuntime(env, wrapper);

  auto& conf = ctx->getConfMap();
  int64_t column2RowMemThreshold;
  auto it = conf.find(kColumnarToRowMemoryThreshold);
  GLUTEN_CHECK(!(it == conf.end()), "Required key not found in runtime config: " + kColumnarToRowMemoryThreshold);
  column2RowMemThreshold = std::stoll(it->second);
  // Convert the native batch to Spark unsafe row.
  return ctx->saveObject(ctx->createColumnar2RowConverter(column2RowMemThreshold));
  JNI_METHOD_END(kInvalidObjectHandle)
}

JNIEXPORT jobject JNICALL
Java_org_apache_gluten_vectorized_NativeColumnarToRowJniWrapper_nativeColumnarToRowConvert( // NOLINT
    JNIEnv* env,
    jobject wrapper,
    jlong c2rHandle,
    jlong batchHandle,
    jlong startRow) {
  JNI_METHOD_START
  auto columnarToRowConverter = ObjectStore::retrieve<ColumnarToRowConverter>(c2rHandle);
  auto cb = ObjectStore::retrieve<ColumnarBatch>(batchHandle);

  columnarToRowConverter->convert(cb, startRow);

  const auto& offsets = columnarToRowConverter->getOffsets();
  const auto& lengths = columnarToRowConverter->getLengths();

  auto numRows = columnarToRowConverter->numRows();

  auto offsetsArr = env->NewIntArray(numRows);
  auto offsetsSrc = reinterpret_cast<const jint*>(offsets.data());
  env->SetIntArrayRegion(offsetsArr, 0, numRows, offsetsSrc);
  auto lengthsArr = env->NewIntArray(numRows);
  auto lengthsSrc = reinterpret_cast<const jint*>(lengths.data());
  env->SetIntArrayRegion(lengthsArr, 0, numRows, lengthsSrc);
  long address = reinterpret_cast<long>(columnarToRowConverter->getBufferAddress());

  jobject nativeColumnarToRowInfo =
      env->NewObject(nativeColumnarToRowInfoClass, nativeColumnarToRowInfoConstructor, offsetsArr, lengthsArr, address);
  return nativeColumnarToRowInfo;
  JNI_METHOD_END(nullptr)
}

JNIEXPORT void JNICALL Java_org_apache_gluten_vectorized_NativeColumnarToRowJniWrapper_nativeClose( // NOLINT
    JNIEnv* env,
    jobject wrapper,
    jlong c2rHandle) {
  JNI_METHOD_START
  ObjectStore::release(c2rHandle);
  JNI_METHOD_END()
}

JNIEXPORT jlong JNICALL Java_org_apache_gluten_vectorized_NativeRowToColumnarJniWrapper_init( // NOLINT
    JNIEnv* env,
    jobject wrapper,
    jlong cSchema) {
  JNI_METHOD_START
  auto ctx = getRuntime(env, wrapper);

  return ctx->saveObject(ctx->createRow2ColumnarConverter(reinterpret_cast<struct ArrowSchema*>(cSchema)));
  JNI_METHOD_END(kInvalidObjectHandle)
}

JNIEXPORT jlong JNICALL
Java_org_apache_gluten_vectorized_NativeRowToColumnarJniWrapper_nativeConvertRowToColumnar( // NOLINT
    JNIEnv* env,
    jobject wrapper,
    jlong r2cHandle,
    jlongArray rowLength,
    jlong memoryAddress) {
  JNI_METHOD_START
  auto ctx = getRuntime(env, wrapper);

  if (rowLength == nullptr) {
    throw GlutenException("Native convert row to columnar: buf_addrs can't be null");
  }
  int numRows = env->GetArrayLength(rowLength);
  auto safeArray = getLongArrayElementsSafe(env, rowLength);
  uint8_t* address = reinterpret_cast<uint8_t*>(memoryAddress);

  auto converter = ObjectStore::retrieve<RowToColumnarConverter>(r2cHandle);
  auto cb = converter->convert(numRows, safeArray.elems(), address);
  return ctx->saveObject(cb);
  JNI_METHOD_END(kInvalidObjectHandle)
}

JNIEXPORT void JNICALL Java_org_apache_gluten_vectorized_NativeRowToColumnarJniWrapper_close( // NOLINT
    JNIEnv* env,
    jobject wrapper,
    jlong r2cHandle) {
  JNI_METHOD_START
  ObjectStore::release(r2cHandle);
  JNI_METHOD_END()
}

JNIEXPORT jstring JNICALL Java_org_apache_gluten_columnarbatch_ColumnarBatchJniWrapper_getType( // NOLINT
    JNIEnv* env,
    jclass,
    jlong batchHandle) {
  JNI_METHOD_START
  auto batch = ObjectStore::retrieve<ColumnarBatch>(batchHandle);
  return env->NewStringUTF(batch->getType().c_str());
  JNI_METHOD_END(nullptr)
}

JNIEXPORT jlong JNICALL Java_org_apache_gluten_columnarbatch_ColumnarBatchJniWrapper_numBytes( // NOLINT
    JNIEnv* env,
    jclass,
    jlong batchHandle) {
  JNI_METHOD_START
  auto batch = ObjectStore::retrieve<ColumnarBatch>(batchHandle);
  return batch->numBytes();
  JNI_METHOD_END(kInvalidObjectHandle)
}

JNIEXPORT jlong JNICALL Java_org_apache_gluten_columnarbatch_ColumnarBatchJniWrapper_numColumns( // NOLINT
    JNIEnv* env,
    jclass,
    jlong batchHandle) {
  JNI_METHOD_START
  auto batch = ObjectStore::retrieve<ColumnarBatch>(batchHandle);
  return batch->numColumns();
  JNI_METHOD_END(kInvalidObjectHandle)
}

JNIEXPORT jlong JNICALL Java_org_apache_gluten_columnarbatch_ColumnarBatchJniWrapper_numRows( // NOLINT
    JNIEnv* env,
    jclass,
    jlong batchHandle) {
  JNI_METHOD_START
  auto batch = ObjectStore::retrieve<ColumnarBatch>(batchHandle);
  return batch->numRows();
  JNI_METHOD_END(kInvalidObjectHandle)
}

JNIEXPORT void JNICALL Java_org_apache_gluten_columnarbatch_ColumnarBatchJniWrapper_exportToArrow( // NOLINT
    JNIEnv* env,
    jclass,
    jlong batchHandle,
    jlong cSchema,
    jlong cArray) {
  JNI_METHOD_START
  auto batch = ObjectStore::retrieve<ColumnarBatch>(batchHandle);
  std::shared_ptr<ArrowSchema> exportedSchema = batch->exportArrowSchema();
  std::shared_ptr<ArrowArray> exportedArray = batch->exportArrowArray();
  ArrowSchemaMove(exportedSchema.get(), reinterpret_cast<struct ArrowSchema*>(cSchema));
  ArrowArrayMove(exportedArray.get(), reinterpret_cast<struct ArrowArray*>(cArray));
  JNI_METHOD_END()
}

JNIEXPORT void JNICALL Java_org_apache_gluten_columnarbatch_ColumnarBatchJniWrapper_close( // NOLINT
    JNIEnv* env,
    jclass,
    jlong batchHandle) {
  JNI_METHOD_START
  ObjectStore::release(batchHandle);
  JNI_METHOD_END()
}

JNIEXPORT jlong JNICALL Java_org_apache_gluten_columnarbatch_ColumnarBatchJniWrapper_createWithArrowArray( // NOLINT
    JNIEnv* env,
    jobject wrapper,
    jlong cSchema,
    jlong cArray) {
  JNI_METHOD_START
  auto ctx = getRuntime(env, wrapper);

  std::unique_ptr<ArrowSchema> targetSchema = std::make_unique<ArrowSchema>();
  std::unique_ptr<ArrowArray> targetArray = std::make_unique<ArrowArray>();
  auto* arrowSchema = reinterpret_cast<ArrowSchema*>(cSchema);
  auto* arrowArray = reinterpret_cast<ArrowArray*>(cArray);
  ArrowArrayMove(arrowArray, targetArray.get());
  ArrowSchemaMove(arrowSchema, targetSchema.get());
  std::shared_ptr<ColumnarBatch> batch =
      std::make_shared<ArrowCStructColumnarBatch>(std::move(targetSchema), std::move(targetArray));
  return ctx->saveObject(batch);
  JNI_METHOD_END(kInvalidObjectHandle)
}

JNIEXPORT jlong JNICALL Java_org_apache_gluten_columnarbatch_ColumnarBatchJniWrapper_getForEmptySchema( // NOLINT
    JNIEnv* env,
    jobject wrapper,
    jint numRows) {
  JNI_METHOD_START
  auto ctx = getRuntime(env, wrapper);
  return ctx->saveObject(ctx->createOrGetEmptySchemaBatch(static_cast<int32_t>(numRows)));
  JNI_METHOD_END(kInvalidObjectHandle)
}

JNIEXPORT jlong JNICALL Java_org_apache_gluten_columnarbatch_ColumnarBatchJniWrapper_select( // NOLINT
    JNIEnv* env,
    jobject wrapper,
    jlong batchHandle,
    jintArray jcolumnIndices) {
  JNI_METHOD_START
  auto ctx = getRuntime(env, wrapper);

  auto safeArray = getIntArrayElementsSafe(env, jcolumnIndices);
  int size = env->GetArrayLength(jcolumnIndices);
  std::vector<int32_t> columnIndices;
  for (int32_t i = 0; i < size; i++) {
    columnIndices.push_back(safeArray.elems()[i]);
  }

  return ctx->saveObject(ctx->select(ObjectStore::retrieve<ColumnarBatch>(batchHandle), std::move(columnIndices)));
  JNI_METHOD_END(kInvalidObjectHandle)
}

// Shuffle
JNIEXPORT jlong JNICALL Java_org_apache_gluten_vectorized_ShuffleWriterJniWrapper_nativeMake( // NOLINT
    JNIEnv* env,
    jobject wrapper,
    jstring partitioningNameJstr,
    jint numPartitions,
    jint bufferSize,
    jint mergeBufferSize,
    jdouble mergeThreshold,
    jstring codecJstr,
    jstring codecBackendJstr,
    jint compressionLevel,
    jint sortEvictBufferSize,
    jint compressionThreshold,
    jstring compressionModeJstr,
    jint sortBufferInitialSize,
    jboolean useRadixSort,
    jstring dataFileJstr,
    jint numSubDirs,
    jstring localDirsJstr,
    jdouble reallocThreshold,
    jlong firstBatchHandle,
    jlong taskAttemptId,
    jint startPartitionId,
    jint pushBufferMaxSize,
    jlong sortBufferMaxSize,
    jobject partitionPusher,
    jstring partitionWriterTypeJstr,
    jstring shuffleWriterTypeJstr) {
  JNI_METHOD_START
  auto ctx = getRuntime(env, wrapper);
  if (partitioningNameJstr == nullptr) {
    throw GlutenException(std::string("Short partitioning name can't be null"));
  }

  // Build ShuffleWriterOptions.
  auto shuffleWriterOptions = ShuffleWriterOptions{
      .bufferSize = bufferSize,
      .bufferReallocThreshold = reallocThreshold,
      .partitioning = toPartitioning(jStringToCString(env, partitioningNameJstr)),
      .taskAttemptId = static_cast<int64_t>(taskAttemptId),
      .startPartitionId = startPartitionId,
      .shuffleWriterType = ShuffleWriter::stringToType(jStringToCString(env, shuffleWriterTypeJstr)),
      .sortBufferInitialSize = sortBufferInitialSize,
      .sortEvictBufferSize = sortEvictBufferSize,
      .useRadixSort = static_cast<bool>(useRadixSort)};

  // Build PartitionWriterOptions.
  auto partitionWriterOptions = PartitionWriterOptions{
      .mergeBufferSize = mergeBufferSize,
      .mergeThreshold = mergeThreshold,
      .compressionThreshold = compressionThreshold,
      .compressionType = getCompressionType(env, codecJstr),
      .compressionTypeStr = getCompressionTypeStr(env, codecJstr),
      .compressionLevel = compressionLevel,
      .bufferedWrite = true,
      .numSubDirs = numSubDirs,
      .pushBufferMaxSize = pushBufferMaxSize > 0 ? pushBufferMaxSize : kDefaultPushMemoryThreshold,
      .sortBufferMaxSize = sortBufferMaxSize > 0 ? sortBufferMaxSize : kDefaultSortBufferThreshold};
  if (codecJstr != NULL) {
    partitionWriterOptions.codecBackend = getCodecBackend(env, codecBackendJstr);
    partitionWriterOptions.compressionMode = getCompressionMode(env, compressionModeJstr);
  }
  const auto& conf = ctx->getConfMap();
  {
    auto it = conf.find(kShuffleFileBufferSize);
    if (it != conf.end()) {
      partitionWriterOptions.shuffleFileBufferSize = static_cast<int64_t>(stoi(it->second));
    }
  }

  std::unique_ptr<PartitionWriter> partitionWriter;

  auto partitionWriterTypeC = env->GetStringUTFChars(partitionWriterTypeJstr, JNI_FALSE);
  auto partitionWriterType = std::string(partitionWriterTypeC);
  env->ReleaseStringUTFChars(partitionWriterTypeJstr, partitionWriterTypeC);

  if (partitionWriterType == "local") {
    if (dataFileJstr == NULL) {
      throw GlutenException(std::string("Shuffle DataFile can't be null"));
    }
    if (localDirsJstr == NULL) {
      throw GlutenException(std::string("Shuffle DataFile can't be null"));
    }
    auto dataFileC = env->GetStringUTFChars(dataFileJstr, JNI_FALSE);
    auto dataFile = std::string(dataFileC);
    env->ReleaseStringUTFChars(dataFileJstr, dataFileC);

    auto localDirsC = env->GetStringUTFChars(localDirsJstr, JNI_FALSE);
    auto configuredDirs = splitPaths(std::string(localDirsC));
    env->ReleaseStringUTFChars(localDirsJstr, localDirsC);

    partitionWriter = std::make_unique<LocalPartitionWriter>(
        numPartitions,
        std::move(partitionWriterOptions),
        ctx->memoryManager()->getArrowMemoryPool(),
        dataFile,
        configuredDirs);
  } else if (partitionWriterType == "celeborn") {
    jclass celebornPartitionPusherClass =
        createGlobalClassReferenceOrError(env, "Lorg/apache/spark/shuffle/CelebornPartitionPusher;");
    jmethodID celebornPushPartitionDataMethod =
        getMethodIdOrError(env, celebornPartitionPusherClass, "pushPartitionData", "(I[BI)I");
    JavaVM* vm;
    if (env->GetJavaVM(&vm) != JNI_OK) {
      throw GlutenException("Unable to get JavaVM instance");
    }
    std::shared_ptr<JavaRssClient> celebornClient =
        std::make_shared<JavaRssClient>(vm, partitionPusher, celebornPushPartitionDataMethod);
    partitionWriter = std::make_unique<RssPartitionWriter>(
        numPartitions,
        std::move(partitionWriterOptions),
        ctx->memoryManager()->getArrowMemoryPool(),
        std::move(celebornClient));
  } else if (partitionWriterType == "uniffle") {
    jclass unifflePartitionPusherClass =
        createGlobalClassReferenceOrError(env, "Lorg/apache/spark/shuffle/writer/PartitionPusher;");
    jmethodID unifflePushPartitionDataMethod =
        getMethodIdOrError(env, unifflePartitionPusherClass, "pushPartitionData", "(I[BI)I");
    JavaVM* vm;
    if (env->GetJavaVM(&vm) != JNI_OK) {
      throw GlutenException("Unable to get JavaVM instance");
    }
    std::shared_ptr<JavaRssClient> uniffleClient =
        std::make_shared<JavaRssClient>(vm, partitionPusher, unifflePushPartitionDataMethod);
    partitionWriter = std::make_unique<RssPartitionWriter>(
        numPartitions,
        std::move(partitionWriterOptions),
        ctx->memoryManager()->getArrowMemoryPool(),
        std::move(uniffleClient));
  } else {
    throw GlutenException("Unrecognizable partition writer type: " + partitionWriterType);
  }

  return ctx->saveObject(
      ctx->createShuffleWriter(numPartitions, std::move(partitionWriter), std::move(shuffleWriterOptions)));
  JNI_METHOD_END(kInvalidObjectHandle)
}

JNIEXPORT jlong JNICALL Java_org_apache_gluten_vectorized_ShuffleWriterJniWrapper_nativeEvict( // NOLINT
    JNIEnv* env,
    jobject wrapper,
    jlong shuffleWriterHandle,
    jlong size,
    jboolean callBySelf) {
  JNI_METHOD_START
  auto shuffleWriter = ObjectStore::retrieve<ShuffleWriter>(shuffleWriterHandle);
  if (!shuffleWriter) {
    std::string errorMessage = "Invalid shuffle writer handle " + std::to_string(shuffleWriterHandle);
    throw GlutenException(errorMessage);
  }
  int64_t evictedSize;
  arrowAssertOkOrThrow(shuffleWriter->reclaimFixedSize(size, &evictedSize), "(shuffle) nativeEvict: evict failed");
  return (jlong)evictedSize;
  JNI_METHOD_END(kInvalidObjectHandle)
}

JNIEXPORT jlong JNICALL Java_org_apache_gluten_vectorized_ShuffleWriterJniWrapper_write( // NOLINT
    JNIEnv* env,
    jobject wrapper,
    jlong shuffleWriterHandle,
    jint numRows,
    jlong batchHandle,
    jlong memLimit) {
  JNI_METHOD_START
  auto shuffleWriter = ObjectStore::retrieve<ShuffleWriter>(shuffleWriterHandle);
  if (!shuffleWriter) {
    std::string errorMessage = "Invalid shuffle writer handle " + std::to_string(shuffleWriterHandle);
    throw GlutenException(errorMessage);
  }

  // The column batch maybe VeloxColumnBatch or ArrowCStructColumnarBatch(FallbackRangeShuffleWriter)
  auto batch = ObjectStore::retrieve<ColumnarBatch>(batchHandle);
  auto numBytes = batch->numBytes();
  arrowAssertOkOrThrow(shuffleWriter->write(batch, memLimit), "Native write: shuffle writer failed");
  return numBytes;
  JNI_METHOD_END(kInvalidObjectHandle)
}

JNIEXPORT jobject JNICALL Java_org_apache_gluten_vectorized_ShuffleWriterJniWrapper_stop( // NOLINT
    JNIEnv* env,
    jobject wrapper,
    jlong shuffleWriterHandle) {
  JNI_METHOD_START
  auto shuffleWriter = ObjectStore::retrieve<ShuffleWriter>(shuffleWriterHandle);
  if (!shuffleWriter) {
    std::string errorMessage = "Invalid shuffle writer handle " + std::to_string(shuffleWriterHandle);
    throw GlutenException(errorMessage);
  }

  arrowAssertOkOrThrow(shuffleWriter->stop(), "Native shuffle write: ShuffleWriter stop failed");

  const auto& partitionLengths = shuffleWriter->partitionLengths();
  auto partitionLengthArr = env->NewLongArray(partitionLengths.size());
  auto src = reinterpret_cast<const jlong*>(partitionLengths.data());
  env->SetLongArrayRegion(partitionLengthArr, 0, partitionLengths.size(), src);

  const auto& rawPartitionLengths = shuffleWriter->rawPartitionLengths();
  auto rawPartitionLengthArr = env->NewLongArray(rawPartitionLengths.size());
  auto rawSrc = reinterpret_cast<const jlong*>(rawPartitionLengths.data());
  env->SetLongArrayRegion(rawPartitionLengthArr, 0, rawPartitionLengths.size(), rawSrc);

  jobject splitResult = env->NewObject(
      splitResultClass,
      splitResultConstructor,
      0L,
      shuffleWriter->totalWriteTime(),
      shuffleWriter->totalEvictTime(),
      shuffleWriter->totalCompressTime(),
      shuffleWriter->totalSortTime(),
      shuffleWriter->totalC2RTime(),
      shuffleWriter->totalBytesWritten(),
      shuffleWriter->totalBytesEvicted(),
      shuffleWriter->totalBytesToEvict(),
      shuffleWriter->peakBytesAllocated(),
      partitionLengthArr,
      rawPartitionLengthArr);

  return splitResult;
  JNI_METHOD_END(nullptr)
}

JNIEXPORT void JNICALL Java_org_apache_gluten_vectorized_ShuffleWriterJniWrapper_close( // NOLINT
    JNIEnv* env,
    jobject wrapper,
    jlong shuffleWriterHandle) {
  JNI_METHOD_START
  ObjectStore::release(shuffleWriterHandle);
  JNI_METHOD_END()
}

JNIEXPORT void JNICALL Java_org_apache_gluten_vectorized_OnHeapJniByteInputStream_memCopyFromHeap( // NOLINT
    JNIEnv* env,
    jobject,
    jbyteArray source,
    jlong destAddress,
    jint size) {
  JNI_METHOD_START
  auto safeArray = getByteArrayElementsSafe(env, source);
  std::memcpy(reinterpret_cast<void*>(destAddress), safeArray.elems(), size);
  JNI_METHOD_END()
}

JNIEXPORT jlong JNICALL Java_org_apache_gluten_vectorized_ShuffleReaderJniWrapper_make( // NOLINT
    JNIEnv* env,
    jobject wrapper,
    jlong cSchema,
    jstring compressionType,
    jstring compressionBackend,
    jint batchSize,
    jlong bufferSize,
    jstring shuffleWriterType) {
  JNI_METHOD_START
  auto ctx = getRuntime(env, wrapper);

  ShuffleReaderOptions options = ShuffleReaderOptions{};
  options.compressionType = getCompressionType(env, compressionType);
  options.compressionTypeStr = getCompressionTypeStr(env, compressionType);
  if (compressionType != nullptr) {
    options.codecBackend = getCodecBackend(env, compressionBackend);
  }
  options.batchSize = batchSize;
  options.bufferSize = bufferSize;

  options.shuffleWriterType = ShuffleWriter::stringToType(jStringToCString(env, shuffleWriterType));
  std::shared_ptr<arrow::Schema> schema =
      arrowGetOrThrow(arrow::ImportSchema(reinterpret_cast<struct ArrowSchema*>(cSchema)));

  return ctx->saveObject(ctx->createShuffleReader(schema, options));
  JNI_METHOD_END(kInvalidObjectHandle)
}

JNIEXPORT jlong JNICALL Java_org_apache_gluten_vectorized_ShuffleReaderJniWrapper_readStream( // NOLINT
    JNIEnv* env,
    jobject wrapper,
    jlong shuffleReaderHandle,
    jobject jniIn) {
  JNI_METHOD_START
  auto ctx = getRuntime(env, wrapper);
  auto reader = ObjectStore::retrieve<ShuffleReader>(shuffleReaderHandle);
  std::shared_ptr<arrow::io::InputStream> in = std::make_shared<JavaInputStreamAdaptor>(env, reader->getPool(), jniIn);
  auto outItr = reader->readStream(in);
  return ctx->saveObject(outItr);
  JNI_METHOD_END(kInvalidObjectHandle)
}

JNIEXPORT void JNICALL Java_org_apache_gluten_vectorized_ShuffleReaderJniWrapper_populateMetrics( // NOLINT
    JNIEnv* env,
    jobject wrapper,
    jlong shuffleReaderHandle,
    jobject metrics) {
  JNI_METHOD_START
  auto reader = ObjectStore::retrieve<ShuffleReader>(shuffleReaderHandle);
  env->CallVoidMethod(metrics, shuffleReaderMetricsSetDecompressTime, reader->getDecompressTime());
  env->CallVoidMethod(metrics, shuffleReaderMetricsSetDeserializeTime, reader->getDeserializeTime());

  checkException(env);
  JNI_METHOD_END()
}

JNIEXPORT void JNICALL Java_org_apache_gluten_vectorized_ShuffleReaderJniWrapper_close( // NOLINT
    JNIEnv* env,
    jobject wrapper,
    jlong shuffleReaderHandle) {
  JNI_METHOD_START
  auto reader = ObjectStore::retrieve<ShuffleReader>(shuffleReaderHandle);
  GLUTEN_THROW_NOT_OK(reader->close());
  ObjectStore::release(shuffleReaderHandle);
  JNI_METHOD_END()
}

JNIEXPORT jobject JNICALL Java_org_apache_gluten_vectorized_ColumnarBatchSerializerJniWrapper_serialize( // NOLINT
    JNIEnv* env,
    jobject wrapper,
    jlongArray handles) {
  JNI_METHOD_START
  auto ctx = getRuntime(env, wrapper);

  int32_t numBatches = env->GetArrayLength(handles);
  auto safeArray = getLongArrayElementsSafe(env, handles);

  std::vector<std::shared_ptr<ColumnarBatch>> batches;
  int64_t numRows = 0L;
  for (int32_t i = 0; i < numBatches; i++) {
    auto batch = ObjectStore::retrieve<ColumnarBatch>(safeArray.elems()[i]);
    GLUTEN_DCHECK(
        batch != nullptr, "Cannot find the ColumnarBatch with handle " + std::to_string(safeArray.elems()[i]));
    numRows += batch->numRows();
    batches.emplace_back(batch);
  }

  auto serializer = ctx->createColumnarBatchSerializer(nullptr);
  auto buffer = serializer->serializeColumnarBatches(batches);
  auto bufferArr = env->NewByteArray(buffer->size());
  GLUTEN_CHECK(
      bufferArr != nullptr,
      "Cannot construct a byte array of size " + std::to_string(buffer->size()) +
          " byte(s) to serialize columnar batches");
  env->SetByteArrayRegion(bufferArr, 0, buffer->size(), reinterpret_cast<const jbyte*>(buffer->data()));

  jobject columnarBatchSerializeResult =
      env->NewObject(columnarBatchSerializeResultClass, columnarBatchSerializeResultConstructor, numRows, bufferArr);

  return columnarBatchSerializeResult;
  JNI_METHOD_END(nullptr)
}

JNIEXPORT jlong JNICALL Java_org_apache_gluten_vectorized_ColumnarBatchSerializerJniWrapper_init( // NOLINT
    JNIEnv* env,
    jobject wrapper,
    jlong cSchema) {
  JNI_METHOD_START
  auto ctx = getRuntime(env, wrapper);
  return ctx->saveObject(ctx->createColumnarBatchSerializer(reinterpret_cast<struct ArrowSchema*>(cSchema)));
  JNI_METHOD_END(kInvalidObjectHandle)
}

JNIEXPORT jlong JNICALL Java_org_apache_gluten_vectorized_ColumnarBatchSerializerJniWrapper_deserialize( // NOLINT
    JNIEnv* env,
    jobject wrapper,
    jlong serializerHandle,
    jbyteArray data) {
  JNI_METHOD_START
  auto ctx = getRuntime(env, wrapper);

  auto serializer = ObjectStore::retrieve<ColumnarBatchSerializer>(serializerHandle);
  GLUTEN_DCHECK(serializer != nullptr, "ColumnarBatchSerializer cannot be null");
  int32_t size = env->GetArrayLength(data);
  auto safeArray = getByteArrayElementsSafe(env, data);
  auto batch = serializer->deserialize(safeArray.elems(), size);
  return ctx->saveObject(batch);
  JNI_METHOD_END(kInvalidObjectHandle)
}

JNIEXPORT void JNICALL Java_org_apache_gluten_vectorized_ColumnarBatchSerializerJniWrapper_close( // NOLINT
    JNIEnv* env,
    jobject wrapper,
    jlong serializerHandle) {
  JNI_METHOD_START
  ObjectStore::release(serializerHandle);
  JNI_METHOD_END()
}

#ifdef __cplusplus
}
#endif
