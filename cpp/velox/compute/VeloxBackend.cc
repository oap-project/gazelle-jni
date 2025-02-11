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
#include <filesystem>

#include "VeloxBackend.h"

#include <folly/executors/IOThreadPoolExecutor.h>

#include "operators/functions/RegistrationAllFunctions.h"
#include "operators/plannodes/RowVectorStream.h"
#include "utils/ConfigExtractor.h"

#ifdef GLUTEN_ENABLE_QAT
#include "utils/qat/QatCodec.h"
#endif
#ifdef GLUTEN_ENABLE_IAA
#include "utils/qpl/QplCodec.h"
#endif
#include "compute/VeloxRuntime.h"
#include "config/VeloxConfig.h"
#include "jni/JniFileSystem.h"
#include "operators/functions/SparkExprToSubfieldFilterParser.h"
#include "udf/UdfLoader.h"
#include "utils/Exception.h"
#include "velox/common/caching/SsdCache.h"
#include "velox/common/file/FileSystems.h"
#include "velox/connectors/hive/HiveConnector.h"
#include "velox/connectors/hive/HiveDataSource.h"
#include "velox/connectors/hive/storage_adapters/abfs/RegisterAbfsFileSystem.h" // @manual
#include "velox/connectors/hive/storage_adapters/gcs/RegisterGcsFileSystem.h" // @manual
#include "velox/connectors/hive/storage_adapters/hdfs/RegisterHdfsFileSystem.h" // @manual
#include "velox/connectors/hive/storage_adapters/s3fs/RegisterS3FileSystem.h" // @manual
#include "velox/dwio/orc/reader/OrcReader.h"
#include "velox/dwio/parquet/RegisterParquetReader.h"
#include "velox/dwio/parquet/RegisterParquetWriter.h"
#include "velox/serializers/PrestoSerializer.h"

DECLARE_bool(velox_exception_user_stacktrace_enabled);
DECLARE_int32(velox_memory_num_shared_leaf_pools);
DECLARE_bool(velox_memory_use_hugepages);
DECLARE_bool(velox_memory_pool_capacity_transfer_across_tasks);
DECLARE_int32(cache_prefetch_min_pct);

DECLARE_int32(gluten_velox_aysnc_timeout_on_task_stopping);
DEFINE_int32(gluten_velox_aysnc_timeout_on_task_stopping, 30000, "Aysnc timout when task is being stopped");

using namespace facebook;

namespace gluten {

namespace {
MemoryManager* veloxMemoryManagerFactory(const std::string& kind, std::unique_ptr<AllocationListener> listener) {
  return new VeloxMemoryManager(kind, std::move(listener));
}

void veloxMemoryManagerReleaser(MemoryManager* memoryManager) {
  delete memoryManager;
}

Runtime* veloxRuntimeFactory(
    const std::string& kind,
    MemoryManager* memoryManager,
    const std::unordered_map<std::string, std::string>& sessionConf) {
  auto* vmm = dynamic_cast<VeloxMemoryManager*>(memoryManager);
  GLUTEN_CHECK(vmm != nullptr, "Not a Velox memory manager");
  return new VeloxRuntime(kind, vmm, sessionConf);
}

void veloxRuntimeReleaser(Runtime* runtime) {
  delete runtime;
}
} // namespace

void VeloxBackend::init(const std::unordered_map<std::string, std::string>& conf) {
  backendConf_ =
      std::make_shared<facebook::velox::config::ConfigBase>(std::unordered_map<std::string, std::string>(conf));

  // Register factories.
  MemoryManager::registerFactory(kVeloxBackendKind, veloxMemoryManagerFactory, veloxMemoryManagerReleaser);
  Runtime::registerFactory(kVeloxBackendKind, veloxRuntimeFactory, veloxRuntimeReleaser);

  if (backendConf_->get<bool>(kDebugModeEnabled, false)) {
    LOG(INFO) << "VeloxBackend config:" << printConfig(backendConf_->rawConfigs());
  }

  // Init glog and log level.
  if (!backendConf_->get<bool>(kDebugModeEnabled, false)) {
    FLAGS_v = backendConf_->get<uint32_t>(kGlogVerboseLevel, kGlogVerboseLevelDefault);
    FLAGS_minloglevel = backendConf_->get<uint32_t>(kGlogSeverityLevel, kGlogSeverityLevelDefault);
  } else {
    if (backendConf_->valueExists(kGlogVerboseLevel)) {
      FLAGS_v = backendConf_->get<uint32_t>(kGlogVerboseLevel, kGlogVerboseLevelDefault);
    } else {
      FLAGS_v = kGlogVerboseLevelMaximum;
    }
  }
  FLAGS_logtostderr = true;
  google::InitGoogleLogging("gluten");

  // Allow growing buffer in another task through its memory pool.
  config::globalConfig().memoryPoolCapacityTransferAcrossTasks = true;

  // Avoid creating too many shared leaf pools.
  config::globalConfig().memoryNumSharedLeafPools = 0;

  // Set velox_exception_user_stacktrace_enabled.
  config::globalConfig().exceptionUserStacktraceEnabled =
      backendConf_->get<bool>(kEnableUserExceptionStacktrace, kEnableUserExceptionStacktraceDefault);

  // Set velox_exception_system_stacktrace_enabled.
  config::globalConfig().exceptionSystemStacktraceEnabled =
      backendConf_->get<bool>(kEnableSystemExceptionStacktrace, kEnableSystemExceptionStacktraceDefault);

  // Set velox_memory_use_hugepages.
  config::globalConfig().memoryUseHugepages = backendConf_->get<bool>(kMemoryUseHugePages, kMemoryUseHugePagesDefault);

  // Async timeout.
  FLAGS_gluten_velox_aysnc_timeout_on_task_stopping =
      backendConf_->get<int32_t>(kVeloxAsyncTimeoutOnTaskStopping, kVeloxAsyncTimeoutOnTaskStoppingDefault);

  // Setup and register.
  velox::filesystems::registerLocalFileSystem();

#ifdef ENABLE_HDFS
  velox::filesystems::registerHdfsFileSystem();
#endif
#ifdef ENABLE_S3
  velox::filesystems::registerS3FileSystem();
#endif
#ifdef ENABLE_GCS
  velox::filesystems::registerGcsFileSystem();
#endif
#ifdef ENABLE_ABFS
  velox::filesystems::registerAbfsFileSystem();
#endif

  initJolFilesystem();
  initCache();
  initConnector();

  velox::dwio::common::registerFileSinks();
  velox::parquet::registerParquetReaderFactory();
  velox::parquet::registerParquetWriterFactory();
  velox::orc::registerOrcReaderFactory();
  velox::exec::ExprToSubfieldFilterParser::registerParserFactory(
      []() { return std::make_shared<SparkExprToSubfieldFilterParser>(); });

  // Register Velox functions
  registerAllFunctions();
  if (!facebook::velox::isRegisteredVectorSerde()) {
    // serde, for spill
    facebook::velox::serializer::presto::PrestoVectorSerde::registerVectorSerde();
  }
  if (!isRegisteredNamedVectorSerde(facebook::velox::VectorSerde::Kind::kPresto)) {
    // RSS shuffle serde.
    facebook::velox::serializer::presto::PrestoVectorSerde::registerNamedVectorSerde();
  }
  velox::exec::Operator::registerOperator(std::make_unique<RowVectorStreamOperatorTranslator>());

  initUdf();

  // Initialize the global memory manager for current process.
  auto sparkOverhead = backendConf_->get<int64_t>(kSparkOverheadMemory);
  int64_t memoryManagerCapacity;
  if (sparkOverhead.hasValue()) {
    // 0.75 * total overhead memory is used for Velox global memory manager.
    // FIXME: Make this configurable.
    memoryManagerCapacity = sparkOverhead.value() * 0.75;
  } else {
    memoryManagerCapacity = facebook::velox::memory::kMaxMemory;
  }
  LOG(INFO) << "Setting global Velox memory manager with capacity: " << memoryManagerCapacity;
  facebook::velox::memory::MemoryManager::initialize({.allocatorCapacity = memoryManagerCapacity});
}

facebook::velox::cache::AsyncDataCache* VeloxBackend::getAsyncDataCache() const {
  return asyncDataCache_.get();
}

// JNI-or-local filesystem, for spilling-to-heap if we have extra JVM heap spaces
void VeloxBackend::initJolFilesystem() {
  int64_t maxSpillFileSize = backendConf_->get<int64_t>(kMaxSpillFileSize, kMaxSpillFileSizeDefault);

  // FIXME It's known that if spill compression is disabled, the actual spill file size may
  //   in crease beyond this limit a little (maximum 64 rows which is by default
  //   one compression page)
  registerJolFileSystem(maxSpillFileSize);
}

void VeloxBackend::initCache() {
  if (backendConf_->get<bool>(kVeloxCacheEnabled, false)) {
    FLAGS_ssd_odirect = true;

    FLAGS_ssd_odirect = backendConf_->get<bool>(kVeloxSsdODirectEnabled, false);

    uint64_t memCacheSize = backendConf_->get<uint64_t>(kVeloxMemCacheSize, kVeloxMemCacheSizeDefault);
    uint64_t ssdCacheSize = backendConf_->get<uint64_t>(kVeloxSsdCacheSize, kVeloxSsdCacheSizeDefault);
    int32_t ssdCacheShards = backendConf_->get<int32_t>(kVeloxSsdCacheShards, kVeloxSsdCacheShardsDefault);
    int32_t ssdCacheIOThreads = backendConf_->get<int32_t>(kVeloxSsdCacheIOThreads, kVeloxSsdCacheIOThreadsDefault);
    std::string ssdCachePathPrefix = backendConf_->get<std::string>(kVeloxSsdCachePath, kVeloxSsdCachePathDefault);

    cachePathPrefix_ = ssdCachePathPrefix;
    cacheFilePrefix_ = getCacheFilePrefix();
    std::string ssdCachePath = ssdCachePathPrefix + "/" + cacheFilePrefix_;
    ssdCacheExecutor_ = std::make_unique<folly::IOThreadPoolExecutor>(ssdCacheIOThreads);
    const cache::SsdCache::Config config(ssdCachePath, ssdCacheSize, ssdCacheShards, ssdCacheExecutor_.get());
    auto ssd = std::make_unique<velox::cache::SsdCache>(config);

    std::error_code ec;
    const std::filesystem::space_info si = std::filesystem::space(ssdCachePathPrefix, ec);
    if (si.available < ssdCacheSize) {
      VELOX_FAIL(
          "not enough space for ssd cache in " + ssdCachePath + " cache size: " + std::to_string(ssdCacheSize) +
          "free space: " + std::to_string(si.available));
    }

    velox::memory::MmapAllocator::Options options;
    options.capacity = memCacheSize;
    cacheAllocator_ = std::make_shared<velox::memory::MmapAllocator>(options);
    if (ssdCacheSize == 0) {
      LOG(INFO) << "AsyncDataCache will do memory caching only as ssd cache size is 0";
      // TODO: this is not tracked by Spark.
      asyncDataCache_ = velox::cache::AsyncDataCache::create(cacheAllocator_.get());
    } else {
      // TODO: this is not tracked by Spark.
      asyncDataCache_ = velox::cache::AsyncDataCache::create(cacheAllocator_.get(), std::move(ssd));
    }

    VELOX_CHECK_NOT_NULL(dynamic_cast<velox::cache::AsyncDataCache*>(asyncDataCache_.get()));
    LOG(INFO) << "STARTUP: Using AsyncDataCache memory cache size: " << memCacheSize
              << ", ssdCache prefix: " << ssdCachePath << ", ssdCache size: " << ssdCacheSize
              << ", ssdCache shards: " << ssdCacheShards << ", ssdCache IO threads: " << ssdCacheIOThreads;
  }
}

void VeloxBackend::initConnector() {
  // The configs below are used at process level.
  std::unordered_map<std::string, std::string> connectorConfMap = backendConf_->rawConfigs();

  auto hiveConf = getHiveConfig(backendConf_);
  for (auto& [k, v] : hiveConf->rawConfigsCopy()) {
    connectorConfMap[k] = v;
  }

  connectorConfMap[velox::connector::hive::HiveConfig::kEnableFileHandleCache] =
      backendConf_->get<bool>(kVeloxFileHandleCacheEnabled, kVeloxFileHandleCacheEnabledDefault) ? "true" : "false";

  connectorConfMap[velox::connector::hive::HiveConfig::kMaxCoalescedBytes] =
      backendConf_->get<std::string>(kMaxCoalescedBytes, "67108864"); // 64M
  connectorConfMap[velox::connector::hive::HiveConfig::kMaxCoalescedDistance] =
      backendConf_->get<std::string>(kMaxCoalescedDistance, "512KB"); // 512KB
  connectorConfMap[velox::connector::hive::HiveConfig::kPrefetchRowGroups] =
      backendConf_->get<std::string>(kPrefetchRowGroups, "1");
  connectorConfMap[velox::connector::hive::HiveConfig::kLoadQuantum] =
      backendConf_->get<std::string>(kLoadQuantum, "268435456"); // 256M
  connectorConfMap[velox::connector::hive::HiveConfig::kFooterEstimatedSize] =
      backendConf_->get<std::string>(kDirectorySizeGuess, "32768"); // 32K
  connectorConfMap[velox::connector::hive::HiveConfig::kFilePreloadThreshold] =
      backendConf_->get<std::string>(kFilePreloadThreshold, "1048576"); // 1M

  // set cache_prefetch_min_pct default as 0 to force all loads are prefetched in DirectBufferInput.
  FLAGS_cache_prefetch_min_pct = backendConf_->get<int>(kCachePrefetchMinPct, 0);

  auto ioThreads = backendConf_->get<int32_t>(kVeloxIOThreads, kVeloxIOThreadsDefault);
  GLUTEN_CHECK(
      ioThreads >= 0,
      kVeloxIOThreads + " was set to negative number " + std::to_string(ioThreads) + ", this should not happen.");
  if (ioThreads > 0) {
    ioExecutor_ = std::make_unique<folly::IOThreadPoolExecutor>(ioThreads);
  }
  velox::connector::registerConnector(std::make_shared<velox::connector::hive::HiveConnector>(
      kHiveConnectorId,
      std::make_shared<facebook::velox::config::ConfigBase>(std::move(connectorConfMap)),
      ioExecutor_.get()));
}

void VeloxBackend::initUdf() {
  auto got = backendConf_->get<std::string>(kVeloxUdfLibraryPaths, "");
  if (!got.empty()) {
    auto udfLoader = UdfLoader::getInstance();
    udfLoader->loadUdfLibraries(got);
    udfLoader->registerUdf();
  }
}

std::unique_ptr<VeloxBackend> VeloxBackend::instance_ = nullptr;

void VeloxBackend::create(const std::unordered_map<std::string, std::string>& conf) {
  instance_ = std::unique_ptr<VeloxBackend>(new VeloxBackend(conf));
}

VeloxBackend* VeloxBackend::get() {
  if (!instance_) {
    LOG(WARNING) << "VeloxBackend instance is null, please invoke VeloxBackend#create before use.";
    throw GlutenException("VeloxBackend instance is null.");
  }
  return instance_.get();
}

} // namespace gluten
