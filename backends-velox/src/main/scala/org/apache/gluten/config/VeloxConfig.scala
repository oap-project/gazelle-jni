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
package org.apache.gluten.config

import org.apache.gluten.config.GlutenConfig.{buildConf, buildStaticConf, COLUMNAR_MAX_BATCH_SIZE}

import org.apache.spark.network.util.ByteUnit
import org.apache.spark.sql.internal.SQLConf

import java.util.Locale
import java.util.concurrent.TimeUnit

class VeloxConfig(conf: SQLConf) extends GlutenConfig(conf) {
  import VeloxConfig._

  def veloxColumnarWindowType: String = getConf(COLUMNAR_VELOX_WINDOW_TYPE)

  def veloxSpillFileSystem: String = getConf(COLUMNAR_VELOX_SPILL_FILE_SYSTEM)

  def veloxResizeBatchesShuffleInput: Boolean =
    getConf(COLUMNAR_VELOX_RESIZE_BATCHES_SHUFFLE_INPUT)

  case class ResizeRange(min: Int, max: Int) {
    assert(max >= min)
    assert(min > 0, "Min batch size should be larger than 0")
    assert(max > 0, "Max batch size should be larger than 0")
  }

  def veloxResizeBatchesShuffleInputRange: ResizeRange = {
    val standardSize = getConf(COLUMNAR_MAX_BATCH_SIZE)
    val defaultMinSize: Int = (0.25 * standardSize).toInt.max(1)
    val minSize = getConf(COLUMNAR_VELOX_RESIZE_BATCHES_SHUFFLE_INPUT_MIN_SIZE)
      .getOrElse(defaultMinSize)
    ResizeRange(minSize, Int.MaxValue)
  }

  def veloxBloomFilterMaxNumBits: Long = getConf(COLUMNAR_VELOX_BLOOM_FILTER_MAX_NUM_BITS)

  def castFromVarcharAddTrimNode: Boolean = getConf(CAST_FROM_VARCHAR_ADD_TRIM_NODE)

  def enableVeloxFlushablePartialAggregation: Boolean =
    getConf(VELOX_FLUSHABLE_PARTIAL_AGGREGATION_ENABLED)

  def enableBroadcastBuildRelationInOffheap: Boolean =
    getConf(VELOX_BROADCAST_BUILD_RELATION_USE_OFFHEAP)

  def veloxOrcScanEnabled: Boolean =
    getConf(VELOX_ORC_SCAN_ENABLED)
}

object VeloxConfig {

  def get: VeloxConfig = {
    new VeloxConfig(SQLConf.get)
  }

  val COLUMNAR_VELOX_WINDOW_TYPE =
    buildConf("spark.gluten.sql.columnar.backend.velox.window.type")
      .internal()
      .doc(
        "Velox backend supports both SortWindow and" +
          " StreamingWindow operators." +
          " The StreamingWindow operator skips the sorting step" +
          " in the input but does not support spill." +
          " On the other hand, the SortWindow operator is " +
          "responsible for sorting the input data within the" +
          " Window operator and also supports spill.")
      .stringConf
      .checkValues(Set("streaming", "sort"))
      .createWithDefault("streaming")

  // velox caching options.
  val COLUMNAR_VELOX_CACHE_ENABLED =
    buildStaticConf("spark.gluten.sql.columnar.backend.velox.cacheEnabled")
      .internal()
      .doc("Enable Velox cache, default off")
      .booleanConf
      .createWithDefault(false)

  val COLUMNAR_VELOX_MEM_CACHE_SIZE =
    buildStaticConf("spark.gluten.sql.columnar.backend.velox.memCacheSize")
      .internal()
      .doc("The memory cache size")
      .bytesConf(ByteUnit.BYTE)
      .createWithDefaultString("1GB")

  val COLUMNAR_VELOX_MEM_INIT_CAPACITY =
    buildConf("spark.gluten.sql.columnar.backend.velox.memInitCapacity")
      .internal()
      .doc("The initial memory capacity to reserve for a newly created Velox query memory pool.")
      .bytesConf(ByteUnit.BYTE)
      .createWithDefaultString("8MB")

  val COLUMNAR_VELOX_MEM_RECLAIM_MAX_WAIT_MS =
    buildConf("spark.gluten.sql.columnar.backend.velox.reclaimMaxWaitMs")
      .internal()
      .doc("The max time in ms to wait for memory reclaim.")
      .timeConf(TimeUnit.MILLISECONDS)
      .createWithDefault(TimeUnit.MINUTES.toMillis(60))

  val COLUMNAR_VELOX_SSD_CACHE_PATH =
    buildStaticConf("spark.gluten.sql.columnar.backend.velox.ssdCachePath")
      .internal()
      .doc("The folder to store the cache files, better on SSD")
      .stringConf
      .createWithDefault("/tmp")

  val COLUMNAR_VELOX_SSD_CACHE_SIZE =
    buildStaticConf("spark.gluten.sql.columnar.backend.velox.ssdCacheSize")
      .internal()
      .doc("The SSD cache size, will do memory caching only if this value = 0")
      .bytesConf(ByteUnit.BYTE)
      .createWithDefaultString("1GB")

  val COLUMNAR_VELOX_SSD_CACHE_SHARDS =
    buildStaticConf("spark.gluten.sql.columnar.backend.velox.ssdCacheShards")
      .internal()
      .doc("The cache shards")
      .intConf
      .createWithDefault(1)

  val COLUMNAR_VELOX_SSD_CACHE_IO_THREADS =
    buildStaticConf("spark.gluten.sql.columnar.backend.velox.ssdCacheIOThreads")
      .internal()
      .doc("The IO threads for cache promoting")
      .intConf
      .createWithDefault(1)

  val COLUMNAR_VELOX_SSD_ODIRECT_ENABLED =
    buildStaticConf("spark.gluten.sql.columnar.backend.velox.ssdODirect")
      .internal()
      .doc("The O_DIRECT flag for cache writing")
      .booleanConf
      .createWithDefault(false)

  val COLUMNAR_VELOX_CONNECTOR_IO_THREADS =
    buildStaticConf("spark.gluten.sql.columnar.backend.velox.IOThreads")
      .internal()
      .doc(
        "The Size of the IO thread pool in the Connector. " +
          "This thread pool is used for split preloading and DirectBufferedInput. " +
          "By default, the value is the same as the maximum task slots per Spark executor.")
      .intConf
      .createOptional

  val COLUMNAR_VELOX_ASYNC_TIMEOUT =
    buildStaticConf("spark.gluten.sql.columnar.backend.velox.asyncTimeoutOnTaskStopping")
      .internal()
      .doc(
        "Timeout for asynchronous execution when task is being stopped in Velox backend. " +
          "It's recommended to set to a number larger than network connection timeout that the " +
          "possible aysnc tasks are relying on.")
      .timeConf(TimeUnit.MILLISECONDS)
      .createWithDefault(30000)

  val COLUMNAR_VELOX_SPLIT_PRELOAD_PER_DRIVER =
    buildConf("spark.gluten.sql.columnar.backend.velox.SplitPreloadPerDriver")
      .internal()
      .doc("The split preload per task")
      .intConf
      .createWithDefault(2)

  val COLUMNAR_VELOX_GLOG_VERBOSE_LEVEL =
    buildConf("spark.gluten.sql.columnar.backend.velox.glogVerboseLevel")
      .internal()
      .doc("Set glog verbose level in Velox backend, same as FLAGS_v.")
      .intConf
      .createWithDefault(0)

  val COLUMNAR_VELOX_GLOG_SEVERITY_LEVEL =
    buildConf("spark.gluten.sql.columnar.backend.velox.glogSeverityLevel")
      .internal()
      .doc("Set glog severity level in Velox backend, same as FLAGS_minloglevel.")
      .intConf
      .createWithDefault(1)

  val COLUMNAR_VELOX_SPILL_STRATEGY =
    buildConf("spark.gluten.sql.columnar.backend.velox.spillStrategy")
      .internal()
      .doc("none: Disable spill on Velox backend; " +
        "auto: Let Spark memory manager manage Velox's spilling")
      .stringConf
      .transform(_.toLowerCase(Locale.ROOT))
      .checkValues(Set("none", "auto"))
      .createWithDefault("auto")

  val COLUMNAR_VELOX_MAX_SPILL_LEVEL =
    buildConf("spark.gluten.sql.columnar.backend.velox.maxSpillLevel")
      .internal()
      .doc("The max allowed spilling level with zero being the initial spilling level")
      .intConf
      .createWithDefault(4)

  val COLUMNAR_VELOX_MAX_SPILL_FILE_SIZE =
    buildConf("spark.gluten.sql.columnar.backend.velox.maxSpillFileSize")
      .internal()
      .doc("The maximum size of a single spill file created")
      .bytesConf(ByteUnit.BYTE)
      .createWithDefaultString("1GB")

  val COLUMNAR_VELOX_SPILL_FILE_SYSTEM =
    buildConf("spark.gluten.sql.columnar.backend.velox.spillFileSystem")
      .internal()
      .doc(
        "The filesystem used to store spill data. local: The local file system. " +
          "heap-over-local: Write file to JVM heap if having extra heap space. " +
          "Otherwise write to local file system.")
      .stringConf
      .checkValues(Set("local", "heap-over-local"))
      .createWithDefaultString("local")

  val COLUMNAR_VELOX_MAX_SPILL_RUN_ROWS =
    buildConf("spark.gluten.sql.columnar.backend.velox.maxSpillRunRows")
      .internal()
      .doc("The maximum row size of a single spill run")
      .bytesConf(ByteUnit.BYTE)
      .createWithDefaultString("3M")

  val COLUMNAR_VELOX_MAX_SPILL_BYTES =
    buildConf("spark.gluten.sql.columnar.backend.velox.maxSpillBytes")
      .internal()
      .doc("The maximum file size of a query")
      .bytesConf(ByteUnit.BYTE)
      .createWithDefaultString("100G")

  val MAX_PARTITION_PER_WRITERS_SESSION =
    buildConf("spark.gluten.sql.columnar.backend.velox.maxPartitionsPerWritersSession")
      .internal()
      .doc("Maximum number of partitions per a single table writer instance.")
      .intConf
      .checkValue(_ > 0, "must be a positive number")
      .createWithDefault(10000)

  val COLUMNAR_VELOX_RESIZE_BATCHES_SHUFFLE_INPUT =
    buildConf("spark.gluten.sql.columnar.backend.velox.resizeBatches.shuffleInput")
      .internal()
      .doc(s"If true, combine small columnar batches together before sending to shuffle. " +
        s"The default minimum output batch size is equal to 0.8 * ${COLUMNAR_MAX_BATCH_SIZE.key}")
      .booleanConf
      .createWithDefault(true)

  val COLUMNAR_VELOX_RESIZE_BATCHES_SHUFFLE_INPUT_MIN_SIZE =
    buildConf("spark.gluten.sql.columnar.backend.velox.resizeBatches.shuffleInput.minSize")
      .internal()
      .doc(
        s"The minimum batch size for shuffle. If size of an input batch is " +
          s"smaller than the value, it will be combined with other " +
          s"batches before sending to shuffle. Only functions when " +
          s"${COLUMNAR_VELOX_RESIZE_BATCHES_SHUFFLE_INPUT.key} is set to true. " +
          s"Default value: 0.25 * <max batch size>")
      .intConf
      .createOptional

  val COLUMNAR_VELOX_ENABLE_USER_EXCEPTION_STACKTRACE =
    buildConf("spark.gluten.sql.columnar.backend.velox.enableUserExceptionStacktrace")
      .internal()
      .doc("Enable the stacktrace for user type of VeloxException")
      .booleanConf
      .createWithDefault(true)

  val COLUMNAR_VELOX_SHOW_TASK_METRICS_WHEN_FINISHED =
    buildConf("spark.gluten.sql.columnar.backend.velox.showTaskMetricsWhenFinished")
      .internal()
      .doc("Show velox full task metrics when finished.")
      .booleanConf
      .createWithDefault(false)

  val COLUMNAR_VELOX_MEMORY_USE_HUGE_PAGES =
    buildConf("spark.gluten.sql.columnar.backend.velox.memoryUseHugePages")
      .internal()
      .doc("Use explicit huge pages for Velox memory allocation.")
      .booleanConf
      .createWithDefault(false)

  val COLUMNAR_VELOX_ENABLE_SYSTEM_EXCEPTION_STACKTRACE =
    buildConf("spark.gluten.sql.columnar.backend.velox.enableSystemExceptionStacktrace")
      .internal()
      .doc("Enable the stacktrace for system type of VeloxException")
      .booleanConf
      .createWithDefault(true)

  val VELOX_FLUSHABLE_PARTIAL_AGGREGATION_ENABLED =
    buildConf("spark.gluten.sql.columnar.backend.velox.flushablePartialAggregation")
      .internal()
      .doc(
        "Enable flushable aggregation. If true, Gluten will try converting regular aggregation " +
          "into Velox's flushable aggregation when applicable. A flushable aggregation could " +
          "emit intermediate result at anytime when memory is full / data reduction ratio is low."
      )
      .booleanConf
      .createWithDefault(true)

  val MAX_PARTIAL_AGGREGATION_MEMORY_RATIO =
    buildConf("spark.gluten.sql.columnar.backend.velox.maxPartialAggregationMemoryRatio")
      .internal()
      .doc(
        "Set the max memory of partial aggregation as "
          + "maxPartialAggregationMemoryRatio of offheap size. Note: this option only works when " +
          "flushable partial aggregation is enabled. Ignored when " +
          "spark.gluten.sql.columnar.backend.velox.flushablePartialAggregation=false."
      )
      .doubleConf
      .createWithDefault(0.1)

  val MAX_EXTENDED_PARTIAL_AGGREGATION_MEMORY_RATIO =
    buildConf("spark.gluten.sql.columnar.backend.velox.maxExtendedPartialAggregationMemoryRatio")
      .internal()
      .doc(
        "Set the max extended memory of partial aggregation as "
          + "maxExtendedPartialAggregationMemoryRatio of offheap size. Note: this option only " +
          "works when flushable partial aggregation is enabled. Ignored when " +
          "spark.gluten.sql.columnar.backend.velox.flushablePartialAggregation=false."
      )
      .doubleConf
      .createWithDefault(0.15)

  val ABANDON_PARTIAL_AGGREGATION_MIN_PCT =
    buildConf("spark.gluten.sql.columnar.backend.velox.abandonPartialAggregationMinPct")
      .internal()
      .doc(
        "If partial aggregation aggregationPct greater than this value, "
          + "partial aggregation may be early abandoned. Note: this option only works when " +
          "flushable partial aggregation is enabled. Ignored when " +
          "spark.gluten.sql.columnar.backend.velox.flushablePartialAggregation=false.")
      .intConf
      .createWithDefault(90)

  val ABANDON_PARTIAL_AGGREGATION_MIN_ROWS =
    buildConf("spark.gluten.sql.columnar.backend.velox.abandonPartialAggregationMinRows")
      .internal()
      .doc(
        "If partial aggregation input rows number greater than this value, "
          + " partial aggregation may be early abandoned. Note: this option only works when " +
          "flushable partial aggregation is enabled. Ignored when " +
          "spark.gluten.sql.columnar.backend.velox.flushablePartialAggregation=false.")
      .intConf
      .createWithDefault(100000)

  val COLUMNAR_VELOX_BLOOM_FILTER_EXPECTED_NUM_ITEMS =
    buildConf("spark.gluten.sql.columnar.backend.velox.bloomFilter.expectedNumItems")
      .internal()
      .doc("The default number of expected items for the velox bloomfilter: " +
        "'spark.bloom_filter.expected_num_items'")
      .longConf
      .createWithDefault(1000000L)

  val COLUMNAR_VELOX_BLOOM_FILTER_NUM_BITS =
    buildConf("spark.gluten.sql.columnar.backend.velox.bloomFilter.numBits")
      .internal()
      .doc("The default number of bits to use for the velox bloom filter: " +
        "'spark.bloom_filter.num_bits'")
      .longConf
      .createWithDefault(8388608L)

  val COLUMNAR_VELOX_BLOOM_FILTER_MAX_NUM_BITS =
    buildConf("spark.gluten.sql.columnar.backend.velox.bloomFilter.maxNumBits")
      .internal()
      .doc("The max number of bits to use for the velox bloom filter: " +
        "'spark.bloom_filter.max_num_bits'")
      .longConf
      .createWithDefault(4194304L)

  val COLUMNAR_VELOX_FILE_HANDLE_CACHE_ENABLED =
    buildStaticConf("spark.gluten.sql.columnar.backend.velox.fileHandleCacheEnabled")
      .internal()
      .doc("Disables caching if false. File handle cache should be disabled " +
        "if files are mutable, i.e. file content may change while file path stays the same.")
      .booleanConf
      .createWithDefault(false)

  val DIRECTORY_SIZE_GUESS =
    buildStaticConf("spark.gluten.sql.columnar.backend.velox.directorySizeGuess")
      .internal()
      .doc("Set the directory size guess for velox file scan")
      .bytesConf(ByteUnit.BYTE)
      .createWithDefaultString("32KB")

  val FILE_PRELOAD_THRESHOLD =
    buildStaticConf("spark.gluten.sql.columnar.backend.velox.filePreloadThreshold")
      .internal()
      .doc("Set the file preload threshold for velox file scan")
      .bytesConf(ByteUnit.BYTE)
      .createWithDefaultString("1MB")

  val PREFETCH_ROW_GROUPS =
    buildStaticConf("spark.gluten.sql.columnar.backend.velox.prefetchRowGroups")
      .internal()
      .doc("Set the prefetch row groups for velox file scan")
      .intConf
      .createWithDefault(1)

  val LOAD_QUANTUM =
    buildStaticConf("spark.gluten.sql.columnar.backend.velox.loadQuantum")
      .internal()
      .doc("Set the load quantum for velox file scan, recommend to use the default value (256MB) " +
        "for performance consideration. If Velox cache is enabled, it can be 8MB at most.")
      .bytesConf(ByteUnit.BYTE)
      .createWithDefaultString("256MB")

  val MAX_COALESCED_DISTANCE_BYTES =
    buildStaticConf("spark.gluten.sql.columnar.backend.velox.maxCoalescedDistance")
      .internal()
      .doc(" Set the max coalesced distance bytes for velox file scan")
      .stringConf
      .createWithDefaultString("512KB")

  val MAX_COALESCED_BYTES =
    buildStaticConf("spark.gluten.sql.columnar.backend.velox.maxCoalescedBytes")
      .internal()
      .doc("Set the max coalesced bytes for velox file scan")
      .bytesConf(ByteUnit.BYTE)
      .createWithDefaultString("64MB")

  val CACHE_PREFETCH_MINPCT =
    buildStaticConf("spark.gluten.sql.columnar.backend.velox.cachePrefetchMinPct")
      .internal()
      .doc("Set prefetch cache min pct for velox file scan")
      .intConf
      .createWithDefault(0)

  val AWS_SDK_LOG_LEVEL =
    buildConf("spark.gluten.velox.awsSdkLogLevel")
      .internal()
      .doc("Log granularity of AWS C++ SDK in velox.")
      .stringConf
      .createWithDefault("FATAL")

  val AWS_S3_RETRY_MODE =
    buildConf("spark.gluten.velox.fs.s3a.retry.mode")
      .internal()
      .doc("Retry mode for AWS s3 connection error: legacy, standard and adaptive.")
      .stringConf
      .createWithDefault("legacy")

  val AWS_S3_CONNECT_TIMEOUT =
    buildConf("spark.gluten.velox.fs.s3a.connect.timeout")
      .internal()
      .doc("Timeout for AWS s3 connection.")
      .stringConf
      .createWithDefault("200s")

  val VELOX_ORC_SCAN_ENABLED =
    buildConf("spark.gluten.sql.columnar.backend.velox.orc.scan.enabled")
      .internal()
      .doc("Enable velox orc scan. If disabled, vanilla spark orc scan will be used.")
      .booleanConf
      .createWithDefault(true)

  val CAST_FROM_VARCHAR_ADD_TRIM_NODE =
    buildConf("spark.gluten.velox.castFromVarcharAddTrimNode")
      .internal()
      .doc(
        "If true, will add a trim node " +
          "which has the same sementic as vanilla Spark to CAST-from-varchar." +
          "Otherwise, do nothing.")
      .booleanConf
      .createWithDefault(false)

  val VELOX_BROADCAST_BUILD_RELATION_USE_OFFHEAP =
    buildConf("spark.gluten.velox.offHeapBroadcastBuildRelation.enabled")
      .internal()
      .doc("Experimental: If enabled, broadcast build relation will use offheap memory. " +
        "Otherwise, broadcast build relation will use onheap memory.")
      .booleanConf
      .createWithDefault(false)

  val QUERY_TRACE_ENABLED = buildConf("spark.gluten.sql.columnar.backend.velox.queryTraceEnabled")
    .doc("Enable query tracing flag.")
    .internal()
    .booleanConf
    .createWithDefault(false)

  val QUERY_TRACE_DIR = buildConf("spark.gluten.sql.columnar.backend.velox.queryTraceDir")
    .doc("Base dir of a query to store tracing data.")
    .internal()
    .stringConf
    .createWithDefault("")

  val QUERY_TRACE_NODE_IDS = buildConf("spark.gluten.sql.columnar.backend.velox.queryTraceNodeIds")
    .doc("A comma-separated list of plan node ids whose input data will be traced. " +
      "Empty string if only want to trace the query metadata.")
    .internal()
    .stringConf
    .createWithDefault("")

  val QUERY_TRACE_MAX_BYTES =
    buildConf("spark.gluten.sql.columnar.backend.velox.queryTraceMaxBytes")
      .doc("The max trace bytes limit. Tracing is disabled if zero.")
      .internal()
      .longConf
      .createWithDefault(0)

  val QUERY_TRACE_TASK_REG_EXP =
    buildConf("spark.gluten.sql.columnar.backend.velox.queryTraceTaskRegExp")
      .doc("The regexp of traced task id. We only enable trace on a task if its id matches.")
      .internal()
      .stringConf
      .createWithDefault("")

  val OP_TRACE_DIRECTORY_CREATE_CONFIG =
    buildConf("spark.gluten.sql.columnar.backend.velox.opTraceDirectoryCreateConfig")
      .doc(
        "Config used to create operator trace directory. This config is provided to" +
          " underlying file system and the config is free form. The form should be " +
          "defined by the underlying file system.")
      .internal()
      .stringConf
      .createWithDefault("")
}
