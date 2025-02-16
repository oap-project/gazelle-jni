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

import org.apache.spark.internal.Logging
import org.apache.spark.network.util.{ByteUnit, JavaUtils}
import org.apache.spark.sql.internal.{SQLConf, SQLConfProvider}

import com.google.common.collect.ImmutableList
import org.apache.hadoop.security.UserGroupInformation

import java.util
import java.util.Locale

import scala.collection.JavaConverters._

case class GlutenNumaBindingInfo(
    enableNumaBinding: Boolean,
    totalCoreRange: Array[String] = null,
    numCoresPerExecutor: Int = -1) {}

class GlutenConfig(conf: SQLConf) extends Logging {
  import GlutenConfig._

  private lazy val configProvider = new SQLConfProvider(conf)

  def getConf[T](entry: ConfigEntry[T]): T = {
    require(ConfigEntry.containsEntry(entry), s"$entry is not registered")
    entry.readFrom(configProvider)
  }

  def enableAnsiMode: Boolean = conf.ansiEnabled

  def enableGluten: Boolean = getConf(GLUTEN_ENABLED)

  // FIXME the option currently controls both JVM and native validation against a Substrait plan.
  def enableNativeValidation: Boolean = getConf(NATIVE_VALIDATION_ENABLED)

  def enableColumnarBatchScan: Boolean = getConf(COLUMNAR_BATCHSCAN_ENABLED)

  def enableColumnarFileScan: Boolean = getConf(COLUMNAR_FILESCAN_ENABLED)

  def enableColumnarHiveTableScan: Boolean = getConf(COLUMNAR_HIVETABLESCAN_ENABLED)

  def enableColumnarHiveTableScanNestedColumnPruning: Boolean =
    getConf(COLUMNAR_HIVETABLESCAN_NESTED_COLUMN_PRUNING_ENABLED)

  def enableColumnarHashAgg: Boolean = getConf(COLUMNAR_HASHAGG_ENABLED)

  def forceToUseHashAgg: Boolean = getConf(COLUMNAR_FORCE_HASHAGG_ENABLED)

  def mergeTwoPhasesAggEnabled: Boolean = getConf(MERGE_TWO_PHASES_ENABLED)

  def enableColumnarProject: Boolean = getConf(COLUMNAR_PROJECT_ENABLED)

  def enableColumnarFilter: Boolean = getConf(COLUMNAR_FILTER_ENABLED)

  def enableColumnarSort: Boolean = getConf(COLUMNAR_SORT_ENABLED)

  def enableColumnarWindow: Boolean = getConf(COLUMNAR_WINDOW_ENABLED)

  def enableColumnarWindowGroupLimit: Boolean = getConf(COLUMNAR_WINDOW_GROUP_LIMIT_ENABLED)

  def enableColumnarShuffledHashJoin: Boolean = getConf(COLUMNAR_SHUFFLED_HASH_JOIN_ENABLED)

  def shuffledHashJoinOptimizeBuildSide: Boolean =
    getConf(COLUMNAR_SHUFFLED_HASH_JOIN_OPTIMIZE_BUILD_SIDE)

  def forceShuffledHashJoin: Boolean = getConf(COLUMNAR_FORCE_SHUFFLED_HASH_JOIN_ENABLED)

  def enableColumnarSortMergeJoin: Boolean = getConf(COLUMNAR_SORTMERGEJOIN_ENABLED)

  def enableColumnarUnion: Boolean = getConf(COLUMNAR_UNION_ENABLED)

  def enableNativeUnion: Boolean = getConf(NATIVE_UNION_ENABLED)

  def enableColumnarExpand: Boolean = getConf(COLUMNAR_EXPAND_ENABLED)

  def enableColumnarBroadcastExchange: Boolean = getConf(COLUMNAR_BROADCAST_EXCHANGE_ENABLED)

  def enableColumnarBroadcastJoin: Boolean = getConf(COLUMNAR_BROADCAST_JOIN_ENABLED)

  def enableColumnarSample: Boolean = getConf(COLUMNAR_SAMPLE_ENABLED)

  def enableColumnarArrowUDF: Boolean = getConf(COLUMNAR_ARROW_UDF_ENABLED)

  def enableColumnarCoalesce: Boolean = getConf(COLUMNAR_COALESCE_ENABLED)

  def enableRewriteDateTimestampComparison: Boolean =
    getConf(ENABLE_REWRITE_DATE_TIMESTAMP_COMPARISON)

  def enableCollapseNestedGetJsonObject: Boolean =
    getConf(ENABLE_COLLAPSE_GET_JSON_OBJECT)

  def enableCommonSubexpressionEliminate: Boolean =
    getConf(ENABLE_COMMON_SUBEXPRESSION_ELIMINATE)

  def enableCountDistinctWithoutExpand: Boolean =
    getConf(ENABLE_COUNT_DISTINCT_WITHOUT_EXPAND)

  def enableExtendedColumnPruning: Boolean =
    getConf(ENABLE_EXTENDED_COLUMN_PRUNING)

  def forceOrcCharTypeScanFallbackEnabled: Boolean =
    getConf(VELOX_FORCE_ORC_CHAR_TYPE_SCAN_FALLBACK)

  def scanFileSchemeValidationEnabled: Boolean =
    getConf(VELOX_SCAN_FILE_SCHEME_VALIDATION_ENABLED)

  // Whether to use GlutenShuffleManager (experimental).
  def isUseGlutenShuffleManager: Boolean =
    conf
      .getConfString("spark.shuffle.manager", "sort")
      .equals("org.apache.spark.shuffle.GlutenShuffleManager")

  // Whether to use ColumnarShuffleManager.
  def isUseColumnarShuffleManager: Boolean =
    conf
      .getConfString("spark.shuffle.manager", "sort")
      .equals("org.apache.spark.shuffle.sort.ColumnarShuffleManager")

  // Whether to use CelebornShuffleManager.
  def isUseCelebornShuffleManager: Boolean =
    conf
      .getConfString("spark.shuffle.manager", "sort")
      .contains("celeborn")

  // Whether to use UniffleShuffleManager.
  def isUseUniffleShuffleManager: Boolean =
    conf
      .getConfString("spark.shuffle.manager", "sort")
      .contains("UniffleShuffleManager")

  def celebornShuffleWriterType: String =
    conf
      .getConfString(
        "spark.celeborn.client.spark.shuffle.writer",
        ReservedKeys.GLUTEN_HASH_SHUFFLE_WRITER)
      .toLowerCase(Locale.ROOT)

  def enableColumnarShuffle: Boolean = getConf(COLUMNAR_SHUFFLE_ENABLED)

  def enablePreferColumnar: Boolean = getConf(COLUMNAR_PREFER_ENABLED)

  def physicalJoinOptimizationThrottle: Integer =
    getConf(COLUMNAR_PHYSICAL_JOIN_OPTIMIZATION_THROTTLE)

  def enablePhysicalJoinOptimize: Boolean =
    getConf(COLUMNAR_PHYSICAL_JOIN_OPTIMIZATION_ENABLED)

  def enableScanOnly: Boolean = getConf(COLUMNAR_SCAN_ONLY_ENABLED)

  def columnarShuffleSortPartitionsThreshold: Int =
    getConf(COLUMNAR_SHUFFLE_SORT_PARTITIONS_THRESHOLD)

  def columnarShuffleSortColumnsThreshold: Int =
    getConf(COLUMNAR_SHUFFLE_SORT_COLUMNS_THRESHOLD)

  def columnarShuffleReallocThreshold: Double = getConf(COLUMNAR_SHUFFLE_REALLOC_THRESHOLD)

  def columnarShuffleMergeThreshold: Double = getConf(SHUFFLE_WRITER_MERGE_THRESHOLD)

  def columnarShuffleCodec: Option[String] = getConf(COLUMNAR_SHUFFLE_CODEC)

  def columnarShuffleCompressionMode: String =
    getConf(COLUMNAR_SHUFFLE_COMPRESSION_MODE)

  def columnarShuffleCodecBackend: Option[String] = getConf(COLUMNAR_SHUFFLE_CODEC_BACKEND)
    .filter(Set(GLUTEN_QAT_BACKEND_NAME, GLUTEN_IAA_BACKEND_NAME).contains(_))

  def columnarShuffleEnableQat: Boolean =
    columnarShuffleCodecBackend.contains(GlutenConfig.GLUTEN_QAT_BACKEND_NAME)

  def columnarShuffleEnableIaa: Boolean =
    columnarShuffleCodecBackend.contains(GlutenConfig.GLUTEN_IAA_BACKEND_NAME)

  def columnarShuffleCompressionThreshold: Int =
    getConf(COLUMNAR_SHUFFLE_COMPRESSION_THRESHOLD)

  def columnarShuffleReaderBufferSize: Long =
    getConf(COLUMNAR_SHUFFLE_READER_BUFFER_SIZE)

  def maxBatchSize: Int = getConf(COLUMNAR_MAX_BATCH_SIZE)

  def shuffleWriterBufferSize: Int = getConf(SHUFFLE_WRITER_BUFFER_SIZE)
    .getOrElse(maxBatchSize)

  def enableColumnarLimit: Boolean = getConf(COLUMNAR_LIMIT_ENABLED)

  def enableColumnarGenerate: Boolean = getConf(COLUMNAR_GENERATE_ENABLED)

  def enableTakeOrderedAndProject: Boolean =
    getConf(COLUMNAR_TAKE_ORDERED_AND_PROJECT_ENABLED)

  def enableNativeBloomFilter: Boolean = getConf(COLUMNAR_NATIVE_BLOOMFILTER_ENABLED)

  def enableNativeHyperLogLogAggregateFunction: Boolean =
    getConf(COLUMNAR_NATIVE_HYPERLOGLOG_AGGREGATE_ENABLED)

  def columnarParquetWriteBlockSize: Long =
    getConf(COLUMNAR_PARQUET_WRITE_BLOCK_SIZE)

  def columnarParquetWriteBlockRows: Long =
    getConf(COLUMNAR_PARQUET_WRITE_BLOCK_ROWS)

  def wholeStageFallbackThreshold: Int = getConf(COLUMNAR_WHOLESTAGE_FALLBACK_THRESHOLD)

  def queryFallbackThreshold: Int = getConf(COLUMNAR_QUERY_FALLBACK_THRESHOLD)

  def fallbackIgnoreRowToColumnar: Boolean = getConf(COLUMNAR_FALLBACK_IGNORE_ROW_TO_COLUMNAR)

  def fallbackExpressionsThreshold: Int = getConf(COLUMNAR_FALLBACK_EXPRESSIONS_THRESHOLD)

  def fallbackPreferColumnar: Boolean = getConf(COLUMNAR_FALLBACK_PREFER_COLUMNAR)

  def numaBindingInfo: GlutenNumaBindingInfo = {
    val enableNumaBinding: Boolean = getConf(COLUMNAR_NUMA_BINDING_ENABLED)
    if (!enableNumaBinding) {
      GlutenNumaBindingInfo(enableNumaBinding = false)
    } else {
      val tmp = getConf(COLUMNAR_NUMA_BINDING_CORE_RANGE)
      if (tmp.isEmpty) {
        GlutenNumaBindingInfo(enableNumaBinding = false)
      } else {
        val numCores = conf.getConfString("spark.executor.cores", "1").toInt
        val coreRangeList: Array[String] = tmp.get.split('|').map(_.trim)
        GlutenNumaBindingInfo(enableNumaBinding = true, coreRangeList, numCores)
      }

    }
  }

  def memoryIsolation: Boolean = getConf(COLUMNAR_MEMORY_ISOLATION)

  def offHeapMemorySize: Long = getConf(COLUMNAR_OFFHEAP_SIZE_IN_BYTES)

  def taskOffHeapMemorySize: Long = getConf(COLUMNAR_TASK_OFFHEAP_SIZE_IN_BYTES)

  def memoryOverAcquiredRatio: Double = getConf(COLUMNAR_MEMORY_OVER_ACQUIRED_RATIO)

  def memoryReservationBlockSize: Long = getConf(COLUMNAR_MEMORY_RESERVATION_BLOCK_SIZE)

  def conservativeTaskOffHeapMemorySize: Long =
    getConf(COLUMNAR_CONSERVATIVE_TASK_OFFHEAP_SIZE_IN_BYTES)

  // Options used by RAS.
  def enableRas: Boolean = getConf(RAS_ENABLED)

  def rasCostModel: String = getConf(RAS_COST_MODEL)

  def cartesianProductTransformerEnabled: Boolean =
    getConf(CARTESIAN_PRODUCT_TRANSFORMER_ENABLED)

  def broadcastNestedLoopJoinTransformerTransformerEnabled: Boolean =
    getConf(BROADCAST_NESTED_LOOP_JOIN_TRANSFORMER_ENABLED)

  def transformPlanLogLevel: String = getConf(TRANSFORM_PLAN_LOG_LEVEL)

  def substraitPlanLogLevel: String = getConf(SUBSTRAIT_PLAN_LOG_LEVEL)

  def validationLogLevel: String = getConf(VALIDATION_LOG_LEVEL)

  def softAffinityLogLevel: String = getConf(SOFT_AFFINITY_LOG_LEVEL)

  // A comma-separated list of classes for the extended columnar pre rules
  def extendedColumnarTransformRules: String = getConf(EXTENDED_COLUMNAR_TRANSFORM_RULES)

  // A comma-separated list of classes for the extended columnar post rules
  def extendedColumnarPostRules: String = getConf(EXTENDED_COLUMNAR_POST_RULES)

  def extendedExpressionTransformer: String = getConf(EXTENDED_EXPRESSION_TRAN_CONF)

  def expressionBlacklist: Set[String] = {
    val blacklist = getConf(EXPRESSION_BLACK_LIST)
    val blacklistSet: Set[String] = if (blacklist.isDefined) {
      blacklist.get.toLowerCase(Locale.ROOT).trim.split(",").toSet
    } else {
      Set.empty
    }

    if (getConf(FALLBACK_REGEXP_EXPRESSIONS)) {
      val regexpList = "rlike,regexp_replace,regexp_extract,regexp_extract_all,split"
      regexpList.trim.split(",").toSet ++ blacklistSet
    } else {
      blacklistSet
    }
  }

  def printStackOnValidationFailure: Boolean =
    getConf(VALIDATION_PRINT_FAILURE_STACK_)

  def enableFallbackReport: Boolean = getConf(FALLBACK_REPORTER_ENABLED)

  def debug: Boolean = getConf(DEBUG_ENABLED)

  def collectUtStats: Boolean = getConf(UT_STATISTIC)

  def benchmarkStageId: Int = getConf(BENCHMARK_TASK_STAGEID)

  def benchmarkPartitionId: String = getConf(BENCHMARK_TASK_PARTITIONID)

  def benchmarkTaskId: String = getConf(BENCHMARK_TASK_TASK_ID)

  def benchmarkSaveDir: String = getConf(BENCHMARK_SAVE_DIR)

  def textInputMaxBlockSize: Long = getConf(TEXT_INPUT_ROW_MAX_BLOCK_SIZE)

  def textIputEmptyAsDefault: Boolean = getConf(TEXT_INPUT_EMPTY_AS_DEFAULT)

  def enableParquetRowGroupMaxMinIndex: Boolean =
    getConf(ENABLE_PARQUET_ROW_GROUP_MAX_MIN_INDEX)

  // Please use `BackendsApiManager.getSettings.enableNativeWriteFiles()` instead
  def enableNativeWriter: Option[Boolean] = getConf(NATIVE_WRITER_ENABLED)

  def enableNativeArrowReader: Boolean = getConf(NATIVE_ARROW_READER_ENABLED)

  def enableColumnarProjectCollapse: Boolean = getConf(ENABLE_COLUMNAR_PROJECT_COLLAPSE)

  def enableColumnarPartialProject: Boolean = getConf(ENABLE_COLUMNAR_PARTIAL_PROJECT)

  def enableCastAvgAggregateFunction: Boolean = getConf(COLUMNAR_NATIVE_CAST_AGGREGATE_ENABLED)

  def dynamicOffHeapSizingEnabled: Boolean =
    getConf(DYNAMIC_OFFHEAP_SIZING_ENABLED)

  def enableHiveFileFormatWriter: Boolean = getConf(NATIVE_HIVEFILEFORMAT_WRITER_ENABLED)

  def enableCelebornFallback: Boolean = getConf(CELEBORN_FALLBACK_ENABLED)

  def enableHdfsViewfs: Boolean = getConf(HDFS_VIEWFS_ENABLED)

  def parquetEncryptionValidationEnabled: Boolean = getConf(ENCRYPTED_PARQUET_FALLBACK_ENABLED)

  def enableAutoAdjustStageResourceProfile: Boolean =
    getConf(AUTO_ADJUST_STAGE_RESOURCE_PROFILE_ENABLED)

  def autoAdjustStageRPHeapRatio: Double = getConf(AUTO_ADJUST_STAGE_RESOURCES_HEAP_RATIO)

  def autoAdjustStageFallenNodeThreshold: Double =
    getConf(AUTO_ADJUST_STAGE_RESOURCES_FALLEN_NODE_RATIO_THRESHOLD)

  def parquetEncryptionValidationFileLimit: Int = getConf(ENCRYPTED_PARQUET_FALLBACK_FILE_LIMIT)
}

object GlutenConfig {
  import SQLConf._

  def buildConf(key: String): ConfigBuilder = ConfigBuilder(key)

  def buildStaticConf(key: String): ConfigBuilder = {
    ConfigBuilder(key).onCreate(_ => SQLConf.registerStaticConfigKey(key))
  }

  // Hive configurations.
  val SPARK_SQL_PARQUET_COMPRESSION_CODEC: String = "spark.sql.parquet.compression.codec"
  val PARQUET_BLOCK_SIZE: String = "parquet.block.size"
  val PARQUET_BLOCK_ROWS: String = "parquet.block.rows"
  val PARQUET_GZIP_WINDOW_SIZE: String = "parquet.gzip.windowSize"
  // Hadoop config
  val HADOOP_PREFIX = "spark.hadoop."

  // S3 config
  val S3A_PREFIX = "fs.s3a."
  val S3_ACCESS_KEY = "fs.s3a.access.key"
  val SPARK_S3_ACCESS_KEY: String = HADOOP_PREFIX + S3_ACCESS_KEY
  val S3_SECRET_KEY = "fs.s3a.secret.key"
  val SPARK_S3_SECRET_KEY: String = HADOOP_PREFIX + S3_SECRET_KEY
  val S3_ENDPOINT = "fs.s3a.endpoint"
  val SPARK_S3_ENDPOINT: String = HADOOP_PREFIX + S3_ENDPOINT
  val S3_CONNECTION_SSL_ENABLED = "fs.s3a.connection.ssl.enabled"
  val SPARK_S3_CONNECTION_SSL_ENABLED: String = HADOOP_PREFIX + S3_CONNECTION_SSL_ENABLED
  val S3_PATH_STYLE_ACCESS = "fs.s3a.path.style.access"
  val SPARK_S3_PATH_STYLE_ACCESS: String = HADOOP_PREFIX + S3_PATH_STYLE_ACCESS
  val S3_USE_INSTANCE_CREDENTIALS = "fs.s3a.use.instance.credentials"
  val SPARK_S3_USE_INSTANCE_CREDENTIALS: String = HADOOP_PREFIX + S3_USE_INSTANCE_CREDENTIALS
  val S3_IAM_ROLE = "fs.s3a.iam.role"
  val SPARK_S3_IAM: String = HADOOP_PREFIX + S3_IAM_ROLE
  val S3_IAM_ROLE_SESSION_NAME = "fs.s3a.iam.role.session.name"
  val SPARK_S3_IAM_SESSION_NAME: String = HADOOP_PREFIX + S3_IAM_ROLE_SESSION_NAME
  val S3_RETRY_MAX_ATTEMPTS = "fs.s3a.retry.limit"
  val SPARK_S3_RETRY_MAX_ATTEMPTS: String = HADOOP_PREFIX + S3_RETRY_MAX_ATTEMPTS
  val S3_CONNECTION_MAXIMUM = "fs.s3a.connection.maximum"
  val SPARK_S3_CONNECTION_MAXIMUM: String = HADOOP_PREFIX + S3_CONNECTION_MAXIMUM

  // ABFS config
  val ABFS_PREFIX = "fs.azure."

  // GCS config
  val GCS_PREFIX = "fs.gs."
  val STORAGE_ROOT_URL = "storage.root.url"
  val AUTH_TYPE = "auth.type"
  val AUTH_SERVICE_ACCOUNT_JSON_KEYFILE = "auth.service.account.json.keyfile"
  val SPARK_GCS_STORAGE_ROOT_URL: String = HADOOP_PREFIX + GCS_PREFIX + STORAGE_ROOT_URL
  val SPARK_GCS_AUTH_TYPE: String = HADOOP_PREFIX + GCS_PREFIX + AUTH_TYPE
  val SPARK_GCS_AUTH_SERVICE_ACCOUNT_JSON_KEYFILE: String =
    HADOOP_PREFIX + GCS_PREFIX + AUTH_SERVICE_ACCOUNT_JSON_KEYFILE

  // QAT config
  val GLUTEN_QAT_BACKEND_NAME = "qat"
  val GLUTEN_QAT_SUPPORTED_CODEC: Set[String] = Set("gzip", "zstd")
  // IAA config
  val GLUTEN_IAA_BACKEND_NAME = "iaa"
  val GLUTEN_IAA_SUPPORTED_CODEC: Set[String] = Set("gzip")

  private val GLUTEN_CONFIG_PREFIX = "spark.gluten.sql.columnar.backend."

  // Private Spark configs.
  val SPARK_ONHEAP_SIZE_KEY = "spark.executor.memory"
  val SPARK_OVERHEAD_SIZE_KEY = "spark.executor.memoryOverhead"
  val SPARK_OVERHEAD_FACTOR_KEY = "spark.executor.memoryOverheadFactor"
  val SPARK_OFFHEAP_SIZE_KEY = "spark.memory.offHeap.size"
  val SPARK_OFFHEAP_ENABLED = "spark.memory.offHeap.enabled"
  val SPARK_REDACTION_REGEX = "spark.redaction.regex"
  val SPARK_SHUFFLE_FILE_BUFFER = "spark.shuffle.file.buffer"
  val SPARK_UNSAFE_SORTER_SPILL_READER_BUFFER_SIZE = "spark.unsafe.sorter.spill.reader.buffer.size"
  val SPARK_UNSAFE_SORTER_SPILL_READER_BUFFER_SIZE_DEFAULT: Int = 1024 * 1024
  val SPARK_SHUFFLE_SPILL_DISK_WRITE_BUFFER_SIZE = "spark.shuffle.spill.diskWriteBufferSize"
  val SPARK_SHUFFLE_SPILL_DISK_WRITE_BUFFER_SIZE_DEFAULT: Int = 1024 * 1024
  val SPARK_SHUFFLE_SPILL_COMPRESS = "spark.shuffle.spill.compress"
  val SPARK_SHUFFLE_SPILL_COMPRESS_DEFAULT: Boolean = true

  def get: GlutenConfig = {
    new GlutenConfig(SQLConf.get)
  }

  def prefixOf(backendName: String): String = {
    GLUTEN_CONFIG_PREFIX + backendName
  }

  /**
   * Get dynamic configs.
   *
   * TODO: Improve the get native conf logic.
   */
  def getNativeSessionConf(
      backendName: String,
      conf: scala.collection.Map[String, String]): util.Map[String, String] = {
    val nativeConfMap = new util.HashMap[String, String]()
    val keys = Set(
      DEBUG_ENABLED.key,
      BENCHMARK_SAVE_DIR.key,
      COLUMNAR_TASK_OFFHEAP_SIZE_IN_BYTES.key,
      COLUMNAR_MAX_BATCH_SIZE.key,
      SHUFFLE_WRITER_BUFFER_SIZE.key,
      SQLConf.SESSION_LOCAL_TIMEZONE.key,
      GLUTEN_DEFAULT_SESSION_TIMEZONE.key,
      SQLConf.LEGACY_SIZE_OF_NULL.key,
      SQLConf.LEGACY_TIME_PARSER_POLICY.key,
      "spark.io.compression.codec",
      "spark.sql.decimalOperations.allowPrecisionLoss",
      "spark.gluten.sql.columnar.backend.velox.bloomFilter.expectedNumItems",
      "spark.gluten.sql.columnar.backend.velox.bloomFilter.numBits",
      "spark.gluten.sql.columnar.backend.velox.bloomFilter.maxNumBits",
      // s3 config
      SPARK_S3_ACCESS_KEY,
      SPARK_S3_SECRET_KEY,
      SPARK_S3_ENDPOINT,
      SPARK_S3_CONNECTION_SSL_ENABLED,
      SPARK_S3_PATH_STYLE_ACCESS,
      SPARK_S3_USE_INSTANCE_CREDENTIALS,
      SPARK_S3_IAM,
      SPARK_S3_IAM_SESSION_NAME,
      SPARK_S3_RETRY_MAX_ATTEMPTS,
      SPARK_S3_CONNECTION_MAXIMUM,
      "spark.gluten.velox.fs.s3a.connect.timeout",
      "spark.gluten.velox.fs.s3a.retry.mode",
      "spark.gluten.velox.awsSdkLogLevel",
      // gcs config
      SPARK_GCS_STORAGE_ROOT_URL,
      SPARK_GCS_AUTH_TYPE,
      SPARK_GCS_AUTH_SERVICE_ACCOUNT_JSON_KEYFILE,
      SPARK_REDACTION_REGEX,
      "spark.gluten.sql.columnar.backend.velox.queryTraceEnabled",
      "spark.gluten.sql.columnar.backend.velox.queryTraceDir",
      "spark.gluten.sql.columnar.backend.velox.queryTraceNodeIds",
      "spark.gluten.sql.columnar.backend.velox.queryTraceMaxBytes",
      "spark.gluten.sql.columnar.backend.velox.queryTraceTaskRegExp",
      "spark.gluten.sql.columnar.backend.velox.opTraceDirectoryCreateConfig"
    )
    nativeConfMap.putAll(conf.filter(e => keys.contains(e._1)).asJava)

    val keyWithDefault = ImmutableList.of(
      (SQLConf.CASE_SENSITIVE.key, SQLConf.CASE_SENSITIVE.defaultValueString),
      (SQLConf.IGNORE_MISSING_FILES.key, SQLConf.IGNORE_MISSING_FILES.defaultValueString),
      (SQLConf.LEGACY_TIME_PARSER_POLICY.key, SQLConf.LEGACY_TIME_PARSER_POLICY.defaultValueString),
      (
        COLUMNAR_MEMORY_BACKTRACE_ALLOCATION.key,
        COLUMNAR_MEMORY_BACKTRACE_ALLOCATION.defaultValueString),
      (
        GLUTEN_COLUMNAR_TO_ROW_MEM_THRESHOLD.key,
        GLUTEN_COLUMNAR_TO_ROW_MEM_THRESHOLD.defaultValue.get.toString),
      (
        SPARK_UNSAFE_SORTER_SPILL_READER_BUFFER_SIZE,
        SPARK_UNSAFE_SORTER_SPILL_READER_BUFFER_SIZE_DEFAULT.toString),
      (
        SPARK_SHUFFLE_SPILL_DISK_WRITE_BUFFER_SIZE,
        SPARK_SHUFFLE_SPILL_DISK_WRITE_BUFFER_SIZE_DEFAULT.toString),
      (SPARK_SHUFFLE_SPILL_COMPRESS, SPARK_SHUFFLE_SPILL_COMPRESS_DEFAULT.toString)
    )
    keyWithDefault.forEach(e => nativeConfMap.put(e._1, conf.getOrElse(e._1, e._2)))

    conf
      .get(SPARK_SHUFFLE_FILE_BUFFER)
      .foreach(
        v =>
          nativeConfMap
            .put(
              SPARK_SHUFFLE_FILE_BUFFER,
              (JavaUtils.byteStringAs(v, ByteUnit.KiB) * 1024).toString))

    conf
      .get(LEGACY_TIME_PARSER_POLICY.key)
      .foreach(
        v =>
          nativeConfMap
            .put(LEGACY_TIME_PARSER_POLICY.key, v.toUpperCase(Locale.ROOT)))

    // Backend's dynamic session conf only.
    val confPrefix = prefixOf(backendName)
    conf
      .filter(entry => entry._1.startsWith(confPrefix) && !SQLConf.isStaticConfigKey(entry._1))
      .foreach(entry => nativeConfMap.put(entry._1, entry._2))

    // Pass the latest tokens to native
    nativeConfMap.put(
      ReservedKeys.GLUTEN_UGI_TOKENS,
      UserGroupInformation.getCurrentUser.getTokens.asScala
        .map(_.encodeToUrlString)
        .mkString("\u0000"))
    nativeConfMap.put(
      ReservedKeys.GLUTEN_UGI_USERNAME,
      UserGroupInformation.getCurrentUser.getUserName)

    // return
    nativeConfMap
  }

  /**
   * Get static and dynamic configs. Some of the config is dynamic in spark, but is static in
   * gluten, these will be used to construct HiveConnector which intends reused in velox
   *
   * TODO: Improve the get native conf logic.
   */
  def getNativeBackendConf(
      backendName: String,
      conf: scala.collection.Map[String, String]): util.Map[String, String] = {

    val nativeConfMap = new util.HashMap[String, String]()

    // some configs having default values
    val keyWithDefault = ImmutableList.of(
      (SPARK_S3_CONNECTION_SSL_ENABLED, "false"),
      (SPARK_S3_PATH_STYLE_ACCESS, "true"),
      (SPARK_S3_USE_INSTANCE_CREDENTIALS, "false"),
      (SPARK_S3_RETRY_MAX_ATTEMPTS, "20"),
      (SPARK_S3_CONNECTION_MAXIMUM, "15"),
      ("spark.gluten.velox.fs.s3a.connect.timeout", "200s"),
      ("spark.gluten.velox.fs.s3a.retry.mode", "legacy"),
      (
        "spark.gluten.sql.columnar.backend.velox.IOThreads",
        conf.getOrElse(
          NUM_TASK_SLOTS_PER_EXECUTOR.key,
          NUM_TASK_SLOTS_PER_EXECUTOR.defaultValueString)),
      (COLUMNAR_SHUFFLE_CODEC.key, ""),
      (COLUMNAR_SHUFFLE_CODEC_BACKEND.key, ""),
      ("spark.hadoop.input.connect.timeout", "180000"),
      ("spark.hadoop.input.read.timeout", "180000"),
      ("spark.hadoop.input.write.timeout", "180000"),
      ("spark.hadoop.dfs.client.log.severity", "INFO"),
      ("spark.sql.orc.compression.codec", "snappy"),
      ("spark.sql.decimalOperations.allowPrecisionLoss", "true"),
      ("spark.gluten.sql.columnar.backend.velox.fileHandleCacheEnabled", "false"),
      ("spark.gluten.velox.awsSdkLogLevel", "FATAL")
    )
    keyWithDefault.forEach(e => nativeConfMap.put(e._1, conf.getOrElse(e._1, e._2)))

    val keys = Set(
      DEBUG_ENABLED.key,
      // datasource config
      SPARK_SQL_PARQUET_COMPRESSION_CODEC,
      // datasource config end

      COLUMNAR_OVERHEAD_SIZE_IN_BYTES.key,
      COLUMNAR_OFFHEAP_SIZE_IN_BYTES.key,
      COLUMNAR_TASK_OFFHEAP_SIZE_IN_BYTES.key,
      SPARK_OFFHEAP_ENABLED,
      SESSION_LOCAL_TIMEZONE.key,
      DECIMAL_OPERATIONS_ALLOW_PREC_LOSS.key,
      SPARK_REDACTION_REGEX,
      LEGACY_TIME_PARSER_POLICY.key
    )
    nativeConfMap.putAll(conf.filter(e => keys.contains(e._1)).asJava)

    val confPrefix = prefixOf(backendName)
    conf
      .filter(_._1.startsWith(confPrefix))
      .foreach(entry => nativeConfMap.put(entry._1, entry._2))

    // put in all S3 configs
    conf
      .filter(_._1.startsWith(HADOOP_PREFIX + S3A_PREFIX))
      .foreach(entry => nativeConfMap.put(entry._1, entry._2))

    // handle ABFS config
    conf
      .filter(_._1.startsWith(HADOOP_PREFIX + ABFS_PREFIX))
      .foreach(entry => nativeConfMap.put(entry._1, entry._2))

    // put in all GCS configs
    conf
      .filter(_._1.startsWith(HADOOP_PREFIX + GCS_PREFIX))
      .foreach(entry => nativeConfMap.put(entry._1, entry._2))

    // return
    nativeConfMap
  }

  val GLUTEN_ENABLED =
    buildConf("spark.gluten.enabled")
      .internal()
      .doc("Whether to enable gluten. Default value is true. Just an experimental property." +
        " Recommend to enable/disable Gluten through the setting for spark.plugins.")
      .booleanConf
      .createWithDefault(true)

  val GLUTEN_UI_ENABLED = buildStaticConf("spark.gluten.ui.enabled")
    .doc(
      "Whether to enable the gluten web UI, If true, attach the gluten UI page " +
        "to the Spark web UI.")
    .booleanConf
    .createWithDefault(true)

  val GLUTEN_DEFAULT_SESSION_TIMEZONE = buildConf("spark.gluten.sql.session.timeZone.default")
    .doc(
      "used to hold default session timezone and will be really used by Gluten only if " +
        "`spark.sql.session.timeZone` is not set.")
    .stringConf
    .createWithDefaultString(SQLConf.SESSION_LOCAL_TIMEZONE.defaultValueString)

  val GLUTEN_LOAD_LIB_OS =
    buildConf("spark.gluten.loadLibOS")
      .doc("The shared library loader's OS name.")
      .stringConf
      .createOptional

  val GLUTEN_LOAD_LIB_OS_VERSION =
    buildConf("spark.gluten.loadLibOSVersion")
      .doc("The shared library loader's OS version.")
      .stringConf
      .createOptional

  val GLUTEN_LOAD_LIB_FROM_JAR =
    buildConf("spark.gluten.loadLibFromJar")
      .doc("Whether to load shared libraries from jars.")
      .booleanConf
      .createWithDefault(false)

  val GLUTEN_RESOURCE_RELATION_EXPIRED_TIME =
    buildConf("spark.gluten.execution.resource.expired.time")
      .doc("Expired time of execution with resource relation has cached.")
      .intConf
      .createWithDefault(86400)

  val GLUTEN_SUPPORTED_HIVE_UDFS = buildConf("spark.gluten.supported.hive.udfs")
    .doc("Supported hive udf names.")
    .stringConf
    .createWithDefault("")

  val GLUTEN_SUPPORTED_PYTHON_UDFS = buildConf("spark.gluten.supported.python.udfs")
    .doc("Supported python udf names.")
    .stringConf
    .createWithDefault("")

  val GLUTEN_SUPPORTED_SCALA_UDFS = buildConf("spark.gluten.supported.scala.udfs")
    .doc("Supported scala udf names.")
    .stringConf
    .createWithDefault("")

  val GLUTEN_SOFT_AFFINITY_ENABLED =
    buildConf("spark.gluten.soft-affinity.enabled")
      .doc("Whether to enable Soft Affinity scheduling.")
      .booleanConf
      .createWithDefault(false)

  val GLUTEN_SOFT_AFFINITY_REPLICATIONS_NUM =
    buildConf("spark.gluten.soft-affinity.replications.num")
      .doc(
        "Calculate the number of the replications for scheduling to the target executors per file")
      .intConf
      .createWithDefault(2)

  val GLUTEN_SOFT_AFFINITY_MIN_TARGET_HOSTS =
    buildConf("spark.gluten.soft-affinity.min.target-hosts")
      .doc(
        "For on HDFS, if there are already target hosts, and then prefer to use the " +
          "original target hosts to schedule")
      .intConf
      .createWithDefault(1)

  val GLUTEN_SOFT_AFFINITY_DUPLICATE_READING_DETECT_ENABLED =
    buildConf("spark.gluten.soft-affinity.duplicateReadingDetect.enabled")
      .doc("If true, Enable Soft Affinity duplicate reading detection")
      .booleanConf
      .createWithDefault(false)

  val GLUTEN_SOFT_AFFINITY_DUPLICATE_READING_MAX_CACHE_ITEMS =
    buildConf("spark.gluten.soft-affinity.duplicateReading.maxCacheItems")
      .doc("Enable Soft Affinity duplicate reading detection")
      .intConf
      .createWithDefault(10000)

  val GLUTEN_LIB_NAME =
    buildConf("spark.gluten.sql.columnar.libname")
      .doc("The gluten library name.")
      .stringConf
      .createWithDefault("gluten")

  val GLUTEN_LIB_PATH =
    buildConf("spark.gluten.sql.columnar.libpath")
      .doc("The gluten library path.")
      .stringConf
      .createWithDefault("")

  val GLUTEN_EXECUTOR_LIB_PATH =
    buildConf("spark.gluten.sql.columnar.executor.libpath")
      .doc("The gluten executor library path.")
      .stringConf
      .createWithDefault("")

  // FIXME the option currently controls both JVM and native validation against a Substrait plan.
  val NATIVE_VALIDATION_ENABLED =
    buildConf("spark.gluten.sql.enable.native.validation")
      .internal()
      .doc(
        "This is tmp config to specify whether to enable the native validation based on " +
          "Substrait plan. After the validations in all backends are correctly implemented, " +
          "this config should be removed.")
      .booleanConf
      .createWithDefault(true)

  val COLUMNAR_BATCHSCAN_ENABLED =
    buildConf("spark.gluten.sql.columnar.batchscan")
      .internal()
      .doc("Enable or disable columnar batchscan.")
      .booleanConf
      .createWithDefault(true)

  val COLUMNAR_FILESCAN_ENABLED =
    buildConf("spark.gluten.sql.columnar.filescan")
      .internal()
      .doc("Enable or disable columnar filescan.")
      .booleanConf
      .createWithDefault(true)

  val COLUMNAR_HIVETABLESCAN_ENABLED =
    buildConf("spark.gluten.sql.columnar.hivetablescan")
      .internal()
      .doc("Enable or disable columnar hivetablescan.")
      .booleanConf
      .createWithDefault(true)

  val COLUMNAR_HIVETABLESCAN_NESTED_COLUMN_PRUNING_ENABLED =
    buildConf("spark.gluten.sql.columnar.enableNestedColumnPruningInHiveTableScan")
      .internal()
      .doc("Enable or disable nested column pruning in hivetablescan.")
      .booleanConf
      .createWithDefault(true)

  val VANILLA_VECTORIZED_READERS_ENABLED =
    buildStaticConf("spark.gluten.sql.columnar.enableVanillaVectorizedReaders")
      .internal()
      .doc("Enable or disable vanilla vectorized scan.")
      .booleanConf
      .createWithDefault(true)

  val COLUMNAR_HASHAGG_ENABLED =
    buildConf("spark.gluten.sql.columnar.hashagg")
      .internal()
      .doc("Enable or disable columnar hashagg.")
      .booleanConf
      .createWithDefault(true)

  val COLUMNAR_FORCE_HASHAGG_ENABLED =
    buildConf("spark.gluten.sql.columnar.force.hashagg")
      .internal()
      .doc("Whether to force to use gluten's hash agg for replacing vanilla spark's sort agg.")
      .booleanConf
      .createWithDefault(true)

  val MERGE_TWO_PHASES_ENABLED =
    buildConf("spark.gluten.sql.mergeTwoPhasesAggregate.enabled")
      .internal()
      .doc("Whether to merge two phases aggregate if there are no other operators between them.")
      .booleanConf
      .createWithDefault(true)

  val COLUMNAR_PROJECT_ENABLED =
    buildConf("spark.gluten.sql.columnar.project")
      .internal()
      .doc("Enable or disable columnar project.")
      .booleanConf
      .createWithDefault(true)

  val COLUMNAR_FILTER_ENABLED =
    buildConf("spark.gluten.sql.columnar.filter")
      .internal()
      .doc("Enable or disable columnar filter.")
      .booleanConf
      .createWithDefault(true)

  val COLUMNAR_SORT_ENABLED =
    buildConf("spark.gluten.sql.columnar.sort")
      .internal()
      .doc("Enable or disable columnar sort.")
      .booleanConf
      .createWithDefault(true)

  val COLUMNAR_WINDOW_ENABLED =
    buildConf("spark.gluten.sql.columnar.window")
      .internal()
      .doc("Enable or disable columnar window.")
      .booleanConf
      .createWithDefault(true)

  val COLUMNAR_WINDOW_GROUP_LIMIT_ENABLED =
    buildConf("spark.gluten.sql.columnar.window.group.limit")
      .internal()
      .doc("Enable or disable columnar window group limit.")
      .booleanConf
      .createWithDefault(true)

  val COLUMNAR_PREFER_STREAMING_AGGREGATE =
    buildConf("spark.gluten.sql.columnar.preferStreamingAggregate")
      .internal()
      .doc(
        "Velox backend supports `StreamingAggregate`. `StreamingAggregate` uses the less " +
          "memory as it does not need to hold all groups in memory, so it could avoid spill. " +
          "When true and the child output ordering satisfies the grouping key then " +
          "Gluten will choose `StreamingAggregate` as the native operator.")
      .booleanConf
      .createWithDefault(true)

  val COLUMNAR_FORCE_SHUFFLED_HASH_JOIN_ENABLED =
    buildConf("spark.gluten.sql.columnar.forceShuffledHashJoin")
      .internal()
      .booleanConf
      .createWithDefault(true)

  val COLUMNAR_SHUFFLED_HASH_JOIN_ENABLED =
    buildConf("spark.gluten.sql.columnar.shuffledHashJoin")
      .internal()
      .doc("Enable or disable columnar shuffledHashJoin.")
      .booleanConf
      .createWithDefault(true)

  val COLUMNAR_SHUFFLED_HASH_JOIN_OPTIMIZE_BUILD_SIDE =
    buildConf("spark.gluten.sql.columnar.shuffledHashJoin.optimizeBuildSide")
      .internal()
      .doc("Whether to allow Gluten to choose an optimal build side for shuffled hash join.")
      .booleanConf
      .createWithDefault(true)

  val COLUMNAR_SORTMERGEJOIN_ENABLED =
    buildConf("spark.gluten.sql.columnar.sortMergeJoin")
      .internal()
      .doc("Enable or disable columnar sortMergeJoin. " +
        "This should be set with preferSortMergeJoin=false.")
      .booleanConf
      .createWithDefault(true)

  val COLUMNAR_UNION_ENABLED =
    buildConf("spark.gluten.sql.columnar.union")
      .internal()
      .doc("Enable or disable columnar union.")
      .booleanConf
      .createWithDefault(true)

  val NATIVE_UNION_ENABLED =
    buildConf("spark.gluten.sql.native.union")
      .internal()
      .doc("Enable or disable native union where computation is completely offloaded to backend.")
      .booleanConf
      .createWithDefault(false)

  val COLUMNAR_EXPAND_ENABLED =
    buildConf("spark.gluten.sql.columnar.expand")
      .internal()
      .doc("Enable or disable columnar expand.")
      .booleanConf
      .createWithDefault(true)

  val COLUMNAR_BROADCAST_EXCHANGE_ENABLED =
    buildConf("spark.gluten.sql.columnar.broadcastExchange")
      .internal()
      .doc("Enable or disable columnar broadcastExchange.")
      .booleanConf
      .createWithDefault(true)

  val COLUMNAR_BROADCAST_JOIN_ENABLED =
    buildConf("spark.gluten.sql.columnar.broadcastJoin")
      .internal()
      .doc("Enable or disable columnar broadcastJoin.")
      .booleanConf
      .createWithDefault(true)

  val COLUMNAR_ARROW_UDF_ENABLED =
    buildConf("spark.gluten.sql.columnar.arrowUdf")
      .internal()
      .doc("Enable or disable columnar arrow udf.")
      .booleanConf
      .createWithDefault(true)

  val COLUMNAR_COALESCE_ENABLED =
    buildConf("spark.gluten.sql.columnar.coalesce")
      .internal()
      .doc("Enable or disable columnar coalesce.")
      .booleanConf
      .createWithDefault(true)

  val COLUMNAR_SHUFFLE_ENABLED =
    buildConf("spark.gluten.sql.columnar.shuffle")
      .internal()
      .doc("Enable or disable columnar shuffle.")
      .booleanConf
      .createWithDefault(true)

  val COLUMNAR_SHUFFLE_SORT_PARTITIONS_THRESHOLD =
    buildConf("spark.gluten.sql.columnar.shuffle.sort.partitions.threshold")
      .internal()
      .doc("The threshold to determine whether to use sort-based columnar shuffle. Sort-based " +
        "shuffle will be used if the number of partitions is greater than this threshold.")
      .intConf
      .createWithDefault(100000)

  val COLUMNAR_SHUFFLE_SORT_COLUMNS_THRESHOLD =
    buildConf("spark.gluten.sql.columnar.shuffle.sort.columns.threshold")
      .internal()
      .doc("The threshold to determine whether to use sort-based columnar shuffle. Sort-based " +
        "shuffle will be used if the number of columns is greater than this threshold.")
      .intConf
      .createWithDefault(100000)

  val COLUMNAR_PREFER_ENABLED =
    buildConf("spark.gluten.sql.columnar.preferColumnar")
      .internal()
      .doc("Prefer to use columnar operators if set to true.")
      .booleanConf
      .createWithDefault(true)

  val COLUMNAR_TABLE_CACHE_ENABLED =
    buildConf("spark.gluten.sql.columnar.tableCache")
      .internal()
      .doc("Enable or disable columnar table cache.")
      .booleanConf
      .createWithDefault(false)

  val COLUMNAR_PHYSICAL_JOIN_OPTIMIZATION_THROTTLE =
    buildConf("spark.gluten.sql.columnar.physicalJoinOptimizationLevel")
      .internal()
      .doc("Fallback to row operators if there are several continuous joins.")
      .intConf
      .createWithDefault(12)

  val COLUMNAR_PHYSICAL_JOIN_OPTIMIZATION_ENABLED =
    buildConf("spark.gluten.sql.columnar.physicalJoinOptimizeEnable")
      .internal()
      .doc("Enable or disable columnar physicalJoinOptimize.")
      .booleanConf
      .createWithDefault(false)

  val COLUMNAR_SCAN_ONLY_ENABLED =
    buildConf("spark.gluten.sql.columnar.scanOnly")
      .internal()
      .doc("When enabled, only scan and the filter after scan will be offloaded to native.")
      .booleanConf
      .createWithDefault(false)

  val COLUMNAR_TEMP_DIR =
    buildConf("spark.gluten.sql.columnar.tmp_dir")
      .internal()
      .doc("A folder to store the codegen files.")
      .stringConf
      .createOptional

  val COLUMNAR_SHUFFLE_REALLOC_THRESHOLD =
    buildConf("spark.gluten.sql.columnar.shuffle.realloc.threshold")
      .internal()
      .doubleConf
      .checkValue(v => v >= 0 && v <= 1, "Buffer reallocation threshold must between [0, 1]")
      .createWithDefault(0.25)

  val COLUMNAR_SHUFFLE_CODEC =
    buildConf("spark.gluten.sql.columnar.shuffle.codec")
      .internal()
      .doc(
        "By default, the supported codecs are lz4 and zstd. " +
          "When spark.gluten.sql.columnar.shuffle.codecBackend=qat," +
          "the supported codecs are gzip and zstd. " +
          "When spark.gluten.sql.columnar.shuffle.codecBackend=iaa," +
          "the supported codec is gzip.")
      .stringConf
      .transform(_.toLowerCase(Locale.ROOT))
      .createOptional

  val COLUMNAR_SHUFFLE_CODEC_BACKEND =
    buildConf("spark.gluten.sql.columnar.shuffle.codecBackend")
      .internal()
      .stringConf
      .transform(_.toLowerCase(Locale.ROOT))
      .createOptional

  val COLUMNAR_SHUFFLE_COMPRESSION_MODE =
    buildConf("spark.gluten.sql.columnar.shuffle.compressionMode")
      .internal()
      .doc("buffer means compress each buffer to pre allocated big buffer," +
        "rowvector means to copy the buffers to a big buffer, and then compress the buffer")
      .stringConf
      .checkValues(Set("buffer", "rowvector"))
      .createWithDefault("buffer")

  val COLUMNAR_SHUFFLE_COMPRESSION_THRESHOLD =
    buildConf("spark.gluten.sql.columnar.shuffle.compression.threshold")
      .internal()
      .doc("If number of rows in a batch falls below this threshold," +
        " will copy all buffers into one buffer to compress.")
      .intConf
      .createWithDefault(100)

  val SHUFFLE_WRITER_MERGE_THRESHOLD =
    buildConf("spark.gluten.sql.columnar.shuffle.merge.threshold")
      .internal()
      .doubleConf
      .checkValue(v => v >= 0 && v <= 1, "Shuffle writer merge threshold must between [0, 1]")
      .createWithDefault(0.25)

  val COLUMNAR_SHUFFLE_READER_BUFFER_SIZE =
    buildConf("spark.gluten.sql.columnar.shuffle.readerBufferSize")
      .internal()
      .doc("Buffer size in bytes for shuffle reader reading input stream from local or remote.")
      .bytesConf(ByteUnit.BYTE)
      .createWithDefaultString("1MB")

  val COLUMNAR_MAX_BATCH_SIZE =
    buildConf("spark.gluten.sql.columnar.maxBatchSize")
      .internal()
      .intConf
      .checkValue(_ > 0, s"must be positive.")
      .createWithDefault(4096)

  val GLUTEN_COLUMNAR_TO_ROW_MEM_THRESHOLD =
    buildConf("spark.gluten.sql.columnarToRowMemoryThreshold")
      .internal()
      .bytesConf(ByteUnit.BYTE)
      .createWithDefaultString("64MB")

  // if not set, use COLUMNAR_MAX_BATCH_SIZE instead
  val SHUFFLE_WRITER_BUFFER_SIZE =
    buildConf("spark.gluten.shuffleWriter.bufferSize")
      .internal()
      .intConf
      .checkValue(_ > 0, s"must be positive.")
      .createOptional

  val COLUMNAR_LIMIT_ENABLED =
    buildConf("spark.gluten.sql.columnar.limit")
      .internal()
      .booleanConf
      .createWithDefault(true)

  val COLUMNAR_GENERATE_ENABLED =
    buildConf("spark.gluten.sql.columnar.generate")
      .internal()
      .booleanConf
      .createWithDefault(true)

  val COLUMNAR_TAKE_ORDERED_AND_PROJECT_ENABLED =
    buildConf("spark.gluten.sql.columnar.takeOrderedAndProject")
      .internal()
      .booleanConf
      .createWithDefault(true)

  val COLUMNAR_NATIVE_BLOOMFILTER_ENABLED =
    buildConf("spark.gluten.sql.native.bloomFilter")
      .internal()
      .booleanConf
      .createWithDefault(true)

  val COLUMNAR_NATIVE_HYPERLOGLOG_AGGREGATE_ENABLED =
    buildConf("spark.gluten.sql.native.hyperLogLog.Aggregate")
      .internal()
      .booleanConf
      .createWithDefault(true)

  val COLUMNAR_PARQUET_WRITE_BLOCK_SIZE =
    buildConf("spark.gluten.sql.columnar.parquet.write.blockSize")
      .internal()
      .bytesConf(ByteUnit.BYTE)
      .createWithDefaultString("128MB")

  val COLUMNAR_PARQUET_WRITE_BLOCK_ROWS =
    buildConf("spark.gluten.sql.native.parquet.write.blockRows")
      .internal()
      .longConf
      .createWithDefault(100 * 1000 * 1000)

  val COLUMNAR_QUERY_FALLBACK_THRESHOLD =
    buildConf("spark.gluten.sql.columnar.query.fallback.threshold")
      .internal()
      .doc("The threshold for whether query will fall back " +
        "by counting the number of ColumnarToRow & vanilla leaf node.")
      .intConf
      .createWithDefault(-1)

  val COLUMNAR_WHOLESTAGE_FALLBACK_THRESHOLD =
    buildConf("spark.gluten.sql.columnar.wholeStage.fallback.threshold")
      .internal()
      .doc("The threshold for whether whole stage will fall back in AQE supported case " +
        "by counting the number of ColumnarToRow & vanilla leaf node.")
      .intConf
      .createWithDefault(-1)

  val COLUMNAR_FALLBACK_IGNORE_ROW_TO_COLUMNAR =
    buildConf("spark.gluten.sql.columnar.fallback.ignoreRowToColumnar")
      .internal()
      .doc(
        "When true, the fallback policy ignores the RowToColumnar when counting fallback number.")
      .booleanConf
      .createWithDefault(true)

  val COLUMNAR_FALLBACK_EXPRESSIONS_THRESHOLD =
    buildConf("spark.gluten.sql.columnar.fallback.expressions.threshold")
      .internal()
      .doc("Fall back filter/project if number of nested expressions reaches this threshold," +
        " considering Spark codegen can bring better performance for such case.")
      .intConf
      .createWithDefault(50)

  val COLUMNAR_FALLBACK_PREFER_COLUMNAR =
    buildConf("spark.gluten.sql.columnar.fallback.preferColumnar")
      .internal()
      .doc(
        "When true, the fallback policy prefers to use Gluten plan rather than vanilla " +
          "Spark plan if the both of them contains ColumnarToRow and the vanilla Spark plan " +
          "ColumnarToRow number is not smaller than Gluten plan.")
      .booleanConf
      .createWithDefault(true)

  val COLUMNAR_NUMA_BINDING_ENABLED =
    buildConf("spark.gluten.sql.columnar.numaBinding")
      .internal()
      .booleanConf
      .createWithDefault(false)

  val COLUMNAR_NUMA_BINDING_CORE_RANGE =
    buildConf("spark.gluten.sql.columnar.coreRange")
      .internal()
      .stringConf
      .createOptional

  val NUM_TASK_SLOTS_PER_EXECUTOR =
    buildConf("spark.gluten.numTaskSlotsPerExecutor")
      .internal()
      .doc(
        "Must provide default value since non-execution operations " +
          "(e.g. org.apache.spark.sql.Dataset#summary) doesn't propagate configurations using " +
          "org.apache.spark.sql.execution.SQLExecution#withSQLConfPropagated")
      .intConf
      .createWithDefaultString("-1")

  val COLUMNAR_OVERHEAD_SIZE_IN_BYTES =
    buildConf("spark.gluten.memoryOverhead.size.in.bytes")
      .internal()
      .doc(
        "Must provide default value since non-execution operations " +
          "(e.g. org.apache.spark.sql.Dataset#summary) doesn't propagate configurations using " +
          "org.apache.spark.sql.execution.SQLExecution#withSQLConfPropagated")
      .bytesConf(ByteUnit.BYTE)
      .createWithDefaultString("0")

  val COLUMNAR_OFFHEAP_SIZE_IN_BYTES =
    buildConf("spark.gluten.memory.offHeap.size.in.bytes")
      .internal()
      .doc(
        "Must provide default value since non-execution operations " +
          "(e.g. org.apache.spark.sql.Dataset#summary) doesn't propagate configurations using " +
          "org.apache.spark.sql.execution.SQLExecution#withSQLConfPropagated")
      .bytesConf(ByteUnit.BYTE)
      .createWithDefaultString("0")

  val COLUMNAR_TASK_OFFHEAP_SIZE_IN_BYTES =
    buildConf("spark.gluten.memory.task.offHeap.size.in.bytes")
      .internal()
      .doc(
        "Must provide default value since non-execution operations " +
          "(e.g. org.apache.spark.sql.Dataset#summary) doesn't propagate configurations using " +
          "org.apache.spark.sql.execution.SQLExecution#withSQLConfPropagated")
      .bytesConf(ByteUnit.BYTE)
      .createWithDefaultString("0")

  val COLUMNAR_CONSERVATIVE_TASK_OFFHEAP_SIZE_IN_BYTES =
    buildConf("spark.gluten.memory.conservative.task.offHeap.size.in.bytes")
      .internal()
      .doc(
        "Must provide default value since non-execution operations " +
          "(e.g. org.apache.spark.sql.Dataset#summary) doesn't propagate configurations using " +
          "org.apache.spark.sql.execution.SQLExecution#withSQLConfPropagated")
      .bytesConf(ByteUnit.BYTE)
      .createWithDefaultString("0")

  val COLUMNAR_MEMORY_ISOLATION =
    buildConf("spark.gluten.memory.isolation")
      .internal()
      .doc("Enable isolated memory mode. If true, Gluten controls the maximum off-heap memory " +
        "can be used by each task to X, X = executor memory / max task slots. It's recommended " +
        "to set true if Gluten serves concurrent queries within a single session, since not all " +
        "memory Gluten allocated is guaranteed to be spillable. In the case, the feature should " +
        "be enabled to avoid OOM.")
      .booleanConf
      .createWithDefault(false)

  val COLUMNAR_MEMORY_BACKTRACE_ALLOCATION =
    buildConf("spark.gluten.memory.backtrace.allocation")
      .internal()
      .doc("Print backtrace information for large memory allocations. This helps debugging when " +
        "Spark OOM happens due to large acquire requests.")
      .booleanConf
      .createWithDefault(false)

  val COLUMNAR_MEMORY_OVER_ACQUIRED_RATIO =
    buildConf("spark.gluten.memory.overAcquiredMemoryRatio")
      .internal()
      .doc("If larger than 0, Velox backend will try over-acquire this ratio of the total " +
        "allocated memory as backup to avoid OOM.")
      .doubleConf
      .checkValue(d => d >= 0.0d, "Over-acquired ratio should be larger than or equals 0")
      .createWithDefault(0.3d)

  val COLUMNAR_MEMORY_RESERVATION_BLOCK_SIZE =
    buildConf("spark.gluten.memory.reservationBlockSize")
      .internal()
      .doc("Block size of native reservation listener reserve memory from Spark.")
      .bytesConf(ByteUnit.BYTE)
      .createWithDefaultString("8MB")

  // Options used by RAS.
  val RAS_ENABLED =
    buildConf("spark.gluten.ras.enabled")
      .doc(
        "Enables RAS (relational algebra selector) during physical " +
          "planning to generate more efficient query plan. Note, this feature doesn't bring " +
          "performance profits by default. Try exploring option `spark.gluten.ras.costModel` " +
          "for advanced usage.")
      .booleanConf
      .createWithDefault(false)

  // FIXME: This option is no longer only used by RAS. Should change key to
  //  `spark.gluten.costModel` or something similar.
  val RAS_COST_MODEL =
    buildConf("spark.gluten.ras.costModel")
      .doc(
        "The class name of user-defined cost model that will be used by Gluten's transition " +
          "planner as well as by RAS. If not specified, a legacy built-in cost model will be " +
          "used. The legacy cost model helps RAS planner exhaustively offload computations, and " +
          "helps transition planner choose columnar-to-columnar transition over others.")
      .stringConf
      .createWithDefaultString("legacy")

  val TRANSFORM_PLAN_LOG_LEVEL =
    buildConf("spark.gluten.sql.transform.logLevel")
      .internal()
      .stringConf
      .transform(_.toUpperCase(Locale.ROOT))
      .checkValue(
        logLevel => Set("TRACE", "DEBUG", "INFO", "WARN", "ERROR").contains(logLevel),
        "Valid values are 'trace', 'debug', 'info', 'warn' and 'error'.")
      .createWithDefault("DEBUG")

  val SUBSTRAIT_PLAN_LOG_LEVEL =
    buildConf("spark.gluten.sql.substrait.plan.logLevel")
      .internal()
      .stringConf
      .transform(_.toUpperCase(Locale.ROOT))
      .checkValue(
        logLevel => Set("TRACE", "DEBUG", "INFO", "WARN", "ERROR").contains(logLevel),
        "Valid values are 'trace', 'debug', 'info', 'warn' and 'error'.")
      .createWithDefault("DEBUG")

  val VALIDATION_LOG_LEVEL =
    buildConf("spark.gluten.sql.validation.logLevel")
      .internal()
      .stringConf
      .transform(_.toUpperCase(Locale.ROOT))
      .checkValue(
        logLevel => Set("TRACE", "DEBUG", "INFO", "WARN", "ERROR").contains(logLevel),
        "Valid values are 'trace', 'debug', 'info', 'warn' and 'error'.")
      .createWithDefault("WARN")

  val VALIDATION_PRINT_FAILURE_STACK_ =
    buildConf("spark.gluten.sql.validation.printStackOnFailure")
      .internal()
      .booleanConf
      .createWithDefault(false)

  val SOFT_AFFINITY_LOG_LEVEL =
    buildConf("spark.gluten.soft-affinity.logLevel")
      .internal()
      .stringConf
      .transform(_.toUpperCase(Locale.ROOT))
      .checkValue(
        logLevel => Set("TRACE", "DEBUG", "INFO", "WARN", "ERROR").contains(logLevel),
        "Valid values are 'trace', 'debug', 'info', 'warn' and 'error'.")
      .createWithDefault("DEBUG")

  val DEBUG_ENABLED =
    buildConf("spark.gluten.sql.debug")
      .internal()
      .booleanConf
      .createWithDefault(false)

  val DEBUG_KEEP_JNI_WORKSPACE =
    buildStaticConf("spark.gluten.sql.debug.keepJniWorkspace")
      .internal()
      .booleanConf
      .createWithDefault(false)

  val DEBUG_KEEP_JNI_WORKSPACE_DIR =
    buildStaticConf("spark.gluten.sql.debug.keepJniWorkspaceDir")
      .internal()
      .stringConf
      .createWithDefault("/tmp")

  val UT_STATISTIC =
    buildStaticConf("spark.gluten.sql.ut.statistic")
      .internal()
      .booleanConf
      .createWithDefault(false)

  val BENCHMARK_TASK_STAGEID =
    buildConf("spark.gluten.sql.benchmark_task.stageId")
      .internal()
      .intConf
      .createWithDefault(-1)

  val BENCHMARK_TASK_PARTITIONID =
    buildConf("spark.gluten.sql.benchmark_task.partitionId")
      .internal()
      .stringConf
      .createWithDefault("")

  val BENCHMARK_TASK_TASK_ID =
    buildConf("spark.gluten.sql.benchmark_task.taskId")
      .internal()
      .stringConf
      .createWithDefault("")

  val BENCHMARK_SAVE_DIR =
    buildConf("spark.gluten.saveDir")
      .internal()
      .stringConf
      .createWithDefault("")

  val NATIVE_WRITER_ENABLED =
    buildConf("spark.gluten.sql.native.writer.enabled")
      .internal()
      .doc("This is config to specify whether to enable the native columnar parquet/orc writer")
      .booleanConf
      .createOptional

  val NATIVE_HIVEFILEFORMAT_WRITER_ENABLED =
    buildConf("spark.gluten.sql.native.hive.writer.enabled")
      .internal()
      .doc(
        "This is config to specify whether to enable the native columnar writer for " +
          "HiveFileFormat. Currently only supports HiveFileFormat with Parquet as the output " +
          "file type.")
      .booleanConf
      .createWithDefault(true)

  val NATIVE_ARROW_READER_ENABLED =
    buildConf("spark.gluten.sql.native.arrow.reader.enabled")
      .internal()
      .doc("This is config to specify whether to enable the native columnar csv reader")
      .booleanConf
      .createWithDefault(false)

  val NATIVE_WRITE_FILES_COLUMN_METADATA_EXCLUSION_LIST =
    buildConf("spark.gluten.sql.native.writeColumnMetadataExclusionList")
      .doc(
        "Native write files does not support column metadata. Metadata in list would be " +
          "removed to support native write files. Multiple values separated by commas.")
      .stringConf
      .createWithDefault("comment")

  val REMOVE_NATIVE_WRITE_FILES_SORT_AND_PROJECT =
    buildConf("spark.gluten.sql.removeNativeWriteFilesSortAndProject")
      .internal()
      .doc("When true, Gluten will remove the vanilla Spark V1Writes added sort and project " +
        "for velox backend.")
      .booleanConf
      .createWithDefault(true)

  // FIXME: This only works with CH backend.
  val EXTENDED_COLUMNAR_TRANSFORM_RULES =
    buildConf("spark.gluten.sql.columnar.extended.columnar.transform.rules")
      .withAlternative("spark.gluten.sql.columnar.extended.columnar.pre.rules")
      .doc("A comma-separated list of classes for the extended columnar transform rules.")
      .stringConf
      .createWithDefaultString("")

  // FIXME: This only works with CH backend.
  val EXTENDED_COLUMNAR_POST_RULES =
    buildConf("spark.gluten.sql.columnar.extended.columnar.post.rules")
      .doc("A comma-separated list of classes for the extended columnar post rules.")
      .stringConf
      .createWithDefaultString("")

  // FIXME: This only works with CH backend.
  val EXTENDED_EXPRESSION_TRAN_CONF =
    buildConf("spark.gluten.sql.columnar.extended.expressions.transformer")
      .doc("A class for the extended expressions transformer.")
      .stringConf
      .createWithDefaultString("")

  val EXPRESSION_BLACK_LIST =
    buildConf("spark.gluten.expression.blacklist")
      .doc("A black list of expression to skip transform, multiple values separated by commas.")
      .stringConf
      .createOptional

  val FALLBACK_REGEXP_EXPRESSIONS =
    buildConf("spark.gluten.sql.fallbackRegexpExpressions")
      .doc(
        "If true, fall back all regexp expressions. There are a few incompatible cases" +
          " between RE2 (used by native engine) and java.util.regex (used by Spark). User should" +
          " enable this property if their incompatibility is intolerable.")
      .booleanConf
      .createWithDefault(false)

  val FALLBACK_REPORTER_ENABLED =
    buildConf("spark.gluten.sql.columnar.fallbackReporter")
      .doc("When true, enable fallback reporter rule to print fallback reason")
      .booleanConf
      .createWithDefault(true)

  val TEXT_INPUT_ROW_MAX_BLOCK_SIZE =
    buildConf("spark.gluten.sql.text.input.max.block.size")
      .internal()
      .doc("the max block size for text input rows")
      .bytesConf(ByteUnit.BYTE)
      .createWithDefaultString("8KB");

  val TEXT_INPUT_EMPTY_AS_DEFAULT =
    buildConf("spark.gluten.sql.text.input.empty.as.default")
      .internal()
      .doc("treat empty fields in CSV input as default values.")
      .booleanConf
      .createWithDefault(false);

  val ENABLE_PARQUET_ROW_GROUP_MAX_MIN_INDEX =
    buildConf("spark.gluten.sql.parquet.maxmin.index")
      .internal()
      .doc("Enable row group max min index for parquet file scan")
      .booleanConf
      .createWithDefault(false)

  val ENABLE_REWRITE_DATE_TIMESTAMP_COMPARISON =
    buildConf("spark.gluten.sql.rewrite.dateTimestampComparison")
      .internal()
      .doc("Rewrite the comparision between date and timestamp to timestamp comparison."
        + "For example `from_unixtime(ts) > date` will be rewritten to `ts > to_unixtime(date)`")
      .booleanConf
      .createWithDefault(true)

  val ENABLE_COLLAPSE_GET_JSON_OBJECT =
    buildConf("spark.gluten.sql.collapseGetJsonObject.enabled")
      .internal()
      .doc("Collapse nested get_json_object functions as one for optimization.")
      .booleanConf
      .createWithDefault(false)

  val ENABLE_COLUMNAR_PROJECT_COLLAPSE =
    buildConf("spark.gluten.sql.columnar.project.collapse")
      .internal()
      .doc("Combines two columnar project operators into one and perform alias substitution")
      .booleanConf
      .createWithDefault(true)

  val ENABLE_COLUMNAR_PARTIAL_PROJECT =
    buildConf("spark.gluten.sql.columnar.partial.project")
      .doc(
        "Break up one project node into 2 phases when some of the expressions are non " +
          "offload-able. Phase one is a regular offloaded project transformer that " +
          "evaluates the offload-able expressions in native, " +
          "phase two preserves the output from phase one and evaluates the remaining " +
          "non-offload-able expressions using vanilla Spark projections")
      .booleanConf
      .createWithDefault(true)

  val ENABLE_COMMON_SUBEXPRESSION_ELIMINATE =
    buildConf("spark.gluten.sql.commonSubexpressionEliminate")
      .internal()
      .doc(
        "Eliminate common subexpressions in logical plan to avoid multiple evaluation of the same"
          + "expression, may improve performance")
      .booleanConf
      .createWithDefault(true)

  val ENABLE_COUNT_DISTINCT_WITHOUT_EXPAND =
    buildConf("spark.gluten.sql.countDistinctWithoutExpand")
      .internal()
      .doc(
        "Convert Count Distinct to a UDAF called count_distinct to " +
          "prevent SparkPlanner converting it to Expand+Count. WARNING: " +
          "When enabled, count distinct queries will fail to fallback!!!")
      .booleanConf
      .createWithDefault(false)

  val ENABLE_EXTENDED_COLUMN_PRUNING =
    buildConf("spark.gluten.sql.extendedColumnPruning.enabled")
      .internal()
      .doc("Do extended nested column pruning for cases ignored by vanilla Spark.")
      .booleanConf
      .createWithDefault(true)

  val CARTESIAN_PRODUCT_TRANSFORMER_ENABLED =
    buildConf("spark.gluten.sql.cartesianProductTransformerEnabled")
      .internal()
      .doc("Config to enable CartesianProductExecTransformer.")
      .booleanConf
      .createWithDefault(true)

  val BROADCAST_NESTED_LOOP_JOIN_TRANSFORMER_ENABLED =
    buildConf("spark.gluten.sql.broadcastNestedLoopJoinTransformerEnabled")
      .internal()
      .doc("Config to enable BroadcastNestedLoopJoinExecTransformer.")
      .booleanConf
      .createWithDefault(true)

  val COLUMNAR_SAMPLE_ENABLED =
    buildConf("spark.gluten.sql.columnarSampleEnabled")
      .internal()
      .doc("Disable or enable columnar sample.")
      .booleanConf
      .createWithDefault(false)

  val CACHE_WHOLE_STAGE_TRANSFORMER_CONTEXT =
    buildConf("spark.gluten.sql.cacheWholeStageTransformerContext")
      .internal()
      .doc("When true, `WholeStageTransformer` will cache the `WholeStageTransformerContext` " +
        "when executing. It is used to get substrait plan node and native plan string.")
      .booleanConf
      .createWithDefault(false)

  val INJECT_NATIVE_PLAN_STRING_TO_EXPLAIN =
    buildConf("spark.gluten.sql.injectNativePlanStringToExplain")
      .internal()
      .doc("When true, Gluten will inject native plan tree to explain string inside " +
        "`WholeStageTransformerContext`.")
      .booleanConf
      .createWithDefault(false)

  val VELOX_FORCE_ORC_CHAR_TYPE_SCAN_FALLBACK =
    buildConf("spark.gluten.sql.orc.charType.scan.fallback.enabled")
      .internal()
      .doc("Force fallback for orc char type scan.")
      .booleanConf
      .createWithDefault(true)

  val VELOX_SCAN_FILE_SCHEME_VALIDATION_ENABLED =
    buildConf("spark.gluten.sql.scan.fileSchemeValidation.enabled")
      .internal()
      .doc(
        "When true, enable file path scheme validation for scan. Validation will fail if" +
          " file scheme is not supported by registered file systems, which will cause scan " +
          " operator fall back.")
      .booleanConf
      .createWithDefault(true)

  val COLUMNAR_NATIVE_CAST_AGGREGATE_ENABLED =
    buildConf("spark.gluten.sql.columnar.cast.avg")
      .internal()
      .booleanConf
      .createWithDefault(true)

  val COST_EVALUATOR_ENABLED =
    buildStaticConf("spark.gluten.sql.adaptive.costEvaluator.enabled")
      .internal()
      .doc(
        "If true, use " +
          "org.apache.spark.sql.execution.adaptive.GlutenCostEvaluator as custom cost " +
          "evaluator class, else follow the configuration " +
          "spark.sql.adaptive.customCostEvaluatorClass.")
      .booleanConf
      .createWithDefault(true)

  val DYNAMIC_OFFHEAP_SIZING_ENABLED =
    buildConf("spark.gluten.memory.dynamic.offHeap.sizing.enabled")
      .internal()
      .doc(
        "Experimental: When set to true, the offheap config (spark.memory.offHeap.size) will " +
          "be ignored and instead we will consider onheap and offheap memory in combination, " +
          "both counting towards the executor memory config (spark.executor.memory). We will " +
          "make use of JVM APIs to determine how much onheap memory is use, alongside tracking " +
          "offheap allocations made by Gluten. We will then proceed to enforcing a total memory " +
          "quota, calculated by the sum of what memory is committed and in use in the Java " +
          "heap. Since the calculation of the total quota happens as offheap allocation happens " +
          "and not as JVM heap memory is allocated, it is possible that we can oversubscribe " +
          "memory. Additionally, note that this change is experimental and may have performance " +
          "implications.")
      .booleanConf
      .createWithDefault(false)

  val DYNAMIC_OFFHEAP_SIZING_MEMORY_FRACTION =
    buildConf("spark.gluten.memory.dynamic.offHeap.sizing.memory.fraction")
      .internal()
      .doc(
        "Experimental: Determines the memory fraction used to determine the total " +
          "memory available for offheap and onheap allocations when the dynamic offheap " +
          "sizing feature is enabled. The default is set to match spark.executor.memoryFraction.")
      .doubleConf
      .checkValue(v => v >= 0 && v <= 1, "offheap sizing memory fraction must between [0, 1]")
      .createWithDefault(0.6)

  val CELEBORN_FALLBACK_ENABLED =
    buildStaticConf("spark.gluten.sql.columnar.shuffle.celeborn.fallback.enabled")
      .internal()
      .doc("If enabled, fall back to ColumnarShuffleManager when celeborn service is unavailable." +
        "Otherwise, throw an exception.")
      .booleanConf
      .createWithDefault(true)

  val HDFS_VIEWFS_ENABLED =
    buildStaticConf("spark.gluten.storage.hdfsViewfs.enabled")
      .internal()
      .doc("If enabled, gluten will convert the viewfs path to hdfs path in scala side")
      .booleanConf
      .createWithDefault(false)

  val ENCRYPTED_PARQUET_FALLBACK_ENABLED =
    buildConf("spark.gluten.sql.fallbackEncryptedParquet")
      .internal()
      .doc("If enabled, gluten will not offload scan when encrypted parquet files are detected")
      .booleanConf
      .createWithDefault(false)

  val AUTO_ADJUST_STAGE_RESOURCE_PROFILE_ENABLED =
    buildStaticConf("spark.gluten.auto.adjustStageResource.enabled")
      .internal()
      .doc("Experimental: If enabled, gluten will try to set the stage resource according " +
        "to stage execution plan. Only worked when aqe is enabled at the same time!!")
      .booleanConf
      .createWithDefault(false)

  val AUTO_ADJUST_STAGE_RESOURCES_HEAP_RATIO =
    buildConf("spark.gluten.auto.adjustStageResources.heap.ratio")
      .internal()
      .doc("Experimental: Increase executor heap memory when match adjust stage resource rule.")
      .doubleConf
      .createWithDefault(2.0d)

  val AUTO_ADJUST_STAGE_RESOURCES_FALLEN_NODE_RATIO_THRESHOLD =
    buildConf("spark.gluten.auto.adjustStageResources.fallenNode.ratio.threshold")
      .internal()
      .doc("Experimental: Increase executor heap memory when stage contains fallen node " +
        "count exceeds the total node count ratio.")
      .doubleConf
      .createWithDefault(0.5d)

  val ENCRYPTED_PARQUET_FALLBACK_FILE_LIMIT =
    buildConf("spark.gluten.sql.fallbackEncryptedParquet.limit")
      .internal()
      .doc("If supplied, `limit` number of files will be checked to determine encryption " +
        "and falling back java scan")
      .intConf
      .checkValue(_ > 0, s"must be positive.")
      .createWithDefault(10)

}
