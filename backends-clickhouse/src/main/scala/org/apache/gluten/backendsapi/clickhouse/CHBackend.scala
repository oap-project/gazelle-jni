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
package org.apache.gluten.backendsapi.clickhouse

import org.apache.gluten.GlutenBuildInfo._
import org.apache.gluten.GlutenConfig
import org.apache.gluten.backendsapi._
import org.apache.gluten.columnarbatch.CHBatch
import org.apache.gluten.component.Component.BuildInfo
import org.apache.gluten.execution.WriteFilesExecTransformer
import org.apache.gluten.expression.WindowFunctionsBuilder
import org.apache.gluten.extension.ValidationResult
import org.apache.gluten.extension.columnar.transition.{Convention, ConventionFunc}
import org.apache.gluten.substrait.rel.LocalFilesNode
import org.apache.gluten.substrait.rel.LocalFilesNode.ReadFileFormat
import org.apache.gluten.substrait.rel.LocalFilesNode.ReadFileFormat._

import org.apache.spark.SparkEnv
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.catalog.BucketSpec
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate.AggregateExpression
import org.apache.spark.sql.catalyst.plans._
import org.apache.spark.sql.connector.read.Scan
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanExec
import org.apache.spark.sql.execution.datasources.FileFormat
import org.apache.spark.sql.execution.datasources.orc.OrcFileFormat
import org.apache.spark.sql.execution.datasources.parquet.ParquetFileFormat
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._
import org.apache.spark.util.SerializableConfiguration

import java.util.Locale

import scala.util.control.Breaks.{break, breakable}

class CHBackend extends SubstraitBackend {
  import CHBackend._
  override def name(): String = CHConf.BACKEND_NAME
  override def buildInfo(): BuildInfo =
    BuildInfo("ClickHouse", CH_BRANCH, CH_COMMIT, "UNKNOWN")
  override def convFuncOverride(): ConventionFunc.Override = new ConvFunc()
  override def iteratorApi(): IteratorApi = new CHIteratorApi
  override def sparkPlanExecApi(): SparkPlanExecApi = new CHSparkPlanExecApi
  override def transformerApi(): TransformerApi = new CHTransformerApi
  override def validatorApi(): ValidatorApi = new CHValidatorApi
  override def metricsApi(): MetricsApi = new CHMetricsApi
  override def listenerApi(): ListenerApi = new CHListenerApi
  override def ruleApi(): RuleApi = new CHRuleApi
  override def settings(): BackendSettingsApi = CHBackendSettings
}

object CHBackend {
  private class ConvFunc() extends ConventionFunc.Override {
    override def batchTypeOf: PartialFunction[SparkPlan, Convention.BatchType] = {
      case a: AdaptiveSparkPlanExec if a.supportsColumnar =>
        CHBatch
    }
  }
}

object CHBackendSettings extends BackendSettingsApi with Logging {
  override def primaryBatchType: Convention.BatchType = CHBatch

  private val GLUTEN_CLICKHOUSE_SEP_SCAN_RDD = "spark.gluten.sql.columnar.separate.scan.rdd.for.ch"
  private val GLUTEN_CLICKHOUSE_SEP_SCAN_RDD_DEFAULT = "false"

  // experimental: when the files count per partition exceeds this threshold,
  // it will put the files into one partition.
  val GLUTEN_CLICKHOUSE_FILES_PER_PARTITION_THRESHOLD: String =
    CHConf.prefixOf("files.per.partition.threshold")
  val GLUTEN_CLICKHOUSE_FILES_PER_PARTITION_THRESHOLD_DEFAULT = "-1"

  private val GLUTEN_CLICKHOUSE_CUSTOMIZED_SHUFFLE_CODEC_ENABLE: String =
    CHConf.prefixOf("customized.shuffle.codec.enable")
  private val GLUTEN_CLICKHOUSE_CUSTOMIZED_SHUFFLE_CODEC_ENABLE_DEFAULT = false
  lazy val useCustomizedShuffleCodec: Boolean = SparkEnv.get.conf.getBoolean(
    CHBackendSettings.GLUTEN_CLICKHOUSE_CUSTOMIZED_SHUFFLE_CODEC_ENABLE,
    CHBackendSettings.GLUTEN_CLICKHOUSE_CUSTOMIZED_SHUFFLE_CODEC_ENABLE_DEFAULT
  )

  private val GLUTEN_CLICKHOUSE_CUSTOMIZED_BUFFER_SIZE: String =
    CHConf.prefixOf("customized.buffer.size")
  private val GLUTEN_CLICKHOUSE_CUSTOMIZED_BUFFER_SIZE_DEFAULT = 4096
  lazy val customizeBufferSize: Int = SparkEnv.get.conf.getInt(
    CHBackendSettings.GLUTEN_CLICKHOUSE_CUSTOMIZED_BUFFER_SIZE,
    CHBackendSettings.GLUTEN_CLICKHOUSE_CUSTOMIZED_BUFFER_SIZE_DEFAULT
  )

  val GLUTEN_CLICKHOUSE_BROADCAST_CACHE_EXPIRED_TIME: String =
    CHConf.prefixOf("broadcast.cache.expired.time")
  // unit: SECONDS, default 1 day
  val GLUTEN_CLICKHOUSE_BROADCAST_CACHE_EXPIRED_TIME_DEFAULT: Int = 86400

  private val GLUTEN_CLICKHOUSE_SHUFFLE_SUPPORTED_CODEC: Set[String] = Set("lz4", "zstd", "snappy")

  // The algorithm for hash partition of the shuffle
  private val GLUTEN_CLICKHOUSE_SHUFFLE_HASH_ALGORITHM: String =
    CHConf.prefixOf("shuffle.hash.algorithm")
  // valid values are: cityHash64 or sparkMurmurHash3_32
  private val GLUTEN_CLICKHOUSE_SHUFFLE_HASH_ALGORITHM_DEFAULT = "sparkMurmurHash3_32"
  def shuffleHashAlgorithm: String = {
    val algorithm = SparkEnv.get.conf.get(
      CHBackendSettings.GLUTEN_CLICKHOUSE_SHUFFLE_HASH_ALGORITHM,
      CHBackendSettings.GLUTEN_CLICKHOUSE_SHUFFLE_HASH_ALGORITHM_DEFAULT
    )
    if (!algorithm.equals("cityHash64") && !algorithm.equals("sparkMurmurHash3_32")) {
      CHBackendSettings.GLUTEN_CLICKHOUSE_SHUFFLE_HASH_ALGORITHM_DEFAULT
    } else {
      algorithm
    }
  }

  private val GLUTEN_CLICKHOUSE_AFFINITY_MODE: String = CHConf.prefixOf("affinity.mode")
  val SOFT: String = "soft"
  val FORCE: String = "force"
  private val GLUTEN_CLICKHOUSE_AFFINITY_MODE_DEFAULT = SOFT

  private val GLUTEN_MAX_BLOCK_SIZE: String = CHConf.runtimeSettings("max_block_size")
  // Same as default value in clickhouse
  private val GLUTEN_MAX_BLOCK_SIZE_DEFAULT = 65409
  private val GLUTEN_MAX_SHUFFLE_READ_BYTES: String =
    CHConf.runtimeConfig("max_source_concatenate_bytes")
  private val GLUTEN_MAX_SHUFFLE_READ_BYTES_DEFAULT = GLUTEN_MAX_BLOCK_SIZE_DEFAULT * 256

  val GLUTEN_AQE_PROPAGATEEMPTY: String = CHConf.prefixOf("aqe.propagate.empty.relation")

  val GLUTEN_CLICKHOUSE_DELTA_SCAN_CACHE_SIZE: String = CHConf.prefixOf("deltascan.cache.size")
  val GLUTEN_CLICKHOUSE_ADDFILES_TO_MTPS_CACHE_SIZE: String =
    CHConf.prefixOf("addfiles.to.mtps.cache.size")
  val GLUTEN_CLICKHOUSE_TABLE_PATH_TO_MTPS_CACHE_SIZE: String =
    CHConf.prefixOf("table.path.to.mtps.cache.size")

  val GLUTEN_CLICKHOUSE_DELTA_METADATA_OPTIMIZE: String =
    CHConf.prefixOf("delta.metadata.optimize")
  val GLUTEN_CLICKHOUSE_DELTA_METADATA_OPTIMIZE_DEFAULT_VALUE: String = "true"

  val GLUTEN_CLICKHOUSE_CONVERT_LEFT_ANTI_SEMI_TO_RIGHT: String =
    CHConf.prefixOf("convert.left.anti_semi.to.right")
  val GLUTEN_CLICKHOUSE_CONVERT_LEFT_ANTI_SEMI_TO_RIGHT_DEFAULT_VALUE: String = "false"

  def affinityMode: String = {
    SparkEnv.get.conf
      .get(
        CHBackendSettings.GLUTEN_CLICKHOUSE_AFFINITY_MODE,
        CHBackendSettings.GLUTEN_CLICKHOUSE_AFFINITY_MODE_DEFAULT
      )
      .toLowerCase(Locale.getDefault)
  }

  override def validateScanExec(
      format: ReadFileFormat,
      fields: Array[StructField],
      rootPaths: Seq[String],
      properties: Map[String, String],
      serializableHadoopConf: Option[SerializableConfiguration] = None): ValidationResult = {

    // Validate if all types are supported.
    def hasComplexType: Boolean = {
      // Collect unsupported types.
      val unsupportedDataTypes = fields.map(_.dataType).collect {
        case _: MapType => "MapType"
        case _: StructType => "StructType"
        case _: ArrayType => "ArrayType"
      }
      for (unsupportedDataType <- unsupportedDataTypes) {
        // scalastyle:off println
        println(
          s"Validation failed for ${this.getClass.toString}" +
            s" due to: data type $unsupportedDataType. in file schema. ")
        // scalastyle:on println
      }
      !unsupportedDataTypes.isEmpty
    }
    format match {
      case ParquetReadFormat => ValidationResult.succeeded
      case OrcReadFormat => ValidationResult.succeeded
      case MergeTreeReadFormat => ValidationResult.succeeded
      case TextReadFormat =>
        if (!hasComplexType) {
          ValidationResult.succeeded
        } else {
          ValidationResult.failed("Has complex type.")
        }
      case JsonReadFormat => ValidationResult.succeeded
      case _ => ValidationResult.failed(s"Unsupported file format $format")
    }
  }

  override def getSubstraitReadFileFormatV1(
      fileFormat: FileFormat): LocalFilesNode.ReadFileFormat = {
    fileFormat.getClass.getSimpleName match {
      case "OrcFileFormat" => ReadFileFormat.OrcReadFormat
      case "ParquetFileFormat" => ReadFileFormat.ParquetReadFormat
      case "DeltaParquetFileFormat" => ReadFileFormat.ParquetReadFormat
      case "DeltaMergeTreeFileFormat" => ReadFileFormat.MergeTreeReadFormat
      case "CSVFileFormat" => ReadFileFormat.TextReadFormat
      case _ => ReadFileFormat.UnknownFormat
    }
  }

  override def getSubstraitReadFileFormatV2(scan: Scan): LocalFilesNode.ReadFileFormat = {
    scan.getClass.getSimpleName match {
      case "OrcScan" => ReadFileFormat.OrcReadFormat
      case "ParquetScan" => ReadFileFormat.ParquetReadFormat
      case "ClickHouseScan" => ReadFileFormat.MergeTreeReadFormat
      case _ => ReadFileFormat.UnknownFormat
    }
  }

  override def supportWriteFilesExec(
      format: FileFormat,
      fields: Array[StructField],
      bucketSpec: Option[BucketSpec],
      options: Map[String, String]): ValidationResult = {

    def validateCompressionCodec(): Option[String] = {
      // FIXME: verify Support compression codec
      val compressionCodec = WriteFilesExecTransformer.getCompressionCodec(options)
      None
    }

    def validateFileFormat(): Option[String] = {
      format match {
        case _: ParquetFileFormat => None
        case _: OrcFileFormat => None
        case f: FileFormat => Some(s"Not support FileFormat: ${f.getClass.getSimpleName}")
      }
    }

    // Validate if all types are supported.
    def validateDateTypes(): Option[String] = {
      None
    }

    def validateFieldMetadata(): Option[String] = {
      // copy CharVarcharUtils.CHAR_VARCHAR_TYPE_STRING_METADATA_KEY
      val CHAR_VARCHAR_TYPE_STRING_METADATA_KEY = "__CHAR_VARCHAR_TYPE_STRING"
      fields
        .find(_.metadata != Metadata.empty)
        .filterNot(_.metadata.contains(CHAR_VARCHAR_TYPE_STRING_METADATA_KEY))
        .map {
          filed =>
            s"StructField contain the metadata information: $filed, metadata: ${filed.metadata}"
        }
    }
    def validateWriteFilesOptions(): Option[String] = {
      val maxRecordsPerFile = options
        .get("maxRecordsPerFile")
        .map(_.toLong)
        .getOrElse(SQLConf.get.maxRecordsPerFile)
      if (maxRecordsPerFile > 0) {
        Some("Unsupported native write: maxRecordsPerFile not supported.")
      } else {
        None
      }
    }

    validateCompressionCodec()
      .orElse(validateFileFormat())
      .orElse(validateFieldMetadata())
      .orElse(validateDateTypes())
      .orElse(validateWriteFilesOptions()) match {
      case Some(reason) => ValidationResult.failed(reason)
      case _ => ValidationResult.succeeded
    }
  }

  override def supportSortExec(): Boolean = {
    GlutenConfig.getConf.enableColumnarSort
  }

  override def supportSortMergeJoinExec(): Boolean = {
    GlutenConfig.getConf.enableColumnarSortMergeJoin
  }

  override def supportWindowExec(windowFunctions: Seq[NamedExpression]): Boolean = {
    var allSupported = true
    breakable {
      windowFunctions.foreach(
        func => {
          val aliasExpr = func.asInstanceOf[Alias]
          val wExpression = WindowFunctionsBuilder.extractWindowExpression(aliasExpr.child)

          def checkLagOrLead(third: Expression): Unit = {
            third match {
              case _: Literal =>
                allSupported = allSupported
              case _ =>
                logInfo("Not support lag/lead function with default value not literal null")
                allSupported = false
                break
            }
          }

          wExpression.windowFunction match {
            case _: RowNumber | _: AggregateExpression | _: Rank | _: DenseRank | _: PercentRank |
                _: NTile =>
              allSupported = allSupported
            case l: Lag =>
              checkLagOrLead(l.third)
            case l: Lead =>
              checkLagOrLead(l.third)
            case _ =>
              logDebug(s"Not support window function: ${wExpression.getClass}")
              allSupported = false
              break
          }
        })
    }
    allSupported
  }

  override def supportStructType(): Boolean = true

  override def structFieldToLowerCase(): Boolean = false

  override def supportExpandExec(): Boolean = true

  override def excludeScanExecFromCollapsedStage(): Boolean =
    SQLConf.get
      .getConfString(GLUTEN_CLICKHOUSE_SEP_SCAN_RDD, GLUTEN_CLICKHOUSE_SEP_SCAN_RDD_DEFAULT)
      .toBoolean

  override def shuffleSupportedCodec(): Set[String] = GLUTEN_CLICKHOUSE_SHUFFLE_SUPPORTED_CODEC
  override def needOutputSchemaForPlan(): Boolean = true

  override def transformCheckOverflow: Boolean = false

  override def requireBloomFilterAggMightContainJointFallback(): Boolean = false

  def maxShuffleReadRows(): Long = {
    SparkEnv.get.conf
      .getLong(GLUTEN_MAX_BLOCK_SIZE, GLUTEN_MAX_BLOCK_SIZE_DEFAULT)
  }

  def maxShuffleReadBytes(): Long = {
    SparkEnv.get.conf
      .getLong(GLUTEN_MAX_SHUFFLE_READ_BYTES, GLUTEN_MAX_SHUFFLE_READ_BYTES_DEFAULT)
  }

  // Move the pre-prejection for a aggregation ahead of the expand node
  // for example, select a, b, sum(c+d) from t group by a, b with cube
  def enablePushdownPreProjectionAheadExpand(): Boolean = {
    SparkEnv.get.conf.getBoolean(
      CHConf.prefixOf("enable_pushdown_preprojection_ahead_expand"),
      defaultValue = true
    )
  }

  // It try to move the expand node after the pre-aggregate node. That is to make the plan from
  //  expand -> pre-aggregate -> shuffle -> final-aggregate
  // to
  //  pre-aggregate -> expand -> shuffle -> final-aggregate
  // It could reduce the overhead of pre-aggregate node.
  def enableLazyAggregateExpand(): Boolean = {
    SparkEnv.get.conf.getBoolean(
      CHConf.runtimeConfig("enable_lazy_aggregate_expand"),
      defaultValue = true
    )
  }

  def enablePreProjectionForJoinConditions(): Boolean = {
    SparkEnv.get.conf.getBoolean(
      CHConf.runtimeConfig("enable_pre_projection_for_join_conditions"),
      defaultValue = true
    )
  }

  // If the partition keys are high cardinality, the aggregation method is slower.
  def enableConvertWindowGroupLimitToAggregate(): Boolean = {
    SparkEnv.get.conf.getBoolean(
      CHConf.runtimeConfig("enable_window_group_limit_to_aggregate"),
      defaultValue = true
    )
  }

  override def enableNativeWriteFiles(): Boolean = {
    GlutenConfig.getConf.enableNativeWriter.getOrElse(false)
  }

  override def supportCartesianProductExec(): Boolean = true

  override def supportCartesianProductExecWithCondition(): Boolean = false

  override def supportHashBuildJoinTypeOnLeft: JoinType => Boolean = {
    t =>
      if (super.supportHashBuildJoinTypeOnLeft(t)) {
        true
      } else {
        t match {
          case LeftAnti | LeftSemi
              if (SQLConf.get
                .getConfString(
                  GLUTEN_CLICKHOUSE_CONVERT_LEFT_ANTI_SEMI_TO_RIGHT,
                  GLUTEN_CLICKHOUSE_CONVERT_LEFT_ANTI_SEMI_TO_RIGHT_DEFAULT_VALUE)
                .toBoolean) =>
            true
          case LeftOuter => true
          case _ => false
        }
      }
  }

  override def supportHashBuildJoinTypeOnRight: JoinType => Boolean = {
    t =>
      if (super.supportHashBuildJoinTypeOnRight(t)) {
        true
      } else {
        t match {
          case RightOuter => true
          case _ => false
        }
      }
  }

  override def supportWindowGroupLimitExec(rankLikeFunction: Expression): Boolean = true
}
