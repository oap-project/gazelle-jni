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
package org.apache.gluten.sql.shims.spark33

import org.apache.gluten.execution.datasource.GlutenFormatFactory
import org.apache.gluten.expression.{ExpressionNames, Sig}
import org.apache.gluten.expression.ExpressionNames.{CEIL, FLOOR, KNOWN_NULLABLE, TIMESTAMP_ADD}
import org.apache.gluten.sql.shims.{ShimDescriptor, SparkShims}
import org.apache.gluten.utils.ExceptionUtils

import org.apache.spark._
import org.apache.spark.scheduler.TaskInfo
import org.apache.spark.shuffle.ShuffleHandle
import org.apache.spark.sql.{AnalysisException, SparkSession}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.catalog.BucketSpec
import org.apache.spark.sql.catalyst.csv.CSVOptions
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate.{BloomFilterAggregate, RegrR2, TypedImperativeAggregate}
import org.apache.spark.sql.catalyst.plans.QueryPlan
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.plans.physical.{ClusteredDistribution, Distribution}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.util.CaseInsensitiveMap
import org.apache.spark.sql.catalyst.util.RebaseDateTime.RebaseSpec
import org.apache.spark.sql.catalyst.util.TimestampFormatter
import org.apache.spark.sql.connector.catalog.Table
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.execution.{FileSourceScanExec, PartitionedFileUtil, SparkPlan}
import org.apache.spark.sql.execution.datasources._
import org.apache.spark.sql.execution.datasources.FileFormatWriter.Empty2Null
import org.apache.spark.sql.execution.datasources.parquet.ParquetFilters
import org.apache.spark.sql.execution.datasources.v2.BatchScanExec
import org.apache.spark.sql.execution.datasources.v2.text.TextScan
import org.apache.spark.sql.execution.datasources.v2.utils.CatalogUtil
import org.apache.spark.sql.execution.exchange.BroadcastExchangeLike
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.internal.SQLConf.LegacyBehaviorPolicy
import org.apache.spark.sql.types.{StructField, StructType}
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.apache.spark.storage.{BlockId, BlockManagerId}

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, LocatedFileStatus, Path}
import org.apache.parquet.crypto.ParquetCryptoRuntimeException
import org.apache.parquet.hadoop.ParquetFileReader
import org.apache.parquet.schema.MessageType

import java.time.ZoneOffset
import java.util.{HashMap => JHashMap, Map => JMap, Properties}

class Spark33Shims extends SparkShims {
  override def getShimDescriptor: ShimDescriptor = SparkShimProvider.DESCRIPTOR

  override def getDistribution(
      leftKeys: Seq[Expression],
      rightKeys: Seq[Expression]): Seq[Distribution] = {
    ClusteredDistribution(leftKeys) :: ClusteredDistribution(rightKeys) :: Nil
  }

  override def scalarExpressionMappings: Seq[Sig] = {
    Seq(
      Sig[SplitPart](ExpressionNames.SPLIT_PART),
      Sig[Sec](ExpressionNames.SEC),
      Sig[Csc](ExpressionNames.CSC),
      Sig[KnownNullable](KNOWN_NULLABLE),
      Sig[Empty2Null](ExpressionNames.EMPTY2NULL),
      Sig[TimestampAdd](TIMESTAMP_ADD),
      Sig[RoundFloor](FLOOR),
      Sig[RoundCeil](CEIL)
    )
  }

  override def aggregateExpressionMappings: Seq[Sig] = {
    Seq(
      Sig[RegrR2](ExpressionNames.REGR_R2)
    )
  }

  override def convertPartitionTransforms(
      partitions: Seq[Transform]): (Seq[String], Option[BucketSpec]) = {
    CatalogUtil.convertPartitionTransforms(partitions)
  }

  override def generateFileScanRDD(
      sparkSession: SparkSession,
      readFunction: PartitionedFile => Iterator[InternalRow],
      filePartitions: Seq[FilePartition],
      fileSourceScanExec: FileSourceScanExec): FileScanRDD = {
    new FileScanRDD(
      sparkSession,
      readFunction,
      filePartitions,
      new StructType(
        fileSourceScanExec.requiredSchema.fields ++
          fileSourceScanExec.relation.partitionSchema.fields),
      fileSourceScanExec.metadataColumns
    )
  }

  override def getTextScan(
      sparkSession: SparkSession,
      fileIndex: PartitioningAwareFileIndex,
      dataSchema: StructType,
      readDataSchema: StructType,
      readPartitionSchema: StructType,
      options: CaseInsensitiveStringMap,
      partitionFilters: Seq[Expression],
      dataFilters: Seq[Expression]): TextScan = {
    TextScan(
      sparkSession,
      fileIndex,
      dataSchema,
      readDataSchema,
      readPartitionSchema,
      options,
      partitionFilters,
      dataFilters)
  }

  override def filesGroupedToBuckets(
      selectedPartitions: Array[PartitionDirectory]): Map[Int, Array[PartitionedFile]] = {
    selectedPartitions
      .flatMap {
        p => p.files.map(f => PartitionedFileUtil.getPartitionedFile(f, f.getPath, p.values))
      }
      .groupBy {
        f =>
          BucketingUtils
            .getBucketId(new Path(f.filePath).getName)
            .getOrElse(throw invalidBucketFile(f.filePath))
      }
  }

  override def getBatchScanExecTable(batchScan: BatchScanExec): Table = null

  override def generatePartitionedFile(
      partitionValues: InternalRow,
      filePath: String,
      start: Long,
      length: Long,
      @transient locations: Array[String] = Array.empty): PartitionedFile =
    PartitionedFile(partitionValues, filePath, start, length, locations)

  override def bloomFilterExpressionMappings(): Seq[Sig] = Seq(
    Sig[BloomFilterMightContain](ExpressionNames.MIGHT_CONTAIN),
    Sig[BloomFilterAggregate](ExpressionNames.BLOOM_FILTER_AGG)
  )

  override def newBloomFilterAggregate[T](
      child: Expression,
      estimatedNumItemsExpression: Expression,
      numBitsExpression: Expression,
      mutableAggBufferOffset: Int,
      inputAggBufferOffset: Int): TypedImperativeAggregate[T] = {
    BloomFilterAggregate(
      child,
      estimatedNumItemsExpression,
      numBitsExpression,
      mutableAggBufferOffset,
      inputAggBufferOffset).asInstanceOf[TypedImperativeAggregate[T]]
  }

  override def newMightContain(
      bloomFilterExpression: Expression,
      valueExpression: Expression): BinaryExpression = {
    BloomFilterMightContain(bloomFilterExpression, valueExpression)
  }

  override def replaceBloomFilterAggregate[T](
      expr: Expression,
      bloomFilterAggReplacer: (
          Expression,
          Expression,
          Expression,
          Int,
          Int) => TypedImperativeAggregate[T]): Expression = expr match {
    case BloomFilterAggregate(
          child,
          estimatedNumItemsExpression,
          numBitsExpression,
          mutableAggBufferOffset,
          inputAggBufferOffset) =>
      bloomFilterAggReplacer(
        child,
        estimatedNumItemsExpression,
        numBitsExpression,
        mutableAggBufferOffset,
        inputAggBufferOffset)
    case other => other
  }

  override def replaceMightContain[T](
      expr: Expression,
      mightContainReplacer: (Expression, Expression) => BinaryExpression): Expression = expr match {
    case BloomFilterMightContain(bloomFilterExpression, valueExpression) =>
      mightContainReplacer(bloomFilterExpression, valueExpression)
    case other => other
  }

  override def getFileSizeAndModificationTime(
      file: PartitionedFile): (Option[Long], Option[Long]) = {
    (Some(file.fileSize), Some(file.modificationTime))
  }

  override def generateMetadataColumns(
      file: PartitionedFile,
      metadataColumnNames: Seq[String]): JMap[String, String] = {
    val metadataColumn = new JHashMap[String, String]()
    val path = new Path(file.filePath)
    for (columnName <- metadataColumnNames) {
      columnName match {
        case FileFormat.FILE_PATH => metadataColumn.put(FileFormat.FILE_PATH, path.toString)
        case FileFormat.FILE_NAME => metadataColumn.put(FileFormat.FILE_NAME, path.getName)
        case FileFormat.FILE_SIZE =>
          metadataColumn.put(FileFormat.FILE_SIZE, file.fileSize.toString)
        case FileFormat.FILE_MODIFICATION_TIME =>
          val fileModifyTime = TimestampFormatter
            .getFractionFormatter(ZoneOffset.UTC)
            .format(file.modificationTime * 1000L)
          metadataColumn.put(FileFormat.FILE_MODIFICATION_TIME, fileModifyTime)
        case _ =>
      }
    }
    metadataColumn.put(InputFileName().prettyName, file.filePath)
    metadataColumn.put(InputFileBlockStart().prettyName, file.start.toString)
    metadataColumn.put(InputFileBlockLength().prettyName, file.length.toString)
    metadataColumn
  }

  private def invalidBucketFile(path: String): Throwable = {
    new SparkException(
      errorClass = "INVALID_BUCKET_FILE",
      messageParameters = Array(path),
      cause = null)
  }

  override def getExtendedColumnarPostRules(): List[SparkSession => Rule[SparkPlan]] = {
    List(session => GlutenFormatFactory.getExtendedColumnarPostRule(session))
  }

  override def createTestTaskContext(properties: Properties): TaskContext = {
    TaskContextUtils.createTestTaskContext(properties)
  }

  def setJobDescriptionOrTagForBroadcastExchange(
      sc: SparkContext,
      broadcastExchange: BroadcastExchangeLike): Unit = {
    // Setup a job group here so later it may get cancelled by groupId if necessary.
    sc.setJobGroup(
      broadcastExchange.runId.toString,
      s"broadcast exchange (runId ${broadcastExchange.runId})",
      interruptOnCancel = true)
  }

  def cancelJobGroupForBroadcastExchange(
      sc: SparkContext,
      broadcastExchange: BroadcastExchangeLike): Unit = {
    sc.cancelJobGroup(broadcastExchange.runId.toString)
  }

  override def getShuffleReaderParam[K, C](
      handle: ShuffleHandle,
      startMapIndex: Int,
      endMapIndex: Int,
      startPartition: Int,
      endPartition: Int): Tuple2[Iterator[(BlockManagerId, Seq[(BlockId, Long, Int)])], Boolean] = {
    ShuffleUtils.getReaderParam(handle, startMapIndex, endMapIndex, startPartition, endPartition)
  }

  override def getPartitionId(taskInfo: TaskInfo): Int = {
    taskInfo.partitionId
  }

  override def supportDuplicateReadingTracking: Boolean = true

  def getFileStatus(partition: PartitionDirectory): Seq[FileStatus] = partition.files

  def isFileSplittable(
      relation: HadoopFsRelation,
      filePath: Path,
      sparkSchema: StructType): Boolean = true

  def isRowIndexMetadataColumn(name: String): Boolean = false

  def findRowIndexColumnIndexInSchema(sparkSchema: StructType): Int = -1

  def splitFiles(
      sparkSession: SparkSession,
      file: FileStatus,
      filePath: Path,
      isSplitable: Boolean,
      maxSplitBytes: Long,
      partitionValues: InternalRow): Seq[PartitionedFile] = {
    PartitionedFileUtil.splitFiles(
      sparkSession,
      file,
      filePath,
      isSplitable,
      maxSplitBytes,
      partitionValues)
  }

  def structFromAttributes(attrs: Seq[Attribute]): StructType = {
    StructType(attrs.map(a => StructField(a.name, a.dataType, a.nullable, a.metadata)))
  }

  def attributesFromStruct(structType: StructType): Seq[Attribute] = {
    structType.fields.map {
      field => AttributeReference(field.name, field.dataType, field.nullable, field.metadata)()
    }
  }

  def getAnalysisExceptionPlan(ae: AnalysisException): Option[LogicalPlan] = {
    ae.plan
  }

  override def getKeyGroupedPartitioning(batchScan: BatchScanExec): Option[Seq[Expression]] = {
    batchScan.keyGroupedPartitioning
  }
  override def getCommonPartitionValues(batchScan: BatchScanExec): Option[Seq[(InternalRow, Int)]] =
    null

  override def extractExpressionTimestampAddUnit(exp: Expression): Option[Seq[String]] = {
    exp match {
      case timestampAdd: TimestampAdd =>
        Option.apply(Seq(timestampAdd.unit, timestampAdd.timeZoneId.getOrElse("")))
      case _ => Option.empty
    }
  }

  override def supportsRowBased(plan: SparkPlan): Boolean = plan.supportsRowBased

  override def dateTimestampFormatInReadIsDefaultValue(
      csvOptions: CSVOptions,
      timeZone: String): Boolean = {
    val default = new CSVOptions(CaseInsensitiveMap(Map()), csvOptions.columnPruning, timeZone)
    csvOptions.dateFormatInRead == default.dateFormatInRead &&
    csvOptions.timestampFormatInRead == default.timestampFormatInRead &&
    csvOptions.timestampNTZFormatInRead == default.timestampNTZFormatInRead
  }

  override def createParquetFilters(
      conf: SQLConf,
      schema: MessageType,
      caseSensitive: Option[Boolean] = None): ParquetFilters = {
    new ParquetFilters(
      schema,
      conf.parquetFilterPushDownDate,
      conf.parquetFilterPushDownTimestamp,
      conf.parquetFilterPushDownDecimal,
      conf.parquetFilterPushDownStringStartWith,
      conf.parquetFilterPushDownInFilterThreshold,
      caseSensitive.getOrElse(conf.caseSensitiveAnalysis),
      RebaseSpec(LegacyBehaviorPolicy.CORRECTED)
    )
  }

  override def getOperatorId(plan: QueryPlan[_]): Option[Int] = {
    plan.getTagValue(QueryPlan.OP_ID_TAG)
  }

  override def setOperatorId(plan: QueryPlan[_], opId: Int): Unit = {
    plan.setTagValue(QueryPlan.OP_ID_TAG, opId)
  }

  override def unsetOperatorId(plan: QueryPlan[_]): Unit = {
    plan.unsetTagValue(QueryPlan.OP_ID_TAG)
  }
  override def isParquetFileEncrypted(
      fileStatus: LocatedFileStatus,
      conf: Configuration): Boolean = {
    try {
      ParquetFileReader.readFooter(new Configuration(), fileStatus.getPath).toString
      false
    } catch {
      case e: Exception if ExceptionUtils.hasCause(e, classOf[ParquetCryptoRuntimeException]) =>
        true
      case e: Throwable =>
        e.printStackTrace()
        false
    }
  }

  override def isColumnarLimitExecSupported(): Boolean = true

}
