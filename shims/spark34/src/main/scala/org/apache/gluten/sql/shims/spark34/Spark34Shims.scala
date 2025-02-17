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
package org.apache.gluten.sql.shims.spark34

import org.apache.gluten.expression.{ExpressionNames, Sig}
import org.apache.gluten.expression.ExpressionNames.KNOWN_NULLABLE
import org.apache.gluten.sql.shims.{ShimDescriptor, SparkShims}
import org.apache.gluten.utils.ExceptionUtils

import org.apache.spark._
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.internal.io.FileCommitProtocol
import org.apache.spark.paths.SparkPath
import org.apache.spark.scheduler.TaskInfo
import org.apache.spark.shuffle.ShuffleHandle
import org.apache.spark.sql.{AnalysisException, SparkSession}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.catalog.BucketSpec
import org.apache.spark.sql.catalyst.csv.CSVOptions
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate._
import org.apache.spark.sql.catalyst.plans.QueryPlan
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.plans.physical.{ClusteredDistribution, Distribution, KeyGroupedPartitioning, Partitioning}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.util.{CaseInsensitiveMap, InternalRowComparableWrapper, TimestampFormatter}
import org.apache.spark.sql.catalyst.util.RebaseDateTime.RebaseSpec
import org.apache.spark.sql.connector.catalog.Table
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.connector.read.{HasPartitionKey, InputPartition, Scan}
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.command.DataWritingCommandExec
import org.apache.spark.sql.execution.datasources._
import org.apache.spark.sql.execution.datasources.parquet.ParquetFilters
import org.apache.spark.sql.execution.datasources.v2.BatchScanExec
import org.apache.spark.sql.execution.datasources.v2.text.TextScan
import org.apache.spark.sql.execution.datasources.v2.utils.CatalogUtil
import org.apache.spark.sql.execution.exchange.BroadcastExchangeLike
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.internal.SQLConf.LegacyBehaviorPolicy
import org.apache.spark.sql.types.{IntegerType, LongType, StructField, StructType}
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.apache.spark.storage.{BlockId, BlockManagerId}

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, LocatedFileStatus, Path}
import org.apache.parquet.crypto.ParquetCryptoRuntimeException
import org.apache.parquet.hadoop.ParquetFileReader
import org.apache.parquet.schema.MessageType

import java.time.ZoneOffset
import java.util.{HashMap => JHashMap, Map => JMap, Properties}

import scala.reflect.ClassTag

class Spark34Shims extends SparkShims {
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
      Sig[TimestampAdd](ExpressionNames.TIMESTAMP_ADD),
      Sig[RoundFloor](ExpressionNames.FLOOR),
      Sig[RoundCeil](ExpressionNames.CEIL),
      Sig[Mask](ExpressionNames.MASK),
      Sig[ArrayInsert](ExpressionNames.ARRAY_INSERT),
      Sig[CheckOverflowInTableInsert](ExpressionNames.CHECK_OVERFLOW_IN_TABLE_INSERT)
    )
  }

  override def aggregateExpressionMappings: Seq[Sig] = {
    Seq(
      Sig[RegrR2](ExpressionNames.REGR_R2),
      Sig[RegrSlope](ExpressionNames.REGR_SLOPE),
      Sig[RegrIntercept](ExpressionNames.REGR_INTERCEPT),
      Sig[RegrSXY](ExpressionNames.REGR_SXY),
      Sig[RegrReplacement](ExpressionNames.REGR_REPLACEMENT)
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
      fileSourceScanExec.fileConstantMetadataColumns
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
    new TextScan(
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
            .getBucketId(f.toPath.getName)
            .getOrElse(throw invalidBucketFile(f.urlEncodedPath))
      }
  }

  override def getBatchScanExecTable(batchScan: BatchScanExec): Table = batchScan.table

  override def generatePartitionedFile(
      partitionValues: InternalRow,
      filePath: String,
      start: Long,
      length: Long,
      @transient locations: Array[String] = Array.empty): PartitionedFile =
    PartitionedFile(partitionValues, SparkPath.fromPathString(filePath), start, length, locations)

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
    val path = new Path(file.filePath.toString)
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
        case FileFormat.FILE_BLOCK_START =>
          metadataColumn.put(FileFormat.FILE_BLOCK_START, file.start.toString)
        case FileFormat.FILE_BLOCK_LENGTH =>
          metadataColumn.put(FileFormat.FILE_BLOCK_LENGTH, file.length.toString)
        case _ =>
      }
    }
    metadataColumn.put(InputFileName().prettyName, file.filePath.toString)
    metadataColumn.put(InputFileBlockStart().prettyName, file.start.toString)
    metadataColumn.put(InputFileBlockLength().prettyName, file.length.toString)
    metadataColumn
  }

  // https://issues.apache.org/jira/browse/SPARK-40400
  private def invalidBucketFile(path: String): Throwable = {
    new SparkException(
      errorClass = "INVALID_BUCKET_FILE",
      messageParameters = Map("path" -> path),
      cause = null)
  }

  private def getLimit(limit: Int, offset: Int): Int = {
    if (limit == -1) {
      // Only offset specified, so fetch the maximum number rows
      Int.MaxValue
    } else {
      assert(limit > offset)
      limit - offset
    }
  }

  override def getLimitAndOffsetFromGlobalLimit(plan: GlobalLimitExec): (Int, Int) = {
    (getLimit(plan.limit, plan.offset), plan.offset)
  }

  override def getLimitAndOffsetFromTopK(plan: TakeOrderedAndProjectExec): (Int, Int) = {
    (getLimit(plan.limit, plan.offset), plan.offset)
  }

  override def getExtendedColumnarPostRules(): List[SparkSession => Rule[SparkPlan]] = List()

  override def writeFilesExecuteTask(
      description: WriteJobDescription,
      jobTrackerID: String,
      sparkStageId: Int,
      sparkPartitionId: Int,
      sparkAttemptNumber: Int,
      committer: FileCommitProtocol,
      iterator: Iterator[InternalRow]): WriteTaskResult = {
    GlutenFileFormatWriter.writeFilesExecuteTask(
      description,
      jobTrackerID,
      sparkStageId,
      sparkPartitionId,
      sparkAttemptNumber,
      committer,
      iterator
    )
  }

  override def enableNativeWriteFilesByDefault(): Boolean = true

  override def createTestTaskContext(properties: Properties): TaskContext = {
    TaskContextUtils.createTestTaskContext(properties)
  }

  override def broadcastInternal[T: ClassTag](sc: SparkContext, value: T): Broadcast[T] = {
    SparkContextUtils.broadcastInternal(sc, value)
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
      sparkSchema: StructType): Boolean = {
    // SPARK-39634: Allow file splitting in combination with row index generation once
    // the fix for PARQUET-2161 is available.
    relation.fileFormat
      .isSplitable(relation.sparkSession, relation.options, filePath) &&
    !(RowIndexUtil.findRowIndexColumnIndexInSchema(sparkSchema) >= 0)
  }

  def isRowIndexMetadataColumn(name: String): Boolean = {
    name == FileFormat.ROW_INDEX_TEMPORARY_COLUMN_NAME
  }

  def findRowIndexColumnIndexInSchema(sparkSchema: StructType): Int = {
    sparkSchema.fields.zipWithIndex.find {
      case (field: StructField, _: Int) =>
        field.name == FileFormat.ROW_INDEX_TEMPORARY_COLUMN_NAME
    } match {
      case Some((field: StructField, idx: Int)) =>
        if (field.dataType != LongType && field.dataType != IntegerType) {
          throw new RuntimeException(
            s"${FileFormat.ROW_INDEX_TEMPORARY_COLUMN_NAME} " +
              s"must be of LongType or IntegerType")
        }
        idx
      case _ => -1
    }
  }

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

  override def getCommonPartitionValues(
      batchScan: BatchScanExec): Option[Seq[(InternalRow, Int)]] = {
    batchScan.commonPartitionValues
  }

  override def orderPartitions(
      scan: Scan,
      keyGroupedPartitioning: Option[Seq[Expression]],
      filteredPartitions: Seq[Seq[InputPartition]],
      outputPartitioning: Partitioning): Seq[InputPartition] = {
    scan match {
      case _ if keyGroupedPartitioning.isDefined =>
        var newPartitions = filteredPartitions
        outputPartitioning match {
          case p: KeyGroupedPartitioning =>
            val partitionMapping = newPartitions
              .map(
                s =>
                  InternalRowComparableWrapper(
                    s.head.asInstanceOf[HasPartitionKey],
                    p.expressions) -> s)
              .toMap
            newPartitions = p.partitionValues.map {
              partValue =>
                // Use empty partition for those partition values that are not present
                partitionMapping.getOrElse(
                  InternalRowComparableWrapper(partValue, p.expressions),
                  Seq.empty)
            }
          case _ =>
        }
        newPartitions.flatten
      case _ =>
        filteredPartitions.flatten
    }
  }

  override def supportsRowBased(plan: SparkPlan): Boolean = plan.supportsRowBased

  override def withTryEvalMode(expr: Expression): Boolean = {
    expr match {
      case a: Add => a.evalMode == EvalMode.TRY
      case s: Subtract => s.evalMode == EvalMode.TRY
      case d: Divide => d.evalMode == EvalMode.TRY
      case m: Multiply => m.evalMode == EvalMode.TRY
      case c: Cast => c.isTryCast
      case _ => false
    }
  }

  override def withAnsiEvalMode(expr: Expression): Boolean = {
    expr match {
      case a: Add => a.evalMode == EvalMode.ANSI
      case s: Subtract => s.evalMode == EvalMode.ANSI
      case d: Divide => d.evalMode == EvalMode.ANSI
      case m: Multiply => m.evalMode == EvalMode.ANSI
      case c: Cast => c.ansiEnabled
      case _ => false
    }
  }

  override def dateTimestampFormatInReadIsDefaultValue(
      csvOptions: CSVOptions,
      timeZone: String): Boolean = {
    val default = new CSVOptions(CaseInsensitiveMap(Map()), csvOptions.columnPruning, timeZone)
    csvOptions.dateFormatInRead == default.dateFormatInRead &&
    csvOptions.timestampFormatInRead == default.timestampFormatInRead &&
    csvOptions.timestampNTZFormatInRead == default.timestampNTZFormatInRead
  }

  override def isPlannedV1Write(write: DataWritingCommandExec): Boolean = {
    write.cmd.isInstanceOf[V1WriteCommand] && SQLConf.get.plannedWriteEnabled
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
      conf.parquetFilterPushDownStringPredicate,
      conf.parquetFilterPushDownInFilterThreshold,
      caseSensitive.getOrElse(conf.caseSensitiveAnalysis),
      RebaseSpec(LegacyBehaviorPolicy.CORRECTED)
    )
  }

  override def extractExpressionArrayInsert(arrayInsert: Expression): Seq[Expression] = {
    val expr = arrayInsert.asInstanceOf[ArrayInsert]
    Seq(expr.srcArrayExpr, expr.posExpr, expr.itemExpr, Literal(expr.legacyNegativeIndex))
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
}
