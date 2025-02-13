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
package org.apache.gluten.backendsapi.velox

import org.apache.gluten.backendsapi.SparkPlanExecApi
import org.apache.gluten.config.{GlutenConfig, ReservedKeys, VeloxConfig}
import org.apache.gluten.exception.GlutenNotSupportException
import org.apache.gluten.execution._
import org.apache.gluten.expression._
import org.apache.gluten.expression.aggregate.{HLLAdapter, VeloxBloomFilterAggregate, VeloxCollectList, VeloxCollectSet}
import org.apache.gluten.extension.columnar.FallbackTags
import org.apache.gluten.sql.shims.SparkShimLoader
import org.apache.gluten.vectorized.{ColumnarBatchSerializer, ColumnarBatchSerializeResult}

import org.apache.spark.{ShuffleDependency, SparkException}
import org.apache.spark.api.python.{ColumnarArrowEvalPythonExec, PullOutArrowEvalPythonPreProjectHelper}
import org.apache.spark.rdd.RDD
import org.apache.spark.serializer.Serializer
import org.apache.spark.shuffle.{GenShuffleWriterParameters, GlutenShuffleWriterWrapper}
import org.apache.spark.shuffle.utils.ShuffleUtil
import org.apache.spark.sql.catalyst.catalog.BucketSpec
import org.apache.spark.sql.catalyst.catalog.CatalogTypes.TablePartitionSpec
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate.{AggregateExpression, CollectList, CollectSet}
import org.apache.spark.sql.catalyst.optimizer.BuildSide
import org.apache.spark.sql.catalyst.plans.JoinType
import org.apache.spark.sql.catalyst.plans.physical._
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.datasources.FileFormat
import org.apache.spark.sql.execution.exchange.{BroadcastExchangeExec, ShuffleExchangeExec}
import org.apache.spark.sql.execution.joins.{BuildSideRelation, HashedRelationBroadcastMode}
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.sql.execution.python.ArrowEvalPythonExec
import org.apache.spark.sql.execution.unsafe.UnsafeColumnarBuildSideRelation
import org.apache.spark.sql.execution.utils.ExecUtil
import org.apache.spark.sql.expression.{UDFExpression, UserDefinedAggregateFunction}
import org.apache.spark.sql.hive.VeloxHiveUDFTransformer
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.task.TaskResources

import org.apache.commons.lang3.ClassUtils

import javax.ws.rs.core.UriBuilder

class VeloxSparkPlanExecApi extends SparkPlanExecApi {

  /** Transform GetArrayItem to Substrait. */
  override def genGetArrayItemTransformer(
      substraitExprName: String,
      left: ExpressionTransformer,
      right: ExpressionTransformer,
      original: Expression): ExpressionTransformer = {
    GenericExpressionTransformer(substraitExprName, Seq(left, right), original)
  }

  /** Transform NaNvl to Substrait. */
  override def genNaNvlTransformer(
      substraitExprName: String,
      left: ExpressionTransformer,
      right: ExpressionTransformer,
      original: NaNvl): ExpressionTransformer = {
    val condExpr = IsNaN(original.left)
    val condFuncName = ExpressionMappings.expressionsMap(classOf[IsNaN])
    val newExpr = If(condExpr, original.right, original.left)
    IfTransformer(
      substraitExprName,
      GenericExpressionTransformer(condFuncName, Seq(left), condExpr),
      right,
      left,
      newExpr)
  }

  override def genAtLeastNNonNullsTransformer(
      substraitExprName: String,
      children: Seq[ExpressionTransformer],
      original: AtLeastNNonNulls): ExpressionTransformer = {
    GenericExpressionTransformer(
      substraitExprName,
      Seq(LiteralTransformer(Literal(original.n))) ++ children,
      original)
  }

  /** Transform Uuid to Substrait. */
  override def genUuidTransformer(
      substraitExprName: String,
      original: Uuid): ExpressionTransformer = {
    GenericExpressionTransformer(
      substraitExprName,
      Seq(LiteralTransformer(original.randomSeed.get)),
      original)
  }

  override def genTryArithmeticTransformer(
      substraitExprName: String,
      left: ExpressionTransformer,
      right: ExpressionTransformer,
      original: TryEval,
      checkArithmeticExprName: String): ExpressionTransformer = {
    if (SparkShimLoader.getSparkShims.withAnsiEvalMode(original.child)) {
      throw new GlutenNotSupportException(
        s"${original.child.prettyName} with ansi mode is not supported")
    }
    original.child.dataType match {
      case LongType | IntegerType | ShortType | ByteType =>
      case _ => throw new GlutenNotSupportException(s"$substraitExprName is not supported")
    }
    // Offload to velox for only IntegralTypes.
    GenericExpressionTransformer(
      substraitExprName,
      Seq(GenericExpressionTransformer(checkArithmeticExprName, Seq(left, right), original)),
      original)
  }

  /**
   * Map arithmetic expr to different functions: substraitExprName or try(checkArithmeticExprName)
   * based on EvalMode.
   */
  override def genArithmeticTransformer(
      substraitExprName: String,
      left: ExpressionTransformer,
      right: ExpressionTransformer,
      original: Expression,
      checkArithmeticExprName: String): ExpressionTransformer = {
    if (SparkShimLoader.getSparkShims.withTryEvalMode(original)) {
      original.dataType match {
        case LongType | IntegerType | ShortType | ByteType =>
        case _ =>
          throw new GlutenNotSupportException(s"$substraitExprName with try mode is not supported")
      }
      // Offload to velox for only IntegralTypes.
      GenericExpressionTransformer(
        ExpressionMappings.expressionsMap(classOf[TryEval]),
        Seq(GenericExpressionTransformer(checkArithmeticExprName, Seq(left, right), original)),
        original)
    } else if (SparkShimLoader.getSparkShims.withAnsiEvalMode(original)) {
      throw new GlutenNotSupportException(s"$substraitExprName with ansi mode is not supported")
    } else {
      if (
        left.dataType.isInstanceOf[DecimalType] && right.dataType
          .isInstanceOf[DecimalType] && !SQLConf.get.decimalOperationsAllowPrecisionLoss
      ) {
        // https://github.com/facebookincubator/velox/pull/10383
        val newName = substraitExprName + "_deny_precision_loss"
        GenericExpressionTransformer(newName, Seq(left, right), original)
      } else {
        GenericExpressionTransformer(substraitExprName, Seq(left, right), original)
      }
    }
  }

  /** Transform map_entries to Substrait. */
  override def genMapEntriesTransformer(
      substraitExprName: String,
      child: ExpressionTransformer,
      expr: Expression): ExpressionTransformer = {
    GenericExpressionTransformer(substraitExprName, Seq(child), expr)
  }

  /** Transform array filter to Substrait. */
  override def genArrayFilterTransformer(
      substraitExprName: String,
      argument: ExpressionTransformer,
      function: ExpressionTransformer,
      expr: ArrayFilter): ExpressionTransformer = {
    expr.function match {
      case LambdaFunction(_, arguments, _) if arguments.size == 2 =>
        throw new GlutenNotSupportException(
          "filter on array with lambda using index argument is not supported yet")
      case _ => GenericExpressionTransformer(substraitExprName, Seq(argument, function), expr)
    }
  }

  /** Transform array forall to Substrait. */
  override def genArrayForAllTransformer(
      substraitExprName: String,
      argument: ExpressionTransformer,
      function: ExpressionTransformer,
      expr: ArrayForAll): ExpressionTransformer = {
    expr.function match {
      case LambdaFunction(_, arguments, _) if arguments.size == 2 =>
        throw new GlutenNotSupportException(
          "forall on array with lambda using index argument is not supported yet")
      case _ => GenericExpressionTransformer(substraitExprName, Seq(argument, function), expr)
    }
  }

  /** Transform array exists to Substrait */
  override def genArrayExistsTransformer(
      substraitExprName: String,
      argument: ExpressionTransformer,
      function: ExpressionTransformer,
      expr: ArrayExists): ExpressionTransformer = {
    expr.function match {
      case LambdaFunction(_, arguments, _) if arguments.size == 2 =>
        throw new GlutenNotSupportException(
          "exists on array with lambda using index argument is not supported yet")
      case _ => GenericExpressionTransformer(substraitExprName, Seq(argument, function), expr)
    }
  }

  /** Transform array transform to Substrait. */
  override def genArrayTransformTransformer(
      substraitExprName: String,
      argument: ExpressionTransformer,
      function: ExpressionTransformer,
      expr: ArrayTransform): ExpressionTransformer = {
    expr.function match {
      case LambdaFunction(_, arguments, _) if arguments.size == 2 =>
        throw new GlutenNotSupportException(
          "transform on array with lambda using index argument is not supported yet")
      case _ => GenericExpressionTransformer(substraitExprName, Seq(argument, function), expr)
    }
  }

  /** Transform posexplode to Substrait. */
  override def genPosExplodeTransformer(
      substraitExprName: String,
      child: ExpressionTransformer,
      original: PosExplode,
      attrSeq: Seq[Attribute]): ExpressionTransformer = {
    GenericExpressionTransformer(substraitExprName, Seq(child), attrSeq.head)
  }

  /** Transform inline to Substrait. */
  override def genInlineTransformer(
      substraitExprName: String,
      child: ExpressionTransformer,
      expr: Expression): ExpressionTransformer = {
    GenericExpressionTransformer(substraitExprName, Seq(child), expr)
  }

  override def genLikeTransformer(
      substraitExprName: String,
      left: ExpressionTransformer,
      right: ExpressionTransformer,
      original: Like): ExpressionTransformer = {
    GenericExpressionTransformer(
      substraitExprName,
      Seq(left, right, LiteralTransformer(original.escapeChar)),
      original)
  }

  /** Transform make_timestamp to Substrait. */
  override def genMakeTimestampTransformer(
      substraitExprName: String,
      children: Seq[ExpressionTransformer],
      expr: Expression): ExpressionTransformer = {
    GenericExpressionTransformer(substraitExprName, children, expr)
  }

  override def genDateDiffTransformer(
      substraitExprName: String,
      endDate: ExpressionTransformer,
      startDate: ExpressionTransformer,
      original: DateDiff): ExpressionTransformer = {
    GenericExpressionTransformer(substraitExprName, Seq(endDate, startDate), original)
  }

  override def genPreciseTimestampConversionTransformer(
      substraitExprName: String,
      children: Seq[ExpressionTransformer],
      expr: PreciseTimestampConversion): ExpressionTransformer = {
    // Expression used internally to convert the TimestampType to Long and back without losing
    // precision, i.e. in microseconds.
    val (newSubstraitName, newExpr) = expr match {
      case _ @PreciseTimestampConversion(_, TimestampType, LongType) =>
        (ExpressionMappings.expressionsMap(classOf[UnixMicros]), UnixMicros(expr.child))
      case _ @PreciseTimestampConversion(_, LongType, TimestampType) =>
        (
          ExpressionMappings.expressionsMap(classOf[MicrosToTimestamp]),
          MicrosToTimestamp(expr.child))
      case _ =>
        // TimestampNTZType is not supported here.
        throw new GlutenNotSupportException("PreciseTimestampConversion is not supported")
    }
    GenericExpressionTransformer(newSubstraitName, children, newExpr)
  }

  /**
   * Generate FilterExecTransformer.
   *
   * @param condition
   *   : the filter condition
   * @param child
   *   : the child of FilterExec
   * @return
   *   the transformer of FilterExec
   */
  override def genFilterExecTransformer(
      condition: Expression,
      child: SparkPlan): FilterExecTransformerBase = {
    FilterExecTransformer(condition, child)
  }

  /** Generate HashAggregateExecTransformer. */
  override def genHashAggregateExecTransformer(
      requiredChildDistributionExpressions: Option[Seq[Expression]],
      groupingExpressions: Seq[NamedExpression],
      aggregateExpressions: Seq[AggregateExpression],
      aggregateAttributes: Seq[Attribute],
      initialInputBufferOffset: Int,
      resultExpressions: Seq[NamedExpression],
      child: SparkPlan): HashAggregateExecBaseTransformer =
    RegularHashAggregateExecTransformer(
      requiredChildDistributionExpressions,
      groupingExpressions,
      aggregateExpressions,
      aggregateAttributes,
      initialInputBufferOffset,
      resultExpressions,
      child)

  /** Generate HashAggregateExecPullOutHelper */
  override def genHashAggregateExecPullOutHelper(
      aggregateExpressions: Seq[AggregateExpression],
      aggregateAttributes: Seq[Attribute]): HashAggregateExecPullOutBaseHelper =
    HashAggregateExecPullOutHelper(aggregateExpressions, aggregateAttributes)

  override def genColumnarShuffleExchange(shuffle: ShuffleExchangeExec): SparkPlan = {
    def allowHashOnMap[T](f: => T): T = {
      val originalAllowHash = SQLConf.get.getConf(SQLConf.LEGACY_ALLOW_HASH_ON_MAPTYPE)
      try {
        SQLConf.get.setConf(SQLConf.LEGACY_ALLOW_HASH_ON_MAPTYPE, true)
        f
      } finally {
        SQLConf.get.setConf(SQLConf.LEGACY_ALLOW_HASH_ON_MAPTYPE, originalAllowHash)
      }
    }

    def maybeAddAppendBatchesExec(plan: SparkPlan): SparkPlan = {
      plan match {
        case shuffle: ColumnarShuffleExchangeExec
            if !shuffle.useSortBasedShuffle &&
              VeloxConfig.get.veloxResizeBatchesShuffleInput =>
          val range = VeloxConfig.get.veloxResizeBatchesShuffleInputRange
          val appendBatches =
            VeloxResizeBatchesExec(shuffle.child, range.min, range.max)
          shuffle.withNewChildren(Seq(appendBatches))
        case _ => plan
      }
    }

    val child = shuffle.child

    val newShuffle = shuffle.outputPartitioning match {
      case HashPartitioning(exprs, _) =>
        val hashExpr = new Murmur3Hash(exprs)
        val projectList = Seq(Alias(hashExpr, "hash_partition_key")()) ++ child.output
        val projectTransformer = ProjectExecTransformer(projectList, child)
        val validationResult = projectTransformer.doValidate()
        if (validationResult.ok()) {
          ColumnarShuffleExchangeExec(
            shuffle,
            projectTransformer,
            projectTransformer.output.drop(1))
        } else {
          FallbackTags.add(shuffle, validationResult)
          shuffle.withNewChildren(child :: Nil)
        }
      case RoundRobinPartitioning(num) if SQLConf.get.sortBeforeRepartition && num > 1 =>
        // scalastyle:off line.size.limit
        // Temporarily allow hash on map if it's disabled, otherwise HashExpression will fail to get
        // resolved if its child contains map type.
        // See https://github.com/apache/spark/blob/609bd4839e5d504917de74ed1cb9c23645fba51f/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/expressions/hash.scala#L279-L283
        // scalastyle:on line.size.limit
        allowHashOnMap {
          // Velox hash expression does not support null type and we also do not need to sort
          // null type since the value always be null.
          val columnsForHash = child.output.filterNot(_.dataType == NullType)
          if (columnsForHash.isEmpty) {
            ColumnarShuffleExchangeExec(shuffle, child, child.output)
          } else {
            val hashExpr = new Murmur3Hash(columnsForHash)
            val projectList = Seq(Alias(hashExpr, "hash_partition_key")()) ++ child.output
            val projectTransformer = ProjectExecTransformer(projectList, child)
            val projectBeforeSortValidationResult = projectTransformer.doValidate()
            // Make sure we support offload hash expression
            val projectBeforeSort = if (projectBeforeSortValidationResult.ok()) {
              projectTransformer
            } else {
              val project = ProjectExec(projectList, child)
              FallbackTags.add(project, projectBeforeSortValidationResult)
              project
            }
            val sortOrder = SortOrder(projectBeforeSort.output.head, Ascending)
            val sortByHashCode =
              SortExecTransformer(Seq(sortOrder), global = false, projectBeforeSort)
            val dropSortColumnTransformer =
              ProjectExecTransformer(projectList.drop(1), sortByHashCode)
            val validationResult = dropSortColumnTransformer.doValidate()
            if (validationResult.ok()) {
              ColumnarShuffleExchangeExec(
                shuffle,
                dropSortColumnTransformer,
                dropSortColumnTransformer.output)
            } else {
              FallbackTags.add(shuffle, validationResult)
              shuffle.withNewChildren(child :: Nil)
            }
          }
        }
      case _ =>
        ColumnarShuffleExchangeExec(shuffle, child, null)
    }
    maybeAddAppendBatchesExec(newShuffle)
  }

  /** Generate ShuffledHashJoinExecTransformer. */
  override def genShuffledHashJoinExecTransformer(
      leftKeys: Seq[Expression],
      rightKeys: Seq[Expression],
      joinType: JoinType,
      buildSide: BuildSide,
      condition: Option[Expression],
      left: SparkPlan,
      right: SparkPlan,
      isSkewJoin: Boolean): ShuffledHashJoinExecTransformerBase =
    ShuffledHashJoinExecTransformer(
      leftKeys,
      rightKeys,
      joinType,
      buildSide,
      condition,
      left,
      right,
      isSkewJoin)

  /** Generate BroadcastHashJoinExecTransformer. */
  override def genBroadcastHashJoinExecTransformer(
      leftKeys: Seq[Expression],
      rightKeys: Seq[Expression],
      joinType: JoinType,
      buildSide: BuildSide,
      condition: Option[Expression],
      left: SparkPlan,
      right: SparkPlan,
      isNullAwareAntiJoin: Boolean = false): BroadcastHashJoinExecTransformerBase =
    BroadcastHashJoinExecTransformer(
      leftKeys,
      rightKeys,
      joinType,
      buildSide,
      condition,
      left,
      right,
      isNullAwareAntiJoin)

  override def genSampleExecTransformer(
      lowerBound: Double,
      upperBound: Double,
      withReplacement: Boolean,
      seed: Long,
      child: SparkPlan): SampleExecTransformer = {
    SampleExecTransformer(lowerBound, upperBound, withReplacement, seed, child)
  }

  override def genSortMergeJoinExecTransformer(
      leftKeys: Seq[Expression],
      rightKeys: Seq[Expression],
      joinType: JoinType,
      condition: Option[Expression],
      left: SparkPlan,
      right: SparkPlan,
      isSkewJoin: Boolean = false,
      projectList: Seq[NamedExpression] = null): SortMergeJoinExecTransformerBase = {
    SortMergeJoinExecTransformer(
      leftKeys,
      rightKeys,
      joinType,
      condition,
      left,
      right,
      isSkewJoin,
      projectList)
  }
  override def genCartesianProductExecTransformer(
      left: SparkPlan,
      right: SparkPlan,
      condition: Option[Expression]): CartesianProductExecTransformer = {
    CartesianProductExecTransformer(
      ColumnarCartesianProductBridge(left),
      ColumnarCartesianProductBridge(right),
      condition)
  }

  override def genBroadcastNestedLoopJoinExecTransformer(
      left: SparkPlan,
      right: SparkPlan,
      buildSide: BuildSide,
      joinType: JoinType,
      condition: Option[Expression]): BroadcastNestedLoopJoinExecTransformer =
    VeloxBroadcastNestedLoopJoinExecTransformer(left, right, buildSide, joinType, condition)

  override def genHashExpressionTransformer(
      substraitExprName: String,
      exprs: Seq[ExpressionTransformer],
      original: HashExpression[_]): ExpressionTransformer = {
    VeloxHashExpressionTransformer(substraitExprName, exprs, original)
  }

  /**
   * Generate ShuffleDependency for ColumnarShuffleExchangeExec.
   *
   * @return
   */
  // scalastyle:off argcount
  override def genShuffleDependency(
      rdd: RDD[ColumnarBatch],
      childOutputAttributes: Seq[Attribute],
      projectOutputAttributes: Seq[Attribute],
      newPartitioning: Partitioning,
      serializer: Serializer,
      writeMetrics: Map[String, SQLMetric],
      metrics: Map[String, SQLMetric],
      isSort: Boolean): ShuffleDependency[Int, ColumnarBatch, ColumnarBatch] = {
    // scalastyle:on argcount
    ExecUtil.genShuffleDependency(
      rdd,
      childOutputAttributes,
      newPartitioning,
      serializer,
      writeMetrics,
      metrics,
      isSort)
  }
  // scalastyle:on argcount

  /** Determine whether to use sort-based shuffle based on shuffle partitioning and output. */
  override def useSortBasedShuffle(partitioning: Partitioning, output: Seq[Attribute]): Boolean = {
    val conf = GlutenConfig.get
    lazy val isCelebornSortBasedShuffle = conf.isUseCelebornShuffleManager &&
      conf.celebornShuffleWriterType == ReservedKeys.GLUTEN_SORT_SHUFFLE_WRITER
    partitioning != SinglePartition &&
    (partitioning.numPartitions >= GlutenConfig.get.columnarShuffleSortPartitionsThreshold ||
      output.size >= GlutenConfig.get.columnarShuffleSortColumnsThreshold) ||
    isCelebornSortBasedShuffle
  }

  /**
   * Generate ColumnarShuffleWriter for ColumnarShuffleManager.
   *
   * @return
   */
  override def genColumnarShuffleWriter[K, V](
      parameters: GenShuffleWriterParameters[K, V]): GlutenShuffleWriterWrapper[K, V] = {
    ShuffleUtil.genColumnarShuffleWriter(parameters)
  }
  override def createColumnarWriteFilesExec(
      child: WriteFilesExecTransformer,
      noop: SparkPlan,
      fileFormat: FileFormat,
      partitionColumns: Seq[Attribute],
      bucketSpec: Option[BucketSpec],
      options: Map[String, String],
      staticPartitions: TablePartitionSpec): ColumnarWriteFilesExec = {
    VeloxColumnarWriteFilesExec(
      child,
      noop,
      child,
      fileFormat,
      partitionColumns,
      bucketSpec,
      options,
      staticPartitions)
  }

  override def createColumnarArrowEvalPythonExec(
      udfs: Seq[PythonUDF],
      resultAttrs: Seq[Attribute],
      child: SparkPlan,
      evalType: Int): SparkPlan = {
    ColumnarArrowEvalPythonExec(udfs, resultAttrs, child, evalType)
  }

  /**
   * Generate ColumnarBatchSerializer for ColumnarShuffleExchangeExec.
   *
   * @return
   */
  override def createColumnarBatchSerializer(
      schema: StructType,
      metrics: Map[String, SQLMetric],
      isSort: Boolean): Serializer = {
    val numOutputRows = metrics("numOutputRows")
    val deserializeTime = metrics("deserializeTime")
    val readBatchNumRows = metrics("avgReadBatchNumRows")
    val decompressTime = metrics("decompressTime")
    if (GlutenConfig.get.isUseCelebornShuffleManager) {
      val clazz = ClassUtils.getClass("org.apache.spark.shuffle.CelebornColumnarBatchSerializer")
      val constructor =
        clazz.getConstructor(classOf[StructType], classOf[SQLMetric], classOf[SQLMetric])
      constructor.newInstance(schema, readBatchNumRows, numOutputRows).asInstanceOf[Serializer]
    } else {
      new ColumnarBatchSerializer(
        schema,
        readBatchNumRows,
        numOutputRows,
        deserializeTime,
        decompressTime,
        isSort)
    }
  }

  /** Create broadcast relation for BroadcastExchangeExec */
  override def createBroadcastRelation(
      mode: BroadcastMode,
      child: SparkPlan,
      numOutputRows: SQLMetric,
      dataSize: SQLMetric): BuildSideRelation = {
    val useOffheapBroadcastBuildRelation =
      VeloxConfig.get.enableBroadcastBuildRelationInOffheap
    val serialized: Array[ColumnarBatchSerializeResult] = child
      .executeColumnar()
      .mapPartitions(itr => Iterator(BroadcastUtils.serializeStream(itr)))
      .filter(_.getNumRows != 0)
      .collect
    val rawSize = serialized.map(_.getSerialized.length).sum
    if (rawSize >= BroadcastExchangeExec.MAX_BROADCAST_TABLE_BYTES) {
      throw new SparkException(
        s"Cannot broadcast the table that is larger than 8GB: ${rawSize >> 30} GB")
    }
    numOutputRows += serialized.map(_.getNumRows).sum
    dataSize += rawSize
    if (useOffheapBroadcastBuildRelation) {
      TaskResources.runUnsafe {
        new UnsafeColumnarBuildSideRelation(child.output, serialized.map(_.getSerialized), mode)
      }
    } else {
      ColumnarBuildSideRelation(child.output, serialized.map(_.getSerialized), mode)
    }
  }

  override def doCanonicalizeForBroadcastMode(mode: BroadcastMode): BroadcastMode = {
    mode match {
      case hash: HashedRelationBroadcastMode =>
        // Node: It's different with vanilla Spark.
        // Vanilla Spark build HashRelation at driver side, so it is build keys sensitive.
        // But we broadcast byte array and build HashRelation at executor side,
        // the build keys are actually meaningless for the broadcast value.
        // This change allows us reuse broadcast exchange for different build keys with same table.
        hash.copy(key = Seq.empty)
      case _ => mode.canonicalized
    }
  }

  /**
   * * Expressions.
   */

  /**
   * Generate Alias transformer.
   *
   * @return
   *   a transformer for alias
   */
  override def genAliasTransformer(
      substraitExprName: String,
      child: ExpressionTransformer,
      original: Expression): ExpressionTransformer =
    VeloxAliasTransformer(substraitExprName, child, original)

  /** Generate an expression transformer to transform GetMapValue to Substrait. */
  override def genGetMapValueTransformer(
      substraitExprName: String,
      left: ExpressionTransformer,
      right: ExpressionTransformer,
      original: GetMapValue): ExpressionTransformer = {
    GenericExpressionTransformer(
      ExpressionMappings.expressionsMap(classOf[ElementAt]),
      Seq(left, right),
      original)
  }

  override def genStringToMapTransformer(
      substraitExprName: String,
      children: Seq[ExpressionTransformer],
      expr: Expression): ExpressionTransformer = {
    if (
      SQLConf.get.getConf(SQLConf.MAP_KEY_DEDUP_POLICY)
        != SQLConf.MapKeyDedupPolicy.EXCEPTION.toString
    ) {
      throw new GlutenNotSupportException("Only EXCEPTION policy is supported!")
    }
    GenericExpressionTransformer(substraitExprName, children, expr)
  }

  /** Generate an expression transformer to transform NamedStruct to Substrait. */
  override def genNamedStructTransformer(
      substraitExprName: String,
      children: Seq[ExpressionTransformer],
      original: CreateNamedStruct,
      attributeSeq: Seq[Attribute]): ExpressionTransformer = {
    VeloxNamedStructTransformer(substraitExprName, original, attributeSeq)
  }

  /** Generate an ExpressionTransformer to transform GetStructFiled expression. */
  override def genGetStructFieldTransformer(
      substraitExprName: String,
      childTransformer: ExpressionTransformer,
      ordinal: Int,
      original: GetStructField): ExpressionTransformer = {
    VeloxGetStructFieldTransformer(substraitExprName, childTransformer, ordinal, original)
  }

  /**
   * To align with spark in casting string type input to other types, add trim node for trimming
   * space or whitespace. See spark's Cast.scala.
   */
  override def genCastWithNewChild(c: Cast): Cast = {
    // scalastyle:off nonascii
    // Common whitespace to be trimmed, including: ' ', '\n', '\r', '\f', etc.
    val trimWhitespaceStr = " \t\n\u000B\u000C\u000D\u001C\u001D\u001E\u001F"
    // Space separator.
    val trimSpaceSepStr = "\u1680\u2008\u2009\u200A\u205F\u3000" +
      ('\u2000' to '\u2006').toList.mkString
    // Line separator.
    val trimLineSepStr = "\u2028"
    // Paragraph separator.
    val trimParaSepStr = "\u2029"
    // Needs to be trimmed for casting to float/double/decimal
    val trimSpaceStr = ('\u0000' to '\u0020').toList.mkString
    // ISOControl characters, refer java.lang.Character.isISOControl(int)
    val isoControlStr = (('\u0000' to '\u001F') ++ ('\u007F' to '\u009F')).toList.mkString
    // scalastyle:on nonascii
    if (VeloxConfig.get.castFromVarcharAddTrimNode && c.child.dataType == StringType) {
      val trimStr = c.dataType match {
        case BinaryType | _: ArrayType | _: MapType | _: StructType | _: UserDefinedType[_] =>
          None
        case FloatType | DoubleType | _: DecimalType =>
          Some(trimSpaceStr)
        case _ =>
          Some(
            (trimWhitespaceStr + trimSpaceSepStr + trimLineSepStr
              + trimParaSepStr + isoControlStr).toSet.mkString
          )
      }
      trimStr
        .map {
          trim =>
            c.withNewChildren(Seq(StringTrim(c.child, Some(Literal(trim))))).asInstanceOf[Cast]
        }
        .getOrElse(c)
    } else {
      c
    }
  }

  /** Define backend specfic expression mappings. */
  override def extraExpressionMappings: Seq[Sig] = {
    Seq(
      Sig[HLLAdapter](ExpressionNames.APPROX_DISTINCT),
      Sig[UDFExpression](ExpressionNames.UDF_PLACEHOLDER),
      Sig[UserDefinedAggregateFunction](ExpressionNames.UDAF_PLACEHOLDER),
      Sig[NaNvl](ExpressionNames.NANVL),
      Sig[VeloxCollectList](ExpressionNames.COLLECT_LIST),
      Sig[CollectList](ExpressionNames.COLLECT_LIST),
      Sig[VeloxCollectSet](ExpressionNames.COLLECT_SET),
      Sig[CollectSet](ExpressionNames.COLLECT_SET),
      Sig[VeloxBloomFilterMightContain](ExpressionNames.MIGHT_CONTAIN),
      Sig[VeloxBloomFilterAggregate](ExpressionNames.BLOOM_FILTER_AGG),
      // For test purpose.
      Sig[VeloxDummyExpression](VeloxDummyExpression.VELOX_DUMMY_EXPRESSION)
    )
  }

  override def rewriteSpillPath(path: String): String = {
    val fs = VeloxConfig.get.veloxSpillFileSystem
    fs match {
      case "local" =>
        path
      case "heap-over-local" =>
        val rewritten = UriBuilder
          .fromPath(path)
          .scheme("jol")
          .toString
        rewritten
      case other =>
        throw new IllegalStateException(s"Unsupported fs: $other")
    }
  }

  override def genGenerateTransformer(
      generator: Generator,
      requiredChildOutput: Seq[Attribute],
      outer: Boolean,
      generatorOutput: Seq[Attribute],
      child: SparkPlan): GenerateExecTransformerBase = {
    GenerateExecTransformer(generator, requiredChildOutput, outer, generatorOutput, child)
  }

  override def genPreProjectForGenerate(generate: GenerateExec): SparkPlan = {
    PullOutGenerateProjectHelper.pullOutPreProject(generate)
  }

  override def genPostProjectForGenerate(generate: GenerateExec): SparkPlan = {
    PullOutGenerateProjectHelper.pullOutPostProject(generate)
  }

  override def genPreProjectForArrowEvalPythonExec(
      arrowEvalPythonExec: ArrowEvalPythonExec): SparkPlan = {
    PullOutArrowEvalPythonPreProjectHelper.pullOutPreProject(arrowEvalPythonExec)
  }

  override def maybeCollapseTakeOrderedAndProject(plan: SparkPlan): SparkPlan = {
    // This to-top-n optimization assumes exchange operators were already placed in input plan.
    plan.transformUp {
      case p @ LimitExecTransformer(SortExecTransformer(sortOrder, _, child, _), 0, count) =>
        val global = child.outputPartitioning.satisfies(AllTuples)
        val topN = TopNTransformer(count, sortOrder, global, child)
        if (topN.doValidate().ok()) {
          topN
        } else {
          p
        }
      case other => other
    }
  }

  override def genHiveUDFTransformer(
      expr: Expression,
      attributeSeq: Seq[Attribute]): ExpressionTransformer = {
    VeloxHiveUDFTransformer.replaceWithExpressionTransformer(expr, attributeSeq)
  }

  override def genColumnarRangeExec(
      start: Long,
      end: Long,
      step: Long,
      numSlices: Int,
      numElements: BigInt,
      outputAttributes: Seq[Attribute],
      child: Seq[SparkPlan]): ColumnarRangeBaseExec =
    ColumnarRangeExec(start, end, step, numSlices, numElements, outputAttributes, child)

}
