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

import org.apache.gluten.GlutenConfig
import org.apache.gluten.backendsapi.{BackendsApiManager, SparkPlanExecApi}
import org.apache.gluten.exception.GlutenNotSupportException
import org.apache.gluten.execution._
import org.apache.gluten.expression._
import org.apache.gluten.extension.{CommonSubexpressionEliminateRule, CountDistinctWithoutExpand, FallbackBroadcastHashJoin, FallbackBroadcastHashJoinPrepQueryStage, RewriteDateTimestampComparisonRule, RewriteToDateExpresstionRule}
import org.apache.gluten.extension.columnar.AddFallbackTagRule
import org.apache.gluten.extension.columnar.MiscColumnarRules.TransformPreOverrides
import org.apache.gluten.extension.columnar.transition.Convention
import org.apache.gluten.sql.shims.SparkShimLoader
import org.apache.gluten.substrait.expression.{ExpressionBuilder, ExpressionNode, WindowFunctionNode}
import org.apache.gluten.utils.{CHJoinValidateUtil, UnknownJoinStrategy}
import org.apache.gluten.vectorized.CHColumnarBatchSerializer

import org.apache.spark.{ShuffleDependency, SparkException}
import org.apache.spark.rdd.RDD
import org.apache.spark.serializer.Serializer
import org.apache.spark.shuffle.{GenShuffleWriterParameters, GlutenShuffleWriterWrapper, HashPartitioningWrapper}
import org.apache.spark.shuffle.utils.CHShuffleUtil
import org.apache.spark.sql.{SparkSession, Strategy}
import org.apache.spark.sql.catalyst.{CHAggregateFunctionRewriteRule, EqualToRewrite}
import org.apache.spark.sql.catalyst.catalog.BucketSpec
import org.apache.spark.sql.catalyst.catalog.CatalogTypes.TablePartitionSpec
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate.{AggregateExpression, CollectList, CollectSet}
import org.apache.spark.sql.catalyst.optimizer.BuildSide
import org.apache.spark.sql.catalyst.plans.JoinType
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.plans.physical.{BroadcastMode, HashPartitioning, Partitioning, RangePartitioning}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.delta.files.TahoeFileIndex
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.adaptive.AQEShuffleReadExec
import org.apache.spark.sql.execution.datasources.{FileFormat, HadoopFsRelation}
import org.apache.spark.sql.execution.datasources.parquet.ParquetFileFormat
import org.apache.spark.sql.execution.datasources.v2.clickhouse.source.DeltaMergeTreeFileFormat
import org.apache.spark.sql.execution.exchange.{BroadcastExchangeExec, ShuffleExchangeExec}
import org.apache.spark.sql.execution.joins.{BuildSideRelation, ClickHouseBuildSideRelation, HashedRelationBroadcastMode}
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.sql.execution.utils.{CHExecUtil, PushDownUtil}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.vectorized.ColumnarBatch

import org.apache.commons.lang3.ClassUtils

import java.lang.{Long => JLong}
import java.util.{ArrayList => JArrayList, List => JList, Map => JMap}

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer

class CHSparkPlanExecApi extends SparkPlanExecApi {

  /** The columnar-batch type this backend is using. */
  override def batchType: Convention.BatchType = CHBatch

  /** Transform GetArrayItem to Substrait. */
  override def genGetArrayItemTransformer(
      substraitExprName: String,
      left: ExpressionTransformer,
      right: ExpressionTransformer,
      original: Expression): ExpressionTransformer = {
    GetArrayItemTransformer(substraitExprName, left, right, original)
  }

  override def genProjectExecTransformer(
      projectList: Seq[NamedExpression],
      child: SparkPlan): ProjectExecTransformer = {
    def processProjectExecTransformer(projectList: Seq[NamedExpression]): Seq[NamedExpression] = {
      // When there is a MergeScalarSubqueries which will create the named_struct with the
      // same name, looks like {'bloomFilter', BF1, 'bloomFilter', BF2}
      // or {'count(1)', count(1)#111L, 'avg(a)', avg(a)#222L, 'count(1)', count(1)#333L},
      // it will cause problem for ClickHouse backend,
      // which cannot tolerate duplicate type names in struct type,
      // so we need to rename 'nameExpr' in the named_struct to make them unique
      // after executing the MergeScalarSubqueries.
      var needToReplace = false
      val newProjectList = projectList.map {
        case alias @ Alias(cns @ CreateNamedStruct(children: Seq[Expression]), "mergedValue") =>
          // check whether there are some duplicate names
          if (cns.nameExprs.distinct.size == cns.nameExprs.size) {
            alias
          } else {
            val newChildren = children
              .grouped(2)
              .flatMap {
                case Seq(name: Literal, value: NamedExpression) =>
                  val newLiteral = Literal(name.toString() + "#" + value.exprId.id)
                  Seq(newLiteral, value)
                case Seq(name, value) => Seq(name, value)
              }
              .toSeq
            needToReplace = true
            Alias.apply(CreateNamedStruct(newChildren), "mergedValue")(alias.exprId)
          }
        case other: NamedExpression => other
      }

      if (!needToReplace) {
        projectList
      } else {
        newProjectList
      }
    }

    ProjectExecTransformer.createUnsafe(processProjectExecTransformer(projectList), child)
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

    def checkMergeTreeFileFormat(relation: HadoopFsRelation): Boolean = {
      relation.location.isInstanceOf[TahoeFileIndex] &&
      relation.fileFormat.isInstanceOf[DeltaMergeTreeFileFormat]
    }

    child match {
      case scan: FileSourceScanExec if (checkMergeTreeFileFormat(scan.relation)) =>
        // For the validation phase of the AddFallbackTagRule
        CHFilterExecTransformer(condition, child)
      case scan: FileSourceScanExecTransformerBase if (checkMergeTreeFileFormat(scan.relation)) =>
        // For the transform phase, the FileSourceScanExec is already transformed
        CHFilterExecTransformer(condition, child)
      case _ =>
        FilterExecTransformer(condition, child)
    }
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
    CHHashAggregateExecTransformer(
      requiredChildDistributionExpressions,
      groupingExpressions.distinct,
      aggregateExpressions,
      aggregateAttributes,
      initialInputBufferOffset,
      resultExpressions.distinct,
      child
    )

  /** Generate HashAggregateExecPullOutHelper */
  override def genHashAggregateExecPullOutHelper(
      aggregateExpressions: Seq[AggregateExpression],
      aggregateAttributes: Seq[Attribute]): HashAggregateExecPullOutBaseHelper =
    CHHashAggregateExecPullOutHelper(aggregateExpressions, aggregateAttributes)

  /**
   * If there are expressions (not field reference) in the partitioning's children, add a projection
   * before shuffle exchange and make a new partitioning with the old expressions replaced by the
   * result columns from the projection.
   */
  private def addProjectionForShuffleExchange(
      plan: ShuffleExchangeExec): (Int, Partitioning, SparkPlan) = {
    def selectExpressions(
        exprs: Seq[Expression],
        attributes: Seq[Attribute]): (Seq[NamedExpression], Seq[Int]) = {
      var expressionPos = Seq[Int]()
      var projectExpressions = Seq[NamedExpression]()

      exprs.foreach(
        expr => {
          if (!expr.isInstanceOf[AttributeReference]) {
            val n = projectExpressions.size
            val namedExpression = Alias(expr, s"projected_partitioning_value_$n")()
            projectExpressions = projectExpressions :+ namedExpression
            expressionPos = expressionPos :+ (attributes.size + n)
          } else {
            // the new projected columns are appended at the end
            expressionPos = expressionPos :+ BindReferences
              .bindReference(expr, attributes)
              .asInstanceOf[BoundReference]
              .ordinal
          }
        })
      (projectExpressions, expressionPos)
    }

    plan.outputPartitioning match {
      case HashPartitioning(exprs, numPartitions) =>
        val (projectExpressions, newExpressionsPosition) = {
          selectExpressions(
            exprs,
            BackendsApiManager.getTransformerApiInstance
              .getPlanOutput(plan.child))
        }
        if (projectExpressions.isEmpty) {
          return (0, plan.outputPartitioning, plan.child)
        }
        // FIXME: The operation happens inside ReplaceSingleNode().
        //  Caller may not know it adds project on top of the shuffle.
        val project = TransformPreOverrides().apply(
          AddFallbackTagRule().apply(
            ProjectExec(plan.child.output ++ projectExpressions, plan.child)))
        var newExprs = Seq[Expression]()
        for (i <- exprs.indices) {
          val pos = newExpressionsPosition(i)
          newExprs = newExprs :+ project.output(pos)
        }
        (
          projectExpressions.size,
          new HashPartitioningWrapper(exprs, newExprs, numPartitions),
          project)
      case RangePartitioning(orderings, numPartitions) =>
        val exprs = orderings.map(ordering => ordering.child)
        val (projectExpressions, newExpressionsPosition) = {
          selectExpressions(
            exprs,
            BackendsApiManager.getTransformerApiInstance
              .getPlanOutput(plan.child))
        }
        if (projectExpressions.isEmpty) {
          return (0, plan.outputPartitioning, plan.child)
        }
        // FIXME: The operation happens inside ReplaceSingleNode().
        //  Caller may not know it adds project on top of the shuffle.
        val project = TransformPreOverrides().apply(
          AddFallbackTagRule().apply(
            ProjectExec(plan.child.output ++ projectExpressions, plan.child)))
        var newOrderings = Seq[SortOrder]()
        for (i <- orderings.indices) {
          val oldOrdering = orderings(i)
          val pos = newExpressionsPosition(i)
          val ordering = SortOrder(
            project.output(pos),
            oldOrdering.direction,
            oldOrdering.nullOrdering,
            oldOrdering.sameOrderExpressions)
          newOrderings = newOrderings :+ ordering
        }
        (projectExpressions.size, RangePartitioning(newOrderings, numPartitions), project)
      case _ =>
        // no change for other cases
        (0, plan.outputPartitioning, plan.child)
    }
  }

  override def genColumnarShuffleExchange(shuffle: ShuffleExchangeExec): SparkPlan = {
    val child = shuffle.child
    if (
      BackendsApiManager.getSettings.supportShuffleWithProject(shuffle.outputPartitioning, child)
    ) {
      val (projectColumnNumber, newPartitioning, newChild) =
        addProjectionForShuffleExchange(shuffle)

      if (projectColumnNumber != 0) {
        if (newChild.supportsColumnar) {
          val newPlan = ShuffleExchangeExec(newPartitioning, newChild, shuffle.shuffleOrigin)
          // the new projections columns are appended at the end.
          ColumnarShuffleExchangeExec(
            newPlan,
            newChild,
            newChild.output.dropRight(projectColumnNumber))
        } else {
          // It's the case that partitioning expressions could be offloaded into native.
          shuffle.withNewChildren(Seq(child))
        }
      } else {
        ColumnarShuffleExchangeExec(shuffle, child, null)
      }
    } else {
      ColumnarShuffleExchangeExec(shuffle, child, null)
    }
  }

  /** Generate ShuffledHashJoinExecTransformer. */
  def genShuffledHashJoinExecTransformer(
      leftKeys: Seq[Expression],
      rightKeys: Seq[Expression],
      joinType: JoinType,
      buildSide: BuildSide,
      condition: Option[Expression],
      left: SparkPlan,
      right: SparkPlan,
      isSkewJoin: Boolean): ShuffledHashJoinExecTransformerBase =
    CHShuffledHashJoinExecTransformer(
      leftKeys,
      rightKeys,
      joinType,
      buildSide,
      condition,
      left,
      right,
      isSkewJoin)

  /** Generate BroadcastHashJoinExecTransformer. */
  def genBroadcastHashJoinExecTransformer(
      leftKeys: Seq[Expression],
      rightKeys: Seq[Expression],
      joinType: JoinType,
      buildSide: BuildSide,
      condition: Option[Expression],
      left: SparkPlan,
      right: SparkPlan,
      isNullAwareAntiJoin: Boolean = false): BroadcastHashJoinExecTransformerBase =
    CHBroadcastHashJoinExecTransformer(
      leftKeys,
      rightKeys,
      joinType,
      buildSide,
      condition,
      left,
      right,
      isNullAwareAntiJoin)

  override def genSortMergeJoinExecTransformer(
      leftKeys: Seq[Expression],
      rightKeys: Seq[Expression],
      joinType: JoinType,
      condition: Option[Expression],
      left: SparkPlan,
      right: SparkPlan,
      isSkewJoin: Boolean = false,
      projectList: Seq[NamedExpression] = null): SortMergeJoinExecTransformerBase =
    CHSortMergeJoinExecTransformer(
      leftKeys,
      rightKeys,
      joinType,
      condition,
      left,
      right,
      isSkewJoin,
      projectList
    )

  /** Generate CartesianProductExecTransformer */
  override def genCartesianProductExecTransformer(
      left: SparkPlan,
      right: SparkPlan,
      condition: Option[Expression]): CartesianProductExecTransformer =
    throw new GlutenNotSupportException(
      "CartesianProductExecTransformer is not supported in ch backend.")

  override def genBroadcastNestedLoopJoinExecTransformer(
      left: SparkPlan,
      right: SparkPlan,
      buildSide: BuildSide,
      joinType: JoinType,
      condition: Option[Expression]): BroadcastNestedLoopJoinExecTransformer =
    CHBroadcastNestedLoopJoinExecTransformer(
      left,
      right,
      buildSide,
      joinType,
      condition
    )

  override def genSampleExecTransformer(
      lowerBound: Double,
      upperBound: Double,
      withReplacement: Boolean,
      seed: Long,
      child: SparkPlan): SampleExecTransformer =
    throw new GlutenNotSupportException("SampleExecTransformer is not supported in ch backend.")

  /** Generate an expression transformer to transform GetMapValue to Substrait. */
  def genGetMapValueTransformer(
      substraitExprName: String,
      left: ExpressionTransformer,
      right: ExpressionTransformer,
      original: GetMapValue): ExpressionTransformer =
    GetMapValueTransformer(substraitExprName, left, right, false, original)

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
      isSort: Boolean
  ): ShuffleDependency[Int, ColumnarBatch, ColumnarBatch] = {
    CHExecUtil.genShuffleDependency(
      rdd,
      childOutputAttributes,
      projectOutputAttributes,
      newPartitioning,
      serializer,
      writeMetrics,
      metrics
    )
  }
  // scalastyle:on argcount

  /**
   * Generate ColumnarShuffleWriter for ColumnarShuffleManager.
   *
   * @return
   */
  override def genColumnarShuffleWriter[K, V](
      parameters: GenShuffleWriterParameters[K, V]): GlutenShuffleWriterWrapper[K, V] = {
    CHShuffleUtil.genColumnarShuffleWriter(parameters)
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
    val readBatchNumRows = metrics("avgReadBatchNumRows")
    val numOutputRows = metrics("numOutputRows")
    val dataSize = metrics("dataSize")
    if (GlutenConfig.getConf.isUseCelebornShuffleManager) {
      val clazz = ClassUtils.getClass("org.apache.spark.shuffle.CHCelebornColumnarBatchSerializer")
      val constructor =
        clazz.getConstructor(classOf[SQLMetric], classOf[SQLMetric], classOf[SQLMetric])
      constructor.newInstance(readBatchNumRows, numOutputRows, dataSize).asInstanceOf[Serializer]
    } else if (GlutenConfig.getConf.isUseUniffleShuffleManager) {
      throw new UnsupportedOperationException("temporarily uniffle not support ch ")
    } else {
      new CHColumnarBatchSerializer(readBatchNumRows, numOutputRows, dataSize)
    }
  }

  /** Create broadcast relation for BroadcastExchangeExec */
  override def createBroadcastRelation(
      mode: BroadcastMode,
      child: SparkPlan,
      numOutputRows: SQLMetric,
      dataSize: SQLMetric): BuildSideRelation = {

    val buildKeys: Seq[Expression] = mode match {
      case mode1: HashedRelationBroadcastMode =>
        mode1.key
      case _ =>
        // IdentityBroadcastMode
        Seq.empty
    }

    val (newChild, newOutput, newBuildKeys) =
      if (
        buildKeys
          .forall(k => k.isInstanceOf[AttributeReference] || k.isInstanceOf[BoundReference])
      ) {
        (child, child.output, Seq.empty[Expression])
      } else {
        // pre projection in case of expression join keys
        val appendedProjections = new ArrayBuffer[NamedExpression]()
        val preProjectionBuildKeys = buildKeys.zipWithIndex.map {
          case (e, idx) =>
            e match {
              case b: BoundReference => child.output(b.ordinal)
              case o: Expression =>
                val newExpr = Alias(o, "col_" + idx)()
                appendedProjections += newExpr
                newExpr
            }
        }

        def wrapChild(child: SparkPlan): WholeStageTransformer = {
          val childWithAdapter = ColumnarCollapseTransformStages.wrapInputIteratorTransformer(child)
          WholeStageTransformer(
            ProjectExecTransformer(child.output ++ appendedProjections, childWithAdapter))(
            ColumnarCollapseTransformStages.transformStageCounter.incrementAndGet()
          )
        }

        val newChild = child match {
          case wt: WholeStageTransformer =>
            wt.withNewChildren(
              Seq(ProjectExecTransformer(child.output ++ appendedProjections, wt.child)))
          case w: WholeStageCodegenExec =>
            w.withNewChildren(Seq(ProjectExec(child.output ++ appendedProjections, w.child)))
          case r: AQEShuffleReadExec if r.supportsColumnar =>
            // when aqe is open
            // TODO: remove this after pushdowning preprojection
            wrapChild(r)
          case r2c: RowToCHNativeColumnarExec =>
            wrapChild(r2c)
          case union: ColumnarUnionExec =>
            wrapChild(union)
          case other =>
            throw new GlutenNotSupportException(
              s"Not supported operator ${other.nodeName} for BroadcastRelation")
        }
        (newChild, (child.output ++ appendedProjections).map(_.toAttribute), preProjectionBuildKeys)
      }
    val countsAndBytes =
      CHExecUtil.buildSideRDD(dataSize, newChild).collect

    val batches = countsAndBytes.map(_._2)
    val rawSize = dataSize.value
    if (rawSize >= BroadcastExchangeExec.MAX_BROADCAST_TABLE_BYTES) {
      throw new SparkException(
        s"Cannot broadcast the table that is larger than 8GB: ${rawSize >> 30} GB")
    }
    val rowCount = countsAndBytes.map(_._1).sum
    numOutputRows += rowCount
    ClickHouseBuildSideRelation(mode, newOutput, batches.flatten, rowCount, newBuildKeys)
  }

  /**
   * Generate extended DataSourceV2 Strategies. Currently only for ClickHouse backend.
   *
   * @return
   */
  override def genExtendedDataSourceV2Strategies(): List[SparkSession => Strategy] = {
    List.empty
  }

  /**
   * Generate extended query stage preparation rules.
   *
   * @return
   */
  override def genExtendedQueryStagePrepRules(): List[SparkSession => Rule[SparkPlan]] =
    List(spark => FallbackBroadcastHashJoinPrepQueryStage(spark))

  /**
   * Generate extended Analyzers. Currently only for ClickHouse backend.
   *
   * @return
   */
  override def genExtendedAnalyzers(): List[SparkSession => Rule[LogicalPlan]] = {
    List(
      spark => new RewriteToDateExpresstionRule(spark, spark.sessionState.conf),
      spark => new RewriteDateTimestampComparisonRule(spark, spark.sessionState.conf))
  }

  /**
   * Generate extended Optimizers.
   *
   * @return
   */
  override def genExtendedOptimizers(): List[SparkSession => Rule[LogicalPlan]] = {
    List(
      spark => new CommonSubexpressionEliminateRule(spark, spark.sessionState.conf),
      spark => CHAggregateFunctionRewriteRule(spark),
      _ => CountDistinctWithoutExpand,
      _ => EqualToRewrite
    )
  }

  /**
   * Generate extended columnar pre-rules, in the validation phase.
   *
   * @return
   */
  override def genExtendedColumnarValidationRules(): List[SparkSession => Rule[SparkPlan]] =
    List(spark => FallbackBroadcastHashJoin(spark))

  /**
   * Generate extended columnar pre-rules.
   *
   * @return
   */
  override def genExtendedColumnarTransformRules(): List[SparkSession => Rule[SparkPlan]] =
    List()

  override def genInjectPostHocResolutionRules(): List[SparkSession => Rule[LogicalPlan]] = {
    List()
  }

  /**
   * Generate extended Strategies.
   *
   * @return
   */
  override def genExtendedStrategies(): List[SparkSession => Strategy] =
    List()

  /** Define backend specfic expression mappings. */
  override def extraExpressionMappings: Seq[Sig] = {
    List(
      Sig[CollectList](ExpressionNames.COLLECT_LIST),
      Sig[CollectSet](ExpressionNames.COLLECT_SET)
    ) ++
      SparkShimLoader.getSparkShims.bloomFilterExpressionMappings()
  }

  override def genStringTranslateTransformer(
      substraitExprName: String,
      srcExpr: ExpressionTransformer,
      matchingExpr: ExpressionTransformer,
      replaceExpr: ExpressionTransformer,
      original: StringTranslate): ExpressionTransformer = {
    CHStringTranslateTransformer(substraitExprName, srcExpr, matchingExpr, replaceExpr, original)
  }

  override def genLikeTransformer(
      substraitExprName: String,
      left: ExpressionTransformer,
      right: ExpressionTransformer,
      original: Like): ExpressionTransformer = {
    // CH backend does not support escapeChar, so skip it here.
    GenericExpressionTransformer(substraitExprName, Seq(left, right), original)
  }

  /** Generate an ExpressionTransformer to transform TruncTimestamp expression for CH. */
  override def genTruncTimestampTransformer(
      substraitExprName: String,
      format: ExpressionTransformer,
      timestamp: ExpressionTransformer,
      timeZoneId: Option[String],
      original: TruncTimestamp): ExpressionTransformer = {
    CHTruncTimestampTransformer(substraitExprName, format, timestamp, timeZoneId, original)
  }

  override def genDateDiffTransformer(
      substraitExprName: String,
      endDate: ExpressionTransformer,
      startDate: ExpressionTransformer,
      original: DateDiff): ExpressionTransformer = {
    GenericExpressionTransformer(
      substraitExprName,
      Seq(LiteralTransformer("day"), startDate, endDate),
      original)
  }

  override def genPosExplodeTransformer(
      substraitExprName: String,
      child: ExpressionTransformer,
      original: PosExplode,
      attributeSeq: Seq[Attribute]): ExpressionTransformer = {
    CHPosExplodeTransformer(substraitExprName, child, original, attributeSeq)
  }

  override def genRegexpReplaceTransformer(
      substraitExprName: String,
      children: Seq[ExpressionTransformer],
      expr: RegExpReplace): ExpressionTransformer = {
    CHRegExpReplaceTransformer(substraitExprName, children, expr)
  }

  override def createColumnarWriteFilesExec(
      child: SparkPlan,
      fileFormat: FileFormat,
      partitionColumns: Seq[Attribute],
      bucketSpec: Option[BucketSpec],
      options: Map[String, String],
      staticPartitions: TablePartitionSpec): SparkPlan = {
    throw new GlutenNotSupportException("ColumnarWriteFilesExec is not support in ch backend.")
  }

  override def createColumnarArrowEvalPythonExec(
      udfs: Seq[PythonUDF],
      resultAttrs: Seq[Attribute],
      child: SparkPlan,
      evalType: Int): SparkPlan = {
    throw new GlutenNotSupportException("ColumnarArrowEvalPythonExec is not support in ch backend.")
  }

  /**
   * This is only used to control whether transform smj into shj or not at present. We always prefer
   * shj.
   */
  override def joinFallback(
      joinType: JoinType,
      leftOutputSet: AttributeSet,
      rightOutputSet: AttributeSet,
      condition: Option[Expression]): Boolean = {
    CHJoinValidateUtil.shouldFallback(
      UnknownJoinStrategy(joinType),
      leftOutputSet,
      rightOutputSet,
      condition)
  }

  /** Generate window function node */
  override def genWindowFunctionsNode(
      windowExpression: Seq[NamedExpression],
      windowExpressionNodes: JList[WindowFunctionNode],
      originalInputAttributes: Seq[Attribute],
      args: JMap[String, JLong]): Unit = {

    windowExpression.map {
      windowExpr =>
        val aliasExpr = windowExpr.asInstanceOf[Alias]
        val columnName = s"${aliasExpr.name}_${aliasExpr.exprId.id}"
        val wExpression = aliasExpr.child.asInstanceOf[WindowExpression]
        wExpression.windowFunction match {
          case wf @ (RowNumber() | Rank(_) | DenseRank(_) | PercentRank(_)) =>
            val aggWindowFunc = wf.asInstanceOf[AggregateWindowFunction]
            val frame = aggWindowFunc.frame.asInstanceOf[SpecifiedWindowFrame]
            val windowFunctionNode = ExpressionBuilder.makeWindowFunction(
              WindowFunctionsBuilder.create(args, aggWindowFunc).toInt,
              new JArrayList[ExpressionNode](),
              columnName,
              ConverterUtils.getTypeNode(aggWindowFunc.dataType, aggWindowFunc.nullable),
              frame.upper,
              frame.lower,
              frame.frameType.sql,
              originalInputAttributes.asJava
            )
            windowExpressionNodes.add(windowFunctionNode)
          case aggExpression: AggregateExpression =>
            val frame = wExpression.windowSpec.frameSpecification.asInstanceOf[SpecifiedWindowFrame]
            val aggregateFunc = aggExpression.aggregateFunction
            val substraitAggFuncName = ExpressionMappings.expressionsMap.get(aggregateFunc.getClass)
            if (substraitAggFuncName.isEmpty) {
              throw new GlutenNotSupportException(s"Not currently supported: $aggregateFunc.")
            }

            val childrenNodeList = new JArrayList[ExpressionNode]()
            aggregateFunc.children.foreach(
              expr =>
                childrenNodeList.add(
                  ExpressionConverter
                    .replaceWithExpressionTransformer(expr, originalInputAttributes)
                    .doTransform(args)))

            val windowFunctionNode = ExpressionBuilder.makeWindowFunction(
              AggregateFunctionsBuilder.create(args, aggExpression.aggregateFunction).toInt,
              childrenNodeList,
              columnName,
              ConverterUtils.getTypeNode(aggExpression.dataType, aggExpression.nullable),
              frame.upper,
              frame.lower,
              frame.frameType.sql,
              originalInputAttributes.asJava
            )
            windowExpressionNodes.add(windowFunctionNode)
          case wf @ (Lead(_, _, _, _) | Lag(_, _, _, _)) =>
            val (offsetWf, frame) = wf match {
              case lead @ Lead(input, offset, default, ignoreNulls) =>
                // When the offset value of the lead is negative, will convert to lag function
                lead.offset match {
                  case IntegerLiteral(value) if value < 0 =>
                    val newWf = Lag(input, Literal(math.abs(value)), default, ignoreNulls)
                    (newWf, newWf.frame.asInstanceOf[SpecifiedWindowFrame])
                  case other => (lead, lead.frame.asInstanceOf[SpecifiedWindowFrame])
                }
              case lag @ Lag(input, offset, default, ignoreNulls) =>
                // When the offset value of the lag is negative, will convert to lead function
                lag.offset match {
                  case IntegerLiteral(value) if value > 0 =>
                    val newWf = Lead(input, Literal(value), default, ignoreNulls)
                    (newWf, newWf.frame.asInstanceOf[SpecifiedWindowFrame])
                  case other => (lag, lag.frame.asInstanceOf[SpecifiedWindowFrame])
                }
            }

            val childrenNodeList = new JArrayList[ExpressionNode]()
            childrenNodeList.add(
              ExpressionConverter
                .replaceWithExpressionTransformer(
                  offsetWf.input,
                  attributeSeq = originalInputAttributes)
                .doTransform(args))
            childrenNodeList.add(
              ExpressionConverter
                .replaceWithExpressionTransformer(
                  offsetWf.offset,
                  attributeSeq = originalInputAttributes)
                .doTransform(args))
            childrenNodeList.add(
              ExpressionConverter
                .replaceWithExpressionTransformer(
                  offsetWf.default,
                  attributeSeq = originalInputAttributes)
                .doTransform(args))
            val windowFunctionNode = ExpressionBuilder.makeWindowFunction(
              WindowFunctionsBuilder.create(args, offsetWf).toInt,
              childrenNodeList,
              columnName,
              ConverterUtils.getTypeNode(offsetWf.dataType, offsetWf.nullable),
              frame.upper,
              frame.lower,
              frame.frameType.sql,
              originalInputAttributes.asJava
            )
            windowExpressionNodes.add(windowFunctionNode)
          case wf @ NTile(buckets: Expression) =>
            val frame = wExpression.windowSpec.frameSpecification.asInstanceOf[SpecifiedWindowFrame]
            val childrenNodeList = new JArrayList[ExpressionNode]()
            val literal = buckets.asInstanceOf[Literal]
            childrenNodeList.add(LiteralTransformer(literal).doTransform(args))
            val windowFunctionNode = ExpressionBuilder.makeWindowFunction(
              WindowFunctionsBuilder.create(args, wf).toInt,
              childrenNodeList,
              columnName,
              ConverterUtils.getTypeNode(wf.dataType, wf.nullable),
              frame.upper,
              frame.lower,
              frame.frameType.sql,
              originalInputAttributes.asJava
            )
            windowExpressionNodes.add(windowFunctionNode)
          case _ =>
            throw new GlutenNotSupportException(
              "unsupported window function type: " +
                wExpression.windowFunction)
        }
    }
  }

  /** Clickhouse Backend only supports part of filters for parquet. */
  override def postProcessPushDownFilter(
      extraFilters: Seq[Expression],
      sparkExecNode: LeafExecNode): Seq[Expression] = {
    // FIXME: DeltaMergeTreeFileFormat should not inherit from ParquetFileFormat.
    def isParquetFormat(fileFormat: FileFormat): Boolean = fileFormat match {
      case p: ParquetFileFormat if p.shortName().equals("parquet") => true
      case _ => false
    }

    // TODO: datasource v2 ?
    // TODO: Push down conditions with scalar subquery
    // For example, consider TPCH 22 'c_acctbal > (select avg(c_acctbal) from customer where ...)'.
    // Vanilla Spark only pushes down the Parquet Filter not Catalyst Filter, which can not get the
    // subquery result, while gluten pushes down the Catalyst Filter can benefit from this case.
    //
    // Let's make push down functionally same as vanilla Spark for now.

    sparkExecNode match {
      case fileSourceScan: FileSourceScanExec
          if isParquetFormat(fileSourceScan.relation.fileFormat) =>
        PushDownUtil.removeNotSupportPushDownFilters(
          fileSourceScan.conf,
          fileSourceScan.output,
          fileSourceScan.dataFilters)
      case _ => super.postProcessPushDownFilter(extraFilters, sparkExecNode)
    }
  }

  override def genGenerateTransformer(
      generator: Generator,
      requiredChildOutput: Seq[Attribute],
      outer: Boolean,
      generatorOutput: Seq[Attribute],
      child: SparkPlan
  ): GenerateExecTransformerBase = {
    CHGenerateExecTransformer(generator, requiredChildOutput, outer, generatorOutput, child)
  }

  /** Transform array filter to Substrait. */
  override def genArrayFilterTransformer(
      substraitExprName: String,
      argument: ExpressionTransformer,
      function: ExpressionTransformer,
      expr: ArrayFilter): ExpressionTransformer = {
    GenericExpressionTransformer(substraitExprName, Seq(argument, function), expr)
  }

  /** Transform array transform to Substrait. */
  override def genArrayTransformTransformer(
      substraitExprName: String,
      argument: ExpressionTransformer,
      function: ExpressionTransformer,
      expr: ArrayTransform): ExpressionTransformer = {
    GenericExpressionTransformer(substraitExprName, Seq(argument, function), expr)
  }

  /** Transform array sort to Substrait. */
  override def genArraySortTransformer(
      substraitExprName: String,
      argument: ExpressionTransformer,
      function: ExpressionTransformer,
      expr: ArraySort): ExpressionTransformer = {
    GenericExpressionTransformer(substraitExprName, Seq(argument, function), expr)
  }

  override def genPreProjectForGenerate(generate: GenerateExec): SparkPlan = generate

  override def genPostProjectForGenerate(generate: GenerateExec): SparkPlan = generate
}
