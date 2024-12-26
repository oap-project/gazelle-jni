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
package org.apache.gluten.extension.columnar.offload

import org.apache.gluten.GlutenConfig
import org.apache.gluten.backendsapi.BackendsApiManager
import org.apache.gluten.execution._
import org.apache.gluten.extension.columnar.FallbackTags
import org.apache.gluten.logging.LogLevelUtil
import org.apache.gluten.sql.shims.SparkShimLoader

import org.apache.spark.api.python.EvalPythonExecTransformer
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.optimizer.{BuildLeft, BuildRight, BuildSide}
import org.apache.spark.sql.catalyst.plans.logical.Join
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.aggregate.{HashAggregateExec, ObjectHashAggregateExec, SortAggregateExec}
import org.apache.spark.sql.execution.datasources.WriteFilesExec
import org.apache.spark.sql.execution.datasources.v2.BatchScanExec
import org.apache.spark.sql.execution.exchange.{BroadcastExchangeExec, ShuffleExchangeExec}
import org.apache.spark.sql.execution.joins._
import org.apache.spark.sql.execution.python.{ArrowEvalPythonExec, BatchEvalPythonExec}
import org.apache.spark.sql.execution.window.{WindowExec, WindowGroupLimitExecShim}
import org.apache.spark.sql.hive.HiveTableScanExecTransformer

// Exchange transformation.
case class OffloadExchange() extends OffloadSingleNode with LogLevelUtil {
  override def offload(plan: SparkPlan): SparkPlan = plan match {
    case p if FallbackTags.nonEmpty(p) =>
      p
    case s: ShuffleExchangeExec
        if (s.child.supportsColumnar || GlutenConfig.getConf.enablePreferColumnar) &&
          BackendsApiManager.getSettings.supportColumnarShuffleExec() =>
      logDebug(s"Columnar Processing for ${s.getClass} is currently supported.")
      BackendsApiManager.getSparkPlanExecApiInstance.genColumnarShuffleExchange(s)
    case b: BroadcastExchangeExec =>
      val child = b.child
      logDebug(s"Columnar Processing for ${b.getClass} is currently supported.")
      ColumnarBroadcastExchangeExec(b.mode, child)
    case other => other
  }
}

// Join transformation.
case class OffloadJoin() extends OffloadSingleNode with LogLevelUtil {
  override def offload(plan: SparkPlan): SparkPlan = {
    if (FallbackTags.nonEmpty(plan)) {
      logDebug(s"Columnar Processing for ${plan.getClass} is under row guard.")
      return plan
    }
    plan match {
      case plan: ShuffledHashJoinExec =>
        val left = plan.left
        val right = plan.right
        logDebug(s"Columnar Processing for ${plan.getClass} is currently supported.")
        BackendsApiManager.getSparkPlanExecApiInstance
          .genShuffledHashJoinExecTransformer(
            plan.leftKeys,
            plan.rightKeys,
            plan.joinType,
            OffloadJoin.getShjBuildSide(plan),
            plan.condition,
            left,
            right,
            plan.isSkewJoin)
      case plan: SortMergeJoinExec =>
        val left = plan.left
        val right = plan.right
        logDebug(s"Columnar Processing for ${plan.getClass} is currently supported.")
        BackendsApiManager.getSparkPlanExecApiInstance
          .genSortMergeJoinExecTransformer(
            plan.leftKeys,
            plan.rightKeys,
            plan.joinType,
            plan.condition,
            left,
            right,
            plan.isSkewJoin)
      case plan: BroadcastHashJoinExec =>
        val left = plan.left
        val right = plan.right
        BackendsApiManager.getSparkPlanExecApiInstance
          .genBroadcastHashJoinExecTransformer(
            plan.leftKeys,
            plan.rightKeys,
            plan.joinType,
            plan.buildSide,
            plan.condition,
            left,
            right,
            isNullAwareAntiJoin = plan.isNullAwareAntiJoin)
      case plan: CartesianProductExec =>
        val left = plan.left
        val right = plan.right
        BackendsApiManager.getSparkPlanExecApiInstance
          .genCartesianProductExecTransformer(left, right, plan.condition)
      case plan: BroadcastNestedLoopJoinExec =>
        val left = plan.left
        val right = plan.right
        BackendsApiManager.getSparkPlanExecApiInstance
          .genBroadcastNestedLoopJoinExecTransformer(
            left,
            right,
            plan.buildSide,
            plan.joinType,
            plan.condition)
      case other => other
    }
  }
}

object OffloadJoin {
  def getShjBuildSide(shj: ShuffledHashJoinExec): BuildSide = {
    val leftBuildable =
      BackendsApiManager.getSettings.supportHashBuildJoinTypeOnLeft(shj.joinType)
    val rightBuildable =
      BackendsApiManager.getSettings.supportHashBuildJoinTypeOnRight(shj.joinType)

    assert(leftBuildable || rightBuildable)

    if (!leftBuildable) {
      return BuildRight
    }
    if (!rightBuildable) {
      return BuildLeft
    }

    // Both left and right are buildable. Find out the better one.
    if (!GlutenConfig.getConf.shuffledHashJoinOptimizeBuildSide) {
      // User disabled build side re-optimization. Return original build side from vanilla Spark.
      return shj.buildSide
    }
    shj.logicalLink
      .flatMap {
        case join: Join => Some(getOptimalBuildSide(join))
        case _ => None
      }
      .getOrElse {
        // Some shj operators generated in certain Spark tests such as OuterJoinSuite,
        // could possibly have no logical link set.
        shj.buildSide
      }
  }

  def getOptimalBuildSide(join: Join): BuildSide = {
    val leftSize = join.left.stats.sizeInBytes
    val rightSize = join.right.stats.sizeInBytes
    val leftRowCount = join.left.stats.rowCount
    val rightRowCount = join.right.stats.rowCount
    if (leftSize == rightSize && rightRowCount.isDefined && leftRowCount.isDefined) {
      if (rightRowCount.get <= leftRowCount.get) {
        return BuildRight
      }
      return BuildLeft
    }
    if (rightSize <= leftSize) {
      return BuildRight
    }
    BuildLeft
  }
}

// Other transformations.
case class OffloadOthers() extends OffloadSingleNode with LogLevelUtil {
  import OffloadOthers._
  private val replace = new ReplaceSingleNode

  override def offload(plan: SparkPlan): SparkPlan = replace.doReplace(plan)
}

object OffloadOthers {
  // Utility to replace single node within transformed Gluten node.
  // Children will be preserved as they are as children of the output node.
  //
  // Do not look up on children on the input node in this rule. Otherwise
  // it may break RAS which would group all the possible input nodes to
  // search for validate candidates.
  private class ReplaceSingleNode extends LogLevelUtil with Logging {

    def doReplace(p: SparkPlan): SparkPlan = {
      val plan = p
      if (FallbackTags.nonEmpty(plan)) {
        return plan
      }
      plan match {
        case plan: BatchScanExec =>
          logDebug(s"Columnar Processing for ${plan.getClass} is currently supported.")
          ScanTransformerFactory.createBatchScanTransformer(plan)
        case plan: FileSourceScanExec =>
          logDebug(s"Columnar Processing for ${plan.getClass} is currently supported.")
          ScanTransformerFactory.createFileSourceScanTransformer(plan)
        case plan if HiveTableScanExecTransformer.isHiveTableScan(plan) =>
          // TODO: Add DynamicPartitionPruningHiveScanSuite.scala
          logDebug(s"Columnar Processing for ${plan.getClass} is currently supported.")
          HiveTableScanExecTransformer(plan)
        case plan: CoalesceExec =>
          logDebug(s"Columnar Processing for ${plan.getClass} is currently supported.")
          ColumnarCoalesceExec(plan.numPartitions, plan.child)
        case plan: FilterExec =>
          logDebug(s"Columnar Processing for ${plan.getClass} is currently supported.")
          BackendsApiManager.getSparkPlanExecApiInstance
            .genFilterExecTransformer(plan.condition, plan.child)
        case plan: ProjectExec =>
          val columnarChild = plan.child
          logDebug(s"Columnar Processing for ${plan.getClass} is currently supported.")
          ProjectExecTransformer(plan.projectList, columnarChild)
        case plan: HashAggregateExec =>
          logDebug(s"Columnar Processing for ${plan.getClass} is currently supported.")
          HashAggregateExecBaseTransformer.from(plan)
        case plan: SortAggregateExec =>
          logDebug(s"Columnar Processing for ${plan.getClass} is currently supported.")
          HashAggregateExecBaseTransformer.from(plan)
        case plan: ObjectHashAggregateExec =>
          logDebug(s"Columnar Processing for ${plan.getClass} is currently supported.")
          HashAggregateExecBaseTransformer.from(plan)
        case plan: UnionExec =>
          val children = plan.children
          logDebug(s"Columnar Processing for ${plan.getClass} is currently supported.")
          ColumnarUnionExec(children)
        case plan: ExpandExec =>
          val child = plan.child
          logDebug(s"Columnar Processing for ${plan.getClass} is currently supported.")
          ExpandExecTransformer(plan.projections, plan.output, child)
        case plan: WriteFilesExec =>
          val child = plan.child
          logDebug(s"Columnar Processing for ${plan.getClass} is currently supported.")
          val writeTransformer = WriteFilesExecTransformer(
            child,
            plan.fileFormat,
            plan.partitionColumns,
            plan.bucketSpec,
            plan.options,
            plan.staticPartitions)
          ColumnarWriteFilesExec(
            writeTransformer,
            plan.fileFormat,
            plan.partitionColumns,
            plan.bucketSpec,
            plan.options,
            plan.staticPartitions)
        case plan: SortExec =>
          val child = plan.child
          logDebug(s"Columnar Processing for ${plan.getClass} is currently supported.")
          SortExecTransformer(plan.sortOrder, plan.global, child, plan.testSpillFrequency)
        case plan: TakeOrderedAndProjectExec =>
          logDebug(s"Columnar Processing for ${plan.getClass} is currently supported.")
          val child = plan.child
          val (limit, offset) = SparkShimLoader.getSparkShims.getLimitAndOffsetFromTopK(plan)
          TakeOrderedAndProjectExecTransformer(
            limit,
            plan.sortOrder,
            plan.projectList,
            child,
            offset)
        case plan: WindowExec =>
          WindowExecTransformer(
            plan.windowExpression,
            plan.partitionSpec,
            plan.orderSpec,
            plan.child)
        case plan if SparkShimLoader.getSparkShims.isWindowGroupLimitExec(plan) =>
          val windowGroupLimitPlan = SparkShimLoader.getSparkShims
            .getWindowGroupLimitExecShim(plan)
            .asInstanceOf[WindowGroupLimitExecShim]
          BackendsApiManager.getSparkPlanExecApiInstance.genWindowGroupLimitTransformer(
            windowGroupLimitPlan.partitionSpec,
            windowGroupLimitPlan.orderSpec,
            windowGroupLimitPlan.rankLikeFunction,
            windowGroupLimitPlan.limit,
            windowGroupLimitPlan.mode,
            windowGroupLimitPlan.child
          )
        case plan: GlobalLimitExec =>
          logDebug(s"Columnar Processing for ${plan.getClass} is currently supported.")
          val child = plan.child
          val (limit, offset) =
            SparkShimLoader.getSparkShims.getLimitAndOffsetFromGlobalLimit(plan)
          LimitExecTransformer(child, offset, limit)
        case plan: LocalLimitExec =>
          logDebug(s"Columnar Processing for ${plan.getClass} is currently supported.")
          val child = plan.child
          LimitExecTransformer(child, 0L, plan.limit)
        case plan: GenerateExec =>
          logDebug(s"Columnar Processing for ${plan.getClass} is currently supported.")
          val child = plan.child
          BackendsApiManager.getSparkPlanExecApiInstance.genGenerateTransformer(
            plan.generator,
            plan.requiredChildOutput,
            plan.outer,
            plan.generatorOutput,
            child)
        case plan: BatchEvalPythonExec =>
          logDebug(s"Columnar Processing for ${plan.getClass} is currently supported.")
          val child = plan.child
          EvalPythonExecTransformer(plan.udfs, plan.resultAttrs, child)
        case plan: ArrowEvalPythonExec =>
          logDebug(s"Columnar Processing for ${plan.getClass} is currently supported.")
          val child = plan.child
          // For ArrowEvalPythonExec, CH supports it through EvalPythonExecTransformer while
          // Velox backend uses ColumnarArrowEvalPythonExec.
          if (
            !BackendsApiManager.getSettings.supportColumnarArrowUdf() ||
            !GlutenConfig.getConf.enableColumnarArrowUDF
          ) {
            EvalPythonExecTransformer(plan.udfs, plan.resultAttrs, child)
          } else {
            BackendsApiManager.getSparkPlanExecApiInstance.createColumnarArrowEvalPythonExec(
              plan.udfs,
              plan.resultAttrs,
              child,
              plan.evalType)
          }
        case plan: SampleExec =>
          logDebug(s"Columnar Processing for ${plan.getClass} is currently supported.")
          val child = plan.child
          BackendsApiManager.getSparkPlanExecApiInstance.genSampleExecTransformer(
            plan.lowerBound,
            plan.upperBound,
            plan.withReplacement,
            plan.seed,
            child)
        case p if !p.isInstanceOf[GlutenPlan] =>
          logDebug(s"Transformation for ${p.getClass} is currently not supported.")
          p
        case other => other
      }
    }
  }
}
