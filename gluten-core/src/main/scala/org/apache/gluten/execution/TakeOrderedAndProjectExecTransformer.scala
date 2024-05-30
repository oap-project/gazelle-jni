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
package org.apache.gluten.execution

import org.apache.gluten.backendsapi.BackendsApiManager
import org.apache.gluten.extension.{GlutenPlan, ValidationResult}
import org.apache.gluten.sql.shims.SparkShimLoader

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, NamedExpression, SortOrder}
import org.apache.spark.sql.catalyst.plans.physical.{AllTuples, Partitioning, SinglePartition}
import org.apache.spark.sql.catalyst.util.truncatedString
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.exchange.ShuffleExchangeExec
import org.apache.spark.sql.vectorized.ColumnarBatch

import java.util.concurrent.atomic.AtomicInteger

// FIXME: The operator is simply a wrapper for sort + limit + project (+ exchange if needed).
//  It's better to remove this wrapper then return the piped sub-operators to query planner
//  directly. Otherwise optimizer may not get enough information to optimize.
//
// Deprecated: Use TakeOrderedAndProjectExecTransformer#offload instead.
@deprecated
case class TakeOrderedAndProjectExecTransformer(
    limit: Long,
    sortOrder: Seq[SortOrder],
    projectList: Seq[NamedExpression],
    child: SparkPlan,
    offset: Int = 0)
  extends UnaryExecNode
  with GlutenPlan {
  override def outputPartitioning: Partitioning = SinglePartition
  override def outputOrdering: Seq[SortOrder] = sortOrder
  override def supportsColumnar: Boolean = true

  override def output: Seq[Attribute] = {
    projectList.map(_.toAttribute)
  }

  override def simpleString(maxFields: Int): String = {
    val orderByString = truncatedString(sortOrder, "[", ",", "]", maxFields)
    val outputString = truncatedString(output, "[", ",", "]", maxFields)

    s"TakeOrderedAndProjectExecTransformer (limit=$limit, " +
      s"orderBy=$orderByString, output=$outputString)"
  }

  override protected def withNewChildInternal(newChild: SparkPlan): SparkPlan = {
    copy(child = newChild)
  }

  override protected def doExecute(): RDD[InternalRow] = {
    throw new UnsupportedOperationException(s"This operator doesn't support doExecute().")
  }

  override protected def doValidateInternal(): ValidationResult = {
    if (offset != 0) {
      return ValidationResult.notOk(s"Native TopK does not support offset: $offset")
    }

    var tagged: ValidationResult = null
    val orderingSatisfies = SortOrder.orderingSatisfies(child.outputOrdering, sortOrder)
    if (orderingSatisfies) {
      val limitPlan = LimitTransformer(child, offset, limit)
      tagged = limitPlan.doValidate()
    } else {
      // Here we are validating sort + limit which is a kind of whole stage transformer,
      // because we would call sort.doTransform in limit.
      // So, we should add adapter to make it work.
      val inputTransformer =
        ColumnarCollapseTransformStages.wrapInputIteratorTransformer(child)
      val sortPlan = SortExecTransformer(sortOrder, false, inputTransformer)
      val sortValidation = sortPlan.doValidate()
      if (!sortValidation.isValid) {
        return sortValidation
      }
      val limitPlan = LimitTransformer(sortPlan, offset, limit)
      tagged = limitPlan.doValidate()
    }

    if (tagged.isValid) {
      val projectPlan = ProjectExecTransformer(projectList, child)
      tagged = projectPlan.doValidate()
    }
    tagged
  }

  override protected def doExecuteColumnar(): RDD[ColumnarBatch] = {
    // No real computation here
    val childRDD = child.executeColumnar()
    val childRDDPartsNum = childRDD.getNumPartitions

    if (childRDDPartsNum == 0) {
      sparkContext.parallelize(Seq.empty, 1)
    } else {
      val orderingSatisfies = SortOrder.orderingSatisfies(child.outputOrdering, sortOrder)
      def withLocalSort(sparkPlan: SparkPlan): SparkPlan = {
        if (orderingSatisfies) {
          sparkPlan
        } else {
          SortExecTransformer(sortOrder, false, sparkPlan)
        }
      }

      val hasShuffle = childRDDPartsNum == 1
      val limitBeforeShuffleOffset = if (hasShuffle) {
        // Local limit does not need offset
        0
      } else {
        offset
      }
      // The child should have been replaced by ColumnarCollapseTransformStages.
      val limitBeforeShuffle = child match {
        case wholeStage: WholeStageTransformer =>
          // remove this WholeStageTransformer, put the new sort, limit and project
          // into a new whole stage.
          val localSortPlan = withLocalSort(wholeStage.child)
          LimitTransformer(localSortPlan, limitBeforeShuffleOffset, limit)
        case other =>
          // if the child it is not WholeStageTransformer, add the adapter first
          // so that, later we can wrap WholeStageTransformer.
          val localSortPlan = withLocalSort(
            ColumnarCollapseTransformStages.wrapInputIteratorTransformer(other))
          LimitTransformer(localSortPlan, limitBeforeShuffleOffset, limit)
      }
      val transformStageCounter: AtomicInteger =
        ColumnarCollapseTransformStages.transformStageCounter
      val finalLimitPlan = if (hasShuffle) {
        limitBeforeShuffle
      } else {
        val limitStagePlan =
          WholeStageTransformer(limitBeforeShuffle)(transformStageCounter.incrementAndGet())
        val shuffleExec = ShuffleExchangeExec(SinglePartition, limitStagePlan)
        val transformedShuffleExec =
          ColumnarShuffleExchangeExec(shuffleExec, limitStagePlan, shuffleExec.child.output)
        val localSortPlan =
          SortExecTransformer(
            sortOrder,
            false,
            ColumnarCollapseTransformStages.wrapInputIteratorTransformer(transformedShuffleExec))
        LimitTransformer(localSortPlan, offset, limit)
      }

      val projectPlan = if (projectList != child.output) {
        ProjectExecTransformer(projectList, finalLimitPlan)
      } else {
        finalLimitPlan
      }

      val collapsed =
        BackendsApiManager.getSparkPlanExecApiInstance.maybeCollapseTakeOrderedAndProject(
          projectPlan)

      val finalPlan =
        WholeStageTransformer(collapsed)(transformStageCounter.incrementAndGet())

      finalPlan.executeColumnar()
    }
  }
}

/**
 * Utility methods for offloading [[TakeOrderedAndProjectExec]].
 *
 * Since:
 *   - https://github.com/apache/incubator-gluten/pull/630
 *   - https://github.com/apache/incubator-gluten/pull/2153
 *   - https://github.com/apache/incubator-gluten/pull/4607
 */
object TakeOrderedAndProjectExecTransformer {
  private def collapse(plan: ProjectExecTransformer): SparkPlan = {
    val collapsed =
      BackendsApiManager.getSparkPlanExecApiInstance.maybeCollapseTakeOrderedAndProject(plan)
    collapsed
  }

  def offload(from: TakeOrderedAndProjectExec): SparkPlan = {
    val (limit, offset) = SparkShimLoader.getSparkShims.getLimitAndOffsetFromTopK(from)
    val child = from.child
    val orderingSatisfies = SortOrder.orderingSatisfies(child.outputOrdering, from.sortOrder)

    val shuffleNeeded = !child.outputPartitioning.satisfies(AllTuples)

    if (!shuffleNeeded) {
      // Child has only a single partition. Thus we don't have to
      // build plan with local + global pattern.
      if (orderingSatisfies) {
        val globalLimit = LimitTransformer(child, offset, limit)
        val project = ProjectExecTransformer(from.projectList, globalLimit)
        return collapse(project)
      }
      // Sort needed.
      val globalSort = SortExecTransformer(from.sortOrder, true, child)
      val globalLimit = LimitTransformer(globalSort, offset, limit)
      val project = ProjectExecTransformer(from.projectList, globalLimit)
      return collapse(project)
    }

    // Local top N + global top N.
    val localPlan = if (orderingSatisfies) {
      // We don't need local sort.
      // And do not set offset on local limit.
      val localLimit = LimitTransformer(child, 0, limit)
      localLimit
    } else {
      // Local sort needed.
      val localSort = SortExecTransformer(from.sortOrder, false, child)
      // And do not set offset on local limit.
      val localLimit = LimitTransformer(localSort, 0, limit)
      localLimit
    }

    // Exchange data from local to global.
    val exchange = ColumnarShuffleExchangeExec(
      ShuffleExchangeExec(SinglePartition, localPlan),
      localPlan,
      localPlan.output)

    // Global sort + limit.
    val globalSort = SortExecTransformer(from.sortOrder, false, exchange)
    val globalLimit = LimitTransformer(globalSort, offset, limit)
    val project = ProjectExecTransformer(from.projectList, globalLimit)

    collapse(project)
  }

  def validate(from: TakeOrderedAndProjectExec): ValidationResult = {
    val (limit, offset) = SparkShimLoader.getSparkShims.getLimitAndOffsetFromTopK(from)
    val child = from.child
    val orderingSatisfies = SortOrder.orderingSatisfies(child.outputOrdering, from.sortOrder)

    val sorted = if (orderingSatisfies) {
      child
    } else {
      // Sort needed.
      val sort = SortExecTransformer(from.sortOrder, false, child)
      val sortValidation = sort.doValidate()
      if (!sortValidation.isValid) {
        return sortValidation
      }
      sort
    }

    val limitPlan = LimitTransformer(sorted, offset, limit)
    val limitValidation = limitPlan.doValidate()

    if (!limitValidation.isValid) {
      return limitValidation
    }

    val project = ProjectExecTransformer(from.projectList, limitPlan)
    val projectValidation = project.doValidate()
    projectValidation
  }
}
