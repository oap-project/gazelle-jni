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
package io.glutenproject.extension

import io.glutenproject.execution._
import io.glutenproject.utils.PullOutProjectHelper

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate.AggregateFunction
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.trees.TreeNodeTag
import org.apache.spark.sql.execution.SparkPlan

import scala.collection.mutable.ListBuffer

trait ProjectProcessedHint
case class PROJECT_PROCESSED() extends ProjectProcessedHint

object ProjectProcessedHint {
  val TAG: TreeNodeTag[ProjectProcessedHint] =
    TreeNodeTag[ProjectProcessedHint]("io.glutenproject.projectprocessedhint")

  def isAlreadyTagged(plan: SparkPlan): Boolean = {
    plan.getTagValue(TAG).isDefined
  }

  def isPostProjectProcessed(plan: SparkPlan): Boolean = {
    plan
      .getTagValue(TAG)
      .isDefined && plan.getTagValue(TAG).get.isInstanceOf[PROJECT_PROCESSED]
  }

  def postProjectProcessDone(plan: SparkPlan): Unit = {
    tag(plan, PROJECT_PROCESSED())
  }

  private def tag(plan: SparkPlan, hint: ProjectProcessedHint): Unit = {
    plan.setTagValue(TAG, hint)
  }
}

object ColumnarPullOutPostProject extends Rule[SparkPlan] with PullOutProjectHelper {

  override def apply(plan: SparkPlan): SparkPlan = plan.transform {
    case agg: HashAggregateExecBaseTransformer
        if !ProjectProcessedHint.isPostProjectProcessed(agg) &&
          agg.needsPostProjection =>
      val projectList = agg.resultExpressions.map {
        case ne: NamedExpression => ne
        case other => Alias(other, s"_post_${generatedNameIndex.getAndIncrement()}")()
      }

      ProjectProcessedHint.postProjectProcessDone(agg)
      val newAgg = agg.copySelf(resultExpressions = agg.allAggregateResultAttributes)
      newAgg.copyTagsFrom(agg)

      ProjectExecTransformer(projectList, newAgg, projectType = ProjectType.POST)
  }
}

object ColumnarPullOutPreProject extends Rule[SparkPlan] with PullOutProjectHelper {

  private def needsPreProject(plan: GlutenPlan): Boolean = plan match {
    case agg: HashAggregateExecBaseTransformer =>
      aggNeedPreProject(agg.groupingExpressions, agg.aggregateExpressions)

    case sort: SortExecTransformer =>
      sort.sortOrder.exists(o => isNotAttribute(o.child))

    case _ => false
  }

  override def apply(plan: SparkPlan): SparkPlan = plan.transform {
    case agg: HashAggregateExecBaseTransformer if needsPreProject(agg) =>
      val projectExprsMap = getProjectExpressionMap

      // Handle groupingExpressions.
      val newGroupingExpressions =
        agg.groupingExpressions.toIndexedSeq.map(
          getAndReplaceProjectAttribute(_, projectExprsMap).asInstanceOf[NamedExpression])

      // Handle aggregateExpressions
      val newAggregateExpressions = agg.aggregateExpressions.toIndexedSeq.map {
        ae =>
          val newAggFuncChildren = ae.aggregateFunction.children.map {
            case literal: Literal => literal
            case other => getAndReplaceProjectAttribute(other, projectExprsMap)
          }
          val newAggFunc = ae.aggregateFunction
            .withNewChildren(newAggFuncChildren)
            .asInstanceOf[AggregateFunction]
          val newFilter =
            ae.filter.map(getAndReplaceProjectAttribute(_, projectExprsMap))
          ae.copy(aggregateFunction = newAggFunc, filter = newFilter)
      }

      agg.copySelf(
        groupingExpressions = newGroupingExpressions,
        aggregateExpressions = newAggregateExpressions,
        child = ProjectExecTransformer(agg.child.output ++ projectExprsMap.values.toSeq, agg.child)
      )

    case sort: SortExecTransformer if needsPreProject(sort) =>
      val projectExprsMap = getProjectExpressionMap
      // The output of the sort operator is the same as the output of the child, therefore it
      // is necessary to retain the output columns of the child in the pre-projection, and
      // then add the expressions that need to be evaluated in the sortOrder. Finally, in the
      // post-projection, the additional columns need to be removed, leaving only the original
      // output of the child.
      val newSortOrder =
        sort.sortOrder.map(_.mapChildren(getAndReplaceProjectAttribute(_, projectExprsMap)))

      val newSort = sort.copy(
        sortOrder = newSortOrder.asInstanceOf[Seq[SortOrder]],
        child = ProjectExecTransformer(
          sort.child.output ++ projectExprsMap.values.toSeq,
          sort.child,
          projectType = ProjectType.PRE)
      )
      ProjectExecTransformer(sort.child.output, newSort, projectType = ProjectType.POST)
  }
}

object ColumnarPullOutProject {

  /**
   * This function is used to generate the transformed plan during validation, and it can return the
   * inserted pre-projection, post-projection, as well as the operator transformer. There are four
   * different scenarios in total.
   *   - post-projection -> transformer -> pre-projection
   *   - post-projection -> transformer
   *   - transformer -> pre-projection
   *   - transformer
   */
  def getTransformedPlan(transformer: SparkPlan): Seq[SparkPlan] = {
    val transformedPlan = ColumnarOverrides.applyExtendedColumnarPreRules(transformer)
    val allPlan = ListBuffer[SparkPlan](transformedPlan)

    def addPlanIfPreProject(plan: SparkPlan): Unit = plan match {
      case pre: ProjectExecTransformer if pre.isPreProject =>
        allPlan += pre
      case _ =>
    }

    transformedPlan match {
      case p: ProjectExecTransformer if p.isPostProject =>
        val originPlan = p.child
        allPlan += originPlan
        originPlan.children.foreach(addPlanIfPreProject)
      case p: SparkPlan =>
        p.children.foreach(addPlanIfPreProject)
      case _ =>
    }
    allPlan
  }
}
