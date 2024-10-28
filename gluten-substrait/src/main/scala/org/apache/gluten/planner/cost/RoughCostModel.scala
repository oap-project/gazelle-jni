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
package org.apache.gluten.planner.cost

import org.apache.gluten.GlutenConfig
import org.apache.gluten.extension.columnar.transition.{ColumnarToRowLike, RowToColumnarLike}
import org.apache.gluten.utils.PlanUtil

import org.apache.spark.sql.catalyst.expressions.{Alias, Attribute, NamedExpression}
import org.apache.spark.sql.execution.{ColumnarToRowExec, DataSourceScanExec, LeafExecNode, ProjectExec, RowToColumnarExec, SparkPlan}
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanExec

/** A rough cost model with some empirical heuristics. */
class RoughCostModel extends LongCostModel {

  private def getSizeFactor(plan: SparkPlan): Long = {
    // Get the bytes size that the plan needs to consume
    val sizeBytes = plan match {
      case scan: DataSourceScanExec => getStatSizeBytes(scan)
      case _: LeafExecNode => 0L
      case p => p.children.map(getStatSizeBytes).sum
    }
    sizeBytes / GlutenConfig.getConf.rasRoughSizeBytesThreshold
  }

  private def getStatSizeBytes(plan: SparkPlan): Long = {
    plan match {
      case a: AdaptiveSparkPlanExec => getStatSizeBytes(a.inputPlan)
      case _ =>
        plan.logicalLink match {
          case Some(logicalPlan) => logicalPlan.stats.sizeInBytes.toLong
          case _ => plan.children.map(getStatSizeBytes).sum
        }
    }
  }

  override def selfLongCostOf(node: SparkPlan): Long = {
    val sizeFactor = getSizeFactor(node)
    val opCost = node match {
      case ProjectExec(projectList, _) if projectList.forall(isCheapExpression) =>
        // Make trivial ProjectExec has the same cost as ProjectExecTransform to reduce unnecessary
        // c2r and r2c.
        1L
      case ColumnarToRowExec(_) => 1L
      case RowToColumnarExec(_) => 1L
      case ColumnarToRowLike(_) => 1L
      case RowToColumnarLike(_) =>
        // If sizeBytes is less than the threshold, the cost of RowToColumnarLike is ignored.
        if (sizeFactor == 0) 1L else GlutenConfig.getConf.rasRoughR2cCost
      case p if PlanUtil.isGlutenColumnarOp(p) => 1L
      case p if PlanUtil.isVanillaColumnarOp(p) => GlutenConfig.getConf.rasRoughVanillaCost
      // Other row ops. Usually a vanilla row op.
      case _ => GlutenConfig.getConf.rasRoughVanillaCost
    }
    opCost * Math.max(1, sizeFactor)
  }

  private def isCheapExpression(ne: NamedExpression): Boolean = ne match {
    case Alias(_: Attribute, _) => true
    case _: Attribute => true
    case _ => false
  }
}
