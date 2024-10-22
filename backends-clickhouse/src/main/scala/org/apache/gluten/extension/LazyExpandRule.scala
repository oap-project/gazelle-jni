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
package org.apache.gluten.extension

import org.apache.gluten.execution._

import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.SparkPlan

/*
 * For aggregation with grouping sets, we need to expand the grouping sets
 * to individual group by.
 * 1. It need to make copies of the original data.
 * 2. run the aggregation on the multi copied data.
 * Both of these two are expensive.
 *
 * We could do this as following
 * 1. Run the aggregation on full grouping keys.
 * 2. Expand the aggregation result to the full grouping sets.
 * 3. Run the aggregation on the expanded data.
 *
 * So the plan is transformed from
 *   expand -> partial aggregating -> shuffle -> final merge aggregating
 * to
 *   partial aggregating -> shuffle -> merge aggregating -> expand
 *   -> final merge aggregating
 *
 * Notice:
 * If the aggregation involves distinct, we can't do this optimization.
 */

case class LazyExpandRule(session: SparkSession) extends Rule[SparkPlan] with Logging {
  override def apply(plan: SparkPlan): SparkPlan = plan.transformUp {
    case shuffle @ ColumnarShuffleExchangeExec(
          outputPartitioning,
          CHHashAggregateExecTransformer(
            requiredChildDistributionExpressions,
            groupingExpressions,
            aggregateExpressions,
            aggregateAttributes,
            initialInputBufferOffset,
            resultExpressions,
            ExpandExecTransformer(projections, output, child)
          ),
          shuffleOrigin,
          projectOutputAttributes,
          advisoryPartitionSize
        ) =>
      if (
        projections.exists(
          projection =>
            projection.forall(
              e => !e.isInstanceOf[Literal] || e.asInstanceOf[Literal].value != null)) &&
        groupingExpressions.forall(_.isInstanceOf[Attribute])
      ) {
        // Build a new hash aggregate node. Need to replace the grouping keys with attributes from
        // expand node's input. aggregateExpressions and aggregateExpressions doesn't neeed to be
        // applied the replacement, since all the attributes in them are from the input of the
        // expand node.
        val attributesToReplace = buildReplaceAttributeMapForAggregate(
          groupingExpressions,
          projections,
          output
        )
        val newGroupingExpresion =
          groupingExpressions
            .filter(_.name.startsWith("spark_grouping_id") == false)
            .map(e => attributesToReplace.getOrElse(e.name, e))
        val newResultExpressions =
          resultExpressions
            .filter(_.name.startsWith("spark_grouping_id") == false)
            .map(e => attributesToReplace.getOrElse(e.name, e))
        val newAggregate = CHHashAggregateExecTransformer(
          requiredChildDistributionExpressions,
          newGroupingExpresion,
          aggregateExpressions,
          aggregateAttributes,
          initialInputBufferOffset,
          newResultExpressions,
          child
        )
        val hashAggregateOutput = newAggregate.output

        // build a new expand node
        val newExpandOutput = shuffle.child
          .asInstanceOf[CHHashAggregateExecTransformer]
          .output
        val newExpandProjectionTemplate =
          newExpandOutput.map(e => attributesToReplace.getOrElse(e.name, e))
        val newExpandProjections = buildNewExpandProjections(
          groupingExpressions,
          projections,
          output,
          newExpandProjectionTemplate
        )
        val newExpand = ExpandExecTransformer(newExpandProjections, newExpandOutput, newAggregate)
        logError(s"xxx new expand: $newExpand")
        ColumnarShuffleExchangeExec(
          outputPartitioning,
          newExpand,
          shuffleOrigin,
          projectOutputAttributes,
          advisoryPartitionSize)
      } else {
        // It may be a case that involves two distinct aggregations.e.g.
        // select k, count(distinct v1), count(distinct v2) from t group by k with cube
        shuffle
      }
    case node =>
      logError(s"xxx node: ${node.getClass}")
      node
  }

  def buildReplaceAttributeMapForAggregate(
      originalGroupingExpressions: Seq[NamedExpression],
      originalExpandProjections: Seq[Seq[Expression]],
      originalExpandOutput: Seq[Attribute]): Map[String, Attribute] = {
    val fullExpandProjection = originalExpandProjections(0)
    var attributeMap = Map[String, Attribute]()
    originalGroupingExpressions.filter(_.name.startsWith("spark_grouping_id") == false).foreach {
      e =>
        val index = originalExpandOutput.indexWhere(_.semanticEquals(e.toAttribute))
        attributeMap += (e.name -> fullExpandProjection(index).asInstanceOf[Attribute])
    }
    attributeMap
  }

  def buildNewExpandProjections(
      originalGroupingExpressions: Seq[NamedExpression],
      originalExpandProjections: Seq[Seq[Expression]],
      originalExpandOutput: Seq[Attribute],
      newExpandOutput: Seq[Attribute]): Seq[Seq[Expression]] = {
    var groupingKeysPosition = Map[String, Int]()
    originalGroupingExpressions.foreach {
      e =>
        e match {
          case ne: NamedExpression =>
            val index = originalExpandOutput.indexWhere(_.semanticEquals(ne.toAttribute))
            if (index != -1) {
              groupingKeysPosition += (ne.name -> index)
            }
          case _ =>
        }
    }

    val newExpandProjections = originalExpandProjections.map {
      projection =>
        val res = newExpandOutput.map {
          attr =>
            groupingKeysPosition.get(attr.name) match {
              case Some(attrPos) => projection(attrPos)
              case None => attr
            }
        }
        res
    }
    newExpandProjections
  }

}
