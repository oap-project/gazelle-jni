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
package org.apache.gluten.extension.columnar

import org.apache.gluten.backendsapi.BackendsApiManager
import org.apache.gluten.config.GlutenConfig
import org.apache.gluten.execution.FilterExecTransformerBase

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.{And, Attribute, Cast, EqualNullSafe, EqualTo, Expression, Literal}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.SparkPlan

/**
 * Rewrite `and` filter conditions, move the equal conditions ahead. e.g. `cast(b as string) != ''
 * and a = 1` => `a = 1 and cast(b as string) != ''`
 */
case class MoveAndFilterEqualConditionsAhead(spark: SparkSession) extends Rule[SparkPlan] {
  override def apply(plan: SparkPlan): SparkPlan = {
    if (GlutenConfig.get.enableMoveAndFilterEqualConditionsAhead) {
      optimize(plan)
    } else {
      plan
    }
  }

  private def optimize(plan: SparkPlan): SparkPlan = {
    plan match {
      case f: FilterExecTransformerBase if f.cond.isInstanceOf[And] =>
        val newCond = optimize(f.cond)
        BackendsApiManager.getSparkPlanExecApiInstance.genFilterExecTransformer(newCond, f.child)
      case _ =>
        val newChildren = plan.children.map(p => optimize(p))
        plan.withNewChildren(newChildren)
    }
  }

  private def optimize(expr: Expression): Expression = expr match {
    case a: And =>
      if (isEqualCondition(a.right)) {
        val newLeft = optimize(a.left)
        val newRight = optimize(a.right)
        val newExpr = And(newRight, newLeft)
        newExpr
      } else {
        val newLeft = optimize(a.left)
        val newRight = optimize(a.right)
        val newExpr = And(newLeft, newRight)
        newExpr
      }
    case _ => expr
  }

  private def isEqualCondition(expr: Expression): Boolean = {
    var leftExpr = null.asInstanceOf[Expression]
    var rightExpr = null.asInstanceOf[Expression]
    expr match {
      case e: EqualTo => leftExpr = e.left; rightExpr = e.right
      case e: EqualNullSafe => leftExpr = e.left; rightExpr = e.right
      case _ =>
        return false
    }
    if (leftExpr == null || rightExpr == null) {
      return false
    }
    def f(left: Expression, right: Expression): Boolean = {
      left match {
        case _: Attribute =>
          right match {
            case _: Literal =>
              return true
            case c: Cast if c.child.isInstanceOf[Literal] =>
              return true
            case _ =>
          }
        case _ =>
      }
      false
    }
    f(leftExpr, rightExpr) || f(rightExpr, leftExpr)
  }
}
