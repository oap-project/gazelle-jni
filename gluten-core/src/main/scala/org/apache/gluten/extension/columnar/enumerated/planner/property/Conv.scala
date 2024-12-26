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
package org.apache.gluten.extension.columnar.enumerated.planner.property

import org.apache.gluten.extension.columnar.transition.{Convention, ConventionReq, Transition}
import org.apache.gluten.ras.{Property, PropertyDef}
import org.apache.gluten.ras.rule.{RasRule, Shape, Shapes}

import org.apache.spark.sql.execution._

sealed trait Conv extends Property[SparkPlan] {
  import Conv._
  override def definition(): PropertyDef[SparkPlan, _ <: Property[SparkPlan]] = {
    ConvDef
  }

  override def satisfies(other: Property[SparkPlan]): Boolean = {
    // The following enforces strict type checking against `this` and `other`
    // to make sure:
    //
    //  1. `this`, which came from user implementation of PropertyDef.getProperty, must be a `Prop`
    //  2. `other` which came from user implementation of PropertyDef.getChildrenConstraints,
    //     must be a `Req`
    //
    // If the user implementation doesn't follow the criteria, cast error will be thrown.
    //
    // This can be a common practice to implement a safe Property for RAS.
    //
    // TODO: Add a similar case to RAS UTs.
    val req = other.asInstanceOf[Req]
    if (req.isAny) {
      return true
    }
    val prop = this.asInstanceOf[Prop]
    val out = Transition.factory().satisfies(prop.prop, req.req)
    out
  }
}

object Conv {
  val any: Conv = Req(ConventionReq.any)

  def of(conv: Convention): Conv = Prop(conv)
  def req(req: ConventionReq): Conv = Req(req)

  def get(plan: SparkPlan): Conv = {
    Conv.of(Convention.get(plan))
  }

  def findTransition(from: Conv, to: Conv): Transition = {
    val prop = from.asInstanceOf[Prop]
    val req = to.asInstanceOf[Req]
    val out = Transition.factory().findTransition(prop.prop, req.req, new IllegalStateException())
    out
  }

  case class Prop(prop: Convention) extends Conv
  case class Req(req: ConventionReq) extends Conv {
    def isAny: Boolean = {
      req.requiredBatchType == ConventionReq.BatchType.Any &&
      req.requiredRowType == ConventionReq.RowType.Any
    }
  }
}

object ConvDef extends PropertyDef[SparkPlan, Conv] {
  // TODO: Should the convention-transparent ops (e.g., aqe shuffle read) support
  //  convention-propagation. Probably need to refactor getChildrenPropertyRequirements.
  override def getProperty(plan: SparkPlan): Conv = {
    conventionOf(plan)
  }

  private def conventionOf(plan: SparkPlan): Conv = {
    val out = Conv.get(plan)
    out
  }

  override def getChildrenConstraints(
      constraint: Property[SparkPlan],
      plan: SparkPlan): Seq[Conv] = {
    val out = ConventionReq.get(plan).map(Conv.req)
    out
  }

  override def any(): Conv = Conv.any
}

case class ConvEnforcerRule(reqConv: Conv) extends RasRule[SparkPlan] {
  override def shift(node: SparkPlan): Iterable[SparkPlan] = {
    val conv = Conv.get(node)
    if (conv.satisfies(reqConv)) {
      return List.empty
    }
    val transition = Conv.findTransition(conv, reqConv)
    val after = transition.apply(node)
    List(after)
  }

  override def shape(): Shape[SparkPlan] = Shapes.fixedHeight(1)
}
