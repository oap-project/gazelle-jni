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
package org.apache.gluten.ras.rule

import org.apache.gluten.ras._
import org.apache.gluten.ras.Ras.UnsafeHashKey
import org.apache.gluten.ras.memo.Closure
import org.apache.gluten.ras.path.InClusterPath
import org.apache.gluten.ras.property.PropertySet

import scala.collection.mutable

trait RuleApplier[T <: AnyRef] {
  def apply(icp: InClusterPath[T]): Unit
  def shape(): Shape[T]
}

object RuleApplier {
  def apply[T <: AnyRef](ras: Ras[T], closure: Closure[T], rule: RasRule[T]): RuleApplier[T] = {
    new RegularRuleApplier(ras, closure, rule)
  }

  def apply[T <: AnyRef](
      ras: Ras[T],
      closure: Closure[T],
      rule: EnforcerRule[T]): RuleApplier[T] = {
    new EnforcerRuleApplier[T](ras, closure, rule)
  }

  private class RegularRuleApplier[T <: AnyRef](ras: Ras[T], closure: Closure[T], rule: RasRule[T])
    extends RuleApplier[T] {
    private val deDup = mutable.Map[RasClusterKey, mutable.Set[UnsafeHashKey[T]]]()

    override def apply(icp: InClusterPath[T]): Unit = {
      if (!shape.identify(icp.path())) {
        return
      }
      val cKey = icp.cluster()
      val path = icp.path()
      val plan = path.plan()
      val appliedPlans = deDup.getOrElseUpdate(cKey, mutable.Set())
      val pKey = ras.toHashKey(plan)
      if (appliedPlans.contains(pKey)) {
        return
      }
      apply0(cKey, plan)
      appliedPlans += pKey
    }

    private def apply0(cKey: RasClusterKey, plan: T): Unit = {
      val equivalents = rule.shift(plan)
      equivalents.foreach {
        equiv =>
          closure
            .openFor(cKey)
            .memorize(equiv, ras.anyPropSet())
      }
    }

    override val shape: Shape[T] = rule.shape()
  }

  private class EnforcerRuleApplier[T <: AnyRef](
      ras: Ras[T],
      closure: Closure[T],
      rule: EnforcerRule[T])
    extends RuleApplier[T] {
    private val deDup = mutable.Map[RasClusterKey, mutable.Set[UnsafeHashKey[T]]]()
    private val constraint = rule.constraint()
    private val constraintDef = constraint.definition()

    override def apply(icp: InClusterPath[T]): Unit = {
      if (!shape.identify(icp.path())) {
        return
      }
      val cKey = icp.cluster()
      val path = icp.path()
      val propSet = path.node().self().propSet()
      if (propSet.get(constraintDef).satisfies(constraint)) {
        return
      }
      val plan = path.plan()
      val pKey = ras.toHashKey(plan)
      val appliedPlans = deDup.getOrElseUpdate(cKey, mutable.Set())
      if (appliedPlans.contains(pKey)) {
        return
      }
      val constraintSet = propSet.withProp(constraint)
      apply0(cKey, constraintSet, plan)
      appliedPlans += pKey
    }

    private def apply0(cKey: RasClusterKey, constraintSet: PropertySet[T], plan: T): Unit = {
      val equivalents = rule.shift(plan)
      equivalents.foreach {
        equiv =>
          closure
            .openFor(cKey)
            .memorize(equiv, constraintSet)
      }
    }

    override val shape: Shape[T] = rule.shape()
  }
}
