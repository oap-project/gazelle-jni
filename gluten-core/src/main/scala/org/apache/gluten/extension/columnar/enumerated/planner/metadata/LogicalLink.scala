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
package org.apache.gluten.extension.columnar.enumerated.planner.metadata

import org.apache.gluten.extension.columnar.enumerated.planner.metadata.GlutenMetadataModel.Verifier

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.plans.logical
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, Statistics}

case class LogicalLink(plan: LogicalPlan) {
  override def hashCode(): Int = System.identityHashCode(plan)
  override def equals(obj: Any): Boolean = obj match {
    // LogicalLink's comparison is based on ref equality of the logical plans being compared.
    case LogicalLink(otherPlan) => plan eq otherPlan
    case _ => false
  }
  override def toString: String = s"${plan.nodeName}[${plan.stats.simpleString}]"
}

object LogicalLink {
  private case class LogicalLinkNotFound() extends logical.LeafNode {
    override def output: Seq[Attribute] = List.empty
    override def canEqual(that: Any): Boolean = throw new UnsupportedOperationException()
    override def computeStats(): Statistics = Statistics(sizeInBytes = 0)
  }

  val notFound = new LogicalLink(LogicalLinkNotFound())
  implicit val verifier: Verifier[LogicalLink] = new Verifier[LogicalLink] with Logging {
    override def verify(one: LogicalLink, other: LogicalLink): Unit = {
      // LogicalLink's comparison is based on ref equality of the logical plans being compared.
      if (one != other) {
        logWarning(s"Warning: Logical link mismatch: one: $one, other: $other")
      }
    }
  }
}
