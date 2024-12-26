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
package org.apache.gluten.extension.columnar.enumerated.planner.plan

import org.apache.gluten.execution.GlutenPlan
import org.apache.gluten.extension.columnar.enumerated.planner.metadata.{GlutenMetadata, LogicalLink}
import org.apache.gluten.extension.columnar.enumerated.planner.property.{Conv, ConvDef}
import org.apache.gluten.extension.columnar.transition.{Convention, ConventionReq}
import org.apache.gluten.ras.{Metadata, PlanModel}
import org.apache.gluten.ras.property.PropertySet
import org.apache.gluten.sql.shims.SparkShimLoader

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.trees.TreeNodeTag
import org.apache.spark.sql.execution.{ColumnarToRowExec, LeafExecNode, SparkPlan}
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2ScanExecBase
import org.apache.spark.task.{SparkTaskUtil, TaskResources}

import java.util.{Objects, Properties}
import java.util.concurrent.atomic.AtomicBoolean

object GlutenPlanModel {
  def apply(): PlanModel[SparkPlan] = {
    PlanModelImpl
  }

  // TODO: Make this inherit from GlutenPlan.
  case class GroupLeafExec(
      groupId: Int,
      metadata: GlutenMetadata,
      constraintSet: PropertySet[SparkPlan])
    extends LeafExecNode
    with Convention.KnownBatchType
    with Convention.KnownRowTypeForSpark33OrLater
    with GlutenPlan.SupportsRowBasedCompatible {

    private val frozen = new AtomicBoolean(false)
    private val req: Conv.Req = constraintSet.get(ConvDef).asInstanceOf[Conv.Req]

    // Set the logical link then make the plan node immutable. All future
    // mutable operations related to tagging will be aborted.
    if (metadata.logicalLink() != LogicalLink.notFound) {
      setLogicalLink(metadata.logicalLink().plan)
    }
    frozen.set(true)

    override protected def doExecute(): RDD[InternalRow] = throw new IllegalStateException()
    override def output: Seq[Attribute] = metadata.schema().output

    override val batchType: Convention.BatchType = {
      val out = req.req.requiredBatchType match {
        case ConventionReq.BatchType.Any => Convention.BatchType.None
        case ConventionReq.BatchType.Is(b) => b
      }
      out
    }

    final override val supportsColumnar: Boolean = {
      batchType != Convention.BatchType.None
    }

    override val rowType0: Convention.RowType = {
      val out = req.req.requiredRowType match {
        case ConventionReq.RowType.Any => Convention.RowType.None
        case ConventionReq.RowType.Is(r) => r
      }
      out
    }

    final override val supportsRowBased: Boolean = {
      rowType() != Convention.RowType.None
    }

    private def ensureNotFrozen(): Unit = {
      if (frozen.get()) {
        throw new UnsupportedOperationException()
      }
    }

    // Enclose mutable APIs.
    override def setLogicalLink(logicalPlan: LogicalPlan): Unit = {
      ensureNotFrozen()
      super.setLogicalLink(logicalPlan)
    }
    override def setTagValue[T](tag: TreeNodeTag[T], value: T): Unit = {
      ensureNotFrozen()
      super.setTagValue(tag, value)
    }
    override def unsetTagValue[T](tag: TreeNodeTag[T]): Unit = {
      ensureNotFrozen()
      super.unsetTagValue(tag)
    }
    override def copyTagsFrom(other: SparkPlan): Unit = {
      ensureNotFrozen()
      super.copyTagsFrom(other)
    }
  }

  private object PlanModelImpl extends PlanModel[SparkPlan] {
    private val fakeTc = SparkShimLoader.getSparkShims.createTestTaskContext(new Properties())
    private def fakeTc[T](body: => T): T = {
      assert(!TaskResources.inSparkTask())
      SparkTaskUtil.setTaskContext(fakeTc)
      try {
        body
      } finally {
        SparkTaskUtil.unsetTaskContext()
      }
    }

    override def childrenOf(node: SparkPlan): Seq[SparkPlan] = node.children

    override def withNewChildren(node: SparkPlan, children: Seq[SparkPlan]): SparkPlan =
      node match {
        case c2r: ColumnarToRowExec =>
          // Workaround: To bypass the assertion in ColumnarToRowExec's code if child is
          // a group leaf.
          fakeTc {
            c2r.withNewChildren(children)
          }
        case other =>
          other.withNewChildren(children)
      }

    override def hashCode(node: SparkPlan): Int = Objects.hashCode(withEqualityWrapper(node))

    override def equals(one: SparkPlan, other: SparkPlan): Boolean =
      Objects.equals(withEqualityWrapper(one), withEqualityWrapper(other))

    override def newGroupLeaf(
        groupId: Int,
        metadata: Metadata,
        constraintSet: PropertySet[SparkPlan]): SparkPlan =
      GroupLeafExec(groupId, metadata.asInstanceOf[GlutenMetadata], constraintSet)

    override def isGroupLeaf(node: SparkPlan): Boolean = node match {
      case _: GroupLeafExec => true
      case _ => false
    }

    override def getGroupId(node: SparkPlan): Int = node match {
      case gl: GroupLeafExec => gl.groupId
      case _ => throw new IllegalStateException()
    }

    private def withEqualityWrapper(node: SparkPlan): AnyRef = node match {
      case scan: DataSourceV2ScanExecBase =>
        // Override V2 scan operators' equality implementation to include output attributes.
        //
        // Spark's V2 scans don't incorporate out attributes in equality so E.g.,
        // BatchScan[date#1] can be considered equal to BatchScan[date#2], which is unexpected
        // in RAS planner because it strictly relies on plan equalities for sanity.
        //
        // Related UT: `VeloxOrcDataTypeValidationSuite#Date type`
        // Related Spark PRs:
        // https://github.com/apache/spark/pull/23086
        // https://github.com/apache/spark/pull/23619
        // https://github.com/apache/spark/pull/23430
        ScanV2ExecEqualityWrapper(scan, scan.output)
      case other => other
    }

    private case class ScanV2ExecEqualityWrapper(
        scan: DataSourceV2ScanExecBase,
        output: Seq[Attribute])
  }
}
