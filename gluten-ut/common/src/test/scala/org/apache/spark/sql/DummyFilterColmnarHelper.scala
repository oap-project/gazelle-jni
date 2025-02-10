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
package org.apache.spark.sql

import org.apache.gluten.utils.BackendTestUtils

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeSet}
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, UnaryNode}
import org.apache.spark.sql.catalyst.plans.physical.Partitioning
import org.apache.spark.sql.execution.{SparkPlan, SparkStrategy, UnaryExecNode}
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.vectorized.ColumnarBatch

case class DummyFilterColumnar(child: LogicalPlan) extends UnaryNode {
  override def output: Seq[Attribute] = child.output
  override def metadataOutput: Seq[Attribute] = child.metadataOutput
  override lazy val references: AttributeSet = child.outputSet
  override protected def withNewChildInternal(newChild: LogicalPlan): DummyFilterColumnar = {
    copy(child = newChild)
  }
}

case class DummyFilterColumnarExec(child: SparkPlan) extends UnaryExecNode {
  override protected def doExecute(): RDD[InternalRow] = {
    throw new IllegalStateException("Do not support row based operation.")
  }
  override lazy val output: Seq[Attribute] = child.output
  override def outputPartitioning: Partitioning = child.outputPartitioning
  override protected def withNewChildInternal(newChild: SparkPlan): SparkPlan = {
    copy(child = newChild)
  }
  override def supportsColumnar: Boolean = true
  override def doExecuteColumnar(): RDD[ColumnarBatch] = {
    var hasNulls = false
    child.executeColumnar().map {
      b =>
        // This is for testing only.
        hasNulls = b.column(0).hasNull()
        b
    }
  }
}

object DummyFilterColumnarStrategy extends SparkStrategy {
  override def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {
    case r: DummyFilterColumnar =>
      DummyFilterColumnarExec(planLater(r.child)) :: Nil
    case _ => Nil
  }
}

object DummyFilterColmnarHelper {
  def dfWithDummyFilterColumnar(spark: SparkSession, plan: LogicalPlan): DataFrame = {
    val modifiedPlan = plan.transformUp {
      case l: LogicalRelation => DummyFilterColumnar(l)
      case p => p
    }

    Dataset.ofRows(spark, modifiedPlan)
  }

  def withSession(builders: Seq[SparkSessionExtensionsProvider])(f: SparkSession => Unit): Unit = {
    val builder = if (BackendTestUtils.isCHBackendLoaded()) {
      SparkSession
        .builder()
        .master("local[1]")
        .config("spark.driver.memory", "1G")
        .config("spark.memory.offHeap.enabled", "true")
        .config("spark.memory.offHeap.size", "1024MB")
        .config("spark.plugins", "org.apache.gluten.GlutenPlugin")
        .config("spark.shuffle.manager", "org.apache.spark.shuffle.sort.ColumnarShuffleManager")
        .config("spark.io.compression.codec", "LZ4")
        .config("spark.gluten.sql.enable.native.validation", "false")
    } else {
      SparkSession
        .builder()
        .master("local[1]")
        .config("spark.driver.memory", "1G")
        .config("spark.memory.offHeap.enabled", "true")
        .config("spark.memory.offHeap.size", "1024MB")
        .config("spark.plugins", "org.apache.gluten.GlutenPlugin")
        .config("spark.shuffle.manager", "org.apache.spark.shuffle.sort.ColumnarShuffleManager")
    }
    builders.foreach(builder.withExtensions)
    val spark = builder.getOrCreate()
    try f(spark)
    finally {
      spark.stop()
      SparkSession.clearActiveSession()
      SparkSession.clearDefaultSession()
    }
  }

  def create(builder: SparkSessionExtensionsProvider): Seq[SparkSessionExtensionsProvider] = Seq(
    builder)
}
