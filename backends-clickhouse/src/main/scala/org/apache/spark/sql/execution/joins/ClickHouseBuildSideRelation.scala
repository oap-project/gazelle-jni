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
package org.apache.spark.sql.execution.joins

import io.glutenproject.backendsapi.clickhouse.CHBackendSettings
import io.glutenproject.execution.{BroadCastHashJoinContext, ColumnarNativeIterator}
import io.glutenproject.utils.PlanNodesUtil
import io.glutenproject.vectorized._

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, Expression}
import org.apache.spark.sql.catalyst.plans.physical.BroadcastMode
import org.apache.spark.sql.execution.utils.CHExecUtil
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.storage.CHShuffleReadStreamFactory

import scala.collection.JavaConverters._

case class ClickHouseBuildSideRelation(
    mode: BroadcastMode,
    output: Seq[Attribute],
    batches: Array[Array[Byte]],
    newBuildKeys: Seq[Expression] = Seq.empty)
  extends BuildSideRelation
  with Logging {

  override def deserialized: Iterator[ColumnarBatch] = Iterator.empty

  override def asReadOnlyCopy(
      broadCastContext: BroadCastHashJoinContext): ClickHouseBuildSideRelation = this

  private var hashTableData: Long = 0L
  def buildHashTable(
      broadCastContext: BroadCastHashJoinContext): (Long, ClickHouseBuildSideRelation) =
    synchronized {
      if (hashTableData == 0) {
        val allBatches = batches.flatten
        logDebug(
          s"BHJ value size: " +
            s"${broadCastContext.buildHashTableId} = ${allBatches.length}")
        val storageJoinBuilder = new StorageJoinBuilder(
          CHShuffleReadStreamFactory.create(allBatches),
          broadCastContext,
          CHBackendSettings.customizeBufferSize,
          output.asJava,
          newBuildKeys.asJava
        )
        // Build the hash table
        hashTableData = storageJoinBuilder.build()
        storageJoinBuilder.close()
        (hashTableData, this)
      } else {
        (StorageJoinBuilder.nativeCloneBuildHashTable(hashTableData), null)
      }
    }

  def reset(): Unit = synchronized {
    hashTableData = 0
  }

  /**
   * Transform columnar broadcast value to Array[InternalRow] by key and distinct.
   *
   * @return
   */
  override def transform(key: Expression): Array[InternalRow] = {
    val allBatches = batches.flatten
    // native block reader
    val blockReader =
      new CHStreamReader(
        CHShuffleReadStreamFactory.create(allBatches),
        CHBackendSettings.customizeBufferSize)
    val broadCastIter = new Iterator[ColumnarBatch] {
      private var current: CHNativeBlock = _

      override def hasNext: Boolean = {
        current = blockReader.next()
        current != null && current.numRows() > 0
      }

      override def next(): ColumnarBatch = {
        current.toColumnarBatch
      }
    }
    // Expression compute, return block iterator
    val expressionEval = new SimpleExpressionEval(
      new ColumnarNativeIterator(broadCastIter.asJava),
      PlanNodesUtil.genProjectionsPlanNode(key, output))

    try {
      // convert columnar to row
      asScalaIterator(expressionEval).flatMap {
        block =>
          val batch = new CHNativeBlock(block)
          if (batch.numRows == 0) {
            Iterator.empty
          } else {
            CHExecUtil
              .getRowIterFromSparkRowInfo(block, batch.numColumns(), batch.numRows())
              .map(row => row.copy())
          }
      }.toArray
    } finally {
      blockReader.close()
      expressionEval.close()
    }
  }
}
