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
package org.apache.spark.sql.execution

import org.apache.gluten.columnarbatch.ColumnarBatches
import org.apache.gluten.iterator.Iterators
import org.apache.gluten.memory.arrow.alloc.ArrowBufferAllocators
import org.apache.gluten.runtime.Runtimes
import org.apache.gluten.sql.shims.SparkShimLoader
import org.apache.gluten.utils.ArrowAbiUtil
import org.apache.gluten.vectorized.{ColumnarBatchSerializerJniWrapper, NativeColumnarToRowJniWrapper}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference, Expression, UnsafeProjection, UnsafeRow}
import org.apache.spark.sql.execution.joins.BuildSideRelation
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.utils.SparkArrowUtil
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.task.TaskResources
import org.apache.arrow.c.ArrowSchema
import org.apache.spark.sql.execution.vectorized.OffHeapColumnVector
import org.apache.spark.sql.types.DataTypes

import scala.collection.JavaConverters.asScalaIteratorConverter

case class ColumnarBuildSideRelation(output: Seq[Attribute], batches: UnsafeArray)
  extends BuildSideRelation {

  override def deserialized: Iterator[ColumnarBatch] = {
    val runtime = Runtimes.contextInstance("BuildSideRelation#deserialized")
    val jniWrapper = ColumnarBatchSerializerJniWrapper.create(runtime)
    val serializeHandle: Long = {
      val allocator = ArrowBufferAllocators.contextInstance()
      val cSchema = ArrowSchema.allocateNew(allocator)
      val arrowSchema = SparkArrowUtil.toArrowSchema(
        SparkShimLoader.getSparkShims.structFromAttributes(output),
        SQLConf.get.sessionLocalTimeZone)
      ArrowAbiUtil.exportSchema(allocator, arrowSchema, cSchema)
      val handle = jniWrapper
        .init(cSchema.memoryAddress())
      cSchema.close()
      handle
    }

    Iterators
      .wrap(new Iterator[ColumnarBatch] {
        var batchId = 0

        override def hasNext: Boolean = {
          batchId < batches.getLength
        }

        override def next: ColumnarBatch = {
          val batch = batches.get(batchId)
          val columnVector = new OffHeapColumnVector(batch.numElements(), DataTypes.BinaryType)
          columnVector.putByteArray(batchId, batch.toByteArray, batch.getBaseOffset.toInt, batch.numElements)
          val columnarBatch = new ColumnarBatch(Array(columnVector))
          batchId += 1
          columnarBatch
        }
      })
      .protectInvocationFlow()
      .recycleIterator {
        jniWrapper.close(serializeHandle)
      }
      .recyclePayload(ColumnarBatches.forceClose) // FIXME why force close?
      .create()
  }

  override def asReadOnlyCopy(): ColumnarBuildSideRelation = this

  /**
   * Transform columnar broadcast value to Array[InternalRow] by key and distinct. NOTE: This method
   * was called in Spark Driver, should manage resources carefully.
   */
  override def transform(key: Expression): Array[InternalRow] = TaskResources.runUnsafe {
    val runtime = Runtimes.contextInstance("BuildSideRelation#transform")
    // This transformation happens in Spark driver, thus resources can not be managed automatically.
    val serializerJniWrapper = ColumnarBatchSerializerJniWrapper.create(runtime)
    val serializeHandle = {
      val allocator = ArrowBufferAllocators.contextInstance()
      val cSchema = ArrowSchema.allocateNew(allocator)
      val arrowSchema = SparkArrowUtil.toArrowSchema(
        SparkShimLoader.getSparkShims.structFromAttributes(output),
        SQLConf.get.sessionLocalTimeZone)
      ArrowAbiUtil.exportSchema(allocator, arrowSchema, cSchema)
      val handle = serializerJniWrapper.init(cSchema.memoryAddress())
      cSchema.close()
      handle
    }

    var closed = false

    val exprIds = output.map(_.exprId)
    val projExpr = key.transformDown {
      case attr: AttributeReference if !exprIds.contains(attr.exprId) =>
        val i = output.count(_.name == attr.name)
        if (i != 1) {
          throw new IllegalArgumentException(s"Only one attr with the same name is supported: $key")
        } else {
          output.find(_.name == attr.name).get
        }
    }
    val proj = UnsafeProjection.create(Seq(projExpr), output)

    // Convert columnar to Row.
    val jniWrapper = NativeColumnarToRowJniWrapper.create(runtime)
    val c2rId = jniWrapper.nativeColumnarToRowInit()
    var batchId = 0
    val iterator = if (batches.length > 0) {
      val res: Iterator[Iterator[InternalRow]] = new Iterator[Iterator[InternalRow]] {
        override def hasNext: Boolean = {
          val itHasNext = batchId < batches.length
          if (!itHasNext && !closed) {
            jniWrapper.nativeClose(c2rId)
            serializerJniWrapper.close(serializeHandle)
            closed = true
          }
          itHasNext
        }

        override def next(): Iterator[InternalRow] = {
          val batchBytes = batches(batchId)
          batchId += 1
          val batchHandle =
            serializerJniWrapper.deserialize(serializeHandle, batchBytes)
          val batch = ColumnarBatches.create(batchHandle)
          if (batch.numRows == 0) {
            batch.close()
            Iterator.empty
          } else if (output.isEmpty) {
            val rows = ColumnarBatches.emptyRowIterator(batch.numRows()).asScala
            batch.close()
            rows
          } else {
            val cols = batch.numCols()
            val rows = batch.numRows()
            var info =
              jniWrapper.nativeColumnarToRowConvert(
                c2rId,
                ColumnarBatches.getNativeHandle(batch),
                0)
            batch.close()

            new Iterator[InternalRow] {
              var rowId = 0
              var baseLength = 0
              val row = new UnsafeRow(cols)

              override def hasNext: Boolean = {
                rowId < rows
              }

              override def next: UnsafeRow = {
                if (rowId >= rows) throw new NoSuchElementException
                if (rowId == baseLength + info.lengths.length) {
                  baseLength += info.lengths.length
                  info = jniWrapper.nativeColumnarToRowConvert(batchHandle, c2rId, rowId)
                }
                val (offset, length) =
                  (info.offsets(rowId - baseLength), info.lengths(rowId - baseLength))
                row.pointTo(null, info.memoryAddress + offset, length.toInt)
                rowId += 1
                row
              }
            }.map(proj).map(_.copy())
          }
        }
      }
      res.flatten
    } else {
      Iterator.empty
    }
    iterator.toArray
  }
}
