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

import org.apache.gluten.GlutenConfig
import org.apache.gluten.backendsapi.BackendsApiManager
import org.apache.gluten.columnarbatch.ColumnarBatches
import org.apache.gluten.execution.{RowToVeloxColumnarExec, VeloxColumnarToRowExec}
import org.apache.gluten.iterator.Iterators
import org.apache.gluten.memory.arrow.alloc.ArrowBufferAllocators
import org.apache.gluten.runtime.Runtimes
import org.apache.gluten.utils.ArrowAbiUtil
import org.apache.gluten.vectorized.ColumnarBatchSerializerJniWrapper

import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.{InternalRow, SQLConfHelper}
import org.apache.spark.sql.catalyst.expressions.{Attribute, Expression}
import org.apache.spark.sql.columnar.{CachedBatch, CachedBatchSerializer}
import org.apache.spark.sql.execution.columnar.DefaultCachedBatchSerializer
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{StructField, StructType}
import org.apache.spark.sql.utils.SparkArrowUtil
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.storage.StorageLevel

import org.apache.arrow.c.ArrowSchema

case class CachedColumnarBatch(
    override val numRows: Int,
    override val sizeInBytes: Long,
    bytes: Array[Byte])
  extends CachedBatch {}

// format: off
/**
 * Feature:
 * 1. This serializer supports column pruning
 * 2. TODO: support push down filter
 * 3. Super TODO: support store offheap object directly
 *
 * The data transformation pipeline:
 *
 *   - Serializer ColumnarBatch -> CachedColumnarBatch
 *     -> serialize to byte[]
 *
 *   - Deserializer CachedColumnarBatch -> ColumnarBatch
 *     -> deserialize to byte[] to create Velox ColumnarBatch
 *
 *   - Serializer InternalRow -> CachedColumnarBatch (support RowToColumnar)
 *     -> Convert InternalRow to ColumnarBatch
 *     -> Serializer ColumnarBatch -> CachedColumnarBatch
 *
 *   - Serializer InternalRow -> DefaultCachedBatch (unsupport RowToColumnar)
 *     -> Convert InternalRow to DefaultCachedBatch using vanilla Spark serializer
 *
 *   - Deserializer CachedColumnarBatch -> InternalRow (support ColumnarToRow)
 *     -> Deserializer CachedColumnarBatch -> ColumnarBatch
 *     -> Convert ColumnarBatch to InternalRow
 *
 *   - Deserializer DefaultCachedBatch -> InternalRow (unsupport ColumnarToRow)
 *     -> Convert DefaultCachedBatch to InternalRow using vanilla Spark serializer
 */
// format: on
class ColumnarCachedBatchSerializer extends CachedBatchSerializer with SQLConfHelper with Logging {
  private lazy val rowBasedCachedBatchSerializer = new DefaultCachedBatchSerializer

  private def toStructType(schema: Seq[Attribute]): StructType = {
    StructType(schema.map(a => StructField(a.name, a.dataType, a.nullable, a.metadata)))
  }

  private def validateSchema(schema: Seq[Attribute]): Boolean = {
    val dt = toStructType(schema)
    validateSchema(dt)
  }

  private def validateSchema(schema: StructType): Boolean = {
    val reason = BackendsApiManager.getValidatorApiInstance.doSchemaValidate(schema)
    if (reason.isDefined) {
      logInfo(s"Columnar cache does not support schema $schema, due to ${reason.get}")
      false
    } else {
      true
    }
  }

  override def supportsColumnarInput(schema: Seq[Attribute]): Boolean = {
    // Note, there is a issue that, if gluten columnar scan is disabled and vanilla Spark
    // columnar is enabled, then the following plan would fail.
    // InMemoryTableScan
    //   InMemoryRelation
    //     (vanilla Spark columnar Scan) Parquet
    // The reason is that, Spark will remove the top level `ColumnarToRow` and call
    // `convertColumnarBatchToCachedBatch`, but the inside ColumnarBatch is not arrow-based.
    // See: `InMemoryRelation.apply()`.
    // So we should disallow columnar input if using vanilla Spark columnar scan.
    val noVanillaSparkColumnarScan = conf.getConf(GlutenConfig.COLUMNAR_FILESCAN_ENABLED) ||
      !conf.getConf(GlutenConfig.VANILLA_VECTORIZED_READERS_ENABLED)
    conf.getConf(GlutenConfig.GLUTEN_ENABLED) && validateSchema(
      schema) && noVanillaSparkColumnarScan
  }

  override def supportsColumnarOutput(schema: StructType): Boolean = {
    conf.getConf(GlutenConfig.GLUTEN_ENABLED) && validateSchema(schema)
  }

  override def convertInternalRowToCachedBatch(
      input: RDD[InternalRow],
      schema: Seq[Attribute],
      storageLevel: StorageLevel,
      conf: SQLConf): RDD[CachedBatch] = {
    val localSchema = toStructType(schema)
    if (!validateSchema(localSchema)) {
      // we can not use columnar cache here, as the `RowToColumnar` does not support this schema
      return rowBasedCachedBatchSerializer.convertInternalRowToCachedBatch(
        input,
        schema,
        storageLevel,
        conf)
    }

    val numRows = conf.columnBatchSize
    val rddColumnarBatch = input.mapPartitions {
      it => RowToVeloxColumnarExec.toColumnarBatchIterator(it, localSchema, numRows)
    }
    convertColumnarBatchToCachedBatch(rddColumnarBatch, schema, storageLevel, conf)
  }

  override def convertCachedBatchToInternalRow(
      input: RDD[CachedBatch],
      cacheAttributes: Seq[Attribute],
      selectedAttributes: Seq[Attribute],
      conf: SQLConf): RDD[InternalRow] = {
    if (!validateSchema(cacheAttributes)) {
      // if we do not support this schema that means we are using row-based serializer,
      // see `convertInternalRowToCachedBatch`, so fallback to vanilla Spark serializer
      return rowBasedCachedBatchSerializer.convertCachedBatchToInternalRow(
        input,
        cacheAttributes,
        selectedAttributes,
        conf)
    }

    val rddColumnarBatch =
      convertCachedBatchToColumnarBatch(input, cacheAttributes, selectedAttributes, conf)
    rddColumnarBatch.mapPartitions {
      it => VeloxColumnarToRowExec.toRowIterator(it, selectedAttributes)
    }
  }

  override def convertColumnarBatchToCachedBatch(
      input: RDD[ColumnarBatch],
      schema: Seq[Attribute],
      storageLevel: StorageLevel,
      conf: SQLConf): RDD[CachedBatch] = {
    input.mapPartitions {
      it =>
        new Iterator[CachedBatch] {
          override def hasNext: Boolean = it.hasNext

          override def next(): CachedBatch = {
            val batch = it.next()
            val results =
              ColumnarBatchSerializerJniWrapper
                .create(
                  Runtimes.contextInstance(
                    BackendsApiManager.getBackendName,
                    "ColumnarCachedBatchSerializer#serialize"))
                .serialize(
                  Array(ColumnarBatches.getNativeHandle(BackendsApiManager.getBackendName, batch)))
            CachedColumnarBatch(
              results.getNumRows.toInt,
              results.getSerialized.length,
              results.getSerialized)
          }
        }
    }
  }

  override def convertCachedBatchToColumnarBatch(
      input: RDD[CachedBatch],
      cacheAttributes: Seq[Attribute],
      selectedAttributes: Seq[Attribute],
      conf: SQLConf): RDD[ColumnarBatch] = {
    // Find the ordinals and data types of the requested columns.
    val requestedColumnIndices = selectedAttributes.map {
      a => cacheAttributes.map(_.exprId).indexOf(a.exprId)
    }
    val shouldSelectAttributes = cacheAttributes != selectedAttributes
    val localSchema = toStructType(cacheAttributes)
    val timezoneId = SQLConf.get.sessionLocalTimeZone
    input.mapPartitions {
      it =>
        val runtime = Runtimes.contextInstance(
          BackendsApiManager.getBackendName,
          "ColumnarCachedBatchSerializer#read")
        val jniWrapper = ColumnarBatchSerializerJniWrapper
          .create(runtime)
        val schema = SparkArrowUtil.toArrowSchema(localSchema, timezoneId)
        val arrowAlloc = ArrowBufferAllocators.contextInstance()
        val cSchema = ArrowSchema.allocateNew(arrowAlloc)
        ArrowAbiUtil.exportSchema(arrowAlloc, schema, cSchema)
        val deserializerHandle = jniWrapper
          .init(cSchema.memoryAddress())
        cSchema.close()

        Iterators
          .wrap(new Iterator[ColumnarBatch] {
            override def hasNext: Boolean = it.hasNext

            override def next(): ColumnarBatch = {
              val cachedBatch = it.next().asInstanceOf[CachedColumnarBatch]
              val batchHandle =
                jniWrapper
                  .deserialize(deserializerHandle, cachedBatch.bytes)
              val batch = ColumnarBatches.create(batchHandle)
              if (shouldSelectAttributes) {
                try {
                  ColumnarBatches.select(
                    BackendsApiManager.getBackendName,
                    batch,
                    requestedColumnIndices.toArray)
                } finally {
                  batch.close()
                }
              } else {
                batch
              }
            }
          })
          .protectInvocationFlow()
          .recycleIterator {
            jniWrapper.close(deserializerHandle)
          }
          .recyclePayload(_.close())
          .create()
    }
  }

  override def buildFilter(
      predicates: Seq[Expression],
      cachedAttributes: Seq[Attribute]): (Int, Iterator[CachedBatch]) => Iterator[CachedBatch] = {
    // TODO, support build filter as we did not support collect min/max value for columnar batch
    (_, it) => it
  }
}
