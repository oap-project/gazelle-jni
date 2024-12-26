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
package org.apache.spark.sql.execution.utils

import org.apache.gluten.GlutenConfig
import org.apache.gluten.backendsapi.clickhouse.CHBackendSettings
import org.apache.gluten.expression.ConverterUtils
import org.apache.gluten.row.SparkRowInfo
import org.apache.gluten.vectorized._
import org.apache.gluten.vectorized.BlockSplitIterator.IteratorOptions

import org.apache.spark.{Partitioner, ShuffleDependency}
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.serializer.Serializer
import org.apache.spark.shuffle.{ColumnarShuffleDependency, GlutenShuffleUtils, HashPartitioningWrapper}
import org.apache.spark.shuffle.utils.RangePartitionerBoundsGenerator
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, BindReferences, BoundReference, UnsafeProjection, UnsafeRow}
import org.apache.spark.sql.catalyst.expressions.codegen.LazilyGeneratedOrdering
import org.apache.spark.sql.catalyst.plans.physical.{SinglePartition, _}
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.exchange.ShuffleExchangeExec
import org.apache.spark.sql.execution.metric.{SQLMetric, SQLShuffleWriteMetricsReporter}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.util.MutablePair

import io.substrait.proto.Type

import java.io.ByteArrayOutputStream

import scala.collection.JavaConverters._

object CHExecUtil extends Logging {

  def inferSparkDataType(substraitType: Array[Byte]): DataType = {
    val (datatype, nullable) =
      ConverterUtils.parseFromSubstraitType(Type.parseFrom(substraitType))
    datatype
  }

  def toBytes(
      dataSize: SQLMetric,
      iter: Iterator[ColumnarBatch],
      isNullAware: Boolean = false,
      keyColumnIndex: Int = 0,
      compressionCodec: Option[String] = Some("lz4"),
      compressionLevel: Option[Int] = None,
      bufferSize: Int = 4 << 10): Iterator[(Long, Array[Byte], Boolean)] = {
    var count = 0L
    var hasNullKeyValues = false
    val bos = new ByteArrayOutputStream()
    val buffer = new Array[Byte](bufferSize) // 4K
    val level = compressionLevel.getOrElse(Int.MinValue)
    val blockOutputStream =
      compressionCodec
        .map(new BlockOutputStream(bos, buffer, dataSize, true, _, level, bufferSize))
        .getOrElse(new BlockOutputStream(bos, buffer, dataSize, false, "", level, bufferSize))
    if (isNullAware) {
      while (iter.hasNext) {
        val batch = iter.next()
        val blockStats = CHNativeBlock.fromColumnarBatch(batch).getBlockStats(keyColumnIndex)
        count += blockStats.getBlockRecordCount
        hasNullKeyValues = hasNullKeyValues || blockStats.isHasNullKeyValues
        blockOutputStream.write(batch)
      }
    } else {
      while (iter.hasNext) {
        val batch = iter.next()
        count += batch.numRows()
        blockOutputStream.write(batch)
      }
    }
    blockOutputStream.flush()
    blockOutputStream.close()
    Iterator((count, bos.toByteArray, hasNullKeyValues))
  }

  def buildSideRDD(
      dataSize: SQLMetric,
      newChild: SparkPlan,
      isNullAware: Boolean,
      keyColumnIndex: Int
  ): RDD[(Long, Array[Byte], Boolean)] = {
    newChild
      .executeColumnar()
      .mapPartitionsInternal(iter => toBytes(dataSize, iter, isNullAware, keyColumnIndex))
  }

  private def buildRangePartitionSampleRDD(
      rdd: RDD[ColumnarBatch],
      rangePartitioning: RangePartitioning,
      outputAttributes: Seq[Attribute]): RDD[MutablePair[InternalRow, Null]] = {
    rangePartitioning match {
      case RangePartitioning(sortingExpressions, _) =>
        val sampleRDD = rdd.mapPartitionsInternal {
          iter =>
            iter.flatMap(
              batch => {
                // generate rows from a columnar batch
                val rowItr: Iterator[InternalRow] = c2r(batch)
                val projection =
                  UnsafeProjection.create(sortingExpressions.map(_.child), outputAttributes)
                val mutablePair = new MutablePair[InternalRow, Null]()
                rowItr.map(row => mutablePair.update(projection(row).copy(), null))
              })
        }
        sampleRDD
    }
  }

  def getRowIterFromSparkRowInfo(
      rowInfo: SparkRowInfo,
      columns: Int,
      rows: Int): Iterator[InternalRow] = {
    new Iterator[InternalRow] {
      var rowId = 0
      val row = new UnsafeRow(columns)
      var closed = false

      override def hasNext: Boolean = {
        val result = rowId < rows
        if (!result && !closed) {
          CHBlockConverterJniWrapper.freeMemory(rowInfo.memoryAddress, rowInfo.totalSize)
          closed = true
        }
        result
      }

      override def next(): UnsafeRow = {
        if (rowId >= rows) throw new NoSuchElementException

        val (offset, length) = (rowInfo.offsets(rowId), rowInfo.lengths(rowId))
        row.pointTo(null, rowInfo.memoryAddress + offset, length.toInt)
        rowId += 1
        row
      }
    }
  }
  def getRowIterFromSparkRowInfo(
      blockAddress: Long,
      columns: Int,
      rows: Int): Iterator[InternalRow] = {
    val rowInfo = CHBlockConverterJniWrapper.convertColumnarToRow(blockAddress, null)
    assert(rowInfo.fieldsNum == columns)
    getRowIterFromSparkRowInfo(rowInfo, columns, rows)
  }

  def c2r(batch: ColumnarBatch): Iterator[InternalRow] = {
    getRowIterFromSparkRowInfo(
      CHNativeBlock.fromColumnarBatch(batch).blockAddress(),
      batch.numCols(),
      batch.numRows())
  }

  private def buildPartitionedBlockIterator(
      cbIter: Iterator[ColumnarBatch],
      options: IteratorOptions,
      records_written_metric: SQLMetric): CloseablePartitionedBlockIterator = {
    val iter: Iterator[Product2[Int, ColumnarBatch]] with AutoCloseable =
      new Iterator[Product2[Int, ColumnarBatch]] with AutoCloseable {
        val splitIterator = new BlockSplitIterator(
          cbIter
            .map(
              CHNativeBlock
                .fromColumnarBatch(_)
                .blockAddress()
                .asInstanceOf[java.lang.Long])
            .asJava,
          options
        )
        override def hasNext: Boolean = splitIterator.hasNext
        override def next(): Product2[Int, ColumnarBatch] = {
          val nextBatch = splitIterator.next()
          // need add rows before shuffle write, one block will convert to one row
          // Because one is always added during shuffle write
          // one line needs to be subtracted here
          records_written_metric.add(nextBatch.numRows() - 1)
          (splitIterator.nextPartitionId(), nextBatch)
        }
        override def close(): Unit = splitIterator.close()
      }
    new CloseablePartitionedBlockIterator(iter)
  }

  private def buildHashPartitioning(
      partitioning: HashPartitioning,
      childOutput: Seq[Attribute],
      output: Seq[Attribute]): NativePartitioning = {
    val hashExpressions = partitioning match {
      case partitioning: HashPartitioningWrapper =>
        partitioning.getNewExpr
      case _ =>
        partitioning.expressions
    }
    val hashFields =
      hashExpressions.map(
        a =>
          BindReferences
            .bindReference(ConverterUtils.getAttrFromExpr(a).toAttribute, childOutput)
            .asInstanceOf[BoundReference]
            .ordinal)
    val outputFields = if (output != null) {
      output.map {
        a: Attribute =>
          BindReferences
            .bindReference(a, childOutput)
            .asInstanceOf[BoundReference]
            .ordinal
      }
    } else {
      Seq[Int]()
    }

    new NativePartitioning(
      GlutenShuffleUtils.HashPartitioningShortName,
      partitioning.numPartitions,
      Array.empty[Byte],
      hashFields.mkString(",").getBytes(),
      outputFields.mkString(",").getBytes()
    )
  }

  private def buildPartitioningOptions(nativePartitioning: NativePartitioning): IteratorOptions = {
    val options = new IteratorOptions
    options.setBufferSize(GlutenConfig.getConf.maxBatchSize)
    options.setName(nativePartitioning.getShortName)
    options.setPartitionNum(nativePartitioning.getNumPartitions)
    options.setExpr(new String(nativePartitioning.getExprList))
    options.setHashAlgorithm(CHBackendSettings.shuffleHashAlgorithm)
    options.setRequiredFields(if (nativePartitioning.getRequiredFields != null) {
      new String(nativePartitioning.getRequiredFields)
    } else {
      new String("")
    })
    options
  }

  // scalastyle:off argcount
  def genShuffleDependency(
      rdd: RDD[ColumnarBatch],
      childOutputAttributes: Seq[Attribute],
      projectOutputAttributes: Seq[Attribute],
      newPartitioning: Partitioning,
      serializer: Serializer,
      writeMetrics: Map[String, SQLMetric],
      metrics: Map[String, SQLMetric]): ShuffleDependency[Int, ColumnarBatch, ColumnarBatch] = {
    // scalastyle:on argcount
    lazy val requiredFields = if (projectOutputAttributes != null) {
      val outputFields = projectOutputAttributes.map {
        a: Attribute =>
          BindReferences
            .bindReference(a, childOutputAttributes)
            .asInstanceOf[BoundReference]
            .ordinal
      }
      outputFields.mkString(",").getBytes()
    } else {
      Array.empty[Byte]
    }
    val nativePartitioning: NativePartitioning = newPartitioning match {
      case SinglePartition =>
        new NativePartitioning(
          GlutenShuffleUtils.SinglePartitioningShortName,
          1,
          Array.empty[Byte],
          Array.empty[Byte],
          requiredFields)
      case RoundRobinPartitioning(n) =>
        new NativePartitioning(
          GlutenShuffleUtils.RoundRobinPartitioningShortName,
          n,
          Array.empty[Byte],
          Array.empty[Byte],
          requiredFields)
      case HashPartitioning(_, _) =>
        buildHashPartitioning(
          newPartitioning.asInstanceOf[HashPartitioning],
          childOutputAttributes,
          projectOutputAttributes)
      case RangePartitioning(sortingExpressions, numPartitions) =>
        val rddForSampling = buildRangePartitionSampleRDD(
          rdd,
          RangePartitioning(sortingExpressions, numPartitions),
          childOutputAttributes)

        // we let spark compute the range bounds here, and then pass to CH.
        val orderingAttributes = sortingExpressions.zipWithIndex.map {
          case (ord, i) =>
            ord.copy(child = BoundReference(i, ord.dataType, ord.nullable))
        }
        implicit val ordering: LazilyGeneratedOrdering =
          new LazilyGeneratedOrdering(orderingAttributes)
        val generator = new RangePartitionerBoundsGenerator(
          numPartitions,
          rddForSampling,
          sortingExpressions,
          childOutputAttributes)
        val rangeBoundsInfo = generator.getRangeBoundsJsonString
        val attributePos = if (projectOutputAttributes != null) {
          projectOutputAttributes.map(
            attr =>
              BindReferences
                .bindReference(attr, childOutputAttributes)
                .asInstanceOf[BoundReference]
                .ordinal)
        } else {
          Seq[Int]()
        }
        new NativePartitioning(
          GlutenShuffleUtils.RangePartitioningShortName,
          rangeBoundsInfo.boundsSize + 1,
          Array.empty[Byte],
          rangeBoundsInfo.json.getBytes,
          attributePos.mkString(",").getBytes
        )
      case p =>
        throw new IllegalStateException(s"Unknow partition type: ${p.getClass.toString}")
    }

    val isRoundRobin = newPartitioning.isInstanceOf[RoundRobinPartitioning] &&
      newPartitioning.numPartitions > 1

    // RDD passed to ShuffleDependency should be the form of key-value pairs.
    // ColumnarShuffleWriter will compute ids from ColumnarBatch on
    // native side other than read the "key" part.
    // Thus in Columnar Shuffle we never use the "key" part.
    val isOrderSensitive = isRoundRobin && !SQLConf.get.sortBeforeRepartition

    val rddWithPartitionKey: RDD[Product2[Int, ColumnarBatch]] =
      if (
        GlutenConfig.getConf.isUseColumnarShuffleManager
        || GlutenConfig.getConf.isUseCelebornShuffleManager
      ) {
        newPartitioning match {
          case _ =>
            rdd.mapPartitionsWithIndexInternal(
              (_, cbIter) => cbIter.map(cb => (0, cb)),
              isOrderSensitive = isOrderSensitive)
        }
      } else {
        val options = buildPartitioningOptions(nativePartitioning)
        rdd.mapPartitionsWithIndexInternal(
          (_, cbIter) => {
            buildPartitionedBlockIterator(
              cbIter,
              options,
              writeMetrics(SQLShuffleWriteMetricsReporter.SHUFFLE_RECORDS_WRITTEN))
          },
          isOrderSensitive = isOrderSensitive
        )
      }

    val dependency =
      new ColumnarShuffleDependency[Int, ColumnarBatch, ColumnarBatch](
        rddWithPartitionKey,
        new PartitionIdPassthrough(nativePartitioning.getNumPartitions),
        serializer,
        shuffleWriterProcessor = ShuffleExchangeExec.createShuffleWriteProcessor(writeMetrics),
        nativePartitioning = nativePartitioning,
        metrics = metrics
      )

    dependency
  }
}

// Copy from the Vanilla Spark
private class PartitionIdPassthrough(override val numPartitions: Int) extends Partitioner {
  override def getPartition(key: Any): Int = key.asInstanceOf[Int]
}
