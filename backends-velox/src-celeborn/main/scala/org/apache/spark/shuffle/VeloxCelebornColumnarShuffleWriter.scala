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
package org.apache.spark.shuffle

import org.apache.gluten.GlutenConfig
import org.apache.gluten.backendsapi.BackendsApiManager
import org.apache.gluten.columnarbatch.ColumnarBatches
import org.apache.gluten.memory.memtarget.{MemoryTarget, Spiller, Spillers}
import org.apache.gluten.runtime.Runtimes
import org.apache.gluten.vectorized._

import org.apache.spark._
import org.apache.spark.internal.config.{SHUFFLE_SORT_INIT_BUFFER_SIZE, SHUFFLE_SORT_USE_RADIXSORT}
import org.apache.spark.memory.SparkMemoryUtil
import org.apache.spark.scheduler.MapStatus
import org.apache.spark.shuffle.celeborn.CelebornShuffleHandle
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.util.SparkResourceUtil

import org.apache.celeborn.client.ShuffleClient
import org.apache.celeborn.common.CelebornConf

import java.io.IOException

class VeloxCelebornColumnarShuffleWriter[K, V](
    shuffleId: Int,
    handle: CelebornShuffleHandle[K, V, V],
    context: TaskContext,
    celebornConf: CelebornConf,
    client: ShuffleClient,
    writeMetrics: ShuffleWriteMetricsReporter)
  extends CelebornColumnarShuffleWriter[K, V](
    shuffleId,
    handle,
    context,
    celebornConf,
    client,
    writeMetrics) {
  private val isSort = !GlutenConfig.GLUTEN_HASH_SHUFFLE_WRITER.equals(shuffleWriterType)

  private val runtime =
    Runtimes.contextInstance(BackendsApiManager.getBackendName, "CelebornShuffleWriter")

  private val jniWrapper = ShuffleWriterJniWrapper.create(runtime)

  private var splitResult: GlutenSplitResult = _

  private def availableOffHeapPerTask(): Long = {
    val perTask =
      SparkMemoryUtil.getCurrentAvailableOffHeapMemory / SparkResourceUtil.getTaskSlots(conf)
    perTask
  }

  @throws[IOException]
  override def internalWrite(records: Iterator[Product2[K, V]]): Unit = {
    if (!records.hasNext) {
      handleEmptyIterator()
      return
    }
    while (records.hasNext) {
      val cb = records.next()._2.asInstanceOf[ColumnarBatch]
      if (cb.numRows == 0 || cb.numCols == 0) {
        logInfo(s"Skip ColumnarBatch of ${cb.numRows} rows, ${cb.numCols} cols")
      } else {
        initShuffleWriter(cb)
        val handle = ColumnarBatches.getNativeHandle(BackendsApiManager.getBackendName, cb)
        val startTime = System.nanoTime()
        jniWrapper.write(nativeShuffleWriter, cb.numRows, handle, availableOffHeapPerTask())
        dep.metrics("shuffleWallTime").add(System.nanoTime() - startTime)
        dep.metrics("numInputRows").add(cb.numRows)
        dep.metrics("inputBatches").add(1)
        // This metric is important, AQE use it to decide if EliminateLimit
        writeMetrics.incRecordsWritten(cb.numRows())
      }
    }

    // If all of the ColumnarBatch have empty rows, the nativeShuffleWriter still equals -1
    if (nativeShuffleWriter == -1L) {
      handleEmptyIterator()
      return
    }

    val startTime = System.nanoTime()
    splitResult = jniWrapper.stop(nativeShuffleWriter)

    dep.metrics("shuffleWallTime").add(System.nanoTime() - startTime)
    val nativeMetrics = if (isSort) {
      dep.metrics("sortTime")
    } else {
      dep.metrics("splitTime")
    }
    nativeMetrics
      .add(
        dep.metrics("shuffleWallTime").value - splitResult.getTotalPushTime -
          splitResult.getTotalWriteTime -
          splitResult.getTotalCompressTime)
    dep.metrics("dataSize").add(splitResult.getRawPartitionLengths.sum)
    writeMetrics.incBytesWritten(splitResult.getTotalBytesWritten)
    writeMetrics.incWriteTime(splitResult.getTotalWriteTime + splitResult.getTotalPushTime)

    partitionLengths = splitResult.getPartitionLengths

    pushMergedDataToCeleborn()
    mapStatus = MapStatus(blockManager.shuffleServerId, partitionLengths, mapId)
  }

  override def createShuffleWriter(columnarBatch: ColumnarBatch): Unit = {
    nativeShuffleWriter = jniWrapper.makeForRSS(
      dep.nativePartitioning.getShortName,
      dep.nativePartitioning.getNumPartitions,
      nativeBufferSize,
      customizedCompressionCodec,
      compressionLevel,
      compressionBufferSize,
      bufferCompressThreshold,
      GlutenConfig.getConf.columnarShuffleCompressionMode,
      conf.get(SHUFFLE_SORT_INIT_BUFFER_SIZE).toInt,
      conf.get(SHUFFLE_SORT_USE_RADIXSORT),
      clientPushBufferMaxSize,
      clientPushSortMemoryThreshold,
      celebornPartitionPusher,
      ColumnarBatches.getNativeHandle(BackendsApiManager.getBackendName, columnarBatch),
      context.taskAttemptId(),
      GlutenShuffleUtils.getStartPartitionId(dep.nativePartitioning, context.partitionId),
      "celeborn",
      shuffleWriterType,
      GlutenConfig.getConf.columnarShuffleReallocThreshold
    )
    runtime
      .memoryManager()
      .addSpiller(new Spiller() {
        override def spill(self: MemoryTarget, phase: Spiller.Phase, size: Long): Long = {
          if (!Spillers.PHASE_SET_SPILL_ONLY.contains(phase)) {
            return 0L
          }
          logInfo(s"Gluten shuffle writer: Trying to push $size bytes of data")
          // fixme pass true when being called by self
          val pushed = jniWrapper.nativeEvict(nativeShuffleWriter, size, false)
          logInfo(s"Gluten shuffle writer: Pushed $pushed / $size bytes of data")
          pushed
        }
      })
  }

  override def closeShuffleWriter(): Unit = {
    jniWrapper.close(nativeShuffleWriter)
  }
}
