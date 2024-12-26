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
import org.apache.spark.internal.Logging
import org.apache.spark.internal.config.{SHUFFLE_COMPRESS, SHUFFLE_SORT_INIT_BUFFER_SIZE, SHUFFLE_SORT_USE_RADIXSORT}
import org.apache.spark.memory.SparkMemoryUtil
import org.apache.spark.scheduler.MapStatus
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.util.{SparkDirectoryUtil, SparkResourceUtil, Utils}

import java.io.IOException

class ColumnarShuffleWriter[K, V](
    shuffleBlockResolver: IndexShuffleBlockResolver,
    handle: BaseShuffleHandle[K, V, V],
    mapId: Long,
    writeMetrics: ShuffleWriteMetricsReporter,
    isSort: Boolean)
  extends ShuffleWriter[K, V]
  with Logging {

  private val dep = handle.dependency.asInstanceOf[ColumnarShuffleDependency[K, V, V]]

  private val conf = SparkEnv.get.conf

  private val blockManager = SparkEnv.get.blockManager

  // Are we in the process of stopping? Because map tasks can call stop() with success = true
  // and then call stop() with success = false if they get an exception, we want to make sure
  // we don't try deleting files, etc twice.
  private var stopping = false

  private var mapStatus: MapStatus = _

  private val localDirs = SparkDirectoryUtil
    .get()
    .namespace("shuffle-write")
    .all
    .map(_.getAbsolutePath)
    .mkString(",")

  private lazy val nativeBufferSize = {
    val bufferSize = GlutenConfig.getConf.shuffleWriterBufferSize
    val maxBatchSize = GlutenConfig.getConf.maxBatchSize
    if (bufferSize > maxBatchSize) {
      logInfo(
        s"${GlutenConfig.SHUFFLE_WRITER_BUFFER_SIZE.key} ($bufferSize) exceeds max " +
          s" batch size. Limited to ${GlutenConfig.COLUMNAR_MAX_BATCH_SIZE.key} ($maxBatchSize).")
      maxBatchSize
    } else {
      bufferSize
    }
  }

  private val nativeMergeBufferSize = GlutenConfig.getConf.maxBatchSize

  private val nativeMergeThreshold = GlutenConfig.getConf.columnarShuffleMergeThreshold

  private val compressionCodec =
    if (conf.getBoolean(SHUFFLE_COMPRESS.key, SHUFFLE_COMPRESS.defaultValue.get)) {
      GlutenShuffleUtils.getCompressionCodec(conf)
    } else {
      null // uncompressed
    }

  private val compressionCodecBackend =
    GlutenConfig.getConf.columnarShuffleCodecBackend.orNull

  private val compressionLevel =
    GlutenShuffleUtils.getCompressionLevel(conf, compressionCodec, compressionCodecBackend)

  private val sortEvictBufferSize =
    GlutenShuffleUtils.getSortEvictBufferSize(conf, compressionCodec)

  private val bufferCompressThreshold =
    GlutenConfig.getConf.columnarShuffleCompressionThreshold

  private val reallocThreshold = GlutenConfig.getConf.columnarShuffleReallocThreshold

  private val runtime = Runtimes.contextInstance(BackendsApiManager.getBackendName, "ShuffleWriter")

  private val jniWrapper = ShuffleWriterJniWrapper.create(runtime)

  private var nativeShuffleWriter: Long = -1L

  private var splitResult: GlutenSplitResult = _

  private var partitionLengths: Array[Long] = _

  private val taskContext: TaskContext = TaskContext.get()

  private val shuffleWriterType: String =
    if (isSort) GlutenConfig.GLUTEN_SORT_SHUFFLE_WRITER else GlutenConfig.GLUTEN_HASH_SHUFFLE_WRITER

  private def availableOffHeapPerTask(): Long = {
    val perTask =
      SparkMemoryUtil.getCurrentAvailableOffHeapMemory / SparkResourceUtil.getTaskSlots(conf)
    perTask
  }

  @throws[IOException]
  def internalWrite(records: Iterator[Product2[K, V]]): Unit = {
    if (!records.hasNext) {
      handleEmptyInput()
      return
    }

    val dataTmp = Utils.tempFileWith(shuffleBlockResolver.getDataFile(dep.shuffleId, mapId))

    while (records.hasNext) {
      val cb = records.next()._2.asInstanceOf[ColumnarBatch]
      if (cb.numRows == 0 || cb.numCols == 0) {
        logInfo(s"Skip ColumnarBatch of ${cb.numRows} rows, ${cb.numCols} cols")
      } else {
        val rows = cb.numRows()
        val handle = ColumnarBatches.getNativeHandle(BackendsApiManager.getBackendName, cb)
        if (nativeShuffleWriter == -1L) {
          nativeShuffleWriter = jniWrapper.make(
            dep.nativePartitioning.getShortName,
            dep.nativePartitioning.getNumPartitions,
            nativeBufferSize,
            nativeMergeBufferSize,
            nativeMergeThreshold,
            compressionCodec,
            compressionCodecBackend,
            compressionLevel,
            sortEvictBufferSize,
            bufferCompressThreshold,
            GlutenConfig.getConf.columnarShuffleCompressionMode,
            conf.get(SHUFFLE_SORT_INIT_BUFFER_SIZE).toInt,
            conf.get(SHUFFLE_SORT_USE_RADIXSORT),
            dataTmp.getAbsolutePath,
            blockManager.subDirsPerLocalDir,
            localDirs,
            reallocThreshold,
            handle,
            taskContext.taskAttemptId(),
            GlutenShuffleUtils.getStartPartitionId(dep.nativePartitioning, taskContext.partitionId),
            shuffleWriterType
          )
          runtime
            .memoryManager()
            .addSpiller(new Spiller() {
              override def spill(self: MemoryTarget, phase: Spiller.Phase, size: Long): Long = {
                if (!Spillers.PHASE_SET_SPILL_ONLY.contains(phase)) {
                  return 0L
                }
                logInfo(s"Gluten shuffle writer: Trying to spill $size bytes of data")
                // fixme pass true when being called by self
                val spilled =
                  jniWrapper.nativeEvict(nativeShuffleWriter, size, false)
                logInfo(s"Gluten shuffle writer: Spilled $spilled / $size bytes of data")
                spilled
              }
            })
        }
        val startTime = System.nanoTime()
        jniWrapper.write(nativeShuffleWriter, rows, handle, availableOffHeapPerTask())
        dep.metrics("shuffleWallTime").add(System.nanoTime() - startTime)
        dep.metrics("numInputRows").add(rows)
        dep.metrics("inputBatches").add(1)
        // This metric is important, AQE use it to decide if EliminateLimit
        writeMetrics.incRecordsWritten(rows)
      }
      cb.close()
    }

    if (nativeShuffleWriter == -1L) {
      handleEmptyInput()
      return
    }

    val startTime = System.nanoTime()
    assert(nativeShuffleWriter != -1L)
    splitResult = jniWrapper.stop(nativeShuffleWriter)
    closeShuffleWriter()
    dep.metrics("shuffleWallTime").add(System.nanoTime() - startTime)
    if (!isSort) {
      dep
        .metrics("splitTime")
        .add(
          dep.metrics("shuffleWallTime").value - splitResult.getTotalSpillTime -
            splitResult.getTotalWriteTime -
            splitResult.getTotalCompressTime)
    } else {
      dep.metrics("sortTime").add(splitResult.getSortTime)
      dep.metrics("c2rTime").add(splitResult.getC2RTime)
    }
    dep.metrics("spillTime").add(splitResult.getTotalSpillTime)
    dep.metrics("bytesSpilled").add(splitResult.getTotalBytesSpilled)
    dep.metrics("dataSize").add(splitResult.getRawPartitionLengths.sum)
    dep.metrics("compressTime").add(splitResult.getTotalCompressTime)
    dep.metrics("peakBytes").add(splitResult.getPeakBytes)
    writeMetrics.incBytesWritten(splitResult.getTotalBytesWritten)
    writeMetrics.incWriteTime(splitResult.getTotalWriteTime + splitResult.getTotalSpillTime)
    taskContext.taskMetrics().incMemoryBytesSpilled(splitResult.getBytesToEvict)
    taskContext.taskMetrics().incDiskBytesSpilled(splitResult.getTotalBytesSpilled)

    partitionLengths = splitResult.getPartitionLengths
    try {
      shuffleBlockResolver.writeMetadataFileAndCommit(
        dep.shuffleId,
        mapId,
        partitionLengths,
        Array[Long](),
        dataTmp)
    } finally {
      if (dataTmp.exists() && !dataTmp.delete()) {
        logError(s"Error while deleting temp file ${dataTmp.getAbsolutePath}")
      }
    }

    // The partitionLength is much more than vanilla spark partitionLengths,
    // almost 3 times than vanilla spark partitionLengths
    // This value is sensitive in rules such as AQE rule OptimizeSkewedJoin DynamicJoinSelection
    // May affect the final plan
    mapStatus = MapStatus(blockManager.shuffleServerId, partitionLengths, mapId)
  }

  private def handleEmptyInput(): Unit = {
    partitionLengths = new Array[Long](dep.partitioner.numPartitions)
    shuffleBlockResolver.writeMetadataFileAndCommit(
      dep.shuffleId,
      mapId,
      partitionLengths,
      Array[Long](),
      null)
    mapStatus = MapStatus(blockManager.shuffleServerId, partitionLengths, mapId)
  }

  @throws[IOException]
  override def write(records: Iterator[Product2[K, V]]): Unit = {
    internalWrite(records)
  }

  private def closeShuffleWriter(): Unit = {
    if (nativeShuffleWriter == -1L) {
      return
    }
    jniWrapper.close(nativeShuffleWriter)
    nativeShuffleWriter = -1L
  }

  override def stop(success: Boolean): Option[MapStatus] = {
    try {
      if (stopping) {
        return None
      }
      stopping = true
      if (success) {
        Option(mapStatus)
      } else {
        None
      }
    } finally {
      closeShuffleWriter()
    }
  }

  def getPartitionLengths: Array[Long] = partitionLengths

}
