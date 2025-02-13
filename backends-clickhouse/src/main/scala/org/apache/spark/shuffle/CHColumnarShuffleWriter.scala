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

import org.apache.gluten.backendsapi.clickhouse.{CHBackendSettings, CHConfig}
import org.apache.gluten.config.GlutenConfig
import org.apache.gluten.execution.ColumnarNativeIterator
import org.apache.gluten.memory.CHThreadGroup
import org.apache.gluten.vectorized._

import org.apache.spark.SparkEnv
import org.apache.spark.internal.Logging
import org.apache.spark.scheduler.MapStatus
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.util.{SparkDirectoryUtil, Utils}

import java.io.IOException
import java.util.{Locale, UUID}

class CHColumnarShuffleWriter[K, V](
    shuffleBlockResolver: IndexShuffleBlockResolver,
    handle: BaseShuffleHandle[K, V, V],
    mapId: Long,
    writeMetrics: ShuffleWriteMetricsReporter)
  extends ShuffleWriter[K, V]
  with Logging {

  private val dep = handle.dependency.asInstanceOf[ColumnarShuffleDependency[K, V, V]]

  private val conf = SparkEnv.get.conf

  private val blockManager = SparkEnv.get.blockManager
  private val localDirs = SparkDirectoryUtil
    .get()
    .namespace("ch-shuffle-write")
    .mkChildDirs(UUID.randomUUID().toString)
    .map(_.getAbsolutePath)
    .mkString(",")
  private val subDirsPerLocalDir = blockManager.diskBlockManager.subDirsPerLocalDir
  private val splitSize = GlutenConfig.get.maxBatchSize
  private val compressionCodec = GlutenShuffleUtils.getCompressionCodec(conf)
  private val capitalizedCompressionCodec = compressionCodec.toUpperCase(Locale.ROOT)
  private val compressionLevel =
    GlutenShuffleUtils.getCompressionLevel(
      conf,
      compressionCodec,
      GlutenConfig.get.columnarShuffleCodecBackend.orNull)
  private val maxSortBufferSize = CHConfig.get.chColumnarMaxSortBufferSize
  private val forceMemorySortShuffle = CHConfig.get.chColumnarForceMemorySortShuffle
  private val spillThreshold = CHConfig.get.chColumnarShuffleSpillThreshold
  private val jniWrapper = new CHShuffleSplitterJniWrapper
  // Are we in the process of stopping? Because map tasks can call stop() with success = true
  // and then call stop() with success = false if they get an exception, we want to make sure
  // we don't try deleting files, etc twice.
  private var stopping = false
  private var mapStatus: MapStatus = _
  private var nativeSplitter: Long = 0

  private var splitResult: CHSplitResult = _

  private var partitionLengths: Array[Long] = _

  private var rawPartitionLengths: Array[Long] = _

  @throws[IOException]
  override def write(records: Iterator[Product2[K, V]]): Unit = {
    CHThreadGroup.registerNewThreadGroup()
    internalCHWrite(records)
  }

  private def internalCHWrite(records: Iterator[Product2[K, V]]): Unit = {
    val splitterJniWrapper: CHShuffleSplitterJniWrapper = jniWrapper

    val dataTmp = Utils.tempFileWith(shuffleBlockResolver.getDataFile(dep.shuffleId, mapId))
    // for fallback
    val iter = new ColumnarNativeIterator(new java.util.Iterator[ColumnarBatch] {
      override def hasNext: Boolean = {
        val has_value = records.hasNext
        has_value
      }

      override def next(): ColumnarBatch = {
        val batch = records.next()._2.asInstanceOf[ColumnarBatch]
        batch
      }
    })
    if (nativeSplitter == 0) {
      nativeSplitter = splitterJniWrapper.make(
        iter,
        dep.nativePartitioning,
        dep.shuffleId,
        mapId,
        splitSize,
        capitalizedCompressionCodec,
        compressionLevel,
        dataTmp.getAbsolutePath,
        localDirs,
        subDirsPerLocalDir,
        spillThreshold,
        CHBackendSettings.shuffleHashAlgorithm,
        maxSortBufferSize,
        forceMemorySortShuffle
      )
    }
    splitResult = splitterJniWrapper.stop(nativeSplitter)
    if (splitResult.getTotalRows > 0) {
      dep.metrics("numInputRows").add(splitResult.getTotalRows)
      dep.metrics("inputBatches").add(splitResult.getTotalBatches)
      dep.metrics("splitTime").add(splitResult.getSplitTime)
      dep.metrics("IOTime").add(splitResult.getDiskWriteTime)
      dep.metrics("serializeTime").add(splitResult.getSerializationTime)
      dep.metrics("spillTime").add(splitResult.getTotalSpillTime)
      dep.metrics("compressTime").add(splitResult.getTotalCompressTime)
      dep.metrics("computePidTime").add(splitResult.getTotalComputePidTime)
      dep.metrics("bytesSpilled").add(splitResult.getTotalBytesSpilled)
      dep.metrics("dataSize").add(splitResult.getTotalBytesWritten)
      dep.metrics("shuffleWallTime").add(splitResult.getWallTime)
      writeMetrics.incRecordsWritten(splitResult.getTotalRows)
      writeMetrics.incBytesWritten(splitResult.getTotalBytesWritten)
      writeMetrics.incWriteTime(splitResult.getTotalWriteTime + splitResult.getTotalSpillTime)
      partitionLengths = splitResult.getPartitionLengths
      rawPartitionLengths = splitResult.getRawPartitionLengths
      CHColumnarShuffleWriter.setOutputMetrics(splitResult)
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
      mapStatus = MapStatus(blockManager.shuffleServerId, partitionLengths, mapId)
    } else {
      partitionLengths = new Array[Long](dep.partitioner.numPartitions)
      shuffleBlockResolver.writeMetadataFileAndCommit(
        dep.shuffleId,
        mapId,
        partitionLengths,
        Array[Long](),
        null)
      mapStatus = MapStatus(blockManager.shuffleServerId, partitionLengths, mapId)
    }
    closeCHSplitter()
  }

  override def stop(success: Boolean): Option[MapStatus] = {
    try {
      if (stopping) {
        None
      }
      stopping = true
      if (success) {
        Option(mapStatus)
      } else {
        None
      }
    } finally {
      closeCHSplitter()
    }
  }

  private def closeCHSplitter(): Unit = {
    if (nativeSplitter != 0) {
      jniWrapper.close(nativeSplitter)
      nativeSplitter = 0
    }
  }

  // VisibleForTesting
  def getPartitionLengths(): Array[Long] = partitionLengths

}

case class OutputMetrics(totalRows: Long, totalBatches: Long)

object CHColumnarShuffleWriter {

  private var metric = new ThreadLocal[OutputMetrics]()

  // Pass the statistics of the last operator before shuffle to CollectMetricIterator.
  def setOutputMetrics(splitResult: CHSplitResult): Unit = {
    metric.set(OutputMetrics(splitResult.getTotalRows, splitResult.getTotalBatches))
  }

  def getTotalOutputRows: Long = {
    if (metric.get() == null) {
      0
    } else {
      metric.get().totalRows
    }
  }

  def getTotalOutputBatches: Long = {
    if (metric.get() == null) {
      0
    } else {
      metric.get().totalBatches
    }
  }
}
