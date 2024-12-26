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

import org.apache.celeborn.client.ShuffleClient
import org.apache.celeborn.common.CelebornConf
import org.apache.gluten.GlutenConfig
import org.apache.spark._
import org.apache.spark.internal.Logging
import org.apache.spark.internal.config.SHUFFLE_COMPRESS
import org.apache.spark.scheduler.MapStatus
import org.apache.spark.shuffle.celeborn.CelebornShuffleHandle
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.storage.BlockManager

import java.io.IOException
import java.util.Locale

abstract class CelebornColumnarShuffleWriter[K, V](
    shuffleId: Int,
    handle: CelebornShuffleHandle[K, V, V],
    context: TaskContext,
    celebornConf: CelebornConf,
    client: ShuffleClient,
    writeMetrics: ShuffleWriteMetricsReporter)
  extends ShuffleWriter[K, V]
  with Logging {

  protected val numMappers: Int = handle.numMappers

  protected val numPartitions: Int = handle.dependency.partitioner.numPartitions

  protected val dep: ColumnarShuffleDependency[K, V, V] =
    handle.dependency.asInstanceOf[ColumnarShuffleDependency[K, V, V]]

  protected val conf: SparkConf = SparkEnv.get.conf

  protected val mapId: Int = context.partitionId()

  protected lazy val nativeBufferSize: Int = {
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

  protected val clientPushBufferMaxSize: Int = celebornConf.clientPushBufferMaxSize

  protected val clientPushSortMemoryThreshold: Long = celebornConf.clientPushSortMemoryThreshold

  protected val shuffleWriterType: String =
    celebornConf.shuffleWriterMode.name
      .toLowerCase(Locale.ROOT)
      .replace(GlutenConfig.GLUTEN_SORT_SHUFFLE_WRITER, GlutenConfig.GLUTEN_RSS_SORT_SHUFFLE_WRITER)

  protected val celebornPartitionPusher = new CelebornPartitionPusher(
    shuffleId,
    numMappers,
    numPartitions,
    context,
    mapId,
    client,
    clientPushBufferMaxSize)

  protected val blockManager: BlockManager = SparkEnv.get.blockManager

  protected val customizedCompressionCodec: String =
    if (conf.getBoolean(SHUFFLE_COMPRESS.key, SHUFFLE_COMPRESS.defaultValue.get)) {
      GlutenShuffleUtils.getCompressionCodec(conf)
    } else {
      null // uncompressed
    }

  protected val compressionLevel: Int =
    GlutenShuffleUtils.getCompressionLevel(
      conf,
      customizedCompressionCodec,
      GlutenConfig.getConf.columnarShuffleCodecBackend.orNull)

  protected val compressionBufferSize: Int =
    GlutenShuffleUtils.getSortEvictBufferSize(conf, customizedCompressionCodec)

  protected val bufferCompressThreshold: Int =
    GlutenConfig.getConf.columnarShuffleCompressionThreshold

  // Are we in the process of stopping? Because map tasks can call stop() with success = true
  // and then call stop() with success = false if they get an exception, we want to make sure
  // we don't try deleting files, etc twice.
  protected var stopping = false
  protected var mapStatus: MapStatus = _
  protected var nativeShuffleWriter: Long = -1L

  protected var partitionLengths: Array[Long] = _

  @throws[IOException]
  final override def write(records: Iterator[Product2[K, V]]): Unit = {
    internalWrite(records)
  }

  @throws[IOException]
  def internalWrite(records: Iterator[Product2[K, V]]): Unit = {}

  final override def stop(success: Boolean): Option[MapStatus] = {
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
      if (nativeShuffleWriter != -1L) {
        closeShuffleWriter()
        nativeShuffleWriter = -1L
      }
      client.cleanup(shuffleId, mapId, context.attemptNumber)
    }
  }

  def createShuffleWriter(columnarBatch: ColumnarBatch): Unit = {}

  def closeShuffleWriter(): Unit = {}

  def getPartitionLengths: Array[Long] = partitionLengths

  def initShuffleWriter(columnarBatch: ColumnarBatch): Unit = {
    if (nativeShuffleWriter == -1L) {
      createShuffleWriter(columnarBatch)
    }
  }

  def pushMergedDataToCeleborn(): Unit = {
    val pushMergedDataTime = System.nanoTime
    client.prepareForMergeData(shuffleId, mapId, context.attemptNumber())
    client.pushMergedData(shuffleId, mapId, context.attemptNumber)
    client.mapperEnd(shuffleId, mapId, context.attemptNumber, numMappers)
    writeMetrics.incWriteTime(System.nanoTime - pushMergedDataTime)
  }

  def handleEmptyIterator(): Unit = {
    partitionLengths = new Array[Long](dep.partitioner.numPartitions)
    client.mapperEnd(shuffleId, mapId, context.attemptNumber, numMappers)
    mapStatus = MapStatus(blockManager.shuffleServerId, partitionLengths, mapId)
  }
}
