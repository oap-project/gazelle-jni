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
import org.apache.gluten.sql.shims.SparkShimLoader
import org.apache.gluten.vectorized.NativePartitioning

import org.apache.spark.{SparkConf, TaskContext}
import org.apache.spark.internal.config._
import org.apache.spark.shuffle.api.ShuffleExecutorComponents
import org.apache.spark.storage.{BlockId, BlockManagerId}
import org.apache.spark.util.random.XORShiftRandom

import java.util.Locale

object GlutenShuffleUtils {
  val SinglePartitioningShortName = "single"
  val RoundRobinPartitioningShortName = "rr"
  val HashPartitioningShortName = "hash"
  val RangePartitioningShortName = "range"

  def getStartPartitionId(partition: NativePartitioning, partitionId: Int): Int = {
    partition.getShortName match {
      case RoundRobinPartitioningShortName =>
        new XORShiftRandom(partitionId).nextInt(partition.getNumPartitions)
      case _ => 0
    }
  }

  def getCompressionCodec(conf: SparkConf): String = {
    def checkCodecValues(codecConf: String, codec: String, validValues: Set[String]): Unit = {
      if (!validValues.contains(codec)) {
        throw new IllegalArgumentException(
          s"The value of $codecConf should be one of " +
            s"${validValues.mkString(", ")}, but was $codec")
      }
    }
    val glutenConfig = GlutenConfig.getConf
    glutenConfig.columnarShuffleCodec match {
      case Some(codec) =>
        val glutenCodecKey = GlutenConfig.COLUMNAR_SHUFFLE_CODEC.key
        if (glutenConfig.columnarShuffleEnableQat) {
          checkCodecValues(glutenCodecKey, codec, GlutenConfig.GLUTEN_QAT_SUPPORTED_CODEC)
        } else if (glutenConfig.columnarShuffleEnableIaa) {
          checkCodecValues(glutenCodecKey, codec, GlutenConfig.GLUTEN_IAA_SUPPORTED_CODEC)
        } else {
          checkCodecValues(
            glutenCodecKey,
            codec,
            BackendsApiManager.getSettings.shuffleSupportedCodec())
        }
        codec
      case None =>
        val sparkCodecKey = IO_COMPRESSION_CODEC.key
        val codec =
          conf
            .get(sparkCodecKey, IO_COMPRESSION_CODEC.defaultValueString)
            .toLowerCase(Locale.ROOT)
        checkCodecValues(
          sparkCodecKey,
          codec,
          BackendsApiManager.getSettings.shuffleSupportedCodec())
        codec
    }
  }

  def getCompressionLevel(conf: SparkConf, codec: String, compressionCodecBackend: String): Int = {
    if ("zstd" == codec && compressionCodecBackend == null) {
      conf.getInt(
        IO_COMPRESSION_ZSTD_LEVEL.key,
        IO_COMPRESSION_ZSTD_LEVEL.defaultValue.getOrElse(1))
    } else {
      // Follow arrow default compression level `kUseDefaultCompressionLevel`
      Int.MinValue
    }
  }

  def getSortEvictBufferSize(conf: SparkConf, codec: String): Int = {
    def checkAndGetBufferSize(entry: ConfigEntry[Long]): Int = {
      val bufferSize = conf.get(entry).toInt
      if (bufferSize < 4) {
        throw new IllegalArgumentException(s"${entry.key} must be >= 4, got $bufferSize")
      }
      bufferSize
    }
    if ("lz4" == codec) {
      checkAndGetBufferSize(IO_COMPRESSION_LZ4_BLOCKSIZE)
    } else if ("zstd" == codec) {
      checkAndGetBufferSize(IO_COMPRESSION_ZSTD_BUFFERSIZE)
    } else {
      checkAndGetBufferSize(SHUFFLE_DISK_WRITE_BUFFER_SIZE)
    }
  }

  def getReaderParam[K, C](
      handle: ShuffleHandle,
      startMapIndex: Int,
      endMapIndex: Int,
      startPartition: Int,
      endPartition: Int
  ): Tuple2[Iterator[(BlockManagerId, Seq[(BlockId, Long, Int)])], Boolean] = {
    SparkShimLoader.getSparkShims.getShuffleReaderParam(
      handle,
      startMapIndex,
      endMapIndex,
      startPartition,
      endPartition)
  }

  def getSortShuffleWriter[K, V](
      handle: ShuffleHandle,
      mapId: Long,
      context: TaskContext,
      metrics: ShuffleWriteMetricsReporter,
      shuffleExecutorComponents: ShuffleExecutorComponents
  ): ShuffleWriter[K, V] = {
    handle match {
      case other: BaseShuffleHandle[K @unchecked, V @unchecked, _] =>
        SparkSortShuffleWriterUtil.create(other, mapId, context, metrics, shuffleExecutorComponents)
    }
  }
}
