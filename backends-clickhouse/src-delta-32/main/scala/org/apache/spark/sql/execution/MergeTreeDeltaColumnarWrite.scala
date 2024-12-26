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

import org.apache.gluten.backendsapi.clickhouse.RuntimeSettings
import org.apache.gluten.vectorized.NativeExpressionEvaluator

import org.apache.spark.internal.Logging
import org.apache.spark.internal.io.FileCommitProtocol.TaskCommitMessage
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, GenericInternalRow}
import org.apache.spark.sql.delta.actions.{AddFile, FileAction}
import org.apache.spark.sql.delta.files.MergeTreeDelayedCommitProtocol2
import org.apache.spark.sql.delta.stats.DeltaStatistics
import org.apache.spark.sql.delta.util.{JsonUtils, MergeTreePartitionUtils}
import org.apache.spark.sql.execution.datasources.{BasicWriteTaskStats, ExecutedWriteSummary, WriteJobDescription, WriteTaskResult}
import org.apache.spark.sql.execution.datasources.mergetree.StorageMeta
import org.apache.spark.sql.types.{LongType, StringType}
import org.apache.spark.util.Utils

import org.apache.hadoop.fs.Path

import java.util.UUID

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.language.implicitConversions

case class MergeTreeWriteResult(
    part_name: String,
    partition_id: String,
    record_count: Long,
    marks_count: Long,
    size_in_bytes: Long) {
  def apply(
      database: String,
      table: String,
      path: Path,
      modificationTime: Long,
      hostName: Seq[String]): FileAction = {
    val (partitionValues, part_path) = if (partition_id == CHColumnarWrite.EMPTY_PARTITION_ID) {
      (Map.empty[String, String], part_name)
    } else {
      (MergeTreePartitionUtils.parsePartitions(partition_id), s"$partition_id/$part_name")
    }
    val tags = Map[String, String](
      "database" -> database,
      "table" -> table,
      "engine" -> "MergeTree",
      "path" -> path.toUri.getPath,
      "targetNode" -> hostName.map(_.trim).mkString(","),
      "partition" -> "",
      "uuid" -> "",
      "rows" -> record_count.toString,
      "bytesOnDisk" -> size_in_bytes.toString,
      "dataCompressedBytes" -> -1L.toString,
      "dataUncompressedBytes" -> -1L.toString,
      "modificationTime" -> modificationTime.toString,
      "partitionId" -> "",
      "minBlockNumber" -> -1L.toString,
      "maxBlockNumber" -> -1L.toString,
      "level" -> -1L.toString,
      "dataVersion" -> -1L.toString,
      "defaultCompressionCodec" -> "lz4",
      "bucketNum" -> "",
      "dirName" -> path.toString,
      "marks" -> marks_count.toString
    )

    val stats = Map[String, Any](
      DeltaStatistics.NUM_RECORDS -> record_count,
      DeltaStatistics.MIN -> "",
      DeltaStatistics.MAX -> "",
      DeltaStatistics.NULL_COUNT -> ""
    )
    AddFile(
      part_path,
      partitionValues,
      size_in_bytes,
      modificationTime,
      dataChange = true,
      JsonUtils.toJson(stats),
      tags)
  }
}

object MergeTreeWriteResult {
  implicit def apply(row: InternalRow): MergeTreeWriteResult = MergeTreeWriteResult(
    row.getString(0),
    row.getString(1),
    row.getLong(2),
    row.getLong(3),
    row.getLong(4))

  /**
   * {{{
   * val schema =
   *   StructType(
   *     StructField("part_name", StringType, false) ::
   *     StructField("partition_id", StringType, false) ::
   *     StructField("record_count", LongType, false) :: <= overlap with vanilla =>
   *     StructField("marks_count", LongType, false) ::
   *     StructField("size_in_bytes", LongType, false) ::
   *     min...
   *     max...
   *     null_count...)
   * }}}
   */
  private val inputBasedSchema: Seq[AttributeReference] = Seq(
    AttributeReference("part_name", StringType, nullable = false)(),
    AttributeReference("partition_id", StringType, nullable = false)(),
    AttributeReference("record_count", LongType, nullable = false)(),
    AttributeReference("marks_count", LongType, nullable = false)(),
    AttributeReference("size_in_bytes", LongType, nullable = false)()
  )

  def nativeStatsSchema(vanilla: Seq[AttributeReference]): Seq[AttributeReference] = {
    inputBasedSchema.take(2) ++ vanilla.take(1) ++ inputBasedSchema.drop(3) ++ vanilla.drop(1)
  }
}

case class MergeTreeCommitInfo(committer: MergeTreeDelayedCommitProtocol2)
  extends (MergeTreeWriteResult => Unit) {
  private val modificationTime = System.currentTimeMillis()
  private val hostName = Seq(Utils.localHostName())
  private val addedFiles: ArrayBuffer[FileAction] = new ArrayBuffer[FileAction]
  private val path = new Path(committer.outputPath)
  def apply(stat: MergeTreeWriteResult): Unit = {
    addedFiles.append(
      stat(committer.database, committer.tableName, path, modificationTime, hostName))
  }
  def result: Seq[FileAction] = addedFiles.toSeq
}

case class MergeTreeBasicWriteTaskStatsTracker() extends (MergeTreeWriteResult => Unit) {
  private val partitions: mutable.ArrayBuffer[InternalRow] = mutable.ArrayBuffer.empty
  private var numRows: Long = 0
  private var numBytes: Long = 0
  private var numFiles: Int = 0

  def apply(stat: MergeTreeWriteResult): Unit = {
    if (stat.partition_id != CHColumnarWrite.EMPTY_PARTITION_ID) {
      partitions.append(new GenericInternalRow(Array[Any](stat.partition_id)))
    }
    numFiles += 1
    numRows += stat.record_count
    numBytes += stat.size_in_bytes
  }

  def result: BasicWriteTaskStats =
    BasicWriteTaskStats(partitions.toSeq, numFiles, numBytes, numRows)
}

case class MergeTreeDeltaColumnarWrite(
    override val jobTrackerID: String,
    override val description: WriteJobDescription,
    override val committer: MergeTreeDelayedCommitProtocol2)
  extends SupportNativeDeltaStats[MergeTreeDelayedCommitProtocol2]
  with Logging {

  override def nativeStatsSchema(vanilla: Seq[AttributeReference]): Seq[AttributeReference] =
    MergeTreeWriteResult.nativeStatsSchema(vanilla)

  override def doSetupNativeTask(): Unit = {
    assert(description.path == committer.outputPath)
    val writePath = StorageMeta.normalizeRelativePath(committer.outputPath)
    val split = taskAttemptContext.getTaskAttemptID.getTaskID.getId
    val partPrefixWithoutPartitionAndBucket =
      if (description.partitionColumns.isEmpty) s"${UUID.randomUUID.toString}_$split"
      else s"_$split"
    logDebug(
      s"Pipeline write path: $writePath with part prefix: $partPrefixWithoutPartitionAndBucket")
    val settings =
      Map(
        RuntimeSettings.TASK_WRITE_TMP_DIR.key -> writePath,
        RuntimeSettings.PART_NAME_PREFIX.key -> partPrefixWithoutPartitionAndBucket)
    NativeExpressionEvaluator.updateQueryRuntimeSettings(settings)
  }

  private def doCollectNativeResult(stats: Seq[InternalRow]): Option[WriteTaskResult] = {
    if (stats.isEmpty) {
      None
    } else {
      val commitInfo = MergeTreeCommitInfo(committer)
      val mergeTreeStat = MergeTreeBasicWriteTaskStatsTracker()
      val basicNativeStats = Seq(commitInfo, mergeTreeStat)
      NativeStatCompute(stats)(basicNativeStats, nativeDeltaStats)

      Some {
        WriteTaskResult(
          new TaskCommitMessage(commitInfo.result),
          ExecutedWriteSummary(
            updatedPartitions = Set.empty,
            stats = nativeDeltaStats.map(_.result).toSeq ++ Seq(mergeTreeStat.result))
        )
      }
    }
  }

  override def commitTask(writeResults: Seq[InternalRow]): Option[WriteTaskResult] = {
    doCollectNativeResult(writeResults)
  }
}
