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
import org.apache.gluten.exception.GlutenNotSupportException
import org.apache.gluten.vectorized.NativeExpressionEvaluator

import org.apache.spark.internal.Logging
import org.apache.spark.internal.io.FileCommitProtocol
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, Projection, UnsafeProjection}
import org.apache.spark.sql.catalyst.expressions.aggregate.{AggregateExpression, DeclarativeAggregate}
import org.apache.spark.sql.delta.files.{FileDelayedCommitProtocol, MergeTreeDelayedCommitProtocol2}
import org.apache.spark.sql.delta.stats.{DeltaFileStatistics, DeltaJobStatisticsTracker}
import org.apache.spark.sql.execution.datasources.{ExecutedWriteSummary, WriteJobDescription, WriteTaskResult}
import org.apache.spark.util.Utils

import org.apache.hadoop.fs.Path

import scala.collection.mutable.ArrayBuffer

case class DeltaFileCommitInfo(committer: FileDelayedCommitProtocol)
  extends (NativeFileWriteResult => Unit) {
  val addedFiles: ArrayBuffer[(Map[String, String], String)] =
    new ArrayBuffer[(Map[String, String], String)]
  override def apply(stat: NativeFileWriteResult): Unit = {
    if (CHColumnarWrite.validatedPartitionID(stat.partition_id)) {
      val partitionValues = committer.parsePartitions(stat.partition_id)
      addedFiles.append((partitionValues, new Path(stat.relativePath).toUri.toString))
    } else {
      addedFiles.append((Map.empty[String, String], stat.filename))
    }
  }

  def result: Seq[(Map[String, String], String)] = addedFiles.toSeq
}

case class NativeDeltaStats(projection: Projection) extends (InternalRow => Unit) {
  protected val results = new collection.mutable.HashMap[String, String]

  override def apply(row: InternalRow): Unit = {
    val filename = row.getString(0)
    val jsonStats = projection(row).getString(0)
    assert(!results.contains(filename), s"Duplicate filename: $filename")
    results.put(filename, jsonStats)
  }

  def result: DeltaFileStatistics = DeltaFileStatistics(results.toMap)
}

trait SupportNativeDeltaStats[T <: FileCommitProtocol] extends CHColumnarWrite[T] {

  private lazy val deltaWriteJobStatsTracker: Option[DeltaJobStatisticsTracker] =
    description.statsTrackers
      .find(_.isInstanceOf[DeltaJobStatisticsTracker])
      .map(_.asInstanceOf[DeltaJobStatisticsTracker])

  lazy val nativeDeltaStats: Option[NativeDeltaStats] = {
    deltaWriteJobStatsTracker
      .map(
        delta => {
          val r = delta.statsColExpr.transform {
            case ae: AggregateExpression
                if ae.aggregateFunction.isInstanceOf[DeclarativeAggregate] =>
              ae.aggregateFunction.asInstanceOf[DeclarativeAggregate].evaluateExpression
          }
          val vanillaSchema =
            delta.statsColExpr
              .collect {
                case ae: AggregateExpression
                    if ae.aggregateFunction.isInstanceOf[DeclarativeAggregate] =>
                  ae.aggregateFunction.asInstanceOf[DeclarativeAggregate]
              }
              .asInstanceOf[Seq[DeclarativeAggregate]]
              .flatMap(_.aggBufferAttributes)
          NativeDeltaStats(
            UnsafeProjection.create(
              exprs = Seq(r),
              inputSchema = nativeStatsSchema(vanillaSchema)
            ))
        })
  }

  def nativeStatsSchema(vanilla: Seq[AttributeReference]): Seq[AttributeReference]
}

case class FileDeltaColumnarWrite(
    override val jobTrackerID: String,
    override val description: WriteJobDescription,
    override val committer: FileDelayedCommitProtocol)
  extends SupportNativeDeltaStats[FileDelayedCommitProtocol]
  with Logging {

  override def nativeStatsSchema(vanilla: Seq[AttributeReference]): Seq[AttributeReference] =
    NativeFileWriteResult.nativeStatsSchema(vanilla)

  override def doSetupNativeTask(): Unit = {
    assert(description.path == committer.outputPath)
    val nameSpec = CreateFileNameSpec(taskAttemptContext, description)
    val writePath = description.path
    val writeFileName = committer.getFileName(taskAttemptContext, nameSpec.suffix, Map.empty)

    /**
     * CDC files (CDC_PARTITION_COL = true) are named with "cdc-..." instead of "part-...".So, using
     * pattern match to replace guid to {}.See the following example:
     * {{{
     *   part-00000-7d672b28-c079-4b00-bb0a-196c15112918-c000.snappy.parquet
     *     =>
     *   part-00000-{id}.snappy.parquet
     * }}}
     */
    val guidPattern =
      """.*-([0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12})(?:-c(\d+)\..*)?$""".r
    val fileNamePattern =
      guidPattern.replaceAllIn(
        writeFileName,
        m => writeFileName.replace(m.group(1), FileNamePlaceHolder.ID))

    logDebug(s"Native staging write path: $writePath and with pattern: $fileNamePattern")
    val settings =
      Map(
        RuntimeSettings.TASK_WRITE_TMP_DIR.key -> writePath,
        RuntimeSettings.TASK_WRITE_FILENAME_PATTERN.key -> fileNamePattern
      )
    NativeExpressionEvaluator.updateQueryRuntimeSettings(settings)
  }

  private def doCollectNativeResult(stats: Seq[InternalRow])
      : Option[(Seq[(Map[String, String], String)], ExecutedWriteSummary)] = {

    // Write an empty iterator
    if (stats.isEmpty) {
      None
    } else {
      // stats.map(row => x.apply(row).getString(0)).foreach(println)
      // process stats
      val commitInfo = DeltaFileCommitInfo(committer)
      val basicNativeStat =
        NativeBasicWriteTaskStatsTracker(description.path, basicWriteJobStatsTracker)
      val basicNativeStats = Seq(commitInfo, basicNativeStat)
      NativeStatCompute(stats)(basicNativeStats, nativeDeltaStats)

      Some(
        (
          commitInfo.result,
          ExecutedWriteSummary(
            updatedPartitions = Set.empty,
            stats = nativeDeltaStats.map(_.result).toSeq ++ Seq(basicNativeStat.result))))
    }
  }

  override def commitTask(writeResults: Seq[InternalRow]): Option[WriteTaskResult] = {
    doCollectNativeResult(writeResults).map {
      case (addedFiles, summary) =>
        require(addedFiles.nonEmpty, "No files to commit")

        committer.updateAddedFiles(addedFiles)

        val (taskCommitMessage, taskCommitTime) = Utils.timeTakenMs {
          committer.commitTask(taskAttemptContext)
        }

        // Just for update task commit time
        description.statsTrackers.foreach {
          stats => stats.newTaskInstance().getFinalStats(taskCommitTime)
        }
        WriteTaskResult(taskCommitMessage, summary)
    }
  }
}

object CHDeltaColumnarWrite {
  def apply(
      jobTrackerID: String,
      description: WriteJobDescription,
      committer: FileCommitProtocol): CHColumnarWrite[FileCommitProtocol] = committer match {
    case c: FileDelayedCommitProtocol =>
      FileDeltaColumnarWrite(jobTrackerID, description, c)
        .asInstanceOf[CHColumnarWrite[FileCommitProtocol]]
    case m: MergeTreeDelayedCommitProtocol2 =>
      MergeTreeDeltaColumnarWrite(jobTrackerID, description, m)
        .asInstanceOf[CHColumnarWrite[FileCommitProtocol]]
    case _ =>
      throw new GlutenNotSupportException(
        s"Unsupported committer type: ${committer.getClass.getSimpleName}")
  }
}
