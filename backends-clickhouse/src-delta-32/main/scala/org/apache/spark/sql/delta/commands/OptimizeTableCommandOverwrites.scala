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
package org.apache.spark.sql.delta.commands

import org.apache.gluten.expression.ConverterUtils
import org.apache.gluten.memory.CHThreadGroup
import org.apache.spark.{TaskContext, TaskOutputFileAlreadyExistException}
import org.apache.spark.internal.Logging
import org.apache.spark.internal.io.FileCommitProtocol.TaskCommitMessage
import org.apache.spark.internal.io.SparkHadoopWriterUtils
import org.apache.spark.shuffle.FetchFailedException
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.{InternalRow, TableIdentifier}
import org.apache.spark.sql.catalyst.catalog.CatalogTableType
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.types.DataTypeUtils
import org.apache.spark.sql.delta._
import org.apache.spark.sql.delta.actions.{AddFile, FileAction}
import org.apache.spark.sql.delta.catalog.{ClickHouseTableV2, DeltaTableV2}
import org.apache.spark.sql.errors.QueryExecutionErrors
import org.apache.spark.sql.execution.datasources.{CHDatasourceJniWrapper, WriteTaskResult}
import org.apache.spark.sql.execution.datasources.v1.CHMergeTreeWriterInjects
import org.apache.spark.sql.execution.datasources.v2.clickhouse.metadata.{AddFileTags, AddMergeTreeParts}
import org.apache.spark.sql.execution.datasources.v2.clickhouse.utils.CHDataSourceUtils
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.StructType
import org.apache.spark.util.{SerializableConfiguration, SystemClock, Utils}
import org.apache.hadoop.fs.{FileAlreadyExistsException, Path}
import org.apache.hadoop.mapreduce.{TaskAttemptContext, TaskAttemptID, TaskID, TaskType}
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl

import java.util.Date
import scala.collection.mutable.ArrayBuffer

object OptimizeTableCommandOverwrites extends Logging {

  case class TaskDescription(
      path: String,
      database: String,
      tableName: String,
      snapshotId: String,
      orderByKeyOption: Option[Seq[String]],
      lowCardKeyOption: Option[Seq[String]],
      minmaxIndexKeyOption: Option[Seq[String]],
      bfIndexKeyOption: Option[Seq[String]],
      setIndexKeyOption: Option[Seq[String]],
      primaryKeyOption: Option[Seq[String]],
      partitionColumns: Seq[String],
      partList: Seq[String],
      tableSchema: StructType,
      clickhouseTableConfigs: Map[String, String],
      serializableHadoopConf: SerializableConfiguration,
      jobIdInstant: Long,
      partitionDir: Option[String],
      bucketDir: Option[String]
  )

  private def executeTask(
      description: TaskDescription,
      sparkStageId: Int,
      sparkPartitionId: Int,
      sparkAttemptNumber: Int
  ): WriteTaskResult = {
    CHThreadGroup.registerNewThreadGroup()
    val jobId = SparkHadoopWriterUtils.createJobID(new Date(description.jobIdInstant), sparkStageId)
    val taskId = new TaskID(jobId, TaskType.MAP, sparkPartitionId)
    val taskAttemptId = new TaskAttemptID(taskId, sparkAttemptNumber)

    // Set up the attempt context required to use in the output committer.
    val taskAttemptContext: TaskAttemptContext = {
      // Set up the configuration object
      val hadoopConf = description.serializableHadoopConf.value
      hadoopConf.set("mapreduce.job.id", jobId.toString)
      hadoopConf.set("mapreduce.task.id", taskAttemptId.getTaskID.toString)
      hadoopConf.set("mapreduce.task.attempt.id", taskAttemptId.toString)
      hadoopConf.setBoolean("mapreduce.task.ismap", true)
      hadoopConf.setInt("mapreduce.task.partition", 0)

      new TaskAttemptContextImpl(hadoopConf, taskAttemptId)
    }

    try {
      Utils.tryWithSafeFinallyAndFailureCallbacks(block = {

        val planWithSplitInfo = CHMergeTreeWriterInjects.genMergeTreeWriteRel(
          description.path,
          description.database,
          description.tableName,
          description.snapshotId,
          description.orderByKeyOption,
          description.lowCardKeyOption,
          description.minmaxIndexKeyOption,
          description.bfIndexKeyOption,
          description.setIndexKeyOption,
          description.primaryKeyOption,
          description.partitionColumns,
          description.partList,
          ConverterUtils.convertNamedStructJson(description.tableSchema),
          description.clickhouseTableConfigs,
          DataTypeUtils.toAttributes(description.tableSchema)
        )

        val returnedMetrics =
          CHDatasourceJniWrapper.nativeMergeMTParts(
            planWithSplitInfo.splitInfo,
            description.partitionDir.getOrElse(""),
            description.bucketDir.getOrElse("")
          )
        if (returnedMetrics != null && returnedMetrics.nonEmpty) {
          val addFiles = AddFileTags.partsMetricsToAddFile(
            description.database,
            description.tableName,
            description.path,
            returnedMetrics,
            Seq(Utils.localHostName()))

          val (taskCommitMessage, taskCommitTime) = Utils.timeTakenMs {
            // committer.commitTask(taskAttemptContext)
            new TaskCommitMessage(addFiles.toSeq)
          }

//          val summary = MergeTreeExecutedWriteSummary(
//            updatedPartitions = updatedPartitions.toSet,
//            stats = statsTrackers.map(_.getFinalStats(taskCommitTime)))
          WriteTaskResult(taskCommitMessage, null)
        } else {
          throw new IllegalStateException()
        }
      })(
        catchBlock = {
          // If there is an error, abort the task
          logError(s"Job $jobId aborted.")
        },
        finallyBlock = {})
    } catch {
      case e: FetchFailedException =>
        throw e
      case f: FileAlreadyExistsException if SQLConf.get.fastFailFileFormatOutput =>
        // If any output file to write already exists, it does not make sense to re-run this task.
        // We throw the exception and let Executor throw ExceptionFailure to abort the job.
        throw new TaskOutputFileAlreadyExistException(f)
      case t: Throwable =>
        throw QueryExecutionErrors.taskFailedWhileWritingRowsError(description.path, t)
    }

  }

  def runOptimizeBinJobClickhouse(
      txn: OptimisticTransaction,
      partitionValues: Map[String, String],
      bucketNum: String,
      bin: Seq[AddFile],
      maxFileSize: Long): Seq[FileAction] = {
    val tableV2 = ClickHouseTableV2.getTable(txn.deltaLog)

    val sparkSession = SparkSession.getActiveSession.get

    val rddWithNonEmptyPartitions =
      sparkSession.sparkContext.parallelize(Array.empty[InternalRow], 1)

    val jobIdInstant = new Date().getTime
    val ret = new Array[WriteTaskResult](rddWithNonEmptyPartitions.partitions.length)

    val serializableHadoopConf = new SerializableConfiguration(
      sparkSession.sessionState.newHadoopConfWithOptions(
        txn.metadata.configuration ++ txn.deltaLog.options))

    val partitionDir = if (tableV2.partitionColumns.isEmpty) {
      None
    } else {
      Some(tableV2.partitionColumns.map(c => c + "=" + partitionValues(c)).mkString("/"))
    }

    val bucketDir = if (tableV2.bucketOption.isEmpty) {
      None
    } else {
      Some(bucketNum)
    }

    val description = TaskDescription.apply(
      txn.deltaLog.dataPath.toString,
      tableV2.dataBaseName,
      tableV2.tableName,
      ClickhouseSnapshot.genSnapshotId(tableV2.initialSnapshot),
      tableV2.orderByKeyOption,
      tableV2.lowCardKeyOption,
      tableV2.minmaxIndexKeyOption,
      tableV2.bfIndexKeyOption,
      tableV2.setIndexKeyOption,
      tableV2.primaryKeyOption,
      tableV2.partitionColumns,
      bin.map(_.asInstanceOf[AddMergeTreeParts].name),
      tableV2.schema(),
      tableV2.clickhouseTableConfigs,
      serializableHadoopConf,
      jobIdInstant,
      partitionDir,
      bucketDir
    )
    sparkSession.sparkContext.runJob(
      rddWithNonEmptyPartitions,
      (taskContext: TaskContext, _: Iterator[InternalRow]) => {
        executeTask(
          description,
          taskContext.stageId(),
          taskContext.partitionId(),
          taskContext.taskAttemptId().toInt & Integer.MAX_VALUE
        )
      },
      rddWithNonEmptyPartitions.partitions.indices,
      (index, res: WriteTaskResult) => {
        ret(index) = res
      }
    )

    val addFiles = ret
      .flatMap(_.commitMsg.obj.asInstanceOf[Seq[AddFile]])
      .toSeq

    val removeFiles =
      bin.map(f => f.removeWithTimestamp(new SystemClock().getTimeMillis(), dataChange = false))
    addFiles ++ removeFiles

  }

  def getDeltaLogClickhouse(
      spark: SparkSession,
      path: Option[String],
      tableIdentifier: Option[TableIdentifier],
      operationName: String,
      hadoopConf: Map[String, String] = Map.empty): DeltaLog = {
    val tablePath =
      if (path.nonEmpty) {
        new Path(path.get)
      } else if (tableIdentifier.nonEmpty) {
        val sessionCatalog = spark.sessionState.catalog
        lazy val metadata = sessionCatalog.getTableMetadata(tableIdentifier.get)

        if (CHDataSourceUtils.isClickhousePath(spark, tableIdentifier.get)) {
          new Path(tableIdentifier.get.table)
        } else if (CHDataSourceUtils.isClickHouseTable(spark, tableIdentifier.get)) {
          new Path(metadata.location)
        } else {
          DeltaTableIdentifier(spark, tableIdentifier.get) match {
            case Some(id) if id.path.nonEmpty =>
              new Path(id.path.get)
            case Some(id) if id.table.nonEmpty =>
              new Path(metadata.location)
            case _ =>
              if (metadata.tableType == CatalogTableType.VIEW) {
                throw DeltaErrors.viewNotSupported(operationName)
              }
              throw DeltaErrors.notADeltaTableException(operationName)
          }
        }
      } else {
        throw DeltaErrors.missingTableIdentifierException(operationName)
      }

    val startTime = Some(System.currentTimeMillis)
    val deltaLog = DeltaLog.forTable(spark, tablePath, hadoopConf)
    if (deltaLog.update(checkIfUpdatedSinceTs = startTime).version < 0) {
      throw DeltaErrors.notADeltaTableException(
        operationName,
        DeltaTableIdentifier(path, tableIdentifier))
    }
    deltaLog
  }

  /**
   * Extracts the [[DeltaTableV2]] from a LogicalPlan iff the LogicalPlan is a [[ResolvedTable]]
   * with either a [[DeltaTableV2]] or a [[V1Table]] that is referencing a Delta table. In all
   * other cases this method will throw a "Table not found" exception.
   */
  def getDeltaTable(target: LogicalPlan, cmd: String): DeltaTableV2 = {
    // TODO: Remove this wrapper and let former callers invoke DeltaTableV2.extractFrom directly.
    ClickHouseTableV2.extractFrom(target, cmd)
  }

  def groupFilesIntoBinsClickhouse(
      partitionsToCompact: Seq[((String, Map[String, String]), Seq[AddFile])],
      maxTargetFileSize: Long): Seq[((String, Map[String, String]), Seq[AddFile])] = {
    partitionsToCompact.flatMap {
      case (partition, files) =>
        val bins = new ArrayBuffer[Seq[AddFile]]()

        val currentBin = new ArrayBuffer[AddFile]()
        var currentBinSize = 0L

        files.sortBy(_.size).foreach {
          file =>
            // Generally, a bin is a group of existing files, whose total size does not exceed the
            // desired maxFileSize. They will be coalesced into a single output file.
            // However, if isMultiDimClustering = true, all files in a partition will be read by the
            // same job, the data will be range-partitioned and
            // numFiles = totalFileSize / maxFileSize
            // will be produced. See below.

            // isMultiDimClustering is always false for Gluten Clickhouse for now
            if (file.size + currentBinSize > maxTargetFileSize /* && !isMultiDimClustering */ ) {
              bins += currentBin.toVector
              currentBin.clear()
              currentBin += file
              currentBinSize = file.size
            } else {
              currentBin += file
              currentBinSize += file.size
            }
        }

        if (currentBin.nonEmpty) {
          bins += currentBin.toVector
        }

        bins
          .map(b => (partition, b))
          // select bins that have at least two files or in case of multi-dim clustering
          // select all bins
          .filter(_._2.size > 1 /* || isMultiDimClustering */ )
    }
  }
}
