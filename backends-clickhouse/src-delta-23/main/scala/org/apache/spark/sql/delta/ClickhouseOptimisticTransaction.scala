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
package org.apache.spark.sql.delta

import org.apache.gluten.backendsapi.clickhouse.CHConfig

import org.apache.spark.SparkException
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.delta.actions._
import org.apache.spark.sql.delta.catalog.ClickHouseTableV2
import org.apache.spark.sql.delta.constraints.{Constraint, Constraints}
import org.apache.spark.sql.delta.files.MergeTreeDelayedCommitProtocol
import org.apache.spark.sql.delta.schema.InvariantViolationException
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.execution.SQLExecution
import org.apache.spark.sql.execution.datasources.{BasicWriteJobStatsTracker, FileFormatWriter, GlutenWriterColumnarRules, WriteJobStatsTracker}
import org.apache.spark.sql.execution.datasources.v1.MergeTreeWriterInjects
import org.apache.spark.sql.execution.datasources.v2.clickhouse.ClickHouseConfig
import org.apache.spark.util.{Clock, SerializableConfiguration}

import org.apache.commons.lang3.exception.ExceptionUtils

import scala.collection.mutable.ListBuffer

class ClickhouseOptimisticTransaction(
    override val deltaLog: DeltaLog,
    override val snapshot: Snapshot)(implicit override val clock: Clock)
  extends OptimisticTransaction(deltaLog, snapshot) {

  def this(deltaLog: DeltaLog, snapshotOpt: Option[Snapshot] = None)(implicit clock: Clock) {
    this(
      deltaLog,
      snapshotOpt.getOrElse(deltaLog.update())
    )
  }

  override def writeFiles(
      inputData: Dataset[_],
      writeOptions: Option[DeltaOptions],
      additionalConstraints: Seq[Constraint]): Seq[FileAction] = {
    if (ClickHouseConfig.isMergeTreeFormatEngine(metadata.configuration)) {
      hasWritten = true

      val spark = inputData.sparkSession
      val (data, partitionSchema) = performCDCPartition(inputData)
      val outputPath = deltaLog.dataPath

      val (queryExecution, output, generatedColumnConstraints, _) =
        normalizeData(deltaLog, data)

      val tableV2 = ClickHouseTableV2.getTable(deltaLog)
      val committer =
        new MergeTreeDelayedCommitProtocol(
          outputPath.toString,
          None,
          tableV2.dataBaseName,
          tableV2.tableName)

      // val (optionalStatsTracker, _) =
      //   getOptionalStatsTrackerAndStatsCollection(output, outputPath, partitionSchema, data)
      val (optionalStatsTracker, _) = (None, None)

      val constraints =
        Constraints.getAll(metadata, spark) ++ generatedColumnConstraints ++ additionalConstraints

      SQLExecution.withNewExecutionId(queryExecution, Option("deltaTransactionalWrite")) {
        val queryPlan = queryExecution.executedPlan
        val (newQueryPlan, newOutput) =
          MergeTreeWriterInjects.insertFakeRowAdaptor(queryPlan, output)
        val outputSpec = FileFormatWriter.OutputSpec(outputPath.toString, Map.empty, newOutput)
        val partitioningColumns = getPartitioningColumns(partitionSchema, newOutput)

        val statsTrackers: ListBuffer[WriteJobStatsTracker] = ListBuffer()

        if (spark.conf.get(DeltaSQLConf.DELTA_HISTORY_METRICS_ENABLED)) {
          val basicWriteJobStatsTracker = new BasicWriteJobStatsTracker(
            new SerializableConfiguration(deltaLog.newDeltaHadoopConf()),
            BasicWriteJobStatsTracker.metrics)
          //        registerSQLMetrics(spark, basicWriteJobStatsTracker.driverSideMetrics)
          statsTrackers.append(basicWriteJobStatsTracker)
        }

        // Retain only a minimal selection of Spark writer options to avoid any potential
        // compatibility issues
        var options = writeOptions match {
          case None => Map.empty[String, String]
          case Some(writeOptions) =>
            writeOptions.options
              .filterKeys {
                key =>
                  key.equalsIgnoreCase(DeltaOptions.MAX_RECORDS_PER_FILE) ||
                  key.equalsIgnoreCase(DeltaOptions.COMPRESSION)
              }
              .map(identity)
        }

        spark.conf.getAll.foreach(
          entry => {
            if (
              CHConfig.startWithSettingsPrefix(entry._1)
              || entry._1.equalsIgnoreCase(DeltaSQLConf.DELTA_OPTIMIZE_MIN_FILE_SIZE.key)
            ) {
              options += (entry._1 -> entry._2)
            }
          })

        try {
          val format = tableV2.getFileFormat(metadata)
          GlutenWriterColumnarRules.injectSparkLocalProperty(spark, Some(format.shortName()))
          FileFormatWriter.write(
            sparkSession = spark,
            plan = newQueryPlan,
            fileFormat = format,
            // formats.
            committer = committer,
            outputSpec = outputSpec,
            // scalastyle:off deltahadoopconfiguration
            hadoopConf = spark.sessionState
              .newHadoopConfWithOptions(metadata.configuration ++ deltaLog.options),
            // scalastyle:on deltahadoopconfiguration
            partitionColumns = partitioningColumns,
            bucketSpec =
              tableV2.normalizedBucketSpec(output.map(_.name), spark.sessionState.conf.resolver),
            statsTrackers = optionalStatsTracker.toSeq ++ statsTrackers,
            options = options
          )
        } catch {
          case s: SparkException =>
            // Pull an InvariantViolationException up to the top level if it was the root cause.
            val violationException = ExceptionUtils.getRootCause(s)
            if (violationException.isInstanceOf[InvariantViolationException]) {
              throw violationException
            } else {
              throw s
            }
        } finally {
          GlutenWriterColumnarRules.injectSparkLocalProperty(spark, None)
        }
      }
      committer.addedStatuses.toSeq ++ committer.changeFiles
    } else {
      // TODO: support native delta parquet write
      // 1. insert FakeRowAdaptor
      // 2. DeltaInvariantCheckerExec transform
      // 3. DeltaTaskStatisticsTracker collect null count / min values / max values
      // 4. set the parameters 'staticPartitionWriteOnly', 'isNativeApplicable',
      //    'nativeFormat' in the LocalProperty of the sparkcontext
      super.writeFiles(inputData, writeOptions, additionalConstraints)
    }
  }
}
