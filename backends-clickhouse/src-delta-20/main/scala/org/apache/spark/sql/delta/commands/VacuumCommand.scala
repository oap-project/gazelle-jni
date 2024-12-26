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

import org.apache.gluten.extension.GlutenSessionExtensions

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.delta._
import org.apache.spark.sql.delta.actions.{FileAction, RemoveFile}
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.delta.util.DeltaFileOperations
import org.apache.spark.sql.execution.datasources.v2.clickhouse.ClickHouseConfig
import org.apache.spark.sql.functions.{col, expr, when}
import org.apache.spark.util.{Clock, SerializableConfiguration, SystemClock}

// scalastyle:off import.ordering.noEmptyLine
import org.apache.hadoop.fs.Path

import java.util.Date
import java.util.concurrent.TimeUnit

/**
 * Vacuums the table by clearing all untracked files and folders within this table. First lists all
 * the files and directories in the table, and gets the relative paths with respect to the base of
 * the table. Then it gets the list of all tracked files for this table, which may or may not be
 * within the table base path, and gets the relative paths of all the tracked files with respect to
 * the base of the table. Files outside of the table path will be ignored. Then we take a diff of
 * the files and delete directories that were already empty, and all files that are within the table
 * that are no longer tracked.
 */
object VacuumCommand extends VacuumCommandImpl with Serializable {

  // --- modified start
  case class FileNameAndSize(path: String, length: Long, isDir: Boolean = false)
  // --- modified end

  /**
   * Additional check on retention duration to prevent people from shooting themselves in the foot.
   */
  protected def checkRetentionPeriodSafety(
      spark: SparkSession,
      retentionMs: Option[Long],
      configuredRetention: Long): Unit = {
    require(retentionMs.forall(_ >= 0), "Retention for Vacuum can't be less than 0.")
    val checkEnabled =
      spark.sessionState.conf.getConf(DeltaSQLConf.DELTA_VACUUM_RETENTION_CHECK_ENABLED)
    val retentionSafe = retentionMs.forall(_ >= configuredRetention)
    var configuredRetentionHours = TimeUnit.MILLISECONDS.toHours(configuredRetention)
    if (TimeUnit.HOURS.toMillis(configuredRetentionHours) < configuredRetention) {
      configuredRetentionHours += 1
    }
    require(
      !checkEnabled || retentionSafe,
      s"""Are you sure you would like to vacuum files with such a low retention period? If you have
         |writers that are currently writing to this table, there is a risk that you may corrupt the
         |state of your Delta table.
         |
         |If you are certain that there are no operations being performed on this table, such as
         |insert/upsert/delete/optimize, then you may turn off this check by setting:
         |spark.databricks.delta.retentionDurationCheck.enabled = false
         |
         |If you are not sure, please use a value not less than "$configuredRetentionHours hours".
       """.stripMargin
    )
  }

  /**
   * Clears all untracked files and folders within this table. First lists all the files and
   * directories in the table, and gets the relative paths with respect to the base of the table.
   * Then it gets the list of all tracked files for this table, which may or may not be within the
   * table base path, and gets the relative paths of all the tracked files with respect to the base
   * of the table. Files outside of the table path will be ignored. Then we take a diff of the files
   * and delete directories that were already empty, and all files that are within the table that
   * are no longer tracked.
   *
   * @param dryRun
   *   If set to true, no files will be deleted. Instead, we will list all files and directories
   *   that will be cleared.
   * @param retentionHours
   *   An optional parameter to override the default Delta tombstone retention period
   * @return
   *   A Dataset containing the paths of the files/folders to delete in dryRun mode. Otherwise
   *   returns the base path of the table.
   */
  def gc(
      spark: SparkSession,
      deltaLog: DeltaLog,
      dryRun: Boolean = true,
      retentionHours: Option[Double] = None,
      clock: Clock = new SystemClock): DataFrame = {
    recordDeltaOperation(deltaLog, "delta.gc") {

      val path = deltaLog.dataPath
      val deltaHadoopConf = deltaLog.newDeltaHadoopConf()
      val fs = path.getFileSystem(deltaHadoopConf)

      import spark.implicits._

      val snapshot = deltaLog.update()

      require(
        snapshot.version >= 0,
        "No state defined for this table. Is this really " +
          "a Delta table? Refusing to garbage collect.")

      // --- modified start
      val isMergeTreeFormat = ClickHouseConfig
        .isMergeTreeFormatEngine(deltaLog.snapshot.metadata.configuration)
      // --- modified end

      val retentionMillis = retentionHours.map(h => TimeUnit.HOURS.toMillis(math.round(h)))
      checkRetentionPeriodSafety(spark, retentionMillis, deltaLog.tombstoneRetentionMillis)

      val deleteBeforeTimestamp = retentionMillis
        .map(millis => clock.getTimeMillis() - millis)
        .getOrElse(deltaLog.minFileRetentionTimestamp)
      logInfo(
        s"Starting garbage collection (dryRun = $dryRun) of untracked files older than " +
          s"${new Date(deleteBeforeTimestamp).toString} in $path")
      val hadoopConf = spark.sparkContext.broadcast(new SerializableConfiguration(deltaHadoopConf))
      val basePath = fs.makeQualified(path).toString
      var isBloomFiltered = false
      val parallelDeleteEnabled =
        spark.sessionState.conf.getConf(DeltaSQLConf.DELTA_VACUUM_PARALLEL_DELETE_ENABLED)
      val parallelDeletePartitions =
        spark.sessionState.conf
          .getConf(DeltaSQLConf.DELTA_VACUUM_PARALLEL_DELETE_PARALLELISM)
          .getOrElse(spark.sessionState.conf.numShufflePartitions)
      val relativizeIgnoreError =
        spark.sessionState.conf.getConf(DeltaSQLConf.DELTA_VACUUM_RELATIVIZE_IGNORE_ERROR)

      // --- modified start
      val originalEnabledGluten =
        spark.sparkContext.getLocalProperty(GlutenSessionExtensions.GLUTEN_ENABLE_FOR_THREAD_KEY)
      // gluten can not support vacuum command
      spark.sparkContext.setLocalProperty(
        GlutenSessionExtensions.GLUTEN_ENABLE_FOR_THREAD_KEY,
        "false")
      // --- modified end

      val validFiles = snapshot.stateDS
        .mapPartitions {
          actions =>
            val reservoirBase = new Path(basePath)
            val fs = reservoirBase.getFileSystem(hadoopConf.value.value)
            actions.flatMap {
              _.unwrap match {
                case tombstone: RemoveFile if tombstone.delTimestamp < deleteBeforeTimestamp =>
                  Nil
                case fa: FileAction =>
                  val filePath = stringToPath(fa.path)
                  val validFileOpt = if (filePath.isAbsolute) {
                    val maybeRelative =
                      DeltaFileOperations.tryRelativizePath(
                        fs,
                        reservoirBase,
                        filePath,
                        relativizeIgnoreError)
                    if (maybeRelative.isAbsolute) {
                      // This file lives outside the directory of the table
                      None
                    } else {
                      Option(pathToString(maybeRelative))
                    }
                  } else {
                    Option(pathToString(filePath))
                  }
                  validFileOpt.toSeq.flatMap {
                    f =>
                      // paths are relative so provide '/' as the basePath.
                      allValidFiles(f, isBloomFiltered).flatMap {
                        file =>
                          val dirs = getAllSubdirs("/", file, fs)
                          dirs ++ Iterator(file)
                      }
                  }
                case _ => Nil
              }
            }
        }
        .toDF("path")

      val partitionColumns = snapshot.metadata.partitionSchema.fieldNames
      val parallelism = spark.sessionState.conf.parallelPartitionDiscoveryParallelism

      val allFilesAndDirs = DeltaFileOperations
        .recursiveListDirs(
          spark,
          Seq(basePath),
          hadoopConf,
          hiddenFileNameFilter = DeltaTableUtils.isHiddenDirectory(partitionColumns, _),
          fileListingParallelism = Option(parallelism)
        )
        .groupByKey(x => x.path)
        .mapGroups {
          (k, v) =>
            val duplicates = v.toSeq
            // of all the duplicates we can return the newest file.
            duplicates.maxBy(_.modificationTime)
        }

      try {
        allFilesAndDirs.cache()

        implicit val fileNameAndSizeEncoder = org.apache.spark.sql.Encoders.product[FileNameAndSize]

        val dirCounts = allFilesAndDirs.where('isDir).count() + 1 // +1 for the base path

        // The logic below is as follows:
        //   1. We take all the files and directories listed in our reservoir
        //   2. We filter all files older than our tombstone retention period and directories
        //   3. We get the subdirectories of all files so that we can find non-empty directories
        //   4. We groupBy each path, and count to get how many files are in each sub-directory
        //   5. We subtract all the valid files and tombstones in our state
        //   6. We filter all paths with a count of 1, which will correspond to files not in the
        //      state, and empty directories. We can safely delete all of these
        // --- modified start
        val diff = if (isMergeTreeFormat) {
          val diff_temp = allFilesAndDirs
            .where('modificationTime < deleteBeforeTimestamp || 'isDir)
            .mapPartitions {
              fileStatusIterator =>
                val reservoirBase = new Path(basePath)
                val fs = reservoirBase.getFileSystem(hadoopConf.value.value)
                fileStatusIterator.flatMap {
                  fileStatus =>
                    if (fileStatus.isDir) {
                      implicit val fileNameAndSizeEncoder =
                        org.apache.spark.sql.Encoders.product[FileNameAndSize]
                      Iterator.single(
                        FileNameAndSize(
                          relativize(fileStatus.getPath, fs, reservoirBase, isDir = true),
                          0,
                          true)
                      )
                    } else {
                      val dirs = getAllSubdirs(basePath, fileStatus.path, fs)
                      val dirsWithSlash = dirs.map {
                        p =>
                          FileNameAndSize(
                            relativize(new Path(p), fs, reservoirBase, isDir = true),
                            0,
                            true)
                      }
                      dirsWithSlash ++ Iterator(
                        FileNameAndSize(
                          relativize(new Path(fileStatus.path), fs, reservoirBase, isDir = false),
                          0,
                          false))
                    }
                }
            }
            .withColumn(
              "dir",
              when(col("isDir"), col("path"))
                .otherwise(expr("substring_index(path, '/',size(split(path, '/')) -1)")))
            .groupBy(col("path"), col("dir"))
            .count()

          diff_temp
            .join(validFiles, diff_temp("dir") === validFiles("path"), "leftanti")
            .where('count === 1)
            .select('path)
            .as[String]
            .map {
              relativePath =>
                assert(
                  !stringToPath(relativePath).isAbsolute,
                  "Shouldn't have any absolute paths for deletion here.")
                pathToString(DeltaFileOperations.absolutePath(basePath, relativePath))
            }
        } else {
          allFilesAndDirs
            .where('modificationTime < deleteBeforeTimestamp || 'isDir)
            .mapPartitions {
              fileStatusIterator =>
                val reservoirBase = new Path(basePath)
                val fs = reservoirBase.getFileSystem(hadoopConf.value.value)
                fileStatusIterator.flatMap {
                  fileStatus =>
                    if (fileStatus.isDir) {
                      Iterator.single(
                        relativize(fileStatus.getPath, fs, reservoirBase, isDir = true))
                    } else {
                      val dirs = getAllSubdirs(basePath, fileStatus.path, fs)
                      val dirsWithSlash = dirs.map {
                        p => relativize(new Path(p), fs, reservoirBase, isDir = true)
                      }
                      dirsWithSlash ++ Iterator(
                        relativize(new Path(fileStatus.path), fs, reservoirBase, isDir = false))
                    }
                }
            }
            .groupBy($"value".as('path))
            .count()
            .join(validFiles, Seq("path"), "leftanti")
            .where('count === 1)
            .select('path)
            .as[String]
            .map {
              relativePath =>
                assert(
                  !stringToPath(relativePath).isAbsolute,
                  "Shouldn't have any absolute paths for deletion here.")
                pathToString(DeltaFileOperations.absolutePath(basePath, relativePath))
            }
        }
        // --- modified end

        if (dryRun) {
          val numFiles = diff.count()
          val stats = DeltaVacuumStats(
            isDryRun = true,
            specifiedRetentionMillis = retentionMillis,
            defaultRetentionMillis = deltaLog.tombstoneRetentionMillis,
            minRetainedTimestamp = deleteBeforeTimestamp,
            dirsPresentBeforeDelete = dirCounts,
            objectsDeleted = numFiles
          )

          recordDeltaEvent(deltaLog, "delta.gc.stats", data = stats)
          logConsole(
            s"Found $numFiles files and directories in a total of " +
              s"$dirCounts directories that are safe to delete.")

          return diff.map(f => stringToPath(f).toString).toDF("path")
        }
        logVacuumStart(
          spark,
          deltaLog,
          path,
          diff,
          retentionMillis,
          deltaLog.tombstoneRetentionMillis)

        val filesDeleted =
          try {
            delete(
              diff,
              spark,
              basePath,
              hadoopConf,
              parallelDeleteEnabled,
              parallelDeletePartitions)
          } catch {
            case t: Throwable =>
              logVacuumEnd(deltaLog, spark, path)
              throw t
          }
        val stats = DeltaVacuumStats(
          isDryRun = false,
          specifiedRetentionMillis = retentionMillis,
          defaultRetentionMillis = deltaLog.tombstoneRetentionMillis,
          minRetainedTimestamp = deleteBeforeTimestamp,
          dirsPresentBeforeDelete = dirCounts,
          objectsDeleted = filesDeleted
        )
        recordDeltaEvent(deltaLog, "delta.gc.stats", data = stats)
        logVacuumEnd(deltaLog, spark, path, Some(filesDeleted), Some(dirCounts))

        spark.createDataset(Seq(basePath)).toDF("path")
      } finally {
        allFilesAndDirs.unpersist()

        // --- modified start
        if (originalEnabledGluten != null) {
          spark.sparkContext.setLocalProperty(
            GlutenSessionExtensions.GLUTEN_ENABLE_FOR_THREAD_KEY,
            originalEnabledGluten)
        } else {
          spark.sparkContext.setLocalProperty(
            GlutenSessionExtensions.GLUTEN_ENABLE_FOR_THREAD_KEY,
            "true")
        }
        // --- modified end
      }
    }
  }
}
