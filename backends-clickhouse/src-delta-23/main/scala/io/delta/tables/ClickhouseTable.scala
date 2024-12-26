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
package io.delta.tables

import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.apache.spark.sql.delta.{DeltaErrors, DeltaTableIdentifier, DeltaTableUtils}
import org.apache.spark.sql.delta.catalog.ClickHouseTableV2

import org.apache.hadoop.fs.Path

import scala.collection.JavaConverters._

class ClickhouseTable(
    @transient private val _df: Dataset[Row],
    @transient private val table: ClickHouseTableV2)
  extends DeltaTable(_df, table) {

  override def optimize(): DeltaOptimizeBuilder = {
    DeltaOptimizeBuilder(
      sparkSession,
      table.tableIdentifier.getOrElse(s"clickhouse.`${deltaLog.dataPath.toString}`"),
      table.options)
  }
}

object ClickhouseTable {

  /**
   * Instantiate a [[DeltaTable]] object representing the data at the given path, If the given path
   * is invalid (i.e. either no table exists or an existing table is not a Delta table), it throws a
   * `not a Delta table` error.
   *
   * Note: This uses the active SparkSession in the current thread to read the table data. Hence,
   * this throws error if active SparkSession has not been set, that is,
   * `SparkSession.getActiveSession()` is empty.
   *
   * @since 0.3.0
   */
  def forPath(path: String): DeltaTable = {
    val sparkSession = SparkSession.getActiveSession.getOrElse {
      throw DeltaErrors.activeSparkSessionNotFound()
    }
    forPath(sparkSession, path)
  }

  /**
   * Instantiate a [[DeltaTable]] object representing the data at the given path, If the given path
   * is invalid (i.e. either no table exists or an existing table is not a Delta table), it throws a
   * `not a Delta table` error.
   *
   * @since 0.3.0
   */
  def forPath(sparkSession: SparkSession, path: String): DeltaTable = {
    forPath(sparkSession, path, Map.empty[String, String])
  }

  /**
   * Instantiate a [[DeltaTable]] object representing the data at the given path, If the given path
   * is invalid (i.e. either no table exists or an existing table is not a Delta table), it throws a
   * `not a Delta table` error.
   *
   * @param hadoopConf
   *   Hadoop configuration starting with "fs." or "dfs." will be picked up by `DeltaTable` to
   *   access the file system when executing queries. Other configurations will not be allowed.
   *
   * {{{
   *   val hadoopConf = Map(
   *     "fs.s3a.access.key" -> "<access-key>",
   *     "fs.s3a.secret.key" -> "<secret-key>"
   *   )
   *   DeltaTable.forPath(spark, "/path/to/table", hadoopConf)
   * }}}
   * @since 2.2.0
   */
  def forPath(
      sparkSession: SparkSession,
      path: String,
      hadoopConf: scala.collection.Map[String, String]): DeltaTable = {
    // We only pass hadoopConf so that we won't pass any unsafe options to Delta.
    val badOptions = hadoopConf.filterKeys {
      k => !DeltaTableUtils.validDeltaTableHadoopPrefixes.exists(k.startsWith)
    }.toMap
    if (!badOptions.isEmpty) {
      throw DeltaErrors.unsupportedDeltaTableForPathHadoopConf(badOptions)
    }
    val fileSystemOptions: Map[String, String] = hadoopConf.toMap
    val hdpPath = new Path(path)
    if (DeltaTableUtils.isDeltaTable(sparkSession, hdpPath, fileSystemOptions)) {
      new ClickhouseTable(
        sparkSession.read.format("clickhouse").options(fileSystemOptions).load(path),
        new ClickHouseTableV2(spark = sparkSession, path = hdpPath, options = fileSystemOptions)
      )
    } else {
      throw DeltaErrors.notADeltaTableException(DeltaTableIdentifier(path = Some(path)))
    }
  }

  /**
   * Java friendly API to instantiate a [[DeltaTable]] object representing the data at the given
   * path, If the given path is invalid (i.e. either no table exists or an existing table is not a
   * Delta table), it throws a `not a Delta table` error.
   *
   * @param hadoopConf
   *   Hadoop configuration starting with "fs." or "dfs." will be picked up by `DeltaTable` to
   *   access the file system when executing queries. Other configurations will be ignored.
   *
   * {{{
   *   val hadoopConf = Map(
   *     "fs.s3a.access.key" -> "<access-key>",
   *     "fs.s3a.secret.key", "<secret-key>"
   *   )
   *   DeltaTable.forPath(spark, "/path/to/table", hadoopConf)
   * }}}
   * @since 2.2.0
   */
  def forPath(
      sparkSession: SparkSession,
      path: String,
      hadoopConf: java.util.Map[String, String]): DeltaTable = {
    val fsOptions = hadoopConf.asScala.toMap
    forPath(sparkSession, path, fsOptions)
  }
}
