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

class ClickhouseTable(
    @transient private val _df: Dataset[Row],
    @transient private val table: ClickHouseTableV2)
  extends DeltaTable(_df, table) {

  override def optimize(): DeltaOptimizeBuilder = {
    DeltaOptimizeBuilder(
      sparkSession,
      table.tableIdentifier.getOrElse(s"clickhouse.`${deltaLog.dataPath.toString}`"))
  }
}

object ClickhouseTable {

  /**
   * Create a DeltaTable for the data at the given `path`.
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
   * Create a DeltaTable for the data at the given `path` using the given SparkSession.
   *
   * @since 0.3.0
   */
  def forPath(sparkSession: SparkSession, path: String): DeltaTable = {
    val hdpPath = new Path(path)
    if (DeltaTableUtils.isDeltaTable(sparkSession, hdpPath)) {
      new ClickhouseTable(
        sparkSession.read.format("clickhouse").load(path),
        new ClickHouseTableV2(spark = sparkSession, path = hdpPath)
      )
    } else {
      throw DeltaErrors.notADeltaTableException(DeltaTableIdentifier(path = Some(path)))
    }
  }
}
