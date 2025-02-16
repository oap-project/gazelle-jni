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
package org.apache.gluten.sql

import org.apache.gluten.config.GlutenConfig
import org.apache.gluten.execution.WholeStageTransformerSuite
import org.apache.gluten.utils.BackendTestUtils

import org.apache.spark.SparkConf
import org.apache.spark.sql.Row
import org.apache.spark.sql.internal.SQLConf

class SQLQuerySuite extends WholeStageTransformerSuite {
  protected val resourcePath: String = null
  protected val fileFormat: String = null
  override protected val logLevel: String = "INFO"

  override protected def sparkConf: SparkConf = {
    val conf = super.sparkConf
      .set("spark.plugins", "org.apache.gluten.GlutenPlugin")
      .set("spark.default.parallelism", "1")
      .set("spark.memory.offHeap.enabled", "true")
      .set("spark.memory.offHeap.size", "1024MB")
      .set("spark.ui.enabled", "false")
      .set("spark.gluten.ui.enabled", "false")
    if (BackendTestUtils.isCHBackendLoaded()) {
      conf
        .set("spark.gluten.sql.enable.native.validation", "false")
    }
    conf
  }

  test("Support run with Vector reader in FileSourceScan or BatchScan") {
    withSQLConf(
      SQLConf.PARQUET_VECTORIZED_READER_ENABLED.key -> "true",
      SQLConf.CACHE_VECTORIZED_READER_ENABLED.key -> "true",
      GlutenConfig.COLUMNAR_BATCHSCAN_ENABLED.key -> "false",
      GlutenConfig.COLUMNAR_FILESCAN_ENABLED.key -> "false"
    ) {
      withTable("t1") {
        sql("""CREATE TABLE t1(name STRING, id BINARY, part BINARY)
              |USING PARQUET PARTITIONED BY (part)""".stripMargin)
        sql("INSERT INTO t1 PARTITION(part = 'Spark SQL') VALUES('a', X'537061726B2053514C')")
        checkAnswer(
          sql("SELECT name, cast(id as string), cast(part as string) FROM t1"),
          Row("a", "Spark SQL", "Spark SQL"))
      }
    }
  }
}
