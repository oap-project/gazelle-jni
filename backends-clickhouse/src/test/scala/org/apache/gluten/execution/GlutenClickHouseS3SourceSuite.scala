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
package org.apache.gluten.execution

import org.apache.gluten.backendsapi.clickhouse.CHConfig

import org.apache.spark.SparkConf

// Some sqls' line length exceeds 100
// scalastyle:off line.size.limit

class GlutenClickHouseS3SourceSuite extends GlutenClickHouseTPCHAbstractSuite {

  override protected val needCopyParquetToTablePath = true

  override protected val tablesPath: String = basePath + "/tpch-data"
  override protected val tpchQueries: String =
    rootPath + "../../../../tools/gluten-it/common/src/main/resources/tpch-queries"
  override protected val queriesResults: String = rootPath + "queries-output"

  override protected def sparkConf: SparkConf = {
    import org.apache.gluten.backendsapi.clickhouse.CHConfig._

    super.sparkConf
      .set("spark.shuffle.manager", "org.apache.spark.shuffle.sort.ColumnarShuffleManager")
      .set("spark.io.compression.codec", "LZ4")
      .set("spark.sql.shuffle.partitions", "5")
      .set("spark.sql.autoBroadcastJoinThreshold", "10MB")
      .set("spark.memory.offHeap.size", "4g")
      // test with minio
      .set("spark.hadoop.fs.s3a.access.key", "admin")
      .set("spark.hadoop.fs.s3a.secret.key", "xxxxxx")
      .set("spark.hadoop.fs.s3a.endpoint", "http://ip:port")
      .set("spark.hadoop.fs.s3a.path.style.access", "true")
      .set("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
      .set("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
      .setCHConfig("s3.local_cache.enabled", true)
      .setCHConfig("s3.local_cache.cache_path", "/data/gluten-ch-cache-dir")
  }

  override protected val createNullableTables = true

  override def beforeAll(): Unit = {
    super.beforeAll()
    createTPCHS3Tables()
  }

  protected def createTPCHS3Tables(): Unit = {
    val s3TablePath = "s3a://gluten-test"

    val nationData = s3TablePath + "/nation"
    spark.sql(s"DROP TABLE IF EXISTS nation_s3")
    spark.sql(s"""
                 | CREATE TABLE IF NOT EXISTS nation_s3 (
                 | n_nationkey bigint,
                 | n_name      string,
                 | n_regionkey bigint,
                 | n_comment   string)
                 | USING PARQUET LOCATION '$nationData'
                 |""".stripMargin)

    val supplierData = s3TablePath + "/supplier"
    spark.sql(s"DROP TABLE IF EXISTS supplier_s3")
    spark.sql(s"""
                 | CREATE TABLE IF NOT EXISTS supplier_s3 (
                 | s_suppkey   bigint,
                 | s_name      string,
                 | s_address   string,
                 | s_nationkey bigint,
                 | s_phone     string,
                 | s_acctbal   double,
                 | s_comment   string)
                 | USING PARQUET LOCATION '$supplierData'
                 |""".stripMargin)
  }

  // ignore this test case, because it needs the minio to test s3
  ignore("test ch backend with s3") {
    var currTime = System.currentTimeMillis()
    // scalastyle:off println
    println(s"currTime=$currTime")
    // scalastyle:on println
    spark.sparkContext.setLocalProperty(
      CHConfig.runtimeSettings("spark.kylin.local-cache.accept-cache-time"),
      currTime.toString)
    spark
      .sql("""
             |select * from supplier_s3
             |""".stripMargin)
      .show(10, truncate = false)

    Thread.sleep(5000)

    // scalastyle:off println
    println(s"currTime=$currTime")
    // scalastyle:on println
    spark.sparkContext.setLocalProperty(
      CHConfig.runtimeSettings("spark.kylin.local-cache.accept-cache-time"),
      currTime.toString)
    spark
      .sql("""
             |select * from supplier_s3
             |""".stripMargin)
      .show(10, truncate = false)

    Thread.sleep(5000)
    currTime = System.currentTimeMillis()
    // scalastyle:off println
    println(s"currTime=$currTime")
    // scalastyle:on println
    spark.sparkContext.setLocalProperty(
      CHConfig.runtimeSettings("spark.kylin.local-cache.accept-cache-time"),
      currTime.toString)
    spark
      .sql("""
             |select * from supplier_s3
             |""".stripMargin)
      .show(10, truncate = false)
  }
}
// scalastyle:on line.size.limit
