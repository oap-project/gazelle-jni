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
import org.apache.spark.scheduler.{SparkListener, SparkListenerTaskEnd}
import org.apache.spark.sql.execution.datasources.v2.clickhouse.ClickHouseConfig

import java.util.concurrent.atomic.AtomicInteger

class GlutenClickHouseJoinSuite extends GlutenClickHouseWholeStageTransformerSuite {

  protected val tablesPath: String = basePath + "/tpch-data"
  protected val tpchQueries: String =
    rootPath + "../../../../tools/gluten-it/common/src/main/resources/tpch-queries"
  protected val queriesResults: String = rootPath + "queries-output"

  private val joinAlgorithm = CHConfig.runtimeSettings("join_algorithm")

  override protected def sparkConf: SparkConf = {
    super.sparkConf
      .set("spark.sql.files.maxPartitionBytes", "1g")
      .set("spark.serializer", "org.apache.spark.serializer.JavaSerializer")
      .set("spark.sql.shuffle.partitions", "5")
      .set("spark.sql.adaptive.enabled", "false")
      .set("spark.sql.files.minPartitionNum", "1")
      .set(ClickHouseConfig.CLICKHOUSE_WORKER_ID, "1")
      .set("spark.gluten.sql.columnar.iterator", "true")
      .set("spark.gluten.sql.columnar.hashagg.enablefinal", "true")
      .set("spark.gluten.sql.enable.native.validation", "false")
      .set("spark.sql.warehouse.dir", warehouse)
      .set(
        "spark.sql.warehouse.dir",
        getClass.getResource("/").getPath + "tests-working-home/spark-warehouse")
      .set("spark.hive.exec.dynamic.partition.mode", "nonstrict")
      .set("spark.shuffle.manager", "sort")
      .set("spark.io.compression.codec", "snappy")
      .set("spark.sql.shuffle.partitions", "5")
      .set("spark.sql.autoBroadcastJoinThreshold", "10MB")
      .set(joinAlgorithm, "hash")
      .set("spark.sql.autoBroadcastJoinThreshold", "-1")
      .setMaster("local[*]")
  }

  test("int to long join key rewrite causes column miss match ") {
    assert("hash".equalsIgnoreCase(sparkConf.get(joinAlgorithm, "hash")))
    withSQLConf(joinAlgorithm -> "grace_hash") {
      withTable("my_customer", "my_store_sales", "my_date_dim") {
        sql("""
              |CREATE TABLE my_customer (
              |  c_customer_sk INT)
              |USING orc
              |""".stripMargin)
        sql("""
              |CREATE TABLE my_store_sales (
              |  ss_sold_date_sk INT,
              |  ss_customer_sk INT)
              | USING orc
              |""".stripMargin)
        sql("""
              |CREATE TABLE my_date_dim (
              |  d_date_sk INT,
              |  d_year INT,
              |  d_qoy INT)
              |USING orc
              |""".stripMargin)

        sql("insert into my_customer values (1), (2), (3), (4)")
        sql("insert into my_store_sales values (1, 1), (2, 2), (3, 3), (4, 4)")
        sql("insert into my_date_dim values (1, 2002, 1), (2, 2002, 2)")
        val q =
          """
            |SELECT
            |  count(*) cnt1
            |FROM
            |  my_customer c
            |WHERE
            |    exists(SELECT *
            |           FROM my_store_sales, my_date_dim
            |           WHERE c.c_customer_sk = ss_customer_sk AND
            |             ss_sold_date_sk = d_date_sk AND
            |             d_year = 2002 AND
            |             d_qoy < 4)
            |LIMIT 100
            |""".stripMargin
        runQueryAndCompare(q)(checkGlutenOperatorMatch[CHShuffledHashJoinExecTransformer])
      }
    }
  }

  test("GLUTEN-8168 eliminate non-attribute expressions in join keys and condition") {
    sql("create table tj1 (a int, b int, c int, d int) using parquet")
    sql("create table tj2 (a int, b int, c int, d int) using parquet")
    sql("insert into tj1 values (1, 2, 3, 4), (2, 2, 4, 5), (3, 4, 5, 4), (4, 5, 3, 7)")
    sql("insert into tj2 values (1, 2, 3, 4), (2, 2, 4, 5), (3, 4, 5, 4), (4, 5, 3, 7)")
    compareResultsAgainstVanillaSpark(
      """
        |SELECT t1.*, t2.*
        |FROM tj1 t1 LEFT JOIN tj2 t2
        |ON t1.a  = t2.a AND (t1.b = t2.b or t1.c + 1 = t2.c) ORDER BY t1.a, t2.a
        |""".stripMargin,
      true,
      { _ => }
    )
    compareResultsAgainstVanillaSpark(
      """
        |SELECT t1.*, t2.*
        |FROM tj1 t1 LEFT JOIN tj2 t2
        |ON t1.a  = t2.a AND (t1.b = t2.b or t1.c = t2.c) ORDER BY t1.a, t2.a
        |""".stripMargin,
      true,
      { _ => }
    )
    compareResultsAgainstVanillaSpark(
      """
        |SELECT t1.*, t2.*
        |FROM tj1 t1 LEFT JOIN tj2 t2
        |ON t1.a  = t2.a + 1 AND (t1.b + 1 = t2.b or t1.c = t2.c + 1) ORDER BY t1.a, t2.a
        |""".stripMargin,
      true,
      { _ => }
    )
    sql("drop table if exists tj1")
    sql("drop table if exists tj2")
  }

  test("GLUTEN-8216 Fix OOM when cartesian product with empty data") {
    // prepare
    spark.sql("create table test_join(a int, b int, c int) using parquet")
    var overrideConfs = Map(
      "spark.sql.autoBroadcastJoinThreshold" -> "-1",
      "spark.sql.shuffle.partitions" -> "1"
    )
    if (isSparkVersionGE("3.5")) {
      // Range partitions will not be reduced if EliminateSorts is enabled in spark35.
      overrideConfs += "spark.sql.optimizer.excludedRules" ->
        "org.apache.spark.sql.catalyst.optimizer.EliminateSorts"
    }

    withSQLConf(overrideConfs.toSeq: _*) {
      val taskCount = new AtomicInteger(0)
      val taskListener = new SparkListener {
        override def onTaskEnd(taskEnd: SparkListenerTaskEnd): Unit = {
          taskCount.incrementAndGet()
          logDebug(s"Task ${taskEnd.taskInfo.id} finished. Total tasks completed: $taskCount")
        }
      }
      spark.sparkContext.addSparkListener(taskListener)
      spark
        .sql(
          "select * from " +
            "(select a from test_join group by a order by a), " +
            "(select b from test_join group by b order by b)" +
            " limit 10000"
        )
        .collect()
      assert(taskCount.get() < 500)
    }
  }

}
