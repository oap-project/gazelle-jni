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

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.execution.datasources.v2.clickhouse.ClickHouseConfig
import org.apache.spark.sql.types._

import java.nio.file.Files

class GlutenCoalesceAggregationUnionSuite extends GlutenClickHouseWholeStageTransformerSuite {
  override protected def sparkConf: SparkConf = {
    super.sparkConf
      .set("spark.sql.files.maxPartitionBytes", "1g")
      .set("spark.serializer", "org.apache.spark.serializer.JavaSerializer")
      .set("spark.sql.shuffle.partitions", "5")
      .set("spark.sql.adaptive.enabled", "false")
      .set("spark.sql.files.minPartitionNum", "1")
      .set(
        "spark.sql.catalog.spark_catalog",
        "org.apache.spark.sql.execution.datasources.v2.clickhouse.ClickHouseSparkCatalog")
      .set("spark.databricks.delta.maxSnapshotLineageLength", "20")
      .set("spark.databricks.delta.snapshotPartitions", "1")
      .set("spark.databricks.delta.properties.defaults.checkpointInterval", "5")
      .set("spark.databricks.delta.stalenessLimit", "3600000")
      .set(ClickHouseConfig.CLICKHOUSE_WORKER_ID, "1")
      .set("spark.gluten.sql.columnar.iterator", "true")
      .set("spark.gluten.sql.columnar.hashagg.enablefinal", "true")
      .set("spark.gluten.sql.enable.native.validation", "false")
      .set("spark.sql.warehouse.dir", warehouse)
      .set("spark.shuffle.manager", "sort")
      .set("spark.io.compression.codec", "snappy")
      .set("spark.sql.shuffle.partitions", "5")
      .set("spark.sql.autoBroadcastJoinThreshold", "10MB")
  }

  def createTestTable(tableName: String, data: DataFrame): Unit = {
    val tempFile = Files.createTempFile("", ".parquet").toFile
    tempFile.deleteOnExit()
    val tempFilePath = tempFile.getAbsolutePath
    data.coalesce(1).write.format("parquet").mode("overwrite").parquet(tempFilePath)
    spark.catalog.createTable(tableName, tempFilePath, "parquet")
  }

  override def beforeAll(): Unit = {
    super.beforeAll()

    val schema = StructType(
      Array(
        StructField("a", StringType, nullable = true),
        StructField("b", IntegerType, nullable = true),
        StructField("x", StringType, nullable = true),
        StructField("y", IntegerType, nullable = true)
      ))
    val data = sparkContext.parallelize(
      Seq(
        Row("a", 1, null, 1),
        Row("a", 2, "a", 2),
        Row("a", 3, "b", 3),
        Row("a", 4, "c", 4),
        Row("b", 1, "d", 5),
        Row("b", 2, "e", 6),
        Row("b", 3, "f", 7),
        Row("b", 4, "g", null)
      ))

    val dataFrame = spark.createDataFrame(data, schema)
    createTestTable("coalesce_union_t1", dataFrame)
    createTestTable("coalesce_union_t2", dataFrame)
  }

  def checkNoUnion(df: DataFrame): Unit = {
    val unions = collectWithSubqueries(df.queryExecution.executedPlan) {
      case e: ColumnarUnionExec => e
    }
    assert(unions.isEmpty)
  }

  def checkHasUnion(df: DataFrame): Unit = {
    val unions = collectWithSubqueries(df.queryExecution.executedPlan) {
      case e: ColumnarUnionExec => e
    }
    assert(unions.size == 1)
  }

  test("coalesce aggregation union. case 1") {
    val sql =
      """
        |select a, x + 1 as x, y from (
        | select a, count(x) as x, sum(y) as y from coalesce_union_t1 where b % 3 = 0
        |   group by a
        | union all
        | select a, count(x) as x, sum(y) as y from coalesce_union_t1 where b % 3 = 1
        |   group by a
        | union all
        | select a, count(x) as x, sum(y) as y from coalesce_union_t1 where b % 3 = 2
        |   group by a
        |) order by a, x, y
        |""".stripMargin
    compareResultsAgainstVanillaSpark(sql, true, checkNoUnion, true)
  }

  test("coalesce aggregation union. case 2") {
    val sql =
      """
        |select a, x + 1 as x, y from (
        | select a, count(x) as x, sum(y) as y from coalesce_union_t1 where b % 3 = 0
        |   group by a
        | union all
        | select a, count(x) as x, sum(y) as y from coalesce_union_t1 where b % 3 = 1
        |   group by a
        | union all
        | select a, count(x) as x, sum(y) as y from coalesce_union_t1 where b > 1
        |   group by a
        |) order by a, x, y
        |""".stripMargin
    compareResultsAgainstVanillaSpark(sql, true, checkNoUnion, true)
  }

  test("coalesce aggregation union. case 3") {
    val sql =
      """
        |select a, x + 1 as x, y from (
        | select a, 1 as t, count(x) as x, sum(y) as y from coalesce_union_t1 where b % 3 = 0
        |   group by a
        | union all
        | select a, 2 as t, count(x) as x, sum(y) as y from coalesce_union_t1 where b % 3 = 1
        |   group by a
        |) order by a, t, x, y
        |""".stripMargin
    compareResultsAgainstVanillaSpark(sql, true, checkNoUnion, true)
  }

  test("coalesce aggregation union. case 4") {
    val sql =
      """
        |select * from (
        | select a, 1 as t, count(x) + sum(y) as n from coalesce_union_t1 where b % 3 = 0
        |   group by a
        | union all
        | select a, 2 as t, count(x) + sum(y) as n from coalesce_union_t1 where b % 3 = 1
        |   group by a
        |) order by a, t, n
        |""".stripMargin
    compareResultsAgainstVanillaSpark(sql, true, checkNoUnion, true)
  }

  test("coalesce aggregation union. case 5") {
    val sql =
      """
        |select * from (
        | select a, count(x) as x, sum(y) as y from coalesce_union_t1 where b % 3 = 0
        |   group by a, b
        | union all
        | select a, count(x) as x, sum(y) as y from coalesce_union_t1 where b % 3 = 1
        |   group by a, b
        |) order by a, x, y
        |""".stripMargin
    compareResultsAgainstVanillaSpark(sql, true, checkNoUnion, true)
  }

  test("coalesce aggregation union. case 6") {
    val sql =
      """
        |select * from (
        | select y + 1 as y , count(x) as x from coalesce_union_t1 where b % 3 = 0
        |   group by y + 1
        | union all
        | select y + 1 as y, count(x) as x from coalesce_union_t1 where b % 3 = 1
        |   group by y + 1
        |) order by y, x
        |""".stripMargin
    compareResultsAgainstVanillaSpark(sql, true, checkNoUnion, true)
  }

  test("coalesce aggregation union. case 7") {
    val sql =
      """
        |select * from (
        | select a, count(x) as x, sum(y + 1) as y from coalesce_union_t1 where b % 3 = 0
        |   group by a, b
        | union all
        | select a, count(x) as x, sum(y + 1) as y from coalesce_union_t1 where b % 3 = 1
        |   group by a, b
        |) order by a, x, y
        |""".stripMargin
    compareResultsAgainstVanillaSpark(sql, true, checkNoUnion, true)
  }

  test("coalesce aggregation union. case 8") {
    val sql =
      """
        |select * from (
        | select a as a, sum(y) as y from coalesce_union_t1 where b % 3 = 0
        |   group by a
        | union all
        | select x as a , sum(y) as y from coalesce_union_t1 where b % 3 = 1
        |   group by x
        |) order by a, y
        |""".stripMargin
    compareResultsAgainstVanillaSpark(sql, true, checkNoUnion, true)
  }

  test("coalesce aggregation union. case 9") {
    val sql =
      """
        |select a, x + 1 as x, y from (
        | select a, count(x) as x, sum(y) as y from coalesce_union_t1 where b % 3 = 0
        |   group by a
        | union all
        | select a, count(x) as x, sum(y) as y from coalesce_union_t1 where b % 3 = 1
        |   group by a
        | union all
        | select a, count(x) as x, sum(y) as y from coalesce_union_t1 where b % 3 = 2
        |   group by a
        | union all
        | select a, count(x) as x, sum(y) as y from coalesce_union_t1 where b % 3 = 3
        |   group by a
        |) order by a, x, y
        |""".stripMargin
    compareResultsAgainstVanillaSpark(sql, true, checkNoUnion, true)
  }

  test("coalesce aggregation union. case 10") {
    val sql =
      """
        |select * from (
        | select a as a, sum(y) as y from (
        |   select concat(a, "x") as a, y from coalesce_union_t1 where b % 3 = 0
        | ) group by a
        | union all
        | select x as a , sum(y) as y from coalesce_union_t1 where b % 3 = 1
        |   group by x
        |) order by a, y
        |""".stripMargin
    compareResultsAgainstVanillaSpark(sql, true, checkNoUnion, true)
  }

  test("coalesce aggregation union. case 11") {
    val sql =
      """
        |select t1.a, t1.y, t2.x from (
        | select a as a, sum(y) as y from (
        |   select concat(a, "x") as a, y from coalesce_union_t1 where b % 3 = 0
        | ) group by a
        | union all
        | select x as a , sum(y) as y from coalesce_union_t1 where b % 3 = 1
        |   group by x
        |) as t1
        |left join (
        | select a, x from coalesce_union_t2
        |) as t2
        |on t1.a = t2.a
        |""".stripMargin
    compareResultsAgainstVanillaSpark(sql, true, checkNoUnion, true)
  }

  test("coalesce aggregation union. case 12") {
    val sql =
      """
        |select a, x, y from (
        | select a, count(distinct x) as x, sum(y) as y from coalesce_union_t1 group by a
        | union all
        | select a, count(distinct x) as x, sum(y) as y from coalesce_union_t1 group by a
        |) order by a, x, y
        |""".stripMargin
    compareResultsAgainstVanillaSpark(sql, true, checkNoUnion, true)
  }

  test("no coalesce aggregation union. case 1") {
    val sql =
      """
        |select * from (
        | select a, count(x) + 1 as x, sum(y) as y from coalesce_union_t1 where b % 3 = 0
        |   group by a
        | union all
        | select a, count(x) + 1 as x, sum(y) as y from coalesce_union_t1 where b % 3 = 1
        |   group by a
        |) order by a, x, y
        |""".stripMargin
    compareResultsAgainstVanillaSpark(sql, true, checkHasUnion, true)
  }

  test("no coalesce aggregation union. case 2") {
    val sql =
      """
        |select * from (
        | select y + 1 as y, count(x) as x from coalesce_union_t1 where b % 3 = 0
        |   group by y
        | union all
        | select y + 1 as y, count(x) as x from coalesce_union_t1 where b % 3 = 1
        |   group by y
        |) order by y, x
        |""".stripMargin
    compareResultsAgainstVanillaSpark(sql, true, checkHasUnion, true)
  }

  test("no coalesce aggregation union. case 3") {
    val sql =
      """
        |select * from (
        | select a, count(x) as x from coalesce_union_t1 where b % 3 = 0
        |   group by a
        | union all
        | select a, count(y) as x from coalesce_union_t1 where b % 3 = 1
        |   group by a
        |) order by a, x
        |""".stripMargin
    compareResultsAgainstVanillaSpark(sql, true, checkHasUnion, true)
  }

  test("no coalesce aggregation union. case 4") {
    val sql =
      """
        |select * from (
        | select a, 1 as b, count(x) as x from coalesce_union_t1 where b % 3 = 0
        |   group by a, 1
        | union all
        | select a, b, count(x) as x from coalesce_union_t1 where b % 3 = 1
        |   group by a, b
        |) order by a, b, x
        |""".stripMargin
    compareResultsAgainstVanillaSpark(sql, true, checkHasUnion, true)
  }

  test("no coalesce aggregation union. case 5") {
    val sql =
      """
        |select * from (
        | select a, b, count(x) as x from coalesce_union_t1 where b % 3 = 0
        |   group by a, b
        | union all
        | select a, b, count(x) as x from coalesce_union_t2 where b % 3 = 1
        |   group by a, b
        |) order by a, b, x
        |""".stripMargin
    compareResultsAgainstVanillaSpark(sql, true, checkHasUnion, true)
  }

  test("no coalesce aggregation union. case 6") {
    val sql =
      """
        |select * from (
        | select a as k1, x as k2, count(y) as c from coalesce_union_t1 where b % 3 = 0
        |   group by a, x
        | union all
        | select x as k1, a as k2, count(y) as c from coalesce_union_t1 where b % 3 = 1
        |   group by a, x
        |) order by k1, k2, c
        |""".stripMargin
    compareResultsAgainstVanillaSpark(sql, true, checkHasUnion, true)
  }

  test("no coalesce aggregation union. case 7") {
    val sql =
      """
        |select * from (
        | select a, count(y) as y from coalesce_union_t1 where b % 3 = 0
        |   group by a
        | union all
        | select a, count(y) as y from coalesce_union_t2
        |   group by a
        |) order by a, y
        |""".stripMargin
    compareResultsAgainstVanillaSpark(sql, true, checkHasUnion, true)
  }

  test("no coalesce aggregation union. case 8") {
    val sql =
      """
        |select a, x, y from (
        | select a, count(distinct x) as x, sum(y) as y from coalesce_union_t1 group by a
        | union all
        | select a, count(x) as x, sum(y) as y from coalesce_union_t1 group by a
        |) order by a, x, y
        |""".stripMargin
    compareResultsAgainstVanillaSpark(sql, true, checkHasUnion, true)
  }

}
