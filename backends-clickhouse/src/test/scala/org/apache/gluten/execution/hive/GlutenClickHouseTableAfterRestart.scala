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
package org.apache.gluten.execution.hive

import org.apache.gluten.backendsapi.clickhouse.{CHConfig, RuntimeConfig, RuntimeSettings}
import org.apache.gluten.config.GlutenConfig
import org.apache.gluten.execution.GlutenClickHouseTPCHAbstractSuite

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SparkSession.{getActiveSession, getDefaultSession}
import org.apache.spark.sql.delta.ClickhouseSnapshot
import org.apache.spark.sql.delta.catalog.ClickHouseTableV2
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanHelper

import org.apache.commons.io.FileUtils

import java.io.File

// Some sqls' line length exceeds 100
// scalastyle:off line.size.limit

// This suite is to make sure clickhouse commands works well even after spark restart
class GlutenClickHouseTableAfterRestart
  extends GlutenClickHouseTPCHAbstractSuite
  with AdaptiveSparkPlanHelper
  with ReCreateHiveSession {

  override protected val needCopyParquetToTablePath = true

  override protected val tablesPath: String = basePath + "/tpch-data"
  override protected val tpchQueries: String = rootPath + "queries/tpch-queries-ch"
  override protected val queriesResults: String = rootPath + "mergetree-queries-output"

  /** Run Gluten + ClickHouse Backend with SortShuffleManager */
  override protected def sparkConf: SparkConf = {
    import org.apache.gluten.backendsapi.clickhouse.CHConfig._

    super.sparkConf
      .set("spark.shuffle.manager", "org.apache.spark.shuffle.sort.ColumnarShuffleManager")
      .set("spark.io.compression.codec", "LZ4")
      .set("spark.sql.shuffle.partitions", "5")
      .set("spark.sql.autoBroadcastJoinThreshold", "10MB")
      .set("spark.sql.adaptive.enabled", "true")
      .set(RuntimeConfig.LOGGER_LEVEL.key, "error")
      .setCHConfig("user_defined_path", "/tmp/user_defined")
      .set("spark.sql.files.maxPartitionBytes", "20000000")
      .set("spark.ui.enabled", "true")
      .set(GlutenConfig.NATIVE_WRITER_ENABLED.key, "true")
      .set(CHConfig.ENABLE_ONEPIPELINE_MERGETREE_WRITE.key, spark35.toString)
      .set(RuntimeSettings.MIN_INSERT_BLOCK_SIZE_ROWS.key, "100000")
      .setCHSettings("mergetree.merge_after_insert", false)
      .setCHSettings("input_format_parquet_max_block_size", 8192)
      .setMaster("local[2]")
  }

  override protected def createTPCHNotNullTables(): Unit = {
    createNotNullTPCHTablesInParquet(tablesPath)
  }

  var current_db_num: Int = 0

  override protected val hiveMetaStoreDB: String =
    metaStorePathAbsolute + "/metastore_db_" + current_db_num

  test("test mergetree after restart") {
    spark.sql(s"""
                 |DROP TABLE IF EXISTS LINEITEM_MERGETREE;
                 |""".stripMargin)

    spark.sql(s"""
                 |CREATE TABLE IF NOT EXISTS LINEITEM_MERGETREE
                 |(
                 | l_orderkey      bigint,
                 | l_partkey       bigint,
                 | l_suppkey       bigint,
                 | l_linenumber    bigint,
                 | l_quantity      double,
                 | l_extendedprice double,
                 | l_discount      double,
                 | l_tax           double,
                 | l_returnflag    string,
                 | l_linestatus    string,
                 | l_shipdate      date,
                 | l_commitdate    date,
                 | l_receiptdate   date,
                 | l_shipinstruct  string,
                 | l_shipmode      string,
                 | l_comment       string
                 |)
                 |USING clickhouse
                 |LOCATION '$basePath/lineitem_mergetree'
                 |""".stripMargin)

    spark.sql(s"""
                 | insert into table lineitem_mergetree
                 | select * from lineitem
                 |""".stripMargin)

    // before restart, check if cache works
    {
      runTPCHQueryBySQL(1, q1("lineitem_mergetree"))(_ => {})
      val oldMissingCount1 = ClickhouseSnapshot.deltaScanCache.stats().missCount()
      val oldMissingCount2 = ClickhouseSnapshot.addFileToAddMTPCache.stats().missCount()

      // for this run, missing count should not increase
      runTPCHQueryBySQL(1, q1("lineitem_mergetree"))(_ => {})
      val stats1 = ClickhouseSnapshot.deltaScanCache.stats()
      assertResult(oldMissingCount1)(stats1.missCount())
      val stats2 = ClickhouseSnapshot.addFileToAddMTPCache.stats()
      assertResult(oldMissingCount2)(stats2.missCount())
    }

    val oldMissingCount1 = ClickhouseSnapshot.deltaScanCache.stats().missCount()
    val oldMissingCount2 = ClickhouseSnapshot.addFileToAddMTPCache.stats().missCount()

    restartSpark()

    runTPCHQueryBySQL(1, q1("lineitem_mergetree"))(_ => {})

    // after restart, additionally check stats of delta scan cache
    val stats1 = ClickhouseSnapshot.deltaScanCache.stats()
    assertResult(oldMissingCount1 + 1)(stats1.missCount())
    val stats2 = ClickhouseSnapshot.addFileToAddMTPCache.stats()
    assertResult(oldMissingCount2 + 6)(stats2.missCount())
  }

  test("test optimize after restart") {
    spark.sql(s"""
                 |DROP TABLE IF EXISTS table_restart_optimize;
                 |""".stripMargin)

    spark.sql(s"""
                 |CREATE TABLE IF NOT EXISTS table_restart_optimize (id bigint,  name string)
                 |USING clickhouse
                 |LOCATION '$basePath/table_restart_optimize'
                 |""".stripMargin)

    spark.sql(s"""
                 | insert into table table_restart_optimize values (1,"tom"), (2, "jim")
                 |""".stripMargin)
    // second file
    spark.sql(s"""
                 | insert into table table_restart_optimize values (1,"tom"), (2, "jim")
                 |""".stripMargin)

    restartSpark()

    spark.sql("optimize table_restart_optimize")
    assertResult(4)(
      spark.sql("select count(*) from table_restart_optimize").collect().apply(0).get(0))
  }

  test("test vacuum after restart") {
    spark.sql(s"""
                 |DROP TABLE IF EXISTS table_restart_vacuum;
                 |""".stripMargin)

    spark.sql(s"""
                 |CREATE TABLE IF NOT EXISTS table_restart_vacuum (id bigint,  name string)
                 |USING clickhouse
                 |LOCATION '$basePath/table_restart_vacuum'
                 |""".stripMargin)

    spark.sql(s"""
                 | insert into table table_restart_vacuum values (1,"tom"), (2, "jim")
                 |""".stripMargin)
    // second file
    spark.sql(s"""
                 | insert into table table_restart_vacuum values (1,"tom"), (2, "jim")
                 |""".stripMargin)

    spark.sql("optimize table_restart_vacuum")

    restartSpark()

    spark.sql("vacuum table_restart_vacuum")

    assertResult(4)(
      spark.sql("select count(*) from table_restart_vacuum").collect().apply(0).get(0))
  }

  test("test update after restart") {
    spark.sql(s"""
                 |DROP TABLE IF EXISTS table_restart_update;
                 |""".stripMargin)

    spark.sql(s"""
                 |CREATE TABLE IF NOT EXISTS table_restart_update (id bigint,  name string)
                 |USING clickhouse
                 |LOCATION '$basePath/table_restart_update'
                 |""".stripMargin)

    spark.sql(s"""
                 | insert into table table_restart_update values (1,"tom"), (2, "jim")
                 |""".stripMargin)
    // second file
    spark.sql(s"""
                 | insert into table table_restart_update values (1,"tom"), (2, "jim")
                 |""".stripMargin)

    restartSpark()

    spark.sql("update table_restart_update set name = 'tom' where id = 1")

    assertResult(4)(
      spark.sql("select count(*) from table_restart_update").collect().apply(0).get(0))
  }

  test("test delete after restart") {
    spark.sql(s"""
                 |DROP TABLE IF EXISTS table_restart_delete;
                 |""".stripMargin)

    spark.sql(s"""
                 |CREATE TABLE IF NOT EXISTS table_restart_delete (id bigint,  name string)
                 |USING clickhouse
                 |LOCATION '$basePath/table_restart_delete'
                 |""".stripMargin)

    spark.sql(s"""
                 | insert into table table_restart_delete values (1,"tom"), (2, "jim")
                 |""".stripMargin)
    // second file
    spark.sql(s"""
                 | insert into table table_restart_delete values (1,"tom"), (2, "jim")
                 |""".stripMargin)

    restartSpark()

    spark.sql("delete from table_restart_delete where where id = 1")

    assertResult(2)(
      spark.sql("select count(*) from table_restart_delete").collect().apply(0).get(0))
  }

  test("test drop after restart") {
    spark.sql(s"""
                 |DROP TABLE IF EXISTS table_restart_drop;
                 |""".stripMargin)

    spark.sql(s"""
                 |CREATE TABLE IF NOT EXISTS table_restart_drop (id bigint,  name string)
                 |USING clickhouse
                 |LOCATION '$basePath/table_restart_drop'
                 |""".stripMargin)

    spark.sql(s"""
                 | insert into table table_restart_drop values (1,"tom"), (2, "jim")
                 |""".stripMargin)
    // second file
    spark.sql(s"""
                 | insert into table table_restart_drop values (1,"tom"), (2, "jim")
                 |""".stripMargin)

    restartSpark()

    spark.sql("drop table table_restart_drop")
  }

  private def restartSpark(): Unit = {
    // now restart
    ClickHouseTableV2.clearCache()
    ClickhouseSnapshot.clearAllFileStatusCache()

    val session = getActiveSession.orElse(getDefaultSession)
    if (session.isDefined) {
      session.get.stop()
      SparkSession.clearActiveSession()
      SparkSession.clearDefaultSession()
    }

    val metaStoreDB = metaStorePathAbsolute + "/metastore_db_"
    // use metastore_db2 to avoid issue: "Another instance of Derby may have already booted the database"
    current_db_num += 1
    val destDir = new File(metaStoreDB + current_db_num)
    destDir.mkdirs()
    FileUtils.copyDirectory(new File(metaStoreDB + (current_db_num - 1)), destDir)
    updateHiveSession(
      SparkSession
        .builder()
        .config(sparkConf)
        .enableHiveSupport()
        .config(
          "javax.jdo.option.ConnectionURL",
          s"jdbc:derby:;databaseName=$metaStoreDB$current_db_num")
        .getOrCreate()
    )
  }
}
// scalastyle:off line.size.limit
