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
import org.apache.spark.internal.Logging
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.delta.{ClickhouseSnapshot, DeltaLog}
import org.apache.spark.sql.execution.datasources.v2.clickhouse.ClickHouseConfig

import org.apache.commons.io.FileUtils
import org.scalatest.time.SpanSugar.convertIntToGrainOfTime

import java.io.File

abstract class GlutenClickHouseTPCHAbstractSuite
  extends GlutenClickHouseWholeStageTransformerSuite
  with Logging {

  protected val createNullableTables = false

  protected val needCopyParquetToTablePath = false

  protected val parquetTableDataPath: String =
    "../../../../gluten-core/src/test/resources/tpch-data"

  protected val tablesPath: String
  protected val tpchQueries: String
  protected val queriesResults: String

  override def beforeAll(): Unit = {

    super.beforeAll()

    if (needCopyParquetToTablePath) {
      val sourcePath = new File(rootPath + parquetTableDataPath)
      FileUtils.copyDirectory(sourcePath, new File(tablesPath))
    }

    spark.sparkContext.setLogLevel(logLevel)
    if (createNullableTables) {
      createTPCHNullableTables()
    } else {
      createTPCHNotNullTables()
    }
  }

  override protected def createTPCHNotNullTables(): Unit = {
    // create parquet data source table
    val parquetSourceDB = "parquet_source"
    spark.sql(s"""
                 |CREATE DATABASE IF NOT EXISTS $parquetSourceDB
                 |""".stripMargin)
    spark.sql(s"use $parquetSourceDB")

    val parquetTablePath = basePath + "/tpch-data"
    FileUtils.copyDirectory(new File(rootPath + parquetTableDataPath), new File(parquetTablePath))

    createNotNullTPCHTablesInParquet(parquetTablePath)

    // create mergetree tables
    spark.sql(s"use default")
    val customerData = tablesPath + "/customer"
    spark.sql(s"DROP TABLE IF EXISTS customer")
    spark.sql(s"""
                 | CREATE EXTERNAL TABLE IF NOT EXISTS customer (
                 | c_custkey    bigint not null,
                 | c_name       string not null,
                 | c_address    string not null,
                 | c_nationkey  bigint not null,
                 | c_phone      string not null,
                 | c_acctbal    double not null,
                 | c_mktsegment string not null,
                 | c_comment    string not null)
                 | USING clickhouse
                 | TBLPROPERTIES (engine='MergeTree'
                 |                )
                 | LOCATION '$customerData'
                 |""".stripMargin)

    val lineitemData = tablesPath + "/lineitem"
    spark.sql(s"DROP TABLE IF EXISTS lineitem")
    spark.sql(s"""
                 | CREATE EXTERNAL TABLE IF NOT EXISTS lineitem (
                 | l_orderkey      bigint not null,
                 | l_partkey       bigint not null,
                 | l_suppkey       bigint not null,
                 | l_linenumber    bigint not null,
                 | l_quantity      double not null,
                 | l_extendedprice double not null,
                 | l_discount      double not null,
                 | l_tax           double not null,
                 | l_returnflag    string not null,
                 | l_linestatus    string not null,
                 | l_shipdate      date not null,
                 | l_commitdate    date not null,
                 | l_receiptdate   date not null,
                 | l_shipinstruct  string not null,
                 | l_shipmode      string not null,
                 | l_comment       string not null)
                 | USING clickhouse
                 | TBLPROPERTIES (engine='MergeTree'
                 |                )
                 | LOCATION '$lineitemData'
                 |""".stripMargin)

    val nationData = tablesPath + "/nation"
    spark.sql(s"DROP TABLE IF EXISTS nation")
    spark.sql(s"""
                 | CREATE EXTERNAL TABLE IF NOT EXISTS nation (
                 | n_nationkey bigint not null,
                 | n_name      string not null,
                 | n_regionkey bigint not null,
                 | n_comment   string not null)
                 | USING clickhouse
                 | TBLPROPERTIES (engine='MergeTree'
                 |                )
                 | LOCATION '$nationData'
                 |""".stripMargin)

    val regionData = tablesPath + "/region"
    spark.sql(s"DROP TABLE IF EXISTS region")
    spark.sql(s"""
                 | CREATE EXTERNAL TABLE IF NOT EXISTS region (
                 | r_regionkey bigint not null,
                 | r_name      string not null,
                 | r_comment   string not null)
                 | USING clickhouse
                 | TBLPROPERTIES (engine='MergeTree'
                 |                )
                 | LOCATION '$regionData'
                 |""".stripMargin)

    val ordersData = tablesPath + "/orders"
    spark.sql(s"DROP TABLE IF EXISTS orders")
    spark.sql(s"""
                 | CREATE EXTERNAL TABLE IF NOT EXISTS orders (
                 | o_orderkey      bigint not null,
                 | o_custkey       bigint not null,
                 | o_orderstatus   string not null,
                 | o_totalprice    double not null,
                 | o_orderdate     date not null,
                 | o_orderpriority string not null,
                 | o_clerk         string not null,
                 | o_shippriority  bigint not null,
                 | o_comment       string not null)
                 | USING clickhouse
                 | TBLPROPERTIES (engine='MergeTree'
                 |                )
                 | LOCATION '$ordersData'
                 |""".stripMargin)

    val partData = tablesPath + "/part"
    spark.sql(s"DROP TABLE IF EXISTS part")
    spark.sql(s"""
                 | CREATE EXTERNAL TABLE IF NOT EXISTS part (
                 | p_partkey     bigint not null,
                 | p_name        string not null,
                 | p_mfgr        string not null,
                 | p_brand       string not null,
                 | p_type        string not null,
                 | p_size        bigint not null,
                 | p_container   string not null,
                 | p_retailprice double not null,
                 | p_comment     string not null)
                 | USING clickhouse
                 | TBLPROPERTIES (engine='MergeTree'
                 |                )
                 | LOCATION '$partData'
                 |""".stripMargin)

    val partsuppData = tablesPath + "/partsupp"
    spark.sql(s"DROP TABLE IF EXISTS partsupp")
    spark.sql(s"""
                 | CREATE EXTERNAL TABLE IF NOT EXISTS partsupp (
                 | ps_partkey    bigint not null,
                 | ps_suppkey    bigint not null,
                 | ps_availqty   bigint not null,
                 | ps_supplycost double not null,
                 | ps_comment    string not null)
                 | USING clickhouse
                 | TBLPROPERTIES (engine='MergeTree'
                 |                )
                 | LOCATION '$partsuppData'
                 |""".stripMargin)

    val supplierData = tablesPath + "/supplier"
    spark.sql(s"DROP TABLE IF EXISTS supplier")
    spark.sql(s"""
                 | CREATE EXTERNAL TABLE IF NOT EXISTS supplier (
                 | s_suppkey   bigint not null,
                 | s_name      string not null,
                 | s_address   string not null,
                 | s_nationkey bigint not null,
                 | s_phone     string not null,
                 | s_acctbal   double not null,
                 | s_comment   string not null)
                 | USING clickhouse
                 | TBLPROPERTIES (engine='MergeTree'
                 |                )
                 | LOCATION '$supplierData'
                 |""".stripMargin)

    val result = spark
      .sql(s"""
              | show tables;
              |""".stripMargin)
      .collect()
    assert(result.length == 8)

    // insert data into mergetree tables from parquet tables
    insertIntoMergeTreeTPCHTables(parquetSourceDB)
  }

  protected def createTPCHNullableTables(): Unit = {
    // create parquet data source table
    val parquetSourceDB = "parquet_source"
    spark.sql(s"""
                 |CREATE DATABASE IF NOT EXISTS $parquetSourceDB

                 |""".stripMargin)
    spark.sql(s"use $parquetSourceDB")

    val parquetTablePath = basePath + "/tpch-data"
    FileUtils.copyDirectory(new File(rootPath + parquetTableDataPath), new File(parquetTablePath))

    createNotNullTPCHTablesInParquet(parquetTablePath)

    // create mergetree tables
    spark.sql(s"""
                 |CREATE DATABASE IF NOT EXISTS tpch_nullable
                 |""".stripMargin)
    spark.sql("use tpch_nullable")
    val customerData = tablesPath + "/customer"
    spark.sql(s"DROP TABLE IF EXISTS customer")
    spark.sql(s"""
                 | CREATE EXTERNAL TABLE IF NOT EXISTS customer (
                 | c_custkey    bigint,
                 | c_name       string,
                 | c_address    string,
                 | c_nationkey  bigint,
                 | c_phone      string,
                 | c_acctbal    double,
                 | c_mktsegment string,
                 | c_comment    string)
                 | USING clickhouse
                 | TBLPROPERTIES (engine='MergeTree'
                 |                )
                 | LOCATION '$customerData'
                 |""".stripMargin)

    val lineitemData = tablesPath + "/lineitem"
    spark.sql(s"DROP TABLE IF EXISTS lineitem")
    spark.sql(s"""
                 | CREATE EXTERNAL TABLE IF NOT EXISTS lineitem (
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
                 | l_comment       string)
                 | USING clickhouse
                 | TBLPROPERTIES (engine='MergeTree'
                 |                )
                 | LOCATION '$lineitemData'
                 |""".stripMargin)

    val nationData = tablesPath + "/nation"
    spark.sql(s"DROP TABLE IF EXISTS nation")
    spark.sql(s"""
                 | CREATE EXTERNAL TABLE IF NOT EXISTS nation (
                 | n_nationkey bigint,
                 | n_name      string,
                 | n_regionkey bigint,
                 | n_comment   string)
                 | USING clickhouse
                 | TBLPROPERTIES (engine='MergeTree'
                 |                )
                 | LOCATION '$nationData'
                 |""".stripMargin)

    val regionData = tablesPath + "/region"
    spark.sql(s"DROP TABLE IF EXISTS region")
    spark.sql(s"""
                 | CREATE EXTERNAL TABLE IF NOT EXISTS region (
                 | r_regionkey bigint,
                 | r_name      string,
                 | r_comment   string)
                 | USING clickhouse
                 | TBLPROPERTIES (engine='MergeTree'
                 |                )
                 | LOCATION '$regionData'
                 |""".stripMargin)

    val ordersData = tablesPath + "/orders"
    spark.sql(s"DROP TABLE IF EXISTS orders")
    spark.sql(s"""
                 | CREATE EXTERNAL TABLE IF NOT EXISTS orders (
                 | o_orderkey      bigint,
                 | o_custkey       bigint,
                 | o_orderstatus   string,
                 | o_totalprice    double,
                 | o_orderdate     date,
                 | o_orderpriority string,
                 | o_clerk         string,
                 | o_shippriority  bigint,
                 | o_comment       string)
                 | USING clickhouse
                 | TBLPROPERTIES (engine='MergeTree'
                 |                )
                 | LOCATION '$ordersData'
                 |""".stripMargin)

    val partData = tablesPath + "/part"
    spark.sql(s"DROP TABLE IF EXISTS part")
    spark.sql(s"""
                 | CREATE EXTERNAL TABLE IF NOT EXISTS part (
                 | p_partkey     bigint,
                 | p_name        string,
                 | p_mfgr        string,
                 | p_brand       string,
                 | p_type        string,
                 | p_size        bigint,
                 | p_container   string,
                 | p_retailprice double,
                 | p_comment     string)
                 | USING clickhouse
                 | TBLPROPERTIES (engine='MergeTree'
                 |                )
                 | LOCATION '$partData'
                 |""".stripMargin)

    val partsuppData = tablesPath + "/partsupp"
    spark.sql(s"DROP TABLE IF EXISTS partsupp")
    spark.sql(s"""
                 | CREATE EXTERNAL TABLE IF NOT EXISTS partsupp (
                 | ps_partkey    bigint,
                 | ps_suppkey    bigint,
                 | ps_availqty   bigint,
                 | ps_supplycost double,
                 | ps_comment    string)
                 | USING clickhouse
                 | TBLPROPERTIES (engine='MergeTree'
                 |                )
                 | LOCATION '$partsuppData'
                 |""".stripMargin)

    val supplierData = tablesPath + "/supplier"
    spark.sql(s"DROP TABLE IF EXISTS supplier")
    spark.sql(s"""
                 | CREATE EXTERNAL TABLE IF NOT EXISTS supplier (
                 | s_suppkey   bigint,
                 | s_name      string,
                 | s_address   string,
                 | s_nationkey bigint,
                 | s_phone     string,
                 | s_acctbal   double,
                 | s_comment   string)
                 | USING clickhouse
                 | TBLPROPERTIES (engine='MergeTree'
                 |                )
                 | LOCATION '$supplierData'
                 |""".stripMargin)

    val result = spark
      .sql(s"""
              | show tables;
              |""".stripMargin)
      .collect()
    assert(result.length == 8)

    insertIntoMergeTreeTPCHTables(parquetSourceDB)
  }

  protected def insertIntoMergeTreeTPCHTables(dataSourceDB: String): Unit = {
    spark.sql(s"""
                 | insert into table customer select * from $dataSourceDB.customer
                 |""".stripMargin)
    spark.sql(s"""
                 | insert into table lineitem select * from $dataSourceDB.lineitem
                 |""".stripMargin)
    spark.sql(s"""
                 | insert into table nation select * from $dataSourceDB.nation
                 |""".stripMargin)
    spark.sql(s"""
                 | insert into table region select * from $dataSourceDB.region
                 |""".stripMargin)
    spark.sql(s"""
                 | insert into table orders select * from $dataSourceDB.orders
                 |""".stripMargin)
    spark.sql(s"""
                 | insert into table part select * from $dataSourceDB.part
                 |""".stripMargin)
    spark.sql(s"""
                 | insert into table partsupp select * from $dataSourceDB.partsupp
                 |""".stripMargin)
    spark.sql(s"""
                 | insert into table supplier select * from $dataSourceDB.supplier
                 |""".stripMargin)
  }

  protected def createNotNullTPCHTablesInParquet(parquetTablePath: String): Unit = {
    val customerData = parquetTablePath + "/customer"
    spark.sql(s"DROP TABLE IF EXISTS customer")
    spark.sql(s"""
                 | CREATE TABLE IF NOT EXISTS customer (
                 | c_custkey    bigint,
                 | c_name       string,
                 | c_address    string,
                 | c_nationkey  bigint,
                 | c_phone      string,
                 | c_acctbal    double,
                 | c_mktsegment string,
                 | c_comment    string)
                 | USING PARQUET LOCATION '$customerData'
                 |""".stripMargin)

    val lineitemData = parquetTablePath + "/lineitem"
    spark.sql(s"DROP TABLE IF EXISTS lineitem")
    spark.sql(s"""
                 | CREATE TABLE IF NOT EXISTS lineitem (
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
                 | l_comment       string)
                 | USING PARQUET LOCATION '$lineitemData'
                 |""".stripMargin)

    val nationData = parquetTablePath + "/nation"
    spark.sql(s"DROP TABLE IF EXISTS nation")
    spark.sql(s"""
                 | CREATE TABLE IF NOT EXISTS nation (
                 | n_nationkey bigint,
                 | n_name      string,
                 | n_regionkey bigint,
                 | n_comment   string)
                 | USING PARQUET LOCATION '$nationData'
                 |""".stripMargin)

    val regionData = parquetTablePath + "/region"
    spark.sql(s"DROP TABLE IF EXISTS region")
    spark.sql(s"""
                 | CREATE TABLE IF NOT EXISTS region (
                 | r_regionkey bigint,
                 | r_name      string,
                 | r_comment   string)
                 | USING PARQUET LOCATION '$regionData'
                 |""".stripMargin)

    val ordersData = parquetTablePath + "/orders"
    spark.sql(s"DROP TABLE IF EXISTS orders")
    spark.sql(s"""
                 | CREATE TABLE IF NOT EXISTS orders (
                 | o_orderkey      bigint,
                 | o_custkey       bigint,
                 | o_orderstatus   string,
                 | o_totalprice    double,
                 | o_orderdate     date,
                 | o_orderpriority string,
                 | o_clerk         string,
                 | o_shippriority  bigint,
                 | o_comment       string)
                 | USING PARQUET LOCATION '$ordersData'
                 |""".stripMargin)

    val partData = parquetTablePath + "/part"
    spark.sql(s"DROP TABLE IF EXISTS part")
    spark.sql(s"""
                 | CREATE TABLE IF NOT EXISTS part (
                 | p_partkey     bigint,
                 | p_name        string,
                 | p_mfgr        string,
                 | p_brand       string,
                 | p_type        string,
                 | p_size        bigint,
                 | p_container   string,
                 | p_retailprice double,
                 | p_comment     string)
                 | USING PARQUET LOCATION '$partData'
                 |""".stripMargin)

    val partsuppData = parquetTablePath + "/partsupp"
    spark.sql(s"DROP TABLE IF EXISTS partsupp")
    spark.sql(s"""
                 | CREATE TABLE IF NOT EXISTS partsupp (
                 | ps_partkey    bigint,
                 | ps_suppkey    bigint,
                 | ps_availqty   bigint,
                 | ps_supplycost double,
                 | ps_comment    string)
                 | USING PARQUET LOCATION '$partsuppData'
                 |""".stripMargin)

    val supplierData = parquetTablePath + "/supplier"
    spark.sql(s"DROP TABLE IF EXISTS supplier")
    spark.sql(s"""
                 | CREATE TABLE IF NOT EXISTS supplier (
                 | s_suppkey   bigint,
                 | s_name      string,
                 | s_address   string,
                 | s_nationkey bigint,
                 | s_phone     string,
                 | s_acctbal   double,
                 | s_comment   string)
                 | USING PARQUET LOCATION '$supplierData'
                 |""".stripMargin)

    val result = spark
      .sql(s"""
              | show tables;
              |""".stripMargin)
      .collect()
    assert(result.length == 8)
  }

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
      .set("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
      .set(ClickHouseConfig.CLICKHOUSE_WORKER_ID, "1")
      .set("spark.gluten.sql.columnar.iterator", "true")
      .set("spark.gluten.sql.columnar.hashagg.enablefinal", "true")
      .set("spark.gluten.sql.enable.native.validation", "false")
      .set("spark.sql.warehouse.dir", warehouse)
    /* .set("spark.sql.catalogImplementation", "hive")
      .set("javax.jdo.option.ConnectionURL", s"jdbc:derby:;databaseName=${
        metaStorePathAbsolute + "/metastore_db"};create=true") */
  }

  override protected def afterAll(): Unit = {
    // guava cache invalidate event trigger remove operation may in seconds delay, so wait a bit
    // normally this doesn't take more than 1s
    eventually(timeout(60.seconds), interval(1.seconds)) {
      // Spark listener message was not sent in time with ci env.
      // In tpch case, there are more then 10 hbj data has build.
      // Let's just verify it was cleaned ever.
      assert(CHBroadcastBuildSideCache.size() <= 10)
    }

    ClickhouseSnapshot.clearAllFileStatusCache()
    DeltaLog.clearCache()
    super.afterAll()
  }

  override protected def runTPCHQuery(
      queryNum: Int,
      tpchQueries: String = tpchQueries,
      queriesResults: String = queriesResults,
      compareResult: Boolean = true,
      noFallBack: Boolean = true)(customCheck: DataFrame => Unit): Unit = {
    super.runTPCHQuery(queryNum, tpchQueries, queriesResults, compareResult, noFallBack)(
      customCheck)
  }

  protected def runTPCHQueryBySQL(
      queryNum: Int,
      sqlStr: String,
      queriesResults: String = queriesResults,
      compareResult: Boolean = true,
      noFallBack: Boolean = true)(customCheck: DataFrame => Unit): Unit = withDataFrame(sqlStr) {
    df =>
      if (compareResult) {
        verifyTPCHResult(df, s"q$queryNum", queriesResults)
      } else {
        df.collect()
      }
      checkDataFrame(noFallBack, customCheck, df)
  }

  def q1(tableName: String): String =
    s"""
       |SELECT
       |    l_returnflag,
       |    l_linestatus,
       |    sum(l_quantity) AS sum_qty,
       |    sum(l_extendedprice) AS sum_base_price,
       |    sum(l_extendedprice * (1 - l_discount)) AS sum_disc_price,
       |    sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) AS sum_charge,
       |    avg(l_quantity) AS avg_qty,
       |    avg(l_extendedprice) AS avg_price,
       |    avg(l_discount) AS avg_disc,
       |    count(*) AS count_order
       |FROM
       |    $tableName
       |WHERE
       |    l_shipdate <= date'1998-09-02' - interval 1 day
       |GROUP BY
       |    l_returnflag,
       |    l_linestatus
       |ORDER BY
       |    l_returnflag,
       |    l_linestatus;
       |
       |""".stripMargin

  def q6(tableName: String): String =
    s"""
       |SELECT
       |    sum(l_extendedprice * l_discount) AS revenue
       |FROM
       |    $tableName
       |WHERE
       |    l_shipdate >= date'1994-01-01'
       |    AND l_shipdate < date'1994-01-01' + interval 1 year
       |    AND l_discount BETWEEN 0.06 - 0.01 AND 0.06 + 0.01
       |    AND l_quantity < 24
       |""".stripMargin
}
