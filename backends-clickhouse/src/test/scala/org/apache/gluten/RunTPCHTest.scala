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
package org.apache.gluten

import org.apache.gluten.backendsapi.clickhouse.RuntimeConfig
import org.apache.gluten.benchmarks.GenTPCHTableScripts

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.datasources.v2.clickhouse.ClickHouseConfig

import org.apache.commons.io.FileUtils

import java.io.File

import scala.collection.mutable.ArrayBuffer
import scala.io.Source

// scalastyle:off
object RunTPCHTest {

  def main(args: Array[String]): Unit = {
    // parquet or mergetree
    val fileFormat = "parquet"
    val libPath = "/usr/local/clickhouse/lib/libch.so"
    if (!new File(libPath).exists()) System.exit(1)
    // TPCH data files path
    val dataFilesPath = "/data/tpch-data/" + fileFormat
    if (!new File(dataFilesPath).exists()) System.exit(1)
    // the time of execution
    val executedCnt = 5
    // local thread count
    val thrdCnt = 3
    val shufflePartitions = 6
    val shuffleManager = "org.apache.spark.shuffle.sort.ColumnarShuffleManager"
    val ioCompressionCodec = "LZ4"
    val columnarColumnToRow = "true"
    val driverMemory = "10G"
    val offHeapSize = "10G"
    val rootPath = this.getClass.getResource("/").getPath
    val basePath = rootPath + "tests-working-home"
    val warehouse = basePath + "/spark-warehouse"
    val metaStorePathAbsolute = basePath + "/meta"
    val hiveMetaStoreDB = metaStorePathAbsolute + "/metastore_db"

    // create dir
    val basePathDir = new File(basePath)
    if (basePathDir.exists()) {
      FileUtils.forceDelete(basePathDir)
    }
    FileUtils.forceMkdir(basePathDir)
    FileUtils.forceMkdir(new File(warehouse))
    FileUtils.forceMkdir(new File(metaStorePathAbsolute))

    val resourcePath = rootPath + "../../../../tools/gluten-it/common/src/main/resources/"
    val queryPath = resourcePath + "/tpch-queries/"
    // which sql to execute
    val sqlFilePath = queryPath + "q01.sql"
    val sqlStr = Source.fromFile(new File(sqlFilePath), "UTF-8")

    val spark = SparkSession
      .builder()
      .appName("Gluten-TPCH-Test")
      .master(s"local[$thrdCnt]")
      .config("spark.driver.memory", driverMemory)
      .config("spark.sql.shuffle.partitions", shufflePartitions)
      .config("spark.sql.files.maxPartitionBytes", 1024 << 10 << 10) // default is 128M
      .config("spark.sql.files.openCostInBytes", 1024 << 10 << 10) // default is 4M
      // .config("spark.sql.sources.useV1SourceList", "avro")
      .config("spark.memory.fraction", "0.6")
      .config("spark.memory.storageFraction", "0.3")
      .config("spark.plugins", "org.apache.gluten.GlutenPlugin")
      .config(
        "spark.sql.catalog.spark_catalog",
        "org.apache.spark.sql.execution.datasources.v2.clickhouse.ClickHouseSparkCatalog")
      .config("spark.shuffle.manager", shuffleManager)
      .config("spark.io.compression.codec", ioCompressionCodec)
      .config("spark.databricks.delta.maxSnapshotLineageLength", 20)
      .config("spark.databricks.delta.snapshotPartitions", 1)
      .config("spark.databricks.delta.properties.defaults.checkpointInterval", 5)
      .config("spark.databricks.delta.stalenessLimit", 3600 * 1000)
      .config("spark.gluten.sql.columnar.columnarToRow", columnarColumnToRow)
      .config(ClickHouseConfig.CLICKHOUSE_WORKER_ID, "1")
      .config(GlutenConfig.GLUTEN_LIB_PATH, libPath)
      .config("spark.gluten.sql.columnar.iterator", "true")
      .config("spark.gluten.sql.columnar.hashagg.enablefinal", "true")
      .config("spark.gluten.sql.enable.native.validation", "false")
      .config("spark.sql.columnVector.offheap.enabled", "true")
      .config("spark.memory.offHeap.enabled", "true")
      .config("spark.memory.offHeap.size", offHeapSize)
      .config(RuntimeConfig.LOGGER_LEVEL.key, "error")
      .config("spark.sql.warehouse.dir", warehouse)
      .config(
        "javax.jdo.option.ConnectionURL",
        s"jdbc:derby:;databaseName=$hiveMetaStoreDB;create=true")
      .enableHiveSupport()
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    createParquetTables(spark, fileFormat, dataFilesPath)
    testTPCHOne(spark, sqlStr.mkString, executedCnt)
  }

  def createParquetTables(spark: SparkSession, fileFormat: String, dataFilesPath: String): Unit = {
    val bucketSQL = if (fileFormat.equalsIgnoreCase("parquet")) {
      GenTPCHTableScripts.genTPCHParquetTables(dataFilesPath)
    } else {
      GenTPCHTableScripts.genTPCHMergeTreeTables(dataFilesPath)
    }

    for (sql <- bucketSQL) {
      spark.sql(sql)
    }
  }

  def testTPCHOne(spark: SparkSession, sqlStr: String, executedCnt: Int): Unit = {
    spark
      .sql(s"""
              |use default;
              |""".stripMargin)
    try {
      val tookTimeArr = ArrayBuffer[Long]()
      for (i <- 1 to executedCnt) {
        val startTime = System.nanoTime()
        val df = spark.sql(sqlStr)
        val result = df.collect()
        df.explain(false)
        println(result.length)
        result.foreach(r => println(r.mkString(",")))
        val tookTime = (System.nanoTime() - startTime) / 1000000
        println(s"Execute $i time, time: $tookTime")
        tookTimeArr += tookTime
      }

      println(tookTimeArr.mkString(","))
    } catch {
      case e: Exception => e.printStackTrace()
    }
  }
}
// scalastyle:on
