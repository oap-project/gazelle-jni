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
package org.apache.spark.sql.execution.benchmark

import org.apache.gluten.GlutenConfig
import org.apache.gluten.execution.Table
import org.apache.gluten.utils.Arm

import org.apache.spark.benchmark.Benchmark
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.internal.SQLConf

import java.io.File

import scala.concurrent.duration.DurationInt
import scala.io.Source

/**
 * The benchmark measures on RAS query optimization performance only. Performance of query execution
 * is not considered.
 */
object VeloxRasBenchmark extends SqlBasedBenchmark {
  private val tpchQueries: String =
    getClass
      .getResource("/")
      .getPath + "../../../../tools/gluten-it/common/src/main/resources/tpch-queries"
  private val dataDirPath: String =
    getClass
      .getResource("/tpch-data-parquet")
      .getFile

  private val tpchTables: Seq[Table] = Seq(
    Table("part", partitionColumns = "p_brand" :: Nil),
    Table("supplier", partitionColumns = Nil),
    Table("partsupp", partitionColumns = Nil),
    Table("customer", partitionColumns = "c_mktsegment" :: Nil),
    Table("orders", partitionColumns = "o_orderdate" :: Nil),
    Table("lineitem", partitionColumns = "l_shipdate" :: Nil),
    Table("nation", partitionColumns = Nil),
    Table("region", partitionColumns = Nil)
  )

  private def sessionBuilder() = {
    SparkSession
      .builder()
      .master("local[1]")
      .appName(this.getClass.getCanonicalName)
      .config(SQLConf.SHUFFLE_PARTITIONS.key, 1)
      .config(SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key, 1)
      .config("spark.plugins", "org.apache.gluten.GlutenPlugin")
      .config("spark.shuffle.manager", "org.apache.spark.shuffle.sort.ColumnarShuffleManager")
      .config("spark.ui.enabled", "false")
      .config("spark.gluten.ui.enabled", "false")
      .config("spark.memory.offHeap.enabled", "true")
      .config("spark.memory.offHeap.size", "2g")
      .config("spark.sql.adaptive.enabled", "false")
  }

  private def createLegacySession(): SparkSession = {
    SparkSession.cleanupAnyExistingSession()
    sessionBuilder()
      .config(GlutenConfig.RAS_ENABLED.key, false)
      .getOrCreate()
  }

  private def createRasSession(): SparkSession = {
    SparkSession.cleanupAnyExistingSession()
    sessionBuilder()
      .config(GlutenConfig.RAS_ENABLED.key, true)
      .getOrCreate()
  }

  private def createTpchTables(spark: SparkSession): Unit = {
    tpchTables
      .map(_.name)
      .map {
        table =>
          val tablePath = new File(dataDirPath, table).getAbsolutePath
          val tableDF = spark.read.format("parquet").load(tablePath)
          tableDF.createOrReplaceTempView(table)
          (table, tableDF)
      }
      .toMap
  }

  private def tpchSQL(queryId: String): String =
    Arm.withResource(Source.fromFile(new File(s"$tpchQueries/$queryId.sql"), "UTF-8"))(_.mkString)

  private val allQueryIds: Seq[String] = Seq(
    "q1",
    "q2",
    "q3",
    "q4",
    "q5",
    "q6",
    "q7",
    "q8",
    "q9",
    "q10",
    "q11",
    "q12",
    "q13",
    "q14",
    "q15",
    "q16",
    "q17",
    "q18",
    "q19",
    "q20",
    "q21",
    "q22"
  )

  override def runBenchmarkSuite(mainArgs: Array[String]): Unit = {
    val benchmark = new Benchmark(
      this.getClass.getCanonicalName,
      allQueryIds.size,
      output = output,
      warmupTime = 15.seconds,
      minTime = 60.seconds)
    benchmark.addTimerCase("RAS Planner") {
      timer =>
        val spark = createRasSession()
        createTpchTables(spark)
        timer.startTiming()
        allQueryIds.foreach {
          id =>
            val p = spark.sql(tpchSQL(id)).queryExecution.executedPlan
            // scalastyle:off println
            println("[RAS] Optimized query plan: " + p.toString())
            // scalastyle:on println
        }
        timer.stopTiming()
    }
    benchmark.addTimerCase("Legacy Planner") {
      timer =>
        val spark = createLegacySession()
        createTpchTables(spark)
        timer.startTiming()
        allQueryIds.foreach {
          id =>
            val p = spark.sql(tpchSQL(id)).queryExecution.executedPlan
            // scalastyle:off println
            println("[Legacy] Optimized query plan: " + p.toString())
            // scalastyle:on println
        }
        timer.stopTiming()
    }
    benchmark.run()
  }
}
