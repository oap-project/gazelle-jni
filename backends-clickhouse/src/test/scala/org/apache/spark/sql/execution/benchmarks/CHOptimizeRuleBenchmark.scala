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
package org.apache.spark.sql.execution.benchmarks

import org.apache.gluten.backendsapi.clickhouse.CHConfig

import org.apache.spark.benchmark.Benchmark
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.benchmark.SqlBasedBenchmark

object CHOptimizeRuleBenchmark extends SqlBasedBenchmark with CHSqlBasedBenchmark {

  protected lazy val appName = "CHOptimizeRuleBenchmark"
  protected lazy val thrdNum = "1"
  protected lazy val memorySize = "4G"
  protected lazy val offheapSize = "4G"

  def beforeAll(): Unit = {}

  override def getSparkSession: SparkSession = {
    beforeAll()
    val conf = getSparkConf
      .setIfMissing("spark.sql.columnVector.offheap.enabled", "true")

    SparkSession.builder.config(conf).getOrCreate()
  }

  override def runBenchmarkSuite(mainArgs: Array[String]): Unit = {
    val (parquetDir, readFileCnt, scanSchema, executedCnt, executedVanilla) =
      if (mainArgs.isEmpty) {
        ("/data/tpch-data-sf1/parquet/lineitem", 3, "l_orderkey,l_receiptdate", 5, true)
      } else {
        (mainArgs(0), mainArgs(1).toInt, mainArgs(2), mainArgs(3).toInt, mainArgs(4).toBoolean)
      }

    val parquetReadBenchmark =
      new Benchmark(s"OptimizeRuleBenchmark", 10, output = output)

    parquetReadBenchmark.addCase(s"ClickHouse rewrite dateConversion: false", executedCnt) {
      _ => testToDateOptimize(parquetDir, "false")
    }

    parquetReadBenchmark.addCase(s"ClickHouse rewrite dateConversion: true", executedCnt) {
      _ => testToDateOptimize(parquetDir, "true")
    }

    parquetReadBenchmark.run()
  }

  def testToDateOptimize(parquetDir: String, enable: String): Unit = {
    withSQLConf((CHConfig.prefixOf("rewrite.dateConversion"), enable)) {
      spark
        .sql(s"""
                |select
                |to_date(
                |  from_unixtime(
                |    unix_timestamp(date_format(l_shipdate, 'yyyyMMdd'), 'yyyyMMdd')
                |  )
                |)
                |from parquet.`$parquetDir`
                |
                |""".stripMargin)
        .collect()
    }
  }
}
