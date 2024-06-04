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
package org.apache.spark.sql.gluten.parquet

import org.apache.gluten.execution.{FileSourceScanExecTransformer, GlutenClickHouseWholeStageTransformerSuite}
import org.apache.gluten.utils.UTSystemParameters

import org.apache.spark.SparkConf
import org.apache.spark.internal.Logging
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.gluten.test.GlutenSQLTestUtils
import org.apache.spark.sql.internal.SQLConf

case class ParquetData(
    column: String,
    parquetDir: String,
    filter: String,
    scanOutput: Long,
    title: Option[String] = None)

class GlutenParquetColumnIndexSuite
  extends GlutenClickHouseWholeStageTransformerSuite
  with GlutenSQLTestUtils
  with Logging {

  override protected val fileFormat: String = "parquet"
  private val testPath: String = s"${UTSystemParameters.testDataPath}/$fileFormat"

  // TODO: we need refactor compareResultsAgainstVanillaSpark to make customCheck accept
  //  both gluten and vanilla spark dataframe
  private val parquetData = Seq(
    ParquetData(
      "count(*)",
      "index/tpch/20003",
      "`27` <> '1-URGENT' and `9` >= '1995-01-01' and `9` < '1996-01-01' ",
      140000),
    ParquetData(
      "count(*)",
      "index/tpch/upper_case",
      "c_comment = '! requests wake. (...)ructions. furiousl'",
      12853),
    ParquetData(
      "*",
      "index/pageindex/query102",
      "`198` = 'Crafts' or `198` = 'Computers' or `198`= 'a' or `198`= ''",
      45),
    ParquetData(
      "count(*)",
      "index/pageindex/query102",
      "`100001` < 30000  and `100001` > 1000.004",
      45,
      Some("push down Decimal filter")),
    ParquetData(
      "count(*)",
      "index/pageindex/query102",
      "`100001` in (30000, 1000.004, 45000, 2323445, 4235423.6, 4546677.245, 56677.5)",
      45,
      Some("push down Decimal filter In")
    ),
    ParquetData("count(*)", "index/pageindex/query05", "`142` = true", 9896)
  )

  parquetData.foreach {
    data =>
      test(data.title.getOrElse(data.parquetDir)) {
        val parquetDir = s"$testPath/${data.parquetDir}"
        val sql1 = s"""|select ${data.column} from $fileFormat.`$parquetDir`
                       |where ${data.filter}
                       |""".stripMargin
        compareResultsAgainstVanillaSpark(
          sql1,
          compareResult = true,
          checkScanOutput(data.scanOutput, _))
      }
  }

  private def checkScanOutput(scanOutput: Long, df: DataFrame): Unit = {
    val chScanPlan = df.queryExecution.executedPlan.collect {
      case scan: FileSourceScanExecTransformer => scan
    }
    assertResult(1)(chScanPlan.length)
    val chFileScan = chScanPlan.head
    assertResult(scanOutput)(chFileScan.longMetric("numOutputRows").value)
  }
  override protected def sparkConf: SparkConf =
    super.sparkConf
      .set(SQLConf.ADAPTIVE_EXECUTION_ENABLED, false)
      .set("spark.gluten.sql.columnar.backend.ch.runtime_config.use_local_format", "true")
}
