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
import org.apache.spark.sql.catalyst.expressions.Alias
import org.apache.spark.sql.catalyst.optimizer.{BuildLeft, BuildRight}

class GlutenClickHouseTPCHNullableSuite extends GlutenClickHouseTPCHAbstractSuite {

  override protected val createNullableTables = true

  override protected val tablesPath: String = basePath + "/tpch-data-ch"
  override protected val tpchQueries: String = rootPath + "queries/tpch-queries-ch"
  override protected val queriesResults: String = rootPath + "mergetree-queries-output"

  /** Run Gluten + ClickHouse Backend with SortShuffleManager */
  override protected def sparkConf: SparkConf = {
    super.sparkConf
      .set("spark.shuffle.manager", "sort")
      .set("spark.io.compression.codec", "SNAPPY")
      .set("spark.sql.shuffle.partitions", "5")
      .set("spark.sql.autoBroadcastJoinThreshold", "10MB")
  }

  test("TPCH Q1") {
    runTPCHQuery(1) {
      df =>
        val scanExec = df.queryExecution.executedPlan.collect {
          case scanExec: BasicScanExecTransformer => true
        }
        assert(scanExec.size == 1)
    }
  }

  test("TPCH Q2") {
    runTPCHQuery(2) {
      df =>
        val scanExec = df.queryExecution.executedPlan.collect {
          case scanExec: BasicScanExecTransformer => scanExec
        }
        assert(scanExec.size == 8)
    }
  }

  test("TPCH Q3") {
    withSQLConf(("spark.sql.autoBroadcastJoinThreshold", "-1")) {
      runTPCHQuery(3) {
        df =>
          val shjBuildLeft = df.queryExecution.executedPlan.collect {
            case shj: ShuffledHashJoinExecTransformerBase if shj.joinBuildSide == BuildLeft => shj
          }
          assert(shjBuildLeft.size == 1)
          val shjBuildRight = df.queryExecution.executedPlan.collect {
            case shj: ShuffledHashJoinExecTransformerBase if shj.joinBuildSide == BuildRight => shj
          }
          assert(shjBuildRight.size == 1)
      }
    }
  }

  test("TPCH Q4") {
    runTPCHQuery(4) { df => }
  }

  test("TPCH Q5") {
    withSQLConf(("spark.sql.autoBroadcastJoinThreshold", "-1")) {
      runTPCHQuery(5) {
        df =>
          val bhjRes = df.queryExecution.executedPlan.collect {
            case bhj: BroadcastHashJoinExecTransformerBase => bhj
          }
          assert(bhjRes.isEmpty)
      }
    }
  }

  test("TPCH Q6") {
    runTPCHQuery(6) { df => }
  }

  test("TPCH Q7") {
    withSQLConf(
      ("spark.sql.shuffle.partitions", "1"),
      ("spark.sql.autoBroadcastJoinThreshold", "-1")) {
      runTPCHQuery(7) { df => }
    }
  }

  test("TPCH Q8") {
    withSQLConf(
      ("spark.sql.shuffle.partitions", "1"),
      ("spark.sql.autoBroadcastJoinThreshold", "-1")) {
      runTPCHQuery(8) { df => }
    }
  }

  test("TPCH Q9") {
    runTPCHQuery(9) { df => }
  }

  test("TPCH Q9 without BHJ") {
    withSQLConf(("spark.sql.autoBroadcastJoinThreshold", "-1")) {
      runTPCHQuery(9) { df => }
    }
  }

  test("TPCH Q10") {
    runTPCHQuery(10) { df => }
  }

  test("TPCH Q11") {
    runTPCHQuery(11) { df => }
  }

  test("TPCH Q12") {
    runTPCHQuery(12) { df => }
  }

  test("TPCH Q13") {
    runTPCHQuery(13) { df => }
  }

  test("TPCH Q14") {
    withSQLConf(
      ("spark.sql.shuffle.partitions", "1"),
      ("spark.sql.autoBroadcastJoinThreshold", "-1")) {
      runTPCHQuery(14) { df => }
    }
  }

  test("TPCH Q15") {
    runTPCHQuery(15) { df => }
  }

  test("TPCH Q16") {
    runTPCHQuery(16) { df => }
  }

  test("TPCH Q17") {
    withSQLConf(("spark.shuffle.sort.bypassMergeThreshold", "2")) {
      runTPCHQuery(17) { df => }
    }
  }

  test("TPCH Q18") {
    withSQLConf(("spark.shuffle.sort.bypassMergeThreshold", "2")) {
      runTPCHQuery(18) { df => }
    }
  }

  test("TPCH Q19") {
    runTPCHQuery(19) { df => }
  }

  test("TPCH Q20") {
    runTPCHQuery(20) { df => }
  }

  test("TPCH Q21") {
    runTPCHQuery(21) { df => }
  }

  test("TPCH Q22") {
    runTPCHQuery(22) { df => }
  }

  test("test 'select count(*) from table'") {
    val result = runSql("""
                          |select count(*) from lineitem
                          |""".stripMargin) { _ => }
  }

  test("test 'select count(*)'") {
    val result = runSql("""
                          |select count(*) from lineitem
                          |where l_quantity < 24
                          |""".stripMargin) { _ => }
    assert(result(0).getLong(0) == 275436L)
  }

  test("test 'select count(1)'") {
    val result = runSql("""
                          |select count(1) from lineitem
                          |where l_quantity < 20
                          |""".stripMargin) { _ => }
    assert(result(0).getLong(0) == 227302L)
  }

  test("test 'GLUTEN-5016'") {
    withSQLConf(("spark.gluten.sql.columnar.preferColumnar", "false")) {
      val sql =
        """
          |SELECT
          |   sum(l_quantity) AS sum_qty
          |FROM
          |   lineitem
          |WHERE
          |   l_shipdate <= date'1998-09-02'
          |""".stripMargin
      runSql(sql, noFallBack = true) { _ => }
    }
  }

  test("test rewrite date conversion") {
    val sqlStr =
      """
        |SELECT
        |to_date(
        |  from_unixtime(
        |    unix_timestamp(date_format(l_shipdate, 'yyyyMMdd'), 'yyyyMMdd')
        |  )
        |)
        |FROM lineitem
        |limit 10
        |""".stripMargin

    Seq(("true", false), ("false", true)).foreach(
      conf => {
        withSQLConf((CHConfig.ENABLE_CH_REWRITE_DATE_CONVERSION.key, conf._1)) {
          runSql(sqlStr)(
            df => {
              val project = df.queryExecution.executedPlan.collect {
                case project: ProjectExecTransformer => project
              }
              assert(project.size == 1)
              assert(
                project
                  .apply(0)
                  .projectList(0)
                  .asInstanceOf[Alias]
                  .child
                  .toString()
                  .contains("from_unixtime") == conf._2)
            })
        }
      })
  }
}
