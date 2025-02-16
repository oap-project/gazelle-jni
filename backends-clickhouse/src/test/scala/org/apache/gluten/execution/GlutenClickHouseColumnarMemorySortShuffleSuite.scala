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
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanHelper

class GlutenClickHouseColumnarMemorySortShuffleSuite
  extends GlutenClickHouseTPCHAbstractSuite
  with AdaptiveSparkPlanHelper {

  override protected val tablesPath: String = basePath + "/tpch-data-ch"
  override protected val tpchQueries: String = rootPath + "queries/tpch-queries-ch"
  override protected val queriesResults: String = rootPath + "mergetree-queries-output"

  /** Run Gluten + ClickHouse Backend with ColumnarShuffleManager */
  override protected def sparkConf: SparkConf = {
    super.sparkConf
      .set("spark.shuffle.manager", "org.apache.spark.shuffle.sort.ColumnarShuffleManager")
      .set("spark.io.compression.codec", "LZ4")
      .set("spark.sql.shuffle.partitions", "5")
      .set("spark.sql.autoBroadcastJoinThreshold", "10MB")
      .set("spark.sql.adaptive.enabled", "true")
      .set(CHConfig.prefixOf("forceMemorySortShuffle"), "true")

    // TODO: forceMemorySortShuffle
  }

  test("TPCH Q1") {
    runTPCHQuery(1) { df => }
  }

  test("TPCH Q2") {
    runTPCHQuery(2) { df => }
  }

  test("TPCH Q3") {
    runTPCHQuery(3) { df => }
  }

  test("TPCH Q4") {
    runTPCHQuery(4) { df => }
  }

  test("TPCH Q5") {
    runTPCHQuery(5) { df => }
  }

  test("TPCH Q6") {
    runTPCHQuery(6) { df => }
  }

  test("TPCH Q7") {
    runTPCHQuery(7) { df => }
  }

  test("TPCH Q8") {
    runTPCHQuery(8) { df => }
  }

  test("TPCH Q9") {
    runTPCHQuery(9) { df => }
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
    runTPCHQuery(14) { df => }
  }

  test("TPCH Q15") {
    runTPCHQuery(15) { df => }
  }

  test("TPCH Q16") {
    runTPCHQuery(16) { df => }
  }

  test("TPCH Q17") {
    runTPCHQuery(17) { df => }
  }

  test("TPCH Q18") {
    runTPCHQuery(18) { df => }
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
}
