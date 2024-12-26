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
import org.apache.spark.sql.execution.CoalescedPartitionSpec
import org.apache.spark.sql.execution.adaptive.{AdaptiveSparkPlanExec, AdaptiveSparkPlanHelper, AQEShuffleReadExec}
import org.apache.spark.sql.internal.SQLConf

case class TestData(id: Int)

class GlutenClickHouseRSSColumnarShuffleAQESuite
  extends GlutenClickHouseTPCHAbstractSuite
  with AdaptiveSparkPlanHelper {

  override protected val tablesPath: String = basePath + "/tpch-data-ch"
  override protected val tpchQueries: String = rootPath + "queries/tpch-queries-ch"
  override protected val queriesResults: String =
    rootPath + "../../../../backends-clickhouse/src/test/resources/mergetree-queries-output"

  override protected val parquetTableDataPath: String =
    "../../../../gluten-core/src/test/resources/tpch-data"

  /** Run Gluten + ClickHouse Backend with ColumnarShuffleManager */
  override protected def sparkConf: SparkConf = {
    super.sparkConf
      .set(
        "spark.shuffle.manager",
        "org.apache.spark.shuffle.gluten.celeborn.CelebornShuffleManager")
      .set("spark.io.compression.codec", "LZ4")
      .set("spark.sql.shuffle.partitions", "5")
      .set("spark.sql.autoBroadcastJoinThreshold", "10MB")
      .set("spark.sql.adaptive.enabled", "true")
      .set("spark.shuffle.service.enabled", "false")
      .set("spark.celeborn.client.spark.shuffle.writer", "hash")
  }

  test("TPCH Q1") {
    runTPCHQuery(1) {
      df =>
        assert(df.queryExecution.executedPlan.isInstanceOf[AdaptiveSparkPlanExec])

        val colCustomShuffleReaderExecs = collect(df.queryExecution.executedPlan) {
          case csr: AQEShuffleReadExec => csr
        }
        assert(colCustomShuffleReaderExecs.size == 2)
        val coalescedPartitionSpec0 = colCustomShuffleReaderExecs(0)
          .partitionSpecs(0)
          .asInstanceOf[CoalescedPartitionSpec]
        assert(coalescedPartitionSpec0.startReducerIndex == 0)
        assert(coalescedPartitionSpec0.endReducerIndex == 4)
        val coalescedPartitionSpec1 = colCustomShuffleReaderExecs(1)
          .partitionSpecs(0)
          .asInstanceOf[CoalescedPartitionSpec]
        assert(coalescedPartitionSpec1.startReducerIndex == 0)
        assert(coalescedPartitionSpec1.endReducerIndex == 5)
    }
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
    runTPCHQuery(11) {
      df =>
        assert(df.queryExecution.executedPlan.isInstanceOf[AdaptiveSparkPlanExec])
        val adaptiveSparkPlanExec = collectWithSubqueries(df.queryExecution.executedPlan) {
          case adaptive: AdaptiveSparkPlanExec => adaptive
        }
        assert(adaptiveSparkPlanExec.size == 2)
    }
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
    runTPCHQuery(15) {
      df =>
        assert(df.queryExecution.executedPlan.isInstanceOf[AdaptiveSparkPlanExec])
        val adaptiveSparkPlanExec = collectWithSubqueries(df.queryExecution.executedPlan) {
          case adaptive: AdaptiveSparkPlanExec => adaptive
        }
        assert(adaptiveSparkPlanExec.size == 2)
    }
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
    runTPCHQuery(22) {
      df =>
        assert(df.queryExecution.executedPlan.isInstanceOf[AdaptiveSparkPlanExec])
        val adaptiveSparkPlanExec = collectWithSubqueries(df.queryExecution.executedPlan) {
          case adaptive: AdaptiveSparkPlanExec => adaptive
        }
        assert(adaptiveSparkPlanExec.size == 3)
        assert(adaptiveSparkPlanExec(1) == adaptiveSparkPlanExec(2))
    }
  }

  test("fix partiton_id when spill_to_celeborn") {
    import testImplicits._
    withSQLConf(
      SQLConf.COALESCE_PARTITIONS_INITIAL_PARTITION_NUM.key -> "5",
      SQLConf.ADVISORY_PARTITION_SIZE_IN_BYTES.key -> "10B",
      SQLConf.COALESCE_PARTITIONS_MIN_PARTITION_NUM.key -> "2",
      SQLConf.COALESCE_PARTITIONS_MIN_PARTITION_SIZE.key -> "1B"
    ) {
      val df = spark.sparkContext
        .parallelize(
          TestData(0) ::
            TestData(0) ::
            TestData(1) ::
            TestData(1) ::
            TestData(2) ::
            TestData(2) :: Nil,
          3)
        .toDF()
      df.createOrReplaceTempView("t")
      val res = spark.sql("select spark_partition_id(), id from t group by id").collect()
      assert(res.length == 3)
      assert(res(0).getInt(0) == 0)
      assert(res(0).getInt(1) == 0)
      assert(res(1).getInt(0) == 1)
      assert(res(1).getInt(1) == 1)
      assert(res(2).getInt(0) == 2)
      assert(res(2).getInt(1) == 2)
    }
  }
}
