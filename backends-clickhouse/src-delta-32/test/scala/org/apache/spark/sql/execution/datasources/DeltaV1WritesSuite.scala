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
package org.apache.spark.sql.execution.datasources

import org.apache.gluten.GlutenConfig
import org.apache.gluten.execution.{GlutenClickHouseWholeStageTransformerSuite, GlutenPlan, SortExecTransformer}
import org.apache.spark.SparkConf
import org.apache.spark.sql.execution.datasources.parquet.ParquetFileFormat
import org.apache.spark.sql.execution.{SortExec, SparkPlan}

class DeltaV1WritesSuite extends GlutenClickHouseWholeStageTransformerSuite {

  import testImplicits._

  override protected def sparkConf: SparkConf = {
    super.sparkConf
      .set(GlutenConfig.NATIVE_WRITER_ENABLED.key, "true")
  }

  override def beforeAll(): Unit = {
    super.beforeAll()
    (0 to 20)
      .map(i => (i, i % 5, (i % 10).toString))
      .toDF("i", "j", "k")
      .write
      .saveAsTable("t0")
  }

  override def afterAll(): Unit = {
    sql("drop table if exists t0")
    super.afterAll()
  }

  val format = new ParquetFileFormat
  def getSort(child: SparkPlan): Option[SortExecTransformer] = {
    child.collectFirst { case w: SortExecTransformer => w }
  }
  test("don't add sort when the required ordering is empty") {
    val df = sql("select * from t0")
    val plan = df.queryExecution.sparkPlan
    val writes = DeltaV1Writes(spark, plan, format, Nil, None, Map.empty)
    assert(writes.sortPlan === plan)
    assert(writes.writePlan != null)
    assert(writes.executedPlan.isInstanceOf[GlutenPlan])
    val writeFilesOpt = V1WritesUtils.getWriteFilesOpt(writes.executedPlan)
    assert(writeFilesOpt.isDefined)
    val sortExec = getSort(writes.executedPlan)
    assert(sortExec.isEmpty)
  }

  test("don't add sort when the required ordering is already satisfied") {
    val df = sql("select * from t0")
    def check(plan: SparkPlan): Unit = {
      val partitionColumns = plan.output.find(_.name == "k").toSeq
      val writes = DeltaV1Writes(spark, plan, format, partitionColumns, None, Map.empty)
      assert(writes.sortPlan === plan)
      assert(writes.writePlan != null)
      assert(writes.executedPlan.isInstanceOf[GlutenPlan])
      val writeFilesOpt = V1WritesUtils.getWriteFilesOpt(writes.executedPlan)
      assert(writeFilesOpt.isDefined)
      val sortExec = getSort(writes.executedPlan)
      assert(sortExec.isDefined)
    }
    check(df.orderBy("k").queryExecution.sparkPlan)
    check(df.orderBy("k", "j").queryExecution.sparkPlan)
  }

  test("add sort when the required ordering is not satisfied") {
    val df = sql("select * from t0")
    def check(plan: SparkPlan): Unit = {
      val partitionColumns = plan.output.find(_.name == "k").toSeq
      val writes = DeltaV1Writes(spark, plan, format, partitionColumns, None, Map.empty)
      val sort = writes.sortPlan.asInstanceOf[SortExec]
      assert(sort.child === plan)
      assert(writes.writePlan != null)
      assert(writes.executedPlan.isInstanceOf[GlutenPlan])
      val writeFilesOpt = V1WritesUtils.getWriteFilesOpt(writes.executedPlan)
      assert(writeFilesOpt.isDefined)
      val sortExec = getSort(writes.executedPlan)
      assert(sortExec.isDefined, s"writes.executedPlan: ${writes.executedPlan}")
    }
    check(df.queryExecution.sparkPlan)
    check(df.orderBy("j", "k").queryExecution.sparkPlan)
  }

}
