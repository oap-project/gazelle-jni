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

import org.apache.gluten.config.GlutenConfig
import org.apache.gluten.execution.{GlutenClickHouseWholeStageTransformerSuite, GlutenPlan, SortExecTransformer}

import org.apache.spark.SparkConf
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.delta.{DeltaLog, DeltaOptions}
import org.apache.spark.sql.execution.{SortExec, SparkPlan}
import org.apache.spark.sql.execution.datasources.parquet.ParquetFileFormat

class DeltaV1WritesSuite extends GlutenClickHouseWholeStageTransformerSuite {

  import testImplicits._

  override protected def sparkConf: SparkConf = {
    super.sparkConf
      .set(GlutenConfig.NATIVE_WRITER_ENABLED.key, "true")
      .set(
        "spark.sql.catalog.spark_catalog",
        "org.apache.spark.sql.execution.datasources.v2.clickhouse.ClickHouseSparkCatalog")
      .set("spark.databricks.delta.maxSnapshotLineageLength", "20")
      .set("spark.databricks.delta.snapshotPartitions", "1")
      .set("spark.databricks.delta.properties.defaults.checkpointInterval", "5")
      .set("spark.databricks.delta.stalenessLimit", "3600000")
      .set("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
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

  protected def checkResult(df: DataFrame, numFileCheck: Long => Boolean, dir: String): Unit = {
    val (_, snapshot) = DeltaLog.forTableWithSnapshot(spark, dir)
    val files = snapshot.numOfFiles
    assert(numFileCheck(files), s"file check failed: received $files")

    checkAnswer(
      spark.read.format("delta").load(dir),
      df
    )
  }

  test("test optimize write") {
    withTempDir {
      dir =>
        val df = sql("select * from t0").toDF()
        df.write
          .format("delta")
          .option(DeltaOptions.OPTIMIZE_WRITE_OPTION, "true")
          .save(dir.getPath)
        checkResult(df, numFileCheck = _ === 1, dir.getPath)
    }
  }

  test("optimize write - partitioned write") {
    withTempDir {
      dir =>
        val df = spark
          .range(0, 100, 1, 4)
          .withColumn("part", 'id % 5)

        df.write
          .partitionBy("part")
          .option(DeltaOptions.OPTIMIZE_WRITE_OPTION, "true")
          .format("delta")
          .save(dir.getPath)

        checkResult(df, numFileCheck = _ <= 5, dir.getPath)
    }
  }

}
