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
package org.apache.spark.sql.hive.execution

import org.apache.gluten.execution.CustomerUDF

import org.apache.spark.{SparkConf, SparkContext, SparkFunSuite}
import org.apache.spark.internal.config
import org.apache.spark.internal.config.UI.UI_ENABLED
import org.apache.spark.sql.{GlutenTestsBaseTrait, QueryTest, SparkSession}
import org.apache.spark.sql.catalyst.expressions.CodegenObjectFactoryMode
import org.apache.spark.sql.catalyst.optimizer.ConvertToLocalRelation
import org.apache.spark.sql.hive.{HiveExternalCatalog, HiveUtils}
import org.apache.spark.sql.hive.client.HiveClient
import org.apache.spark.sql.hive.test.TestHiveContext
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.internal.StaticSQLConf.WAREHOUSE_PATH
import org.apache.spark.sql.test.SQLTestUtils

import org.scalatest.BeforeAndAfterAll

import java.io.File

trait GlutenTestHiveSingleton extends SparkFunSuite with BeforeAndAfterAll {
  override protected val enableAutoThreadAudit = false

}

object GlutenTestHive
  extends TestHiveContext(
    new SparkContext(
      System.getProperty("spark.sql.test.master", "local[1]"),
      "TestSQLContext",
      new SparkConf()
        .set("spark.sql.test", "")
        .set(SQLConf.CODEGEN_FALLBACK.key, "false")
        .set(SQLConf.CODEGEN_FACTORY_MODE.key, CodegenObjectFactoryMode.CODEGEN_ONLY.toString)
        .set(
          HiveUtils.HIVE_METASTORE_BARRIER_PREFIXES.key,
          "org.apache.spark.sql.hive.execution.PairSerDe")
        .set(WAREHOUSE_PATH.key, TestHiveContext.makeWarehouseDir().toURI.getPath)
        // SPARK-8910
        .set(UI_ENABLED, false)
        .set(config.UNSAFE_EXCEPTION_ON_MEMORY_LEAK, true)
        // Hive changed the default of hive.metastore.disallow.incompatible.col.type.changes
        // from false to true. For details, see the JIRA HIVE-12320 and HIVE-17764.
        .set("spark.hadoop.hive.metastore.disallow.incompatible.col.type.changes", "false")
        .set("spark.driver.memory", "1G")
        .set("spark.sql.adaptive.enabled", "true")
        .set("spark.sql.shuffle.partitions", "1")
        .set("spark.sql.files.maxPartitionBytes", "134217728")
        .set("spark.memory.offHeap.enabled", "true")
        .set("spark.memory.offHeap.size", "1024MB")
        .set("spark.plugins", "org.apache.gluten.GlutenPlugin")
        .set("spark.shuffle.manager", "org.apache.spark.shuffle.sort.ColumnarShuffleManager")
        // Disable ConvertToLocalRelation for better test coverage. Test cases built on
        // LocalRelation will exercise the optimization rules better by disabling it as
        // this rule may potentially block testing of other optimization rules such as
        // ConstantPropagation etc.
        .set(SQLConf.OPTIMIZER_EXCLUDED_RULES.key, ConvertToLocalRelation.ruleName)
    ),
    false
  ) {}

class GlutenHiveUDFSuite
  extends QueryTest
  with GlutenTestHiveSingleton
  with SQLTestUtils
  with GlutenTestsBaseTrait {
  override protected lazy val spark: SparkSession = GlutenTestHive.sparkSession
  protected lazy val hiveContext: TestHiveContext = GlutenTestHive
  protected lazy val hiveClient: HiveClient =
    spark.sharedState.externalCatalog.unwrapped.asInstanceOf[HiveExternalCatalog].client

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    val table = "lineitem"
    val tableDir =
      getClass.getResource("").getPath + "/../../../../../../../../../../../" +
        "/backends-velox/src/test/resources/tpch-data-parquet/"
    val tablePath = new File(tableDir, table).getAbsolutePath
    val tableDF = spark.read.format("parquet").load(tablePath)
    tableDF.createOrReplaceTempView(table)
  }

  override protected def afterAll(): Unit = {
    try {
      hiveContext.reset()
    } finally {
      super.afterAll()
    }
  }

  override protected def shouldRun(testName: String): Boolean = {
    false
  }

  test("customer udf") {
    sql(s"CREATE TEMPORARY FUNCTION testUDF AS '${classOf[CustomerUDF].getName}'")
    val df = spark.sql("""select testUDF(l_comment)
                         | from   lineitem""".stripMargin)
    df.show()
    print(df.queryExecution.executedPlan)
    sql("DROP TEMPORARY FUNCTION IF EXISTS testUDF")
    hiveContext.reset()
  }

  test("customer udf wrapped in function") {
    sql(s"CREATE TEMPORARY FUNCTION testUDF AS '${classOf[CustomerUDF].getName}'")
    val df = spark.sql("""select hash(testUDF(l_comment))
                         | from   lineitem""".stripMargin)
    df.show()
    print(df.queryExecution.executedPlan)
    sql("DROP TEMPORARY FUNCTION IF EXISTS testUDF")
    hiveContext.reset()
  }

  test("example") {
    spark.sql("CREATE TEMPORARY FUNCTION testUDF AS 'org.apache.hadoop.hive.ql.udf.UDFSubstr';")
    spark.sql("select testUDF('l_commen', 1, 5)").show()
    sql("DROP TEMPORARY FUNCTION IF EXISTS testUDF")
    hiveContext.reset()
  }

}
