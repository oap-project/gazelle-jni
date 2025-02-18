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
package org.apache.spark.sql.execution

import org.apache.gluten.execution.ColumnarPartialProjectExec
import org.apache.gluten.udf.CustomerUDF

import org.apache.spark.SparkConf
import org.apache.spark.internal.config
import org.apache.spark.internal.config.UI.UI_ENABLED
import org.apache.spark.sql.{DataFrame, GlutenQueryTest, Row, SparkSession}
import org.apache.spark.sql.catalyst.expressions.CodegenObjectFactoryMode
import org.apache.spark.sql.catalyst.optimizer.ConvertToLocalRelation
import org.apache.spark.sql.hive.HiveUtils
import org.apache.spark.sql.internal.{SQLConf, StaticSQLConf}
import org.apache.spark.sql.test.SQLTestUtils

import java.io.File

import scala.collection.mutable
import scala.reflect.ClassTag

class GlutenHiveUDFSuite extends GlutenQueryTest with SQLTestUtils {
  private var _spark: SparkSession = _

  override protected def beforeAll(): Unit = {
    super.beforeAll()

    if (_spark == null) {
      _spark = SparkSession.builder().config(sparkConf).enableHiveSupport().getOrCreate()
    }

    _spark.sparkContext.setLogLevel("info")

    createTestTable()
  }

  override def afterAll(): Unit = {
    super.afterAll()
  }

  override protected def spark: SparkSession = _spark

  protected def defaultSparkConf: SparkConf = {
    val conf = new SparkConf()
      .set("spark.master", "local[1]")
      .set("spark.sql.test", "")
      .set("spark.sql.testkey", "true")
      .set(SQLConf.CODEGEN_FALLBACK.key, "false")
      .set(SQLConf.CODEGEN_FACTORY_MODE.key, CodegenObjectFactoryMode.CODEGEN_ONLY.toString)
      .set(
        HiveUtils.HIVE_METASTORE_BARRIER_PREFIXES.key,
        "org.apache.spark.sql.hive.execution.PairSerDe")
      // SPARK-8910
      .set(UI_ENABLED, false)
      .set(config.UNSAFE_EXCEPTION_ON_MEMORY_LEAK, true)
      // Hive changed the default of hive.metastore.disallow.incompatible.col.type.changes
      // from false to true. For details, see the JIRA HIVE-12320 and HIVE-17764.
      .set("spark.hadoop.hive.metastore.disallow.incompatible.col.type.changes", "false")
      // Disable ConvertToLocalRelation for better test coverage. Test cases built on
      // LocalRelation will exercise the optimization rules better by disabling it as
      // this rule may potentially block testing of other optimization rules such as
      // ConstantPropagation etc.
      .set(SQLConf.OPTIMIZER_EXCLUDED_RULES.key, ConvertToLocalRelation.ruleName)

    conf.set(
      StaticSQLConf.WAREHOUSE_PATH,
      conf.get(StaticSQLConf.WAREHOUSE_PATH) + "/" + getClass.getCanonicalName)
  }

  protected def sparkConf: SparkConf = {
    defaultSparkConf
      .set("spark.plugins", "org.apache.gluten.GlutenPlugin")
      .set("spark.default.parallelism", "1")
      .set("spark.memory.offHeap.enabled", "true")
      .set("spark.memory.offHeap.size", "1024MB")
      .set("spark.gluten.sql.native.writer.enabled", "true")
  }

  private def withTempFunction(funcName: String)(f: => Unit): Unit = {
    try f
    finally sql(s"DROP TEMPORARY FUNCTION IF EXISTS $funcName")
  }

  private def checkOperatorMatch[T <: SparkPlan](df: DataFrame)(implicit tag: ClassTag[T]): Unit = {
    val executedPlan = getExecutedPlan(df)
    assert(executedPlan.exists(plan => plan.getClass == tag.runtimeClass))
  }

  private def createTestTable(): Unit = {
    val table = "lineitem"
    val tableDir = getClass.getResource("/tpch-data-parquet").getFile
    val tablePath = new File(tableDir, table).getAbsolutePath
    val tableDF = spark.read.format("parquet").load(tablePath)
    tableDF.createOrReplaceTempView(table)
  }

  test("customer udf") {
    withTempFunction("testUDF") {
      sql(s"CREATE TEMPORARY FUNCTION testUDF AS '${classOf[CustomerUDF].getName}'")
      val df = sql("select l_partkey, testUDF(l_comment) from lineitem")
      df.show()
      checkOperatorMatch[ColumnarPartialProjectExec](df)
    }
  }

  test("customer udf wrapped in function") {
    withTempFunction("testUDF") {
      sql(s"CREATE TEMPORARY FUNCTION testUDF AS '${classOf[CustomerUDF].getName}'")
      val df = sql("select l_partkey, hash(testUDF(l_comment)) from lineitem")
      df.show()
      checkOperatorMatch[ColumnarPartialProjectExec](df)
    }
  }

  test("example") {
    withTempFunction("testUDF") {
      sql("CREATE TEMPORARY FUNCTION testUDF AS 'org.apache.hadoop.hive.ql.udf.UDFSubstr';")
      val df = sql("select testUDF('l_commen', 1, 5)")
      df.show()
      // It should not be converted to ColumnarPartialProjectExec, since
      // the UDF need all the columns in child output.
      assert(!getExecutedPlan(df).exists {
        case _: ColumnarPartialProjectExec => true
        case _ => false
      })
    }
  }

  test("udf with array") {
    withTempFunction("udf_sort_array") {
      sql("""
            |CREATE TEMPORARY FUNCTION udf_sort_array AS
            |'org.apache.hadoop.hive.ql.udf.generic.GenericUDFSortArray';
            |""".stripMargin)

      val df = sql("""
                     |SELECT
                     |  l_orderkey,
                     |  l_partkey,
                     |  udf_sort_array(array(10, l_orderkey, 1)) as udf_result
                     |FROM lineitem WHERE l_partkey <= 5 and l_orderkey <1000
                     |""".stripMargin)

      checkAnswer(
        df,
        Seq(
          Row(35, 5, mutable.WrappedArray.make(Array(1, 10, 35))),
          Row(321, 4, mutable.WrappedArray.make(Array(1, 10, 321))),
          Row(548, 2, mutable.WrappedArray.make(Array(1, 10, 548))),
          Row(640, 5, mutable.WrappedArray.make(Array(1, 10, 640))),
          Row(807, 2, mutable.WrappedArray.make(Array(1, 10, 807)))
        )
      )
      checkOperatorMatch[ColumnarPartialProjectExec](df)
    }
  }

  test("udf with map") {
    withTempFunction("udf_str_to_map") {
      sql("""
            |CREATE TEMPORARY FUNCTION udf_str_to_map AS
            |'org.apache.hadoop.hive.ql.udf.generic.GenericUDFStringToMap';
            |""".stripMargin)

      val df = sql(
        """
          |SELECT
          |  l_orderkey,
          |  l_partkey,
          |  udf_str_to_map(
          |    concat_ws(',', array(concat('hello', l_partkey), 'world')), ',', 'l') as udf_result
          |FROM lineitem WHERE l_partkey <= 5 and l_orderkey <1000
          |""".stripMargin)

      checkAnswer(
        df,
        Seq(
          Row(321, 4, Map("he" -> "lo4", "wor" -> "d")),
          Row(35, 5, Map("he" -> "lo5", "wor" -> "d")),
          Row(548, 2, Map("he" -> "lo2", "wor" -> "d")),
          Row(640, 5, Map("he" -> "lo5", "wor" -> "d")),
          Row(807, 2, Map("he" -> "lo2", "wor" -> "d"))
        )
      )
      checkOperatorMatch[ColumnarPartialProjectExec](df)
    }
  }
}
