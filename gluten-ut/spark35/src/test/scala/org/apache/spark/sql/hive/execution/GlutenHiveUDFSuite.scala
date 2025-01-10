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

import org.apache.gluten.execution.{ColumnarPartialProjectExec, CustomerUDF}
import org.apache.spark.SparkConf
import org.apache.spark.sql.Row

import java.io.File
import scala.collection.mutable

class GlutenHiveUDFSuite extends GlutenHiveSQLQuerySuiteBase {

  override def sparkConf: SparkConf = {
    defaultSparkConf
      .set("spark.plugins", "org.apache.gluten.GlutenPlugin")
      .set("spark.default.parallelism", "1")
      .set("spark.memory.offHeap.enabled", "true")
      .set("spark.memory.offHeap.size", "1024MB")
  }

  def withTempFunction(funcName: String)(f: => Unit): Unit = {
    try f finally sql(s"DROP TEMPORARY FUNCTION IF EXISTS $funcName")
  }

  override def beforeAll(): Unit = {
    super.beforeAll()
    val table = "lineitem"
    val tableDir =
      getClass.getResource("").getPath + "/../../../../../../../../../../../" +
        "/backends-velox/src/test/resources/tpch-data-parquet/"
    val tablePath = new File(tableDir, table).getAbsolutePath
    val tableDF = spark.read.format("parquet").load(tablePath)
    tableDF.createOrReplaceTempView(table)
  }

  override def afterAll(): Unit = {
    super.afterAll()
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
      sql(
        """
          |CREATE TEMPORARY FUNCTION udf_sort_array AS
          |'org.apache.hadoop.hive.ql.udf.generic.GenericUDFSortArray';
          |""".stripMargin)

      val df = sql(
        """
          |SELECT
          |  l_orderkey,
          |  l_partkey,
          |  udf_sort_array(array(10, l_orderkey, 1)) as udf_result
          |FROM lineitem WHERE l_partkey <= 5 and l_orderkey <1000
          |""".stripMargin)

      checkAnswer(df, Seq(
        Row(35, 5, mutable.WrappedArray.make(Array(1, 10, 35))),
        Row(321, 4, mutable.WrappedArray.make(Array(1, 10, 321))),
        Row(548, 2, mutable.WrappedArray.make(Array(1, 10, 548))),
        Row(640, 5, mutable.WrappedArray.make(Array(1, 10, 640))),
        Row(807, 2, mutable.WrappedArray.make(Array(1, 10, 807))),
      ))
      checkOperatorMatch[ColumnarPartialProjectExec](df)
    }
  }

  test("udf with map") {
    withTempFunction("udf_str_to_map") {
      sql(
        """
          |CREATE TEMPORARY FUNCTION udf_str_to_map AS
          |'org.apache.hadoop.hive.ql.udf.generic.GenericUDFStringToMap';
          |""".stripMargin)

      val df = sql(
        """
          |SELECT
          |  l_orderkey,
          |  l_partkey,
          |  udf_str_to_map(concat_ws(',', array(concat('hello', l_partkey), 'world')), ',', 'l') as udf_result
          |FROM lineitem WHERE l_partkey <= 5 and l_orderkey <1000
          |""".stripMargin)

      checkAnswer(df, Seq(
        Row(321, 4, Map("he" -> "lo4", "wor" -> "d")),
        Row(35, 5, Map("he" -> "lo5", "wor" -> "d")),
        Row(548, 2, Map("he" -> "lo2", "wor" -> "d")),
        Row(640, 5, Map("he" -> "lo5", "wor" -> "d")),
        Row(807, 2, Map("he" -> "lo2", "wor" -> "d"))
      ))
      checkOperatorMatch[ColumnarPartialProjectExec](df)
    }
  }
}
