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

import org.apache.gluten.config.GlutenConfig

import org.apache.spark.{SparkConf, SparkException}
import org.apache.spark.sql.catalyst.optimizer.NullPropagation
import org.apache.spark.sql.execution.ProjectExec
import org.apache.spark.sql.types._

import org.scalactic.source.Position
import org.scalatest.Tag

import java.sql.Timestamp

class ScalarFunctionsValidateSuiteRasOff extends ScalarFunctionsValidateSuite {
  override protected def sparkConf: SparkConf = {
    super.sparkConf
      .set("spark.gluten.ras.enabled", "false")
  }
}

class ScalarFunctionsValidateSuiteRasOn extends ScalarFunctionsValidateSuite {
  override protected def sparkConf: SparkConf = {
    super.sparkConf
      .set("spark.gluten.ras.enabled", "true")
  }

  // TODO: Fix the incompatibilities then remove this method. See GLUTEN-7600.
  override protected def test(testName: String, testTags: Tag*)(testFun: => Any)(implicit
      pos: Position): Unit = {
    val exclusions = Set(
      "isnull function",
      "null input for array_size",
      "Test make_ym_interval function"
    )
    if (exclusions.contains(testName)) {
      super.ignore(testName, testTags: _*)(testFun)(pos)
      return
    }
    super.test(testName, testTags: _*)(testFun)(pos)
  }
}

abstract class ScalarFunctionsValidateSuite extends FunctionsValidateSuite {
  disableFallbackCheck
  import testImplicits._

  // Test "SELECT ..." without a from clause.
  test("isnull function") {
    runQueryAndCompare("SELECT isnull(1)")(checkGlutenOperatorMatch[ProjectExecTransformer])
  }

  test("Test bit_count function") {
    runQueryAndCompare("SELECT bit_count(l_partkey) from lineitem limit 1") {
      checkGlutenOperatorMatch[ProjectExecTransformer]
    }
  }

  test("Test bit_get and getbit function") {
    runQueryAndCompare("SELECT bit_get(l_partkey, 0) from lineitem limit 1") {
      checkGlutenOperatorMatch[ProjectExecTransformer]
    }
    runQueryAndCompare("SELECT getbit(l_partkey, 0) from lineitem limit 1") {
      checkGlutenOperatorMatch[ProjectExecTransformer]
    }
  }

  test("Test chr function") {
    runQueryAndCompare("SELECT chr(l_orderkey + 64) from lineitem limit 1") {
      checkGlutenOperatorMatch[ProjectExecTransformer]
    }
  }

  test("Test abs function") {
    runQueryAndCompare("SELECT abs(l_orderkey) from lineitem limit 1") {
      checkGlutenOperatorMatch[ProjectExecTransformer]
    }
  }

  test("Test ceil function") {
    runQueryAndCompare("SELECT ceil(cast(l_orderkey as long)) from lineitem limit 1") {
      checkGlutenOperatorMatch[ProjectExecTransformer]
    }
  }

  test("Test floor function") {
    runQueryAndCompare("SELECT floor(cast(l_orderkey as long)) from lineitem limit 1") {
      checkGlutenOperatorMatch[ProjectExecTransformer]
    }
  }

  test("Test Exp function") {
    runQueryAndCompare("SELECT exp(l_orderkey) from lineitem limit 1") {
      checkGlutenOperatorMatch[ProjectExecTransformer]
    }
  }

  test("Test Power function") {
    runQueryAndCompare("SELECT power(l_orderkey, 2) from lineitem limit 1") {
      checkGlutenOperatorMatch[ProjectExecTransformer]
    }
  }

  test("Test Pmod function") {
    runQueryAndCompare("SELECT pmod(cast(l_orderkey as int), 3) from lineitem limit 1") {
      checkGlutenOperatorMatch[ProjectExecTransformer]
    }
  }

  test("Test round function") {
    runQueryAndCompare(
      "SELECT round(cast(l_orderkey as int), 2)" +
        "from lineitem limit 1") {
      checkGlutenOperatorMatch[ProjectExecTransformer]
    }

    runQueryAndCompare("""
                         |select round(l_quantity, 2) from lineitem;
                         |""".stripMargin) {
      checkGlutenOperatorMatch[ProjectExecTransformer]
    }
  }

  testWithSpecifiedSparkVersion("null input for array_size", Some("3.3")) {
    runQueryAndCompare("SELECT array_size(null)") {
      checkGlutenOperatorMatch[ProjectExecTransformer]
    }
  }

  test("chr function") {
    val df = runQueryAndCompare(
      "SELECT chr(l_orderkey + 64) " +
        "from lineitem limit 1") { _ => }
    checkLengthAndPlan(df, 1)
  }

  test("bin function") {
    val df = runQueryAndCompare(
      "SELECT bin(l_orderkey) " +
        "from lineitem limit 1") {
      checkGlutenOperatorMatch[ProjectExecTransformer]
    }
    checkLengthAndPlan(df, 1)
  }

  test("abs function") {
    val df = runQueryAndCompare(
      "SELECT abs(l_orderkey) " +
        "from lineitem limit 1") { _ => }
    checkLengthAndPlan(df, 1)
  }

  test("ceil function") {
    val df = runQueryAndCompare(
      "SELECT ceil(cast(l_orderkey as long)) " +
        "from lineitem limit 1") { _ => }
    checkLengthAndPlan(df, 1)
  }

  test("floor function") {
    val df = runQueryAndCompare(
      "SELECT floor(cast(l_orderkey as long)) " +
        "from lineitem limit 1") { _ => }
    checkLengthAndPlan(df, 1)
  }

  test("exp function") {
    val df = spark.sql("SELECT exp(l_orderkey) from lineitem limit 1")
    checkLengthAndPlan(df, 1)
  }

  test("power function") {
    val df = runQueryAndCompare(
      "SELECT power(l_orderkey, 2.0) " +
        "from lineitem limit 1") { _ => }
    checkLengthAndPlan(df, 1)
  }

  test("pmod function") {
    val df = runQueryAndCompare(
      "SELECT pmod(cast(l_orderkey as int), 3) " +
        "from lineitem limit 1") { _ => }
    checkLengthAndPlan(df, 1)
  }

  test("Test greatest function") {
    runQueryAndCompare(
      "SELECT greatest(l_orderkey, l_orderkey)" +
        "from lineitem limit 1") {
      checkGlutenOperatorMatch[ProjectExecTransformer]
    }
    withTempPath {
      path =>
        spark
          .sql("""SELECT *
                FROM VALUES (CAST(5.345 AS DECIMAL(6, 2)), CAST(5.35 AS DECIMAL(5, 4))),
                (CAST(5.315 AS DECIMAL(6, 2)), CAST(5.355 AS DECIMAL(5, 4))),
                (CAST(3.345 AS DECIMAL(6, 2)), CAST(4.35 AS DECIMAL(5, 4))) AS data(a, b);""")
          .write
          .parquet(path.getCanonicalPath)

        spark.read.parquet(path.getCanonicalPath).createOrReplaceTempView("view")

        runQueryAndCompare("SELECT greatest(a, b) from view") {
          checkGlutenOperatorMatch[ProjectExecTransformer]
        }
    }
  }

  test("Test least function") {
    runQueryAndCompare(
      "SELECT least(l_orderkey, l_orderkey)" +
        "from lineitem limit 1") {
      checkGlutenOperatorMatch[ProjectExecTransformer]
    }
    withTempPath {
      path =>
        spark
          .sql("""SELECT *
                FROM VALUES (CAST(5.345 AS DECIMAL(6, 2)), CAST(5.35 AS DECIMAL(5, 4))),
                (CAST(5.315 AS DECIMAL(6, 2)), CAST(5.355 AS DECIMAL(5, 4))),
                (CAST(3.345 AS DECIMAL(6, 2)), CAST(4.35 AS DECIMAL(5, 4))) AS data(a, b);""")
          .write
          .parquet(path.getCanonicalPath)

        spark.read.parquet(path.getCanonicalPath).createOrReplaceTempView("view")

        runQueryAndCompare("SELECT least(a, b) from view") {
          checkGlutenOperatorMatch[ProjectExecTransformer]
        }
    }
  }

  test("Test hash function") {
    runQueryAndCompare("SELECT hash(l_orderkey) from lineitem limit 1") {
      checkGlutenOperatorMatch[ProjectExecTransformer]
    }
  }

  test("Test acos function") {
    runQueryAndCompare("SELECT acos(l_orderkey) from lineitem limit 1") {
      checkGlutenOperatorMatch[ProjectExecTransformer]
    }
  }

  test("Test asin function") {
    runQueryAndCompare("SELECT asin(l_orderkey) from lineitem limit 1") {
      checkGlutenOperatorMatch[ProjectExecTransformer]
    }
  }

  test("Test atan function") {
    runQueryAndCompare("SELECT atan(l_orderkey) from lineitem limit 1") {
      checkGlutenOperatorMatch[ProjectExecTransformer]
    }
  }

  ignore("Test atan2 function datatab") {
    runQueryAndCompare("SELECT atan2(double_field1, 0) from datatab limit 1") {
      checkGlutenOperatorMatch[ProjectExecTransformer]
    }
  }

  test("Test ceiling function") {
    runQueryAndCompare("SELECT ceiling(cast(l_orderkey as long)) from lineitem limit 1") {
      checkGlutenOperatorMatch[ProjectExecTransformer]
    }
  }

  test("Test cos function") {
    runQueryAndCompare("SELECT cos(l_orderkey) from lineitem limit 1") {
      checkGlutenOperatorMatch[ProjectExecTransformer]
    }
  }

  test("Test cosh function") {
    runQueryAndCompare("SELECT cosh(l_orderkey) from lineitem limit 1") {
      checkGlutenOperatorMatch[ProjectExecTransformer]
    }
  }

  test("Test degrees function") {
    runQueryAndCompare("SELECT degrees(l_orderkey) from lineitem limit 1") {
      checkGlutenOperatorMatch[ProjectExecTransformer]
    }
  }

  test("Test log10 function") {
    runQueryAndCompare("SELECT log10(l_orderkey) from lineitem limit 1") {
      checkGlutenOperatorMatch[ProjectExecTransformer]
    }
  }

  test("Test log function") {
    runQueryAndCompare("SELECT log(10, l_orderkey) from lineitem limit 1") {
      checkGlutenOperatorMatch[ProjectExecTransformer]
    }
  }

  test("Test shiftleft function") {
    val df = runQueryAndCompare("SELECT shiftleft(int_field1, 1) from datatab limit 1") {
      checkGlutenOperatorMatch[ProjectExecTransformer]
    }
  }

  test("Test shiftright function") {
    val df = runQueryAndCompare("SELECT shiftright(int_field1, 1) from datatab limit 1") {
      checkGlutenOperatorMatch[ProjectExecTransformer]
    }
  }

  test("date_add") {
    withTempPath {
      path =>
        Seq(
          (java.sql.Date.valueOf("2022-03-11"), 1: Integer),
          (java.sql.Date.valueOf("2022-03-12"), 2: Integer),
          (java.sql.Date.valueOf("2022-03-13"), 3: Integer),
          (java.sql.Date.valueOf("2022-03-14"), 4: Integer),
          (java.sql.Date.valueOf("2022-03-15"), 5: Integer),
          (java.sql.Date.valueOf("2022-03-16"), 6: Integer)
        )
          .toDF("a", "b")
          .write
          .parquet(path.getCanonicalPath)

        spark.read.parquet(path.getCanonicalPath).createOrReplaceTempView("view")

        runQueryAndCompare("SELECT date_add(a, b) from view") {
          checkGlutenOperatorMatch[ProjectExecTransformer]
        }
    }
  }

  test("date_diff") {
    withTempPath {
      path =>
        Seq(
          (java.sql.Date.valueOf("2022-03-11"), java.sql.Date.valueOf("2022-02-11")),
          (java.sql.Date.valueOf("2022-03-12"), java.sql.Date.valueOf("2022-01-12")),
          (java.sql.Date.valueOf("2022-09-13"), java.sql.Date.valueOf("2022-05-12")),
          (java.sql.Date.valueOf("2022-07-14"), java.sql.Date.valueOf("2022-03-12")),
          (java.sql.Date.valueOf("2022-06-15"), java.sql.Date.valueOf("2022-01-12")),
          (java.sql.Date.valueOf("2022-05-16"), java.sql.Date.valueOf("2022-06-12"))
        )
          .toDF("a", "b")
          .write
          .parquet(path.getCanonicalPath)

        spark.read.parquet(path.getCanonicalPath).createOrReplaceTempView("view")

        runQueryAndCompare("SELECT datediff(a, b) from view") {
          checkGlutenOperatorMatch[ProjectExecTransformer]
        }
    }
  }

  test("unix_date") {
    withTempPath {
      path =>
        Seq(
          (java.sql.Date.valueOf("1970-01-01")),
          (java.sql.Date.valueOf("1969-12-31")),
          (java.sql.Date.valueOf("2022-09-13"))
        )
          .toDF("a")
          .write
          .parquet(path.getCanonicalPath)

        spark.read.parquet(path.getCanonicalPath).createOrReplaceTempView("view")

        runQueryAndCompare("SELECT unix_date(a) from view") {
          checkGlutenOperatorMatch[ProjectExecTransformer]
        }
    }
  }

  test("to_utc_timestamp") {
    withTempPath {
      path =>
        Seq(
          (Timestamp.valueOf("2015-07-24 00:00:00"), "America/Los_Angeles"),
          (Timestamp.valueOf("2015-07-25 00:00:00"), "America/Los_Angeles")
        ).toDF("a", "b")
          .write
          .parquet(path.getCanonicalPath)

        spark.read.parquet(path.getCanonicalPath).createOrReplaceTempView("view")

        runQueryAndCompare("SELECT to_utc_timestamp(a, \"America/Los_Angeles\") from view") {
          checkGlutenOperatorMatch[ProjectExecTransformer]
        }
        runQueryAndCompare("SELECT to_utc_timestamp(a, b) from view") {
          checkGlutenOperatorMatch[ProjectExecTransformer]
        }
    }
  }

  test("array_aggregate") {
    withTempPath {
      path =>
        Seq[Seq[Integer]](
          Seq(1, 9, 8, 7),
          Seq(5, null, 8, 9, 7, 2),
          Seq.empty,
          null
        )
          .toDF("i")
          .write
          .parquet(path.getCanonicalPath)

        spark.read.parquet(path.getCanonicalPath).createOrReplaceTempView("array_tbl")

        runQueryAndCompare(
          "select aggregate(i, 0, (acc, x) -> acc + x," +
            " acc -> acc * 3) as v from array_tbl;") {
          checkGlutenOperatorMatch[ProjectExecTransformer]
        }
    }
    withTempPath {
      path =>
        Seq(
          (1, Array[Int](1, 2, 3)),
          (5, Array[Int](4, 5, 6))
        )
          .toDF("x", "ys")
          .write
          .parquet(path.getCanonicalPath)

        spark.read.parquet(path.getCanonicalPath).createOrReplaceTempView("array_tbl")

        runQueryAndCompare("select aggregate(ys, 0, (y, a) -> y + a + x) as v from array_tbl;") {
          checkGlutenOperatorMatch[ProjectExecTransformer]
        }
    }
  }

  test("Support HOUR function") {
    withTable("t1") {
      sql("create table t1 (c1 int, c2 timestamp) USING PARQUET")
      sql("INSERT INTO t1 VALUES(1, NOW())")
      runQueryAndCompare("SELECT c1, HOUR(c2) FROM t1 LIMIT 1")(df => checkFallbackOperators(df, 0))
    }
  }

  test("from_utc_timestamp") {
    withTempPath {
      path =>
        Seq(
          (Timestamp.valueOf("2015-07-24 00:00:00"), "America/Los_Angeles"),
          (Timestamp.valueOf("2015-07-25 00:00:00"), "America/Los_Angeles")
        ).toDF("a", "b")
          .write
          .parquet(path.getCanonicalPath)

        spark.read.parquet(path.getCanonicalPath).createOrReplaceTempView("view")

        runQueryAndCompare("SELECT from_utc_timestamp(a, \"America/Los_Angeles\") from view") {
          checkGlutenOperatorMatch[ProjectExecTransformer]
        }
        runQueryAndCompare("SELECT from_utc_timestamp(a, b) from view") {
          checkGlutenOperatorMatch[ProjectExecTransformer]
        }
    }
  }

  test("map extract - getmapvalue") {
    withTempPath {
      path =>
        Seq(
          Map[Int, Int](1 -> 100, 2 -> 200),
          Map[Int, Int](),
          Map[Int, Int](1 -> 100, 2 -> 200, 3 -> 300),
          null
        )
          .toDF("i")
          .write
          .parquet(path.getCanonicalPath)

        spark.read.parquet(path.getCanonicalPath).createOrReplaceTempView("map_tbl")

        runQueryAndCompare("select i[\"1\"] from map_tbl") {
          checkGlutenOperatorMatch[ProjectExecTransformer]
        }
    }
  }

  test("test map_entries function") {
    withTempPath {
      path =>
        Seq(
          Map[Int, String](1 -> null, 2 -> "200"),
          Map[Int, String](1 -> "100", 2 -> "200", 3 -> "300"),
          null
        )
          .toDF("i")
          .write
          .parquet(path.getCanonicalPath)

        spark.read.parquet(path.getCanonicalPath).createOrReplaceTempView("map_tbl")

        runQueryAndCompare("select map_entries(i) from map_tbl") {
          checkGlutenOperatorMatch[ProjectExecTransformer]
        }
    }
  }

  test("test map_keys function") {
    withTempPath {
      path =>
        Seq(
          Map[Int, String](1 -> null, 2 -> "200"),
          Map[Int, String](1 -> "100", 2 -> "200", 3 -> "300"),
          null
        )
          .toDF("i")
          .write
          .parquet(path.getCanonicalPath)

        spark.read.parquet(path.getCanonicalPath).createOrReplaceTempView("map_tbl")

        runQueryAndCompare("select map_keys(i) from map_tbl") {
          checkGlutenOperatorMatch[ProjectExecTransformer]
        }
    }
  }

  test("test map_values function") {
    withTempPath {
      path =>
        Seq(
          Map[Int, String](1 -> null, 2 -> "200"),
          Map[Int, String](1 -> "100", 2 -> "200", 3 -> "300"),
          null
        )
          .toDF("i")
          .write
          .parquet(path.getCanonicalPath)

        spark.read.parquet(path.getCanonicalPath).createOrReplaceTempView("map_tbl")

        runQueryAndCompare("select map_values(i) from map_tbl") {
          checkGlutenOperatorMatch[ProjectExecTransformer]
        }
    }
  }

  test("map_zip_with") {
    withTempPath {
      path =>
        Seq((Map("a" -> 1, "b" -> 2), Map("a" -> 2, "b" -> 3)))
          .toDF("m1", "m2")
          .write
          .parquet(path.getCanonicalPath)

        spark.read.parquet(path.getCanonicalPath).createOrReplaceTempView("map_tbl")

        runQueryAndCompare(
          "select map_zip_with(m1, m2, (k, v1, v2) -> k == v1 + v2) from map_tbl") {
          checkGlutenOperatorMatch[ProjectExecTransformer]
        }
    }
  }

  test("test transform_keys function") {
    withTempPath {
      path =>
        Seq(
          Map[String, Int]("a" -> 1, "b" -> 2),
          Map[String, Int]("a" -> 2, "b" -> 3),
          null
        )
          .toDF("m")
          .write
          .parquet(path.getCanonicalPath)

        spark.read.parquet(path.getCanonicalPath).createOrReplaceTempView("map_tbl")

        runQueryAndCompare("select transform_keys(m, (k, v) -> upper(k)) from map_tbl") {
          checkGlutenOperatorMatch[ProjectExecTransformer]
        }
    }
  }

  test("test transform_values function") {
    withTempPath {
      path =>
        Seq(
          Map[String, Int]("a" -> 1, "b" -> 2),
          Map[String, Int]("a" -> 2, "b" -> 3),
          null
        )
          .toDF("m")
          .write
          .parquet(path.getCanonicalPath)

        spark.read.parquet(path.getCanonicalPath).createOrReplaceTempView("map_tbl")

        runQueryAndCompare("select transform_values(m, (k, v) -> v + 1) from map_tbl") {
          checkGlutenOperatorMatch[ProjectExecTransformer]
        }
    }
  }

  test("zip_with") {
    withTempPath {
      path =>
        Seq[(Seq[Integer], Seq[Integer])](
          (Seq(9001, 9002, 9003), Seq(4, 5, 6)),
          (Seq(1, 2), Seq(3, 4)),
          (Seq.empty, Seq.empty),
          (null, null)
        ).toDF("val1", "val2")
          .write
          .parquet(path.getCanonicalPath)

        spark.read.parquet(path.getCanonicalPath).createOrReplaceTempView("array_tbl")

        runQueryAndCompare("select zip_with(val1, val2, (x, y) -> x + y) from array_tbl") {
          checkGlutenOperatorMatch[ProjectExecTransformer]
        }
    }
  }

  test("Test isnan function") {
    runQueryAndCompare(
      "SELECT isnan(l_orderkey), isnan(cast('NaN' as double)), isnan(0.0F/0.0F)" +
        " from lineitem limit 1") {
      checkGlutenOperatorMatch[ProjectExecTransformer]
    }
  }

  test("Test nanvl function") {
    runQueryAndCompare("""SELECT nanvl(cast('nan' as float), 1f),
                         | nanvl(l_orderkey, cast('null' as double)),
                         | nanvl(cast('null' as double), l_orderkey),
                         | nanvl(l_orderkey, l_orderkey / 0.0d),
                         | nanvl(cast('nan' as float), l_orderkey)
                         | from lineitem limit 1""".stripMargin) {
      checkGlutenOperatorMatch[ProjectExecTransformer]
    }
  }

  // FIXME: Ignored: https://github.com/apache/incubator-gluten/issues/7600.
  ignore("Test monotonically_increasintestg_id function") {
    runQueryAndCompare("""SELECT monotonically_increasing_id(), l_orderkey
                         | from lineitem limit 100""".stripMargin) {
      checkGlutenOperatorMatch[ProjectExecTransformer]
    }
  }

  test("Test sequence function optimized by Spark constant folding") {
    withSQLConf(("spark.sql.optimizer.excludedRules", NullPropagation.ruleName)) {
      runQueryAndCompare("""SELECT sequence(1, 5), l_orderkey
                           | from lineitem limit 100""".stripMargin) {
        checkGlutenOperatorMatch[ProjectExecTransformer]
      }
    }
  }

  test("Test raise_error, assert_true function") {
    runQueryAndCompare("""SELECT assert_true(l_orderkey >= 1), l_orderkey
                         | from lineitem limit 100""".stripMargin) {
      checkGlutenOperatorMatch[ProjectExecTransformer]
    }
    val e = intercept[SparkException] {
      sql("""SELECT assert_true(l_orderkey >= 100), l_orderkey from
            | lineitem limit 100""".stripMargin).collect()
    }
    assert(e.getCause.isInstanceOf[RuntimeException])
    assert(e.getMessage.contains("l_orderkey"))
  }

  test("Test E function") {
    runQueryAndCompare("""SELECT E() from lineitem limit 100""".stripMargin) {
      checkGlutenOperatorMatch[ProjectExecTransformer]
    }
    runQueryAndCompare("""SELECT E(), l_orderkey
                         | from lineitem limit 100""".stripMargin) {
      checkGlutenOperatorMatch[ProjectExecTransformer]
    }
  }

  test("Test Pi function") {
    runQueryAndCompare("""SELECT Pi() from lineitem limit 100""".stripMargin) {
      checkGlutenOperatorMatch[ProjectExecTransformer]
    }
    runQueryAndCompare("""SELECT Pi(), l_orderkey
                         | from lineitem limit 100""".stripMargin) {
      checkGlutenOperatorMatch[ProjectExecTransformer]
    }
  }

  test("Test version function") {
    runQueryAndCompare("""SELECT version() from lineitem limit 100""".stripMargin) {
      checkGlutenOperatorMatch[ProjectExecTransformer]
    }
    runQueryAndCompare("""SELECT version(), l_orderkey
                         | from lineitem limit 100""".stripMargin) {
      checkGlutenOperatorMatch[ProjectExecTransformer]
    }
  }

  test("Test sum/count function") {
    runQueryAndCompare("""SELECT sum(2),count(2) from lineitem""".stripMargin) {
      checkGlutenOperatorMatch[BatchScanExecTransformer]
    }
  }

  test("Test spark_partition_id function") {
    runQueryAndCompare("""SELECT spark_partition_id(), l_orderkey
                         | from lineitem limit 100""".stripMargin) {
      checkGlutenOperatorMatch[ProjectExecTransformer]
    }
    runQueryAndCompare("""SELECT spark_partition_id()
                         |from lineitem limit 100""".stripMargin) {
      checkGlutenOperatorMatch[ProjectExecTransformer]
    }
  }

  testWithSpecifiedSparkVersion("Test width_bucket function", Some("3.4")) {
    runQueryAndCompare("""SELECT width_bucket(2, 0, 4, 3), l_orderkey
                         | from lineitem limit 100""".stripMargin) {
      checkGlutenOperatorMatch[ProjectExecTransformer]
    }
  }

  testWithSpecifiedSparkVersion("Test url_decode function", Some("3.4")) {
    withTempPath {
      path =>
        Seq("https%3A%2F%2Fspark.apache.org")
          .toDF("a")
          .write
          .parquet(path.getCanonicalPath)
        spark.sparkContext.setLogLevel("info")
        spark.read.parquet(path.getCanonicalPath).createOrReplaceTempView("url_tbl")
        runQueryAndCompare("select url_decode(a) from url_tbl") {
          checkGlutenOperatorMatch[ProjectExecTransformer]
        }
    }
  }

  testWithSpecifiedSparkVersion("Test url_encode function", Some("3.4")) {
    withTempPath {
      path =>
        Seq("https://spark.apache.org")
          .toDF("a")
          .write
          .parquet(path.getCanonicalPath)
        spark.sparkContext.setLogLevel("info")
        spark.read.parquet(path.getCanonicalPath).createOrReplaceTempView("url_tbl")
        runQueryAndCompare("select url_encode(a) from url_tbl") {
          checkGlutenOperatorMatch[ProjectExecTransformer]
        }
    }
  }

  test("Test hex function") {
    runQueryAndCompare("SELECT hex(l_partkey), hex(l_shipmode) FROM lineitem limit 1") {
      checkGlutenOperatorMatch[ProjectExecTransformer]
    }
  }

  test("Test unhex function") {
    runQueryAndCompare("SELECT unhex(hex(l_shipmode)) FROM lineitem limit 1") {
      checkGlutenOperatorMatch[ProjectExecTransformer]
    }
  }

  test("soundex") {
    runQueryAndCompare("select soundex(c_comment) from customer limit 50") {
      checkGlutenOperatorMatch[ProjectExecTransformer]
    }
  }

  test("Test make_timestamp function") {
    withTempPath {
      path =>
        // w/o timezone.
        Seq(
          (2017, 7, 11, 6, 30, Decimal(45678000, 18, 6)),
          (1, 1, 1, 1, 1, Decimal(1, 18, 6)),
          (1, 1, 1, 1, 1, null)
        )
          .toDF("year", "month", "day", "hour", "min", "sec")
          .write
          .parquet(path.getCanonicalPath)

        spark.read.parquet(path.getCanonicalPath).createOrReplaceTempView("make_timestamp_tbl1")

        runQueryAndCompare(
          "select make_timestamp(year, month, day, hour, min, sec) from make_timestamp_tbl1") {
          checkGlutenOperatorMatch[ProjectExecTransformer]
        }
    }
    withTempPath {
      path =>
        // w/ timezone.
        Seq(
          (2017, 7, 11, 6, 30, Decimal(45678000, 18, 6), "CET"),
          (1, 1, 1, 1, 1, Decimal(1, 18, 6), null),
          (1, 1, 1, 1, 1, null, "CST")
        )
          .toDF("year", "month", "day", "hour", "min", "sec", "timezone")
          .write
          .parquet(path.getCanonicalPath)

        spark.read.parquet(path.getCanonicalPath).createOrReplaceTempView("make_timestamp_tbl2")

        runQueryAndCompare("""
                             |select make_timestamp(year, month, day, hour, min, sec, timezone)
                             |from make_timestamp_tbl2
                             |""".stripMargin) {
          checkGlutenOperatorMatch[ProjectExecTransformer]
        }
    }
  }

  test("Test make_ym_interval function") {
    runQueryAndCompare("select make_ym_interval(1, 1)") {
      checkGlutenOperatorMatch[ProjectExecTransformer]
    }

    runQueryAndCompare("select make_ym_interval(1)") {
      checkGlutenOperatorMatch[ProjectExecTransformer]
    }

    runQueryAndCompare("select make_ym_interval()") {
      checkGlutenOperatorMatch[ProjectExecTransformer]
    }

    withTempPath {
      path =>
        Seq[Tuple2[Integer, Integer]]((1, 0), (-1, 1), (null, 1), (1, null))
          .toDF("year", "month")
          .write
          .parquet(path.getCanonicalPath)

        spark.read.parquet(path.getCanonicalPath).createOrReplaceTempView("make_ym_interval_tbl")

        runQueryAndCompare("select make_ym_interval(year, month) from make_ym_interval_tbl") {
          checkGlutenOperatorMatch[ProjectExecTransformer]
        }

        runQueryAndCompare("select make_ym_interval(year) from make_ym_interval_tbl") {
          checkGlutenOperatorMatch[ProjectExecTransformer]
        }
    }
  }

  test("Test uuid function") {
    runQueryAndCompare("""SELECT uuid() from lineitem limit 100""".stripMargin, false) {
      checkGlutenOperatorMatch[ProjectExecTransformer]
    }
  }

  test("Test rand function") {
    runQueryAndCompare("""SELECT rand() from lineitem limit 100""".stripMargin, false) {
      checkGlutenOperatorMatch[ProjectExecTransformer]
    }
  }

  test("regexp_replace") {
    runQueryAndCompare(
      "SELECT regexp_replace(c_comment, '\\w', 'something') FROM customer limit 50") {
      checkGlutenOperatorMatch[ProjectExecTransformer]
    }
    runQueryAndCompare(
      "SELECT regexp_replace(c_comment, '\\w', 'something', 3) FROM customer limit 50") {
      checkGlutenOperatorMatch[ProjectExecTransformer]
    }
  }

  testWithSpecifiedSparkVersion("mask", Some("3.4")) {
    runQueryAndCompare("SELECT mask(c_comment) FROM customer limit 50") {
      checkGlutenOperatorMatch[ProjectExecTransformer]
    }
    runQueryAndCompare("SELECT mask(c_comment, 'Y') FROM customer limit 50") {
      checkGlutenOperatorMatch[ProjectExecTransformer]
    }
    runQueryAndCompare("SELECT mask(c_comment, 'Y', 'y') FROM customer limit 50") {
      checkGlutenOperatorMatch[ProjectExecTransformer]
    }
    runQueryAndCompare("SELECT mask(c_comment, 'Y', 'y', 'o') FROM customer limit 50") {
      checkGlutenOperatorMatch[ProjectExecTransformer]
    }
    runQueryAndCompare("SELECT mask(c_comment, 'Y', 'y', 'o', '*') FROM customer limit 50") {
      checkGlutenOperatorMatch[ProjectExecTransformer]
    }
  }

  test("bit_length") {
    runQueryAndCompare(
      "select bit_length(c_comment), bit_length(cast(c_comment as binary))" +
        " from customer limit 50") {
      checkGlutenOperatorMatch[ProjectExecTransformer]
    }
  }

  test("bitwise_and") {
    runQueryAndCompare(
      "select cast(l_orderkey as tinyint) & cast(l_partkey as tinyint)," +
        " cast(l_orderkey as int) & cast(l_partkey as int), l_orderkey & l_partkey" +
        " from lineitem") {
      checkGlutenOperatorMatch[ProjectExecTransformer]
    }
  }

  test("bitwise_not") {
    runQueryAndCompare(
      "select ~(cast(l_orderkey as tinyint)), ~(cast(l_orderkey as int)), ~l_orderkey" +
        " from lineitem") {
      checkGlutenOperatorMatch[ProjectExecTransformer]
    }
  }

  test("bitwise_or") {
    runQueryAndCompare(
      "select cast(l_orderkey as tinyint) | cast(l_partkey as tinyint)," +
        " cast(l_orderkey as int) | cast(l_partkey as int), l_orderkey | l_partkey" +
        " from lineitem") {
      checkGlutenOperatorMatch[ProjectExecTransformer]
    }
  }

  test("bitwise_xor") {
    runQueryAndCompare(
      "select cast(l_orderkey as tinyint) ^ cast(l_partkey as tinyint)," +
        " cast(l_orderkey as int) ^ cast(l_partkey as int), l_orderkey ^ l_partkey" +
        " from lineitem") {
      checkGlutenOperatorMatch[ProjectExecTransformer]
    }
  }

  test("test array filter") {
    withTempPath {
      path =>
        Seq[Seq[Integer]](Seq(1, null, 5, 4), Seq(5, -1, 8, 9, -7, 2), Seq.empty, null)
          .toDF("value")
          .write
          .parquet(path.getCanonicalPath)

        spark.read.parquet(path.getCanonicalPath).createOrReplaceTempView("array_tbl")

        runQueryAndCompare("select filter(value, x -> x % 2 == 1) as res from array_tbl;") {
          checkGlutenOperatorMatch[ProjectExecTransformer]
        }

        runQueryAndCompare("select filter(value, x -> x is not null) as res from array_tbl;") {
          checkGlutenOperatorMatch[ProjectExecTransformer]
        }
    }
  }

  test("test array transform") {
    withTable("t") {
      sql("create table t (arr ARRAY<INT>) using parquet")
      sql("insert into t values(array(1, 2, 3, null))")
      runQueryAndCompare("select transform(arr, x -> x + 1) from t") {
        checkGlutenOperatorMatch[ProjectExecTransformer]
      }
    }
  }

  test("extract date field") {
    withTable("t") {
      sql("create table t (dt date) using parquet")
      sql("insert into t values(date '2008-02-20'), (date '2022-01-01')")
      runQueryAndCompare("select weekofyear(dt) from t") {
        checkGlutenOperatorMatch[ProjectExecTransformer]
      }
      runQueryAndCompare(
        "SELECT date_part('yearofweek', dt), extract(yearofweek from dt)" +
          " from t") {
        checkGlutenOperatorMatch[ProjectExecTransformer]
      }
    }
  }

  test("try_add") {
    runQueryAndCompare(
      "select try_add(cast(l_orderkey as int), 1), try_add(cast(l_orderkey as int), 2147483647)" +
        " from lineitem") {
      checkGlutenOperatorMatch[ProjectExecTransformer]
    }
  }

  testWithSpecifiedSparkVersion("try_subtract", Some("3.3")) {
    runQueryAndCompare(
      "select try_subtract(2147483647, cast(l_orderkey as int)), " +
        "try_subtract(-2147483648, cast(l_orderkey as int)) from lineitem") {
      checkGlutenOperatorMatch[ProjectExecTransformer]
    }
  }

  test("try_divide") {
    runQueryAndCompare(
      "select try_divide(cast(l_orderkey as int), 0) from lineitem",
      noFallBack = false) {
      _ => // Spark would always cast inputs to double for this function.
    }
  }

  testWithSpecifiedSparkVersion("try_multiply", Some("3.3")) {
    runQueryAndCompare(
      "select try_multiply(2147483647, cast(l_orderkey as int)), " +
        "try_multiply(-2147483648, cast(l_orderkey as int)) from lineitem") {
      checkGlutenOperatorMatch[ProjectExecTransformer]
    }
  }

  test("test array forall") {
    withTempPath {
      path =>
        Seq[Seq[Integer]](Seq(1, null, 5, 4), Seq(5, -1, 8, 9, -7, 2), Seq.empty, null)
          .toDF("value")
          .write
          .parquet(path.getCanonicalPath)

        spark.read.parquet(path.getCanonicalPath).createOrReplaceTempView("array_tbl")

        runQueryAndCompare("select forall(value, x -> x % 2 == 1) as res from array_tbl;") {
          checkGlutenOperatorMatch[ProjectExecTransformer]
        }

        runQueryAndCompare("select forall(value, x -> x is not null) as res from array_tbl;") {
          checkGlutenOperatorMatch[ProjectExecTransformer]
        }
    }
  }

  test("test array exists") {
    withTempPath {
      path =>
        Seq[Seq[Integer]](Seq(1, null, 5, 4), Seq(5, -1, 8, 9, -7, 2), Seq.empty, null)
          .toDF("value")
          .write
          .parquet(path.getCanonicalPath)

        spark.read.parquet(path.getCanonicalPath).createOrReplaceTempView("array_tbl")

        runQueryAndCompare("select exists(value, x -> x % 2 == 1) as res from array_tbl;") {
          checkGlutenOperatorMatch[ProjectExecTransformer]
        }

        runQueryAndCompare("select exists(value, x -> x is not null) as res from array_tbl;") {
          checkGlutenOperatorMatch[ProjectExecTransformer]
        }
    }
  }

  test("arrays_zip") {
    withTempPath {
      path =>
        Seq[(Seq[Integer], Seq[Integer])](
          (Seq(1, 2, 3), Seq(3, 4)),
          (Seq(5, null), Seq(null, 1, 2)))
          .toDF("v1", "v2")
          .write
          .parquet(path.getCanonicalPath)

        spark.read.parquet(path.getCanonicalPath).createOrReplaceTempView("array_tbl")

        runQueryAndCompare("select arrays_zip(v1, v2) from array_tbl;") {
          checkGlutenOperatorMatch[ProjectExecTransformer]
        }
    }
  }

  test("test shuffle") {
    withTempPath {
      path =>
        Seq[Seq[Integer]](Seq(1, null, 5, 4), Seq(5, -1, 8, 9, -7, 2), Seq.empty, null)
          .toDF("value")
          .write
          .parquet(path.getCanonicalPath)

        spark.read.parquet(path.getCanonicalPath).createOrReplaceTempView("array_tbl")

        runQueryAndCompare("select shuffle(value) from array_tbl;", false) {
          checkGlutenOperatorMatch[ProjectExecTransformer]
        }
    }
  }

  test("negative") {
    runQueryAndCompare("select negative(l_orderkey) from lineitem") {
      checkGlutenOperatorMatch[ProjectExecTransformer]
    }
  }

  test("unix_seconds") {
    withTempPath {
      path =>
        val t1 = Timestamp.valueOf("2024-08-22 10:10:10.010")
        val t2 = Timestamp.valueOf("2014-12-31 00:00:00.012")
        val t3 = Timestamp.valueOf("1968-12-31 23:59:59.001")
        Seq(t1, t2, t3).toDF("t").write.parquet(path.getCanonicalPath)

        spark.read.parquet(path.getCanonicalPath).createOrReplaceTempView("view")
        runQueryAndCompare("select unix_seconds(t) from view") {
          checkGlutenOperatorMatch[ProjectExecTransformer]
        }
    }
  }

  test("unix_millis") {
    withTempPath {
      path =>
        val t1 = Timestamp.valueOf("2015-07-22 10:00:00.012")
        val t2 = Timestamp.valueOf("2014-12-31 23:59:59.012")
        val t3 = Timestamp.valueOf("2014-12-31 23:59:59.001")
        Seq(t1, t2, t3).toDF("t").write.parquet(path.getCanonicalPath)

        spark.read.parquet(path.getCanonicalPath).createOrReplaceTempView("time")
        runQueryAndCompare("select unix_millis(t) from time") {
          checkGlutenOperatorMatch[ProjectExecTransformer]
        }
    }
  }

  test("unix_micros") {
    withTempPath {
      path =>
        val t1 = Timestamp.valueOf("2015-07-22 10:00:00.012")
        val t2 = Timestamp.valueOf("2014-12-31 23:59:59.012")
        val t3 = Timestamp.valueOf("2014-12-31 23:59:59.001")
        Seq(t1, t2, t3).toDF("t").write.parquet(path.getCanonicalPath)

        spark.read.parquet(path.getCanonicalPath).createOrReplaceTempView("time")
        runQueryAndCompare("select unix_micros(t) from time") {
          checkGlutenOperatorMatch[ProjectExecTransformer]
        }
    }
  }

  test("timestamp_millis") {
    runQueryAndCompare("select timestamp_millis(l_orderkey) from lineitem") {
      checkGlutenOperatorMatch[ProjectExecTransformer]
    }
  }

  test("timestamp_micros") {
    runQueryAndCompare("select timestamp_micros(l_orderkey) from lineitem") {
      checkGlutenOperatorMatch[ProjectExecTransformer]
    }
  }

  test("test flatten nested array") {
    withTempPath {
      path =>
        Seq[Seq[Seq[Integer]]](
          Seq(Seq(1, 2), Seq(4, 5)),
          null,
          Seq(null, Seq(1, 2)),
          Seq(null, null),
          Seq(Seq(1, 2, null), Seq(null, null), Seq(3, 4), Seq.empty))
          .toDF("arrays")
          .write
          .parquet(path.getCanonicalPath)

        spark.read.parquet(path.getCanonicalPath).createOrReplaceTempView("array_tbl")

        runQueryAndCompare("select flatten(arrays) as res from array_tbl;") {
          checkGlutenOperatorMatch[ProjectExecTransformer]
        }
    }
  }

  testWithSpecifiedSparkVersion("get", Some("3.4")) {
    withTempPath {
      path =>
        Seq[Seq[Integer]](Seq(1, null, 5, 4), Seq(5, -1, 8, 9, -7, 2), Seq.empty, null)
          .toDF("value")
          .write
          .parquet(path.getCanonicalPath)

        spark.read.parquet(path.getCanonicalPath).createOrReplaceTempView("array_tbl")

        runQueryAndCompare(
          "select get(value, 0), get(value, 1), get(value, 2), get(value, 3) from array_tbl;") {
          checkGlutenOperatorMatch[ProjectExecTransformer]
        }
    }
  }

  test("length") {
    runQueryAndCompare(
      "select length(c_comment), length(cast(c_comment as binary))" +
        " from customer limit 50") {
      checkGlutenOperatorMatch[ProjectExecTransformer]
    }
  }

  test("rint") {
    withTempPath {
      path =>
        Seq(1.2, 1.5, 1.9).toDF("d").write.parquet(path.getCanonicalPath)

        spark.read.parquet(path.getCanonicalPath).createOrReplaceTempView("double")
        runQueryAndCompare("select rint(d) from double") {
          checkGlutenOperatorMatch[ProjectExecTransformer]
        }
    }
  }

  test("arrays_overlap") {
    withTempPath {
      path =>
        Seq[(Seq[Integer], Seq[Integer])]((Seq(1, 2, 3), Seq(3, 4)), (Seq(5, null), Seq()))
          .toDF("v1", "v2")
          .write
          .parquet(path.getCanonicalPath)

        spark.read.parquet(path.getCanonicalPath).createOrReplaceTempView("array_tbl")

        runQueryAndCompare("select arrays_overlap(v1, v2) from array_tbl;") {
          checkGlutenOperatorMatch[ProjectExecTransformer]
        }
    }
  }

  test("PreciseTimestampConversion") {
    withTempPath {
      path =>
        val df = spark
          .sql(
            "select * from VALUES ('A1', TIMESTAMP'2021-01-01 00:00:00'), " +
              "('A1', TIMESTAMP'2021-01-01 00:04:30'), ('A1', TIMESTAMP'2021-01-01 00:06:00'), " +
              "('A2', TIMESTAMP'2021-01-01 00:01:00') AS tab(a, b)")
          .write
          .parquet(path.getCanonicalPath)

        spark.read.parquet(path.getCanonicalPath).createOrReplaceTempView("string_timestamp")

        runQueryAndCompare(
          "SELECT a, window.start, window.end, count(*) as cnt FROM" +
            " string_timestamp GROUP by a, window(b, '5 minutes') ORDER BY a, start;") {
          df =>
            val executedPlan = getExecutedPlan(df)
            assert(
              executedPlan.exists(plan => plan.isInstanceOf[ProjectExecTransformer]),
              s"Expect ProjectExecTransformer exists " +
                s"in executedPlan:\n ${executedPlan.last}"
            )
            assert(
              !executedPlan.exists(plan => plan.isInstanceOf[ProjectExec]),
              s"Expect ProjectExec doesn't exist " +
                s"in executedPlan:\n ${executedPlan.last}"
            )
        }
    }
  }

  test("levenshtein") {
    runQueryAndCompare("select levenshtein(c_comment, c_address) from customer limit 50") {
      checkGlutenOperatorMatch[ProjectExecTransformer]
    }
  }

  testWithSpecifiedSparkVersion("levenshtein with limit", Some("3.5")) {
    runQueryAndCompare("select levenshtein(c_comment, c_address, 3) from customer limit 50") {
      checkGlutenOperatorMatch[ProjectExecTransformer]
    }
  }

  test("Test substring_index") {
    withTempView("substring_index_table") {
      withTempPath {
        path =>
          Seq[(String, String, Int)](
            ("www.apache.org", ".", 3),
            ("www.apache.org", ".", 2),
            ("www.apache.org", ".", 1),
            ("www.apache.org", ".", 0),
            ("www.apache.org", ".", -1),
            ("www.apache.org", ".", -2),
            ("www.apache.org", ".", -3),
            ("www.apache.org", "", 1),
            ("www.apache.org", "#", 1),
            ("www||apache||org", "||", 2),
            ("www||apache||org", "||", -2),
            ("", ".", 1),
            ("||||||", "|||", 3),
            ("||||||", "|||", -4)
          )
            .toDF("str", "delim", "count")
            .write
            .parquet(path.getCanonicalPath)
          spark.read.parquet(path.getCanonicalPath).createOrReplaceTempView("substring_index_table")
          runQueryAndCompare(
            """
              |select substring_index(str, delim, count) from substring_index_table
              |""".stripMargin
          ) {
            checkGlutenOperatorMatch[ProjectExecTransformer]
          }
      }
    }
  }

  test("repeat") {
    runQueryAndCompare("select repeat(c_comment, 5) from customer limit 50") {
      checkGlutenOperatorMatch[ProjectExecTransformer]
    }
  }

  test("concat_ws") {
    runQueryAndCompare("SELECT concat_ws('~~', c_comment, c_address) FROM customer LIMIT 50") {
      checkGlutenOperatorMatch[ProjectExecTransformer]
    }

    withTempPath {
      path =>
        Seq[Seq[String]](Seq("ab", null, "cd", "", "ef"), Seq(null, "x", "", "y"), Seq.empty, null)
          .toDF("col")
          .write
          .parquet(path.getCanonicalPath)

        spark.read.parquet(path.getCanonicalPath).createOrReplaceTempView("array_tbl")

        runQueryAndCompare("SELECT concat_ws('~~', col, 'end') AS res from array_tbl;") {
          checkGlutenOperatorMatch[ProjectExecTransformer]
        }
    }
  }

  test("Test input_file_name function") {
    runQueryAndCompare("""SELECT input_file_name(), l_orderkey
                         | from lineitem limit 100""".stripMargin) {
      checkGlutenOperatorMatch[ProjectExecTransformer]
    }

    runQueryAndCompare("""SELECT input_file_name(), l_orderkey
                         | from
                         | (select l_orderkey from lineitem
                         | union all
                         | select o_orderkey as l_orderkey from orders)
                         | limit 100""".stripMargin) {
      checkGlutenOperatorMatch[ProjectExecTransformer]
    }

    withTempPath {
      path =>
        Seq(1, 2, 3).toDF("a").write.json(path.getCanonicalPath)
        spark.read.json(path.getCanonicalPath).createOrReplaceTempView("json_table")
        val sql =
          """
            |SELECT input_file_name(), a
            |FROM
            |(SELECT a FROM json_table
            |UNION ALL
            |SELECT l_orderkey as a FROM lineitem)
            |LIMIT 100
            |""".stripMargin
        compareResultsAgainstVanillaSpark(sql, true, { _ => })
    }

    // Collapse project if scan is fallback and the outer project is cheap or fallback.
    Seq("true", "false").foreach {
      flag =>
        withSQLConf(
          GlutenConfig.COLUMNAR_PROJECT_ENABLED.key -> flag,
          GlutenConfig.COLUMNAR_BATCHSCAN_ENABLED.key -> "false") {
          runQueryAndCompare("SELECT l_orderkey, input_file_name() as name FROM lineitem") {
            df =>
              val plan = df.queryExecution.executedPlan
              assert(collect(plan) { case f: ProjectExecTransformer => f }.size == 0)
              assert(collect(plan) { case f: ProjectExec => f }.size == 1)
          }
        }
    }
  }

  testWithSpecifiedSparkVersion("array insert", Some("3.4")) {
    withTempPath {
      path =>
        Seq[Seq[Integer]](Seq(1, null, 5, 4), Seq(5, -1, 8, 9, -7, 2), Seq.empty, null)
          .toDF("value")
          .write
          .parquet(path.getCanonicalPath)

        spark.read.parquet(path.getCanonicalPath).createOrReplaceTempView("array_tbl")

        Seq("true", "false").foreach {
          legacyNegativeIndex =>
            withSQLConf("spark.sql.legacy.negativeIndexInArrayInsert" -> legacyNegativeIndex) {
              runQueryAndCompare("""
                                   |select
                                   |  array_insert(value, 1, 0), array_insert(value, 10, 0),
                                   |  array_insert(value, -1, 0), array_insert(value, -10, 0)
                                   |from array_tbl
                                   |""".stripMargin) {
                checkGlutenOperatorMatch[ProjectExecTransformer]
              }
            }
        }
    }
  }

  test("round on integral types should return same values as spark") {
    // Scale > 0 should return same value as input on integral values
    compareResultsAgainstVanillaSpark("select round(78, 1)", true, { _ => })
    // Scale < 0 should round down even on integral values
    compareResultsAgainstVanillaSpark("select round(44, -1)", true, { _ => })
  }

  test("test internal function: AtLeastNNonNulls") {
    // AtLeastNNonNulls is called by drop DataFrameNafunction
    withTempPath {
      path =>
        val input = Seq[(String, java.lang.Integer, java.lang.Double)](
          ("Bob", 16, 176.5),
          ("Alice", null, 164.3),
          ("David", 60, null),
          ("Nina", 25, Double.NaN),
          ("Amy", null, null),
          (null, null, null)
        ).toDF("name", "age", "height")
        val rows = input.collect()
        input.write.parquet(path.getCanonicalPath)

        val df = spark.read.parquet(path.getCanonicalPath).na.drop(2, Seq("age", "height"))
        checkAnswer(df, rows(0) :: Nil)
        checkGlutenOperatorMatch[FilterExecTransformer](df)
    }
  }

  testWithSpecifiedSparkVersion("Test try_cast", Some("3.4")) {
    withTempView("try_cast_table") {
      withTempPath {
        path =>
          Seq[(String)](("123456"), ("000A1234"))
            .toDF("str")
            .write
            .parquet(path.getCanonicalPath)
          spark.read.parquet(path.getCanonicalPath).createOrReplaceTempView("try_cast_table")
          runQueryAndCompare("select try_cast(str as bigint) from try_cast_table") {
            checkGlutenOperatorMatch[ProjectExecTransformer]
          }
          runQueryAndCompare("select try_cast(str as double) from try_cast_table") {
            checkGlutenOperatorMatch[ProjectExecTransformer]
          }
      }
    }
  }

  test("Test cast") {
    withTempView("cast_table") {
      withTempPath {
        path =>
          Seq[(String)](("123456"), ("000A1234"))
            .toDF("str")
            .write
            .parquet(path.getCanonicalPath)
          spark.read.parquet(path.getCanonicalPath).createOrReplaceTempView("cast_table")
          runQueryAndCompare("select cast(str as bigint) from cast_table") {
            checkGlutenOperatorMatch[ProjectExecTransformer]
          }
          runQueryAndCompare("select cast(str as double) from cast_table") {
            checkGlutenOperatorMatch[ProjectExecTransformer]
          }
      }
    }
  }
}
