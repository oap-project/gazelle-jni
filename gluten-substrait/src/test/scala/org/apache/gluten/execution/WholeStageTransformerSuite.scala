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

import org.apache.gluten.GlutenConfig
import org.apache.gluten.test.FallbackUtil
import org.apache.gluten.utils.Arm

import org.apache.spark.SparkConf
import org.apache.spark.internal.Logging
import org.apache.spark.sql.{DataFrame, GlutenQueryTest, Row}
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanHelper
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.DoubleType

import java.io.File
import java.util.concurrent.atomic.AtomicBoolean

import scala.io.Source

case class Table(name: String, partitionColumns: Seq[String])

abstract class WholeStageTransformerSuite
  extends GlutenQueryTest
  with SharedSparkSession
  with AdaptiveSparkPlanHelper {

  protected val resourcePath: String
  protected val fileFormat: String
  protected val logLevel: String = "WARN"

  protected val TPCHTables: Seq[Table] = Seq(
    Table("part", partitionColumns = "p_brand" :: Nil),
    Table("supplier", partitionColumns = Nil),
    Table("partsupp", partitionColumns = Nil),
    Table("customer", partitionColumns = "c_mktsegment" :: Nil),
    Table("orders", partitionColumns = "o_orderdate" :: Nil),
    Table("lineitem", partitionColumns = "l_shipdate" :: Nil),
    Table("nation", partitionColumns = Nil),
    Table("region", partitionColumns = Nil)
  )

  protected var TPCHTableDataFrames: Map[String, DataFrame] = _

  private val isFallbackCheckDisabled0 = new AtomicBoolean(false)

  final protected def disableFallbackCheck: Boolean =
    isFallbackCheckDisabled0.compareAndSet(false, true)

  protected def needCheckFallback: Boolean = !isFallbackCheckDisabled0.get()

  override def beforeAll(): Unit = {
    super.beforeAll()
    sparkContext.setLogLevel(logLevel)
  }

  override protected def afterAll(): Unit = {
    if (TPCHTableDataFrames != null) {
      TPCHTableDataFrames.keys.foreach(v => spark.sessionState.catalog.dropTempView(v))
    }
    super.afterAll()
  }

  protected def createTPCHNotNullTables(): Unit = {
    TPCHTableDataFrames = TPCHTables
      .map(_.name)
      .map {
        table =>
          val tableDir = getClass.getResource(resourcePath).getFile
          val tablePath = new File(tableDir, table).getAbsolutePath
          val tableDF = spark.read.format(fileFormat).load(tablePath)
          tableDF.createOrReplaceTempView(table)
          (table, tableDF)
      }
      .toMap
  }

  override protected def sparkConf: SparkConf = {
    super.sparkConf
      .set("spark.plugins", "org.apache.gluten.GlutenPlugin")
      .set("spark.default.parallelism", "1")
      .set("spark.memory.offHeap.enabled", "true")
      .set("spark.memory.offHeap.size", "1024MB")
      .set("spark.ui.enabled", "false")
      .set("spark.gluten.ui.enabled", "false")
  }

  protected def checkFallbackOperators(df: DataFrame, num: Int): Unit = {
    // Decrease one VeloxColumnarToRowExec for the top level node
    assert(
      collect(df.queryExecution.executedPlan) {
        case p if p.isInstanceOf[ColumnarToRowExecBase] => p
        case p if p.isInstanceOf[RowToColumnarExecBase] => p
      }.size - 1 == num,
      df.queryExecution
    )
  }

  protected def compareResultStr(sqlNum: String, result: Seq[Row], queriesResults: String): Unit = {
    val resultStr = new StringBuffer()
    resultStr.append(result.length).append("\n")
    result.foreach(r => resultStr.append(r.mkString("|-|")).append("\n"))
    val queryResultStr =
      Arm.withResource(Source.fromFile(new File(queriesResults + "/" + sqlNum + ".out"), "UTF-8"))(
        _.mkString)
    assert(queryResultStr.equals(resultStr.toString))
  }

  protected def compareDoubleResult(
      sqlNum: String,
      result: Seq[Row],
      queriesResults: String): Unit =
    Arm.withResource(Source.fromFile(new File(queriesResults + "/" + sqlNum + ".out"), "UTF-8")) {
      resource =>
        val queryResults = resource.getLines()
        val recordCnt = queryResults.next().toInt
        assert(result.length == recordCnt)
        for (row <- result) {
          assert(queryResults.hasNext)
          val result = queryResults.next().split("\\|-\\|")
          var i = 0
          row.schema.foreach {
            s =>
              val isNull = row.isNullAt(i)
              val v1 = if (s.dataType == DoubleType) row.getDouble(i) else row.get(i)
              val v2 = result(i)

              assert(isNull == v2.equals("null"))
              if (!isNull) {
                if (s.dataType == DoubleType) {
                  assert(Math.abs(v1.asInstanceOf[Double] - v2.toDouble) < 0.0005)
                } else {
                  assert(v1.toString.equals(v2))
                }
              }
              i += 1
          }
        }
    }

  protected def runTPCHQuery(
      queryNum: Int,
      tpchQueries: String,
      queriesResults: String,
      compareResult: Boolean = true,
      noFallBack: Boolean = true)(customCheck: DataFrame => Unit): Unit =
    withDataFrame(tpchSQL(queryNum, tpchQueries)) {
      df =>
        if (compareResult) {
          verifyTPCHResult(df, s"q$queryNum", queriesResults)
        } else {
          df.collect()
        }
        checkDataFrame(noFallBack, customCheck, df)
    }

  protected def runSql(sql: String, noFallBack: Boolean = true)(
      customCheck: DataFrame => Unit): Seq[Row] = withDataFrame(sql) {
    df =>
      val result = df.collect()
      checkDataFrame(noFallBack, customCheck, df)
      result
  }

  protected def compareResultsAgainstVanillaSpark(
      sql: String,
      compareResult: Boolean = true,
      customCheck: DataFrame => Unit,
      noFallBack: Boolean = true,
      cache: Boolean = false): DataFrame = {
    compareDfResultsAgainstVanillaSpark(
      () => spark.sql(sql),
      compareResult,
      customCheck,
      noFallBack,
      cache)
  }

  /**
   * run a query with native engine as well as vanilla spark then compare the result set for
   * correctness check
   */
  protected def compareDfResultsAgainstVanillaSpark(
      dataframe: () => DataFrame,
      compareResult: Boolean = true,
      customCheck: DataFrame => Unit,
      noFallBack: Boolean = true,
      cache: Boolean = false): DataFrame = {
    var expected: Seq[Row] = null
    withSQLConf(vanillaSparkConfs(): _*) {
      val df = dataframe()
      expected = df.collect()
    }
    // By default, we will fallback complex type scan but here we should allow
    // to test support of complex type
    spark.conf.set("spark.gluten.sql.complexType.scan.fallback.enabled", "false");
    val df = dataframe()
    if (cache) {
      df.cache()
    }
    try {
      if (compareResult) {
        checkAnswer(df, expected)
      } else {
        df.collect()
      }
    } finally {
      if (cache) {
        df.unpersist()
      }
    }
    checkDataFrame(noFallBack, customCheck, df)
    df
  }

  /**
   * Some rule on LogicalPlan will not only apply in select query, the total df.load() should in
   * spark environment with gluten disabled config.
   *
   * @param sql
   * @param f
   * @return
   */
  protected def runAndCompare(sql: String)(f: => Unit): DataFrame = {
    var expected: Seq[Row] = null
    withSQLConf(vanillaSparkConfs(): _*) {
      f
      expected = spark.sql(sql).collect()
    }
    f
    val df = spark.sql(sql)
    checkAnswer(df, expected)
    df
  }

  protected def runQueryAndCompare(
      sqlStr: String,
      compareResult: Boolean = true,
      noFallBack: Boolean = true,
      cache: Boolean = false)(customCheck: DataFrame => Unit): DataFrame = {

    compareDfResultsAgainstVanillaSpark(
      () => spark.sql(sqlStr),
      compareResult,
      customCheck,
      noFallBack,
      cache)
  }

  /**
   * run a query with native engine as well as vanilla spark then compare the result set for
   * correctness check
   *
   * @param queryNum
   * @param tpchQueries
   * @param customCheck
   */
  protected def compareTPCHQueryAgainstVanillaSpark(
      queryNum: Int,
      tpchQueries: String,
      customCheck: DataFrame => Unit,
      noFallBack: Boolean = true,
      compareResult: Boolean = true): Unit =
    compareDfResultsAgainstVanillaSpark(
      () => spark.sql(tpchSQL(queryNum, tpchQueries)),
      compareResult,
      customCheck,
      noFallBack)

  protected def vanillaSparkConfs(): Seq[(String, String)] = {
    List((GlutenConfig.GLUTEN_ENABLED.key, "false"))
  }

  protected def checkDataFrame(
      noFallBack: Boolean,
      customCheck: DataFrame => Unit,
      df: DataFrame): Unit = {
    if (needCheckFallback) {
      WholeStageTransformerSuite.checkFallBack(df, noFallBack)
    }
    customCheck(df)
  }
  protected def withDataFrame[R](sql: String)(f: DataFrame => R): R = f(spark.sql(sql))

  protected def tpchSQL(queryNum: Int, tpchQueries: String): String =
    Arm.withResource(Source.fromFile(new File(s"$tpchQueries/q$queryNum.sql"), "UTF-8"))(_.mkString)

  protected def verifyTPCHResult(df: DataFrame, sqlNum: String, queriesResults: String): Unit = {
    val result = df.collect()
    if (df.schema.exists(_.dataType == DoubleType)) {
      compareDoubleResult(sqlNum, result, queriesResults)
    } else {
      compareResultStr(sqlNum, result, queriesResults)
    }
  }
}

object WholeStageTransformerSuite extends Logging {

  /** Check whether the sql is fallback */
  def checkFallBack(
      df: DataFrame,
      noFallback: Boolean = true,
      skipAssert: Boolean = false): Unit = {
    // When noFallBack is true, it means there is no fallback plan,
    // otherwise there must be some fallback plans.
    val hasFallbacks = FallbackUtil.hasFallback(df.queryExecution.executedPlan)
    if (!skipAssert) {
      assert(
        !hasFallbacks == noFallback,
        s"FallBack $noFallback check error: ${df.queryExecution.executedPlan}")
    } else {
      logWarning(s"FallBack $noFallback check error: ${df.queryExecution.executedPlan}")
    }
  }
}
