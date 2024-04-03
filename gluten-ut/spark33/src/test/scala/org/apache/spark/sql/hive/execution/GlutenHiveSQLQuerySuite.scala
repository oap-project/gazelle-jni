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

import org.apache.gluten.execution.FileSourceScanExecTransformer

import org.apache.spark.SparkConf
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.hive.HiveTableScanExecTransformer

class GlutenHiveSQLQuerySuite extends GlutenHiveSQLQuerySuiteBase {

  override def sparkConf: SparkConf = {
    defaultSparkConf
      .set("spark.plugins", "org.apache.gluten.GlutenPlugin")
      .set("spark.default.parallelism", "1")
      .set("spark.memory.offHeap.enabled", "true")
      .set("spark.memory.offHeap.size", "1024MB")
  }

  testGluten("hive orc scan") {
    withSQLConf("spark.sql.hive.convertMetastoreOrc" -> "false") {
      sql("DROP TABLE IF EXISTS test_orc")
      sql(
        "CREATE TABLE test_orc (name STRING, favorite_color STRING)" +
          " USING hive OPTIONS(fileFormat 'orc')")
      sql("INSERT INTO test_orc VALUES('test_1', 'red')");
      val df = spark.sql("select * from test_orc")
      checkAnswer(df, Seq(Row("test_1", "red")))
      checkOperatorMatch[HiveTableScanExecTransformer](df)
    }
    spark.sessionState.catalog.dropTable(
      TableIdentifier("test_orc"),
      ignoreIfNotExists = true,
      purge = false)
  }

  testGluten("Add orc char type validation") {
    withSQLConf("spark.sql.hive.convertMetastoreOrc" -> "false") {
      sql("DROP TABLE IF EXISTS test_orc")
      sql(
        "CREATE TABLE test_orc (name char(10), id int)" +
          " USING hive OPTIONS(fileFormat 'orc')")
      sql("INSERT INTO test_orc VALUES('test', 1)")
    }

    def testExecPlan(
        convertMetastoreOrc: String,
        charTypeFallbackEnabled: String,
        shouldFindTransformer: Boolean,
        transformerClass: Class[_ <: SparkPlan]
    ): Unit = {

      withSQLConf(
        "spark.sql.hive.convertMetastoreOrc" -> convertMetastoreOrc,
        "spark.gluten.sql.orc.charType.scan.fallback.enabled" -> charTypeFallbackEnabled
      ) {
        val queries = Seq("select id from test_orc", "select name, id from test_orc")

        queries.foreach {
          query =>
            val executedPlan = getExecutedPlan(spark.sql(query))
            val planCondition = executedPlan.exists(_.find(transformerClass.isInstance).isDefined)

            if (shouldFindTransformer) {
              assert(planCondition)
            } else {
              assert(!planCondition)
            }
        }
      }
    }

    testExecPlan(
      "false",
      "true",
      shouldFindTransformer = false,
      classOf[HiveTableScanExecTransformer])
    testExecPlan(
      "false",
      "false",
      shouldFindTransformer = true,
      classOf[HiveTableScanExecTransformer])

    testExecPlan(
      "true",
      "true",
      shouldFindTransformer = false,
      classOf[FileSourceScanExecTransformer])
    testExecPlan(
      "true",
      "false",
      shouldFindTransformer = true,
      classOf[FileSourceScanExecTransformer])
    spark.sessionState.catalog.dropTable(
      TableIdentifier("test_orc"),
      ignoreIfNotExists = true,
      purge = false)
  }

  testGluten("avoid unnecessary filter binding for subfield during scan") {
    withSQLConf(
      "spark.sql.hive.convertMetastoreParquet" -> "false",
      "spark.gluten.sql.complexType.scan.fallback.enabled" -> "false") {
      sql("DROP TABLE IF EXISTS test_subfield")
      sql(
        "CREATE TABLE test_subfield (name STRING, favorite_color STRING," +
          " label STRUCT<label_1:STRING, label_2:STRING>) USING hive OPTIONS(fileFormat 'parquet')")
      sql(
        "INSERT INTO test_subfield VALUES('test_1', 'red', named_struct('label_1', 'label-a'," +
          "'label_2', 'label-b'))");
      val df = spark.sql("select * from test_subfield where name='test_1'")
      checkAnswer(df, Seq(Row("test_1", "red", Row("label-a", "label-b"))))
      checkOperatorMatch[HiveTableScanExecTransformer](df)
    }
    spark.sessionState.catalog.dropTable(
      TableIdentifier("test_subfield"),
      ignoreIfNotExists = true,
      purge = false)
  }

  testGluten("4990: dynamic partition may lose data") {
    withSQLConf(
      "spark.sql.hive.convertMetastoreParquet" -> "false",
      "spark.gluten.sql.complexType.scan.fallback.enabled" -> "false",
      "spark.gluten.sql.columnar.backend.ch.runtime_settings.input_format_parquet_max_block_size"
        -> 1,
      "spark.default.parallelism" -> 1
    ) {
      sql("DROP TABLE IF EXISTS test_4990_0;")
      sql("DROP TABLE IF EXISTS test_4990_1;")
      sql(
        "CREATE TABLE test_4990_0 (name STRING, favorite_color STRING, day STRING) " +
          "USING hive OPTIONS(fileFormat 'parquet');")
      sql(
        "CREATE TABLE test_4990_1 (name STRING, favorite_color STRING) " +
          "USING hive OPTIONS(fileFormat 'parquet') PARTITIONED BY (`day` STRING);")
      sql(
        "INSERT INTO test_4990_0 VALUES('test_1', 'red', '2024-03-30'), " +
          "('test_2', 'green', '2024-03-31'), ('test_3', 'blue', '2024-03-30');")
      sql(
        "INSERT overwrite table test_4990_1 partition(day) " +
          "select name, favorite_color, day from test_4990_0;")
      val df = spark.sql("select * from test_4990_1")
      checkAnswer(
        df,
        Seq(
          Row("test_1", "red", "2024-03-30"),
          Row("test_2", "green", "2024-03-31"),
          Row("test_3", "blue", "2024-03-30")))
      checkOperatorMatch[HiveTableScanExecTransformer](df)
    }
    spark.sessionState.catalog.dropTable(
      TableIdentifier("test_4990_0"),
      ignoreIfNotExists = true,
      purge = false)
    spark.sessionState.catalog.dropTable(
      TableIdentifier("test_4990_1"),
      ignoreIfNotExists = true,
      purge = false)
  }
}
