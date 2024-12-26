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
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._

import scala.collection.JavaConverters._

abstract class DeltaSuite extends WholeStageTransformerSuite {
  protected val rootPath: String = getClass.getResource("/").getPath
  // FIXME: This folder doesn't exist in module gluten-delta so should be provided by
  //  backend modules that rely on this suite.
  override protected val resourcePath: String = "/tpch-data-parquet"
  override protected val fileFormat: String = "parquet"

  override protected def sparkConf: SparkConf = {
    super.sparkConf
      .set("spark.shuffle.manager", "org.apache.spark.shuffle.sort.ColumnarShuffleManager")
      .set("spark.sql.files.maxPartitionBytes", "1g")
      .set("spark.sql.shuffle.partitions", "1")
      .set("spark.memory.offHeap.size", "2g")
      .set("spark.unsafe.exceptionOnMemoryLeak", "true")
      .set("spark.sql.autoBroadcastJoinThreshold", "-1")
      .set("spark.sql.sources.useV1SourceList", "avro")
      .set("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
      .set("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
  }

  // IdMapping is supported in Delta 2.2 (related to Spark3.3.1)
  testWithSpecifiedSparkVersion("column mapping mode = id", Some("3.3")) {
    withTable("delta_cm1") {
      spark.sql(s"""
                   |create table delta_cm1 (id int, name string) using delta
                   |tblproperties ("delta.columnMapping.mode"= "id")
                   |""".stripMargin)
      spark.sql(s"""
                   |insert into delta_cm1 values (1, "v1"), (2, "v2")
                   |""".stripMargin)
      val df1 = runQueryAndCompare("select * from delta_cm1") { _ => }
      checkLengthAndPlan(df1, 2)
      checkAnswer(df1, Row(1, "v1") :: Row(2, "v2") :: Nil)

      val df2 = runQueryAndCompare("select name from delta_cm1 where id = 2") { _ => }
      checkLengthAndPlan(df2, 1)
      checkAnswer(df2, Row("v2") :: Nil)
    }
  }

  // NameMapping is supported in Delta 2.0 (related to Spark3.2.0)
  testWithSpecifiedSparkVersion("column mapping mode = name", Some("3.2")) {
    withTable("delta_cm2") {
      spark.sql(s"""
                   |create table delta_cm2 (id int, name string) using delta
                   |tblproperties ("delta.columnMapping.mode"= "name")
                   |""".stripMargin)
      spark.sql(s"""
                   |insert into delta_cm2 values (1, "v1"), (2, "v2")
                   |""".stripMargin)
      val df1 = runQueryAndCompare("select * from delta_cm2") { _ => }
      checkLengthAndPlan(df1, 2)
      checkAnswer(df1, Row(1, "v1") :: Row(2, "v2") :: Nil)

      val df2 = runQueryAndCompare("select name from delta_cm2 where id = 2") { _ => }
      checkLengthAndPlan(df2, 1)
      checkAnswer(df2, Row("v2") :: Nil)
    }
  }

  testWithSpecifiedSparkVersion("delta: time travel", Some("3.3")) {
    withTable("delta_tm") {
      spark.sql(s"""
                   |create table delta_tm (id int, name string) using delta
                   |""".stripMargin)
      spark.sql(s"""
                   |insert into delta_tm values (1, "v1"), (2, "v2")
                   |""".stripMargin)
      spark.sql(s"""
                   |insert into delta_tm values (3, "v3"), (4, "v4")
                   |""".stripMargin)
      val df1 = runQueryAndCompare("select * from delta_tm VERSION AS OF 1") { _ => }
      checkLengthAndPlan(df1, 2)
      checkAnswer(df1, Row(1, "v1") :: Row(2, "v2") :: Nil)
      val df2 = runQueryAndCompare("select * from delta_tm VERSION AS OF 2") { _ => }
      checkLengthAndPlan(df2, 4)
      checkAnswer(df2, Row(1, "v1") :: Row(2, "v2") :: Row(3, "v3") :: Row(4, "v4") :: Nil)
      val df3 = runQueryAndCompare("select name from delta_tm VERSION AS OF 2 where id = 2") {
        _ =>
      }
      checkLengthAndPlan(df3, 1)
      checkAnswer(df3, Row("v2") :: Nil)
    }
  }

  testWithSpecifiedSparkVersion("delta: partition filters", Some("3.2")) {
    withTable("delta_pf") {
      spark.sql(s"""
                   |create table delta_pf (id int, name string) using delta partitioned by (name)
                   |""".stripMargin)
      spark.sql(s"""
                   |insert into delta_pf values (1, "v1"), (2, "v2"), (3, "v1"), (4, "v2")
                   |""".stripMargin)
      val df1 = runQueryAndCompare("select * from delta_pf where name = 'v1'") { _ => }
      val deltaScanTransformer = df1.queryExecution.executedPlan.collect {
        case f: DeltaScanTransformer => f
      }.head
      // No data filters as only partition filters exist
      assert(deltaScanTransformer.filterExprs().size == 0)
      checkLengthAndPlan(df1, 2)
      checkAnswer(df1, Row(1, "v1") :: Row(3, "v1") :: Nil)
    }
  }

  testWithSpecifiedSparkVersion("basic test with stats.skipping disabled", Some("3.2")) {
    withTable("delta_test2") {
      withSQLConf("spark.databricks.delta.stats.skipping" -> "false") {
        spark.sql(s"""
                     |create table delta_test2 (id int, name string) using delta
                     |""".stripMargin)
        spark.sql(s"""
                     |insert into delta_test2 values (1, "v1"), (2, "v2")
                     |""".stripMargin)
        val df1 = runQueryAndCompare("select * from delta_test2") { _ => }
        checkLengthAndPlan(df1, 2)
        checkAnswer(df1, Row(1, "v1") :: Row(2, "v2") :: Nil)

        val df2 = runQueryAndCompare("select name from delta_test2 where id = 2") { _ => }
        checkLengthAndPlan(df2, 1)
        checkAnswer(df2, Row("v2") :: Nil)
      }
    }
  }

  testWithSpecifiedSparkVersion("column mapping with complex type", Some("3.2")) {
    withTable("t1") {
      val simpleNestedSchema = new StructType()
        .add("a", StringType, true)
        .add("b", new StructType().add("c", StringType, true).add("d", IntegerType, true))
        .add("map", MapType(StringType, StringType), true)
        .add("arr", ArrayType(IntegerType), true)

      val simpleNestedData = spark.createDataFrame(
        Seq(
          Row("str1", Row("str1.1", 1), Map("k1" -> "v1"), Array(1, 11)),
          Row("str2", Row("str1.2", 2), Map("k2" -> "v2"), Array(2, 22))).asJava,
        simpleNestedSchema)

      spark.sql(
        """CREATE TABLE t1
          | (a STRING,b STRUCT<c: STRING NOT NULL, d: INT>,map MAP<STRING, STRING>,arr ARRAY<INT>)
          | USING DELTA
          | PARTITIONED BY (`a`)
          | TBLPROPERTIES ('delta.columnMapping.mode' = 'name')""".stripMargin)

      simpleNestedData.write.format("delta").mode("append").saveAsTable("t1")

      val df1 = runQueryAndCompare("select * from t1") { _ => }
      checkAnswer(
        df1,
        Seq(
          Row("str1", Row("str1.1", 1), Map("k1" -> "v1"), Array(1, 11)),
          Row("str2", Row("str1.2", 2), Map("k2" -> "v2"), Array(2, 22))))
      spark.sql(s"Alter table t1 RENAME COLUMN b to b1")
      spark.sql(
        "insert into t1 " +
          "values ('str3', struct('str1.3', 3), map('k3', 'v3'), array(3, 33))")

      val df2 = runQueryAndCompare("select b1 from t1") { _ => }
      checkAnswer(df2, Seq(Row(Row("str1.1", 1)), Row(Row("str1.2", 2)), Row(Row("str1.3", 3))))

      spark.sql(s"Alter table t1 RENAME COLUMN b1.c to c1")
      val df3 = runQueryAndCompare("select * from t1") { _ => }
      checkAnswer(
        df3,
        Seq(
          Row("str1", Row("str1.1", 1), Map("k1" -> "v1"), Array(1, 11)),
          Row("str2", Row("str1.2", 2), Map("k2" -> "v2"), Array(2, 22)),
          Row("str3", Row("str1.3", 3), Map("k3" -> "v3"), Array(3, 33))
        )
      )
    }
  }

  testWithSpecifiedSparkVersion("deletion vector", Some("3.4")) {
    withTempPath {
      p =>
        import testImplicits._
        val path = p.getCanonicalPath
        val df1 = Seq(1, 2, 3, 4, 5).toDF("id")
        val values2 = Seq(6, 7, 8, 9, 10)
        val df2 = values2.toDF("id")
        df1.union(df2).coalesce(1).write.format("delta").save(path)
        spark.sql(
          s"ALTER TABLE delta.`$path` SET TBLPROPERTIES ('delta.enableDeletionVectors' = true)")
        checkAnswer(spark.read.format("delta").load(path), df1.union(df2))
        spark.sql(s"DELETE FROM delta.`$path` WHERE id IN (${values2.mkString(", ")})")
        checkAnswer(spark.read.format("delta").load(path), df1)
    }
  }

  testWithSpecifiedSparkVersion("delta: push down input_file_name expression", Some("3.2")) {
    withTable("source_table") {
      withTable("target_table") {
        spark.sql(s"""
                     |CREATE TABLE source_table(id INT, name STRING, age INT) USING delta;
                     |""".stripMargin)

        spark.sql(s"""
                     |CREATE TABLE target_table(id INT, name STRING, age INT) USING delta;
                     |
                     |""".stripMargin)

        spark.sql(s"""
                     |INSERT INTO source_table VALUES(1, 'a', 10),(2, 'b', 20);
                     |""".stripMargin)

        spark.sql(s"""
                     |INSERT INTO target_table VALUES(1, 'c', 10),(3, 'c', 30);
                     |""".stripMargin)

        spark.sql(s"""
                     |MERGE INTO target_table AS target
                     |USING source_table AS source
                     |ON target.id = source.id
                     |WHEN MATCHED THEN
                     |UPDATE SET
                     |  target.name = source.name,
                     |  target.age = source.age
                     |WHEN NOT MATCHED THEN
                     |INSERT (id, name, age) VALUES (source.id, source.name, source.age);
                     |""".stripMargin)

        val df1 = runQueryAndCompare("SELECT * FROM target_table") { _ => }
        checkAnswer(df1, Row(1, "a", 10) :: Row(2, "b", 20) :: Row(3, "c", 30) :: Nil)
      }
    }
  }
}
