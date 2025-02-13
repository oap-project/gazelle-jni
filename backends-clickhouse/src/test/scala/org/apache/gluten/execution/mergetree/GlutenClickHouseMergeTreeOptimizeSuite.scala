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
package org.apache.gluten.execution.mergetree

import org.apache.gluten.backendsapi.clickhouse.{CHConfig, RuntimeConfig, RuntimeSettings}
import org.apache.gluten.config.GlutenConfig
import org.apache.gluten.execution.{FileSourceScanExecTransformer, GlutenClickHouseTPCHAbstractSuite}

import org.apache.spark.SparkConf
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.delta.MergeTreeConf
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanHelper

import io.delta.tables.ClickhouseTable

import java.io.File

import scala.concurrent.duration.DurationInt

class GlutenClickHouseMergeTreeOptimizeSuite
  extends GlutenClickHouseTPCHAbstractSuite
  with AdaptiveSparkPlanHelper {

  override protected val needCopyParquetToTablePath = true

  override protected val tablesPath: String = basePath + "/tpch-data"
  override protected val tpchQueries: String = rootPath + "queries/tpch-queries-ch"
  override protected val queriesResults: String = rootPath + "mergetree-queries-output"

  /** Run Gluten + ClickHouse Backend with SortShuffleManager */
  override protected def sparkConf: SparkConf = {
    import org.apache.gluten.backendsapi.clickhouse.CHConfig._

    super.sparkConf
      .set("spark.shuffle.manager", "org.apache.spark.shuffle.sort.ColumnarShuffleManager")
      .set("spark.io.compression.codec", "LZ4")
      .set("spark.sql.shuffle.partitions", "5")
      .set("spark.sql.autoBroadcastJoinThreshold", "10MB")
      .set("spark.sql.adaptive.enabled", "true")
      .set(RuntimeConfig.LOGGER_LEVEL.key, "error")
      .set(GlutenConfig.NATIVE_WRITER_ENABLED.key, "true")
      .set(CHConfig.ENABLE_ONEPIPELINE_MERGETREE_WRITE.key, spark35.toString)
      .set(RuntimeSettings.MIN_INSERT_BLOCK_SIZE_ROWS.key, "10000")
      .set(
        "spark.databricks.delta.retentionDurationCheck.enabled",
        "false"
      ) // otherwise, RETAIN 0 HOURS will fail
      .setCHSettings("mergetree.merge_after_insert", false)
      .setCHSettings("input_format_parquet_max_block_size", 8192)
  }

  private def with_ut_conf(f: => Unit): Unit = {
    val defaultBlockSize = RuntimeSettings.MIN_INSERT_BLOCK_SIZE_ROWS.key -> "1048449"

    /** The old merge-path will create uuid.txt by default, so we need to enable it for UT. */
    val assign_part_uuids = MergeTreeConf.ASSIGN_PART_UUIDS.key -> true.toString

    /**
     * The old merge-path uses uncompressed bytes to choose wide or compaction mode, which is more
     * accurate. By Using min_rows_for_wide_part, we can more accurately control the choosing of the
     * mergetree table mode.
     */
    val min_rows_for_wide_part = MergeTreeConf.MIN_ROWS_FOR_WIDE_PART.key -> "65536"

    val optimized = MergeTreeConf.OPTIMIZE_TASK.key -> true.toString
    withSQLConf(defaultBlockSize, assign_part_uuids, optimized, min_rows_for_wide_part)(f)
  }

  override protected def createTPCHNotNullTables(): Unit = {
    createNotNullTPCHTablesInParquet(tablesPath)
  }

  test("test mergetree optimize basic") {
    withSQLConf("spark.databricks.delta.optimize.maxFileSize" -> "2000000") {
      spark.sql(s"""
                   |DROP TABLE IF EXISTS lineitem_mergetree_optimize;
                   |""".stripMargin)

      spark.sql(s"""
                   |CREATE TABLE IF NOT EXISTS lineitem_mergetree_optimize
                   |USING clickhouse
                   |LOCATION '$basePath/lineitem_mergetree_optimize'
                   | as select * from lineitem
                   |""".stripMargin)

      with_ut_conf(spark.sql("optimize lineitem_mergetree_optimize"))
      val ret = spark.sql("select count(*) from lineitem_mergetree_optimize").collect()
      assertResult(600572)(ret.apply(0).get(0))

      assertResult(462)(
        countFiles(new File(s"$basePath/lineitem_mergetree_optimize"))
      ) // many merged parts
    }
  }

  def countFiles(directory: File): Int = {
    if (directory.exists && directory.isDirectory && !directory.getName.equals("_commits")) {
      val files = directory.listFiles
      val count = files
        .filter(!_.getName.endsWith(".crc"))
        .count(_.isFile) + files.filter(_.isDirectory).map(countFiles).sum
      count + 1
    } else {
      0
    }
  }

  test("test mergetree optimize partitioned, each partition too small to trigger optimize") {
    spark.sql(s"""
                 |DROP TABLE IF EXISTS lineitem_mergetree_optimize_p;
                 |""".stripMargin)

    spark.sql(s"""
                 |CREATE TABLE IF NOT EXISTS lineitem_mergetree_optimize_p
                 |USING clickhouse
                 |PARTITIONED BY (l_shipdate)
                 |LOCATION '$basePath/lineitem_mergetree_optimize_p'
                 | as select * from lineitem
                 |""".stripMargin)

    spark.sparkContext.setJobGroup("test", "test")
    with_ut_conf(spark.sql("optimize lineitem_mergetree_optimize_p"))
    val job_ids = spark.sparkContext.statusTracker.getJobIdsForGroup("test")
    if (spark35) {
      assertResult(4)(job_ids.length)
    } else {
      assertResult(1)(job_ids.length) // will not trigger actual merge job
    }
    spark.sparkContext.clearJobGroup()

    val ret = spark.sql("select count(*) from lineitem_mergetree_optimize_p").collect()
    assertResult(600572)(ret.apply(0).get(0))

    assertResult(22728)(countFiles(new File(s"$basePath/lineitem_mergetree_optimize_p")))
    spark.sql("VACUUM lineitem_mergetree_optimize_p RETAIN 0 HOURS")
    if (spark32) {
      assertResult(22728)(countFiles(new File(s"$basePath/lineitem_mergetree_optimize_p")))
    } else {
      // For Spark 3.3 + Delta 2.3, vacuum command will create two commit files in deltalog dir.
      assertResult(22730)(countFiles(new File(s"$basePath/lineitem_mergetree_optimize_p")))
    }

    val ret2 = spark.sql("select count(*) from lineitem_mergetree_optimize_p").collect()
    assertResult(600572)(ret2.apply(0).get(0))
  }

  test("test mergetree optimize partitioned by one low card column") {
    spark.sql(s"""
                 |DROP TABLE IF EXISTS lineitem_mergetree_optimize_p2;
                 |""".stripMargin)

    spark.sql(s"""
                 |CREATE TABLE IF NOT EXISTS lineitem_mergetree_optimize_p2
                 |USING clickhouse
                 |PARTITIONED BY (l_returnflag)
                 |LOCATION '$basePath/lineitem_mergetree_optimize_p2'
                 | as select * from lineitem
                 |""".stripMargin)

    spark.sparkContext.setJobGroup("test2", "test2")
    with_ut_conf(spark.sql("optimize lineitem_mergetree_optimize_p2"))
    val job_ids = spark.sparkContext.statusTracker.getJobIdsForGroup("test2")
    if (spark32) {
      assertResult(7)(job_ids.length) // WILL trigger actual merge job
    } else {
      assertResult(8)(job_ids.length) // WILL trigger actual merge job
    }

    spark.sparkContext.clearJobGroup()

    val ret = spark.sql("select count(*) from lineitem_mergetree_optimize_p2").collect()
    assertResult(600572)(ret.apply(0).get(0))

    assertResult(372)(countFiles(new File(s"$basePath/lineitem_mergetree_optimize_p2")))
    spark.sql("VACUUM lineitem_mergetree_optimize_p2 RETAIN 0 HOURS")
    if (spark32) {
      assertResult(239)(countFiles(new File(s"$basePath/lineitem_mergetree_optimize_p2")))
    } else {
      assertResult(241)(countFiles(new File(s"$basePath/lineitem_mergetree_optimize_p2")))
    }
    spark.sql("VACUUM lineitem_mergetree_optimize_p2 RETAIN 0 HOURS")
    // the second VACUUM will remove some empty folders
    if (spark32) {
      assertResult(220)(countFiles(new File(s"$basePath/lineitem_mergetree_optimize_p2")))
    } else {
      assertResult(226)(countFiles(new File(s"$basePath/lineitem_mergetree_optimize_p2")))
    }

    val ret2 = spark.sql("select count(*) from lineitem_mergetree_optimize_p2").collect()
    assertResult(600572)(ret2.apply(0).get(0))
  }

  test("test mergetree optimize partitioned by two low card column") {
    withSQLConf("spark.databricks.delta.vacuum.parallelDelete.enabled" -> "false") {
      spark.sql(s"""
                   |DROP TABLE IF EXISTS lineitem_mergetree_optimize_p3;
                   |""".stripMargin)

      spark.sql(s"""
                   |CREATE TABLE IF NOT EXISTS lineitem_mergetree_optimize_p3
                   |USING clickhouse
                   |PARTITIONED BY (l_linenumber,l_returnflag)
                   |LOCATION '$basePath/lineitem_mergetree_optimize_p3'
                   | as select * from lineitem
                   |""".stripMargin)

      with_ut_conf(spark.sql("optimize lineitem_mergetree_optimize_p3"))
      val ret = spark.sql("select count(*) from lineitem_mergetree_optimize_p3").collect()
      assertResult(600572)(ret.apply(0).get(0))

      assertResult(516)(countFiles(new File(s"$basePath/lineitem_mergetree_optimize_p3")))
      spark.sql("VACUUM lineitem_mergetree_optimize_p3 RETAIN 0 HOURS")
      if (spark32) {
        assertResult(306)(countFiles(new File(s"$basePath/lineitem_mergetree_optimize_p3")))
      } else {
        assertResult(308)(countFiles(new File(s"$basePath/lineitem_mergetree_optimize_p3")))
      }
      spark.sql("VACUUM lineitem_mergetree_optimize_p3 RETAIN 0 HOURS")
      if (spark32) {
        assertResult(276)(countFiles(new File(s"$basePath/lineitem_mergetree_optimize_p3")))
      } else {
        assertResult(282)(countFiles(new File(s"$basePath/lineitem_mergetree_optimize_p3")))
      }

      val ret2 = spark.sql("select count(*) from lineitem_mergetree_optimize_p3").collect()
      assertResult(600572)(ret2.apply(0).get(0))
    }
  }

  test("test mergetree optimize parallel delete") {
    withSQLConf("spark.databricks.delta.vacuum.parallelDelete.enabled" -> "true") {
      spark.sql(s"""
                   |DROP TABLE IF EXISTS lineitem_mergetree_optimize_p4;
                   |""".stripMargin)

      spark.sql(s"""
                   |CREATE TABLE IF NOT EXISTS lineitem_mergetree_optimize_p4
                   |USING clickhouse
                   |PARTITIONED BY (l_linenumber,l_returnflag)
                   |LOCATION '$basePath/lineitem_mergetree_optimize_p4'
                   | as select * from lineitem
                   |""".stripMargin)

      with_ut_conf(spark.sql("optimize lineitem_mergetree_optimize_p4"))
      val ret = spark.sql("select count(*) from lineitem_mergetree_optimize_p4").collect()
      assertResult(600572)(ret.apply(0).get(0))

      assertResult(516)(countFiles(new File(s"$basePath/lineitem_mergetree_optimize_p4")))
      spark.sql("VACUUM lineitem_mergetree_optimize_p4 RETAIN 0 HOURS")
      if (spark32) {
        assertResult(306)(countFiles(new File(s"$basePath/lineitem_mergetree_optimize_p4")))
      } else {
        assertResult(308)(countFiles(new File(s"$basePath/lineitem_mergetree_optimize_p4")))
      }
      spark.sql("VACUUM lineitem_mergetree_optimize_p4 RETAIN 0 HOURS")
      if (spark32) {
        assertResult(276)(countFiles(new File(s"$basePath/lineitem_mergetree_optimize_p4")))
      } else {
        assertResult(282)(countFiles(new File(s"$basePath/lineitem_mergetree_optimize_p4")))
      }

      val ret2 = spark.sql("select count(*) from lineitem_mergetree_optimize_p4").collect()
      assertResult(600572)(ret2.apply(0).get(0))
    }
  }

  test("test mergetree optimize with optimize.minFileSize and optimize.maxFileSize") {
    withSQLConf("spark.databricks.delta.optimize.minFileSize" -> "838000") {
      // 3 from 37 parts are larger than this, so after optimize there should be 4 parts:
      // 3 original parts and 1 merged part
      spark.sql(s"""
                   |DROP TABLE IF EXISTS lineitem_mergetree_optimize_p5;
                   |""".stripMargin)

      spark.sql(s"""
                   |CREATE TABLE IF NOT EXISTS lineitem_mergetree_optimize_p5
                   |USING clickhouse
                   |LOCATION '$basePath/lineitem_mergetree_optimize_p5'
                   | as select * from lineitem
                   |""".stripMargin)

      with_ut_conf(spark.sql("optimize lineitem_mergetree_optimize_p5"))

      spark.sql("VACUUM lineitem_mergetree_optimize_p5 RETAIN 0 HOURS")
      spark.sql("VACUUM lineitem_mergetree_optimize_p5 RETAIN 0 HOURS")
      if (spark32) {
        assertResult(99)(countFiles(new File(s"$basePath/lineitem_mergetree_optimize_p5")))
      } else {
        // For Spark 3.3 + Delta 2.3, vacuum command will create two commit files in deltalog dir.
        // this case will create a checkpoint
        assertResult(105)(countFiles(new File(s"$basePath/lineitem_mergetree_optimize_p5")))
      }

      val ret = spark.sql("select count(*) from lineitem_mergetree_optimize_p5").collect()
      assertResult(600572)(ret.apply(0).get(0))
    }

    withSQLConf(
      "spark.databricks.delta.optimize.maxFileSize" -> "10000000",
      "spark.databricks.delta.optimize.minFileSize" -> "838250") {
      // of the remaing 3 original parts, 2 are less than 838250, 1 is larger (size 838255)
      // the merged part is ~27MB, so after optimize there should be 3 parts:
      // 1 merged part from 2 original parts, 1 merged part from 34 original parts
      // and 1 original part (size 838255)

      with_ut_conf(spark.sql("optimize lineitem_mergetree_optimize_p5"))

      spark.sql("VACUUM lineitem_mergetree_optimize_p5 RETAIN 0 HOURS")
      spark.sql("VACUUM lineitem_mergetree_optimize_p5 RETAIN 0 HOURS")
      if (spark32) {
        assertResult(93)(countFiles(new File(s"$basePath/lineitem_mergetree_optimize_p5")))
      } else {
        // For Spark 3.3 + Delta 2.3, vacuum command will create two commit files in deltalog dir.
        assertResult(104)(countFiles(new File(s"$basePath/lineitem_mergetree_optimize_p5")))
      }

      val ret = spark.sql("select count(*) from lineitem_mergetree_optimize_p5").collect()
      assertResult(600572)(ret.apply(0).get(0))
    }

    // now merge all parts (testing merging from merged parts)
    with_ut_conf(spark.sql("optimize lineitem_mergetree_optimize_p5"))

    spark.sql("VACUUM lineitem_mergetree_optimize_p5 RETAIN 0 HOURS")
    spark.sql("VACUUM lineitem_mergetree_optimize_p5 RETAIN 0 HOURS")
    if (spark32) {
      assertResult(77)(countFiles(new File(s"$basePath/lineitem_mergetree_optimize_p5")))
    } else {
      // For Spark 3.3 + Delta 2.3, vacuum command will create two commit files in deltalog dir.
      assertResult(93)(countFiles(new File(s"$basePath/lineitem_mergetree_optimize_p5")))
    }

    val ret = spark.sql("select count(*) from lineitem_mergetree_optimize_p5").collect()
    assertResult(600572)(ret.apply(0).get(0))
  }

  testSparkVersionLE33("test mergetree optimize table with partition and bucket") {
    spark.sql(s"""
                 |DROP TABLE IF EXISTS lineitem_mergetree_optimize_p6;
                 |""".stripMargin)

    spark.sql(s"""
                 |CREATE TABLE IF NOT EXISTS lineitem_mergetree_optimize_p6
                 |USING clickhouse
                 |PARTITIONED BY (l_returnflag)
                 |LOCATION '$basePath/lineitem_mergetree_optimize_p6'
                 | CLUSTERED BY (l_partkey)
                 | ${if (spark32) "" else "SORTED BY (l_partkey)"} INTO 2 BUCKETS
                 | as select * from lineitem
                 |""".stripMargin)

    spark.sql("optimize lineitem_mergetree_optimize_p6")

    val ret = spark.sql("select count(*) from lineitem_mergetree_optimize_p6").collect()
    assertResult(600572)(ret.apply(0).get(0))

    assertResult(countFiles(new File(s"$basePath/lineitem_mergetree_optimize_p6")))(
      if (spark32) 499 else 528)
    spark.sql("VACUUM lineitem_mergetree_optimize_p6 RETAIN 0 HOURS")
    spark.sql("VACUUM lineitem_mergetree_optimize_p6 RETAIN 0 HOURS")
    assertResult(countFiles(new File(s"$basePath/lineitem_mergetree_optimize_p6")))(
      if (spark32) 315 else 327)

    val ret2 = spark.sql("select count(*) from lineitem_mergetree_optimize_p6").collect()
    assertResult(600572)(ret2.apply(0).get(0))
  }

  test("test skip index after optimize") {
    withSQLConf(
      "spark.databricks.delta.optimize.maxFileSize" -> "2000000",
      "spark.sql.adaptive.enabled" -> "false") {
      spark.sql(s"""
                   |DROP TABLE IF EXISTS lineitem_mergetree_index;
                   |""".stripMargin)

      spark.sql(s"""
                   |CREATE TABLE IF NOT EXISTS lineitem_mergetree_index
                   |USING clickhouse
                   |LOCATION '$basePath/lineitem_mergetree_index'
                   |TBLPROPERTIES('bloomfilterIndexKey'='l_orderkey')
                   | as select * from lineitem
                   |""".stripMargin)

      with_ut_conf(spark.sql("optimize lineitem_mergetree_index"))
      spark.sql("vacuum lineitem_mergetree_index")

      val df = spark
        .sql(s"""
                |select count(*) from lineitem_mergetree_index  where l_orderkey = '600000'
                |""".stripMargin)

      val scanExec = collect(df.queryExecution.executedPlan) {
        case f: FileSourceScanExecTransformer => f
      }
      assertResult(1)(scanExec.size)
      val mergetreeScan = scanExec.head
      val ret = df.collect()
      assertResult(2)(ret.apply(0).get(0))
      val marks = mergetreeScan.metrics("selectedMarks").value
      assertResult(1)(marks)

      val directory = new File(s"$basePath/lineitem_mergetree_index")
      val partDir = directory.listFiles().filter(f => f.getName.endsWith("merged")).head
      assert(
        partDir.listFiles().exists(p => p.getName.contains("skp_idx__bloomfilter_l_orderkey.idx")))

    }
  }

  test("test mergetree optimize with the path based table") {
    val dataPath = s"$basePath/lineitem_mergetree_optimize_path_based"
    clearDataPath(dataPath)
    withSQLConf("spark.databricks.delta.optimize.minFileSize" -> "838000") {
      // 3 from 37 parts are larger than this, so after optimize there should be 4 parts:
      // 3 original parts and 1 merged part

      val sourceDF = spark.sql(s"""
                                  |select * from lineitem
                                  |""".stripMargin)

      sourceDF.write
        .format("clickhouse")
        .mode(SaveMode.Append)
        .save(dataPath)

      val clickhouseTable = ClickhouseTable.forPath(spark, dataPath)
      with_ut_conf(clickhouseTable.optimize().executeCompaction())

      clickhouseTable.vacuum(0.0)
      clickhouseTable.vacuum(0.0)
      if (spark32) {
        assertResult(99)(countFiles(new File(dataPath)))
      } else {
        assertResult(105)(countFiles(new File(dataPath)))
      }

      val ret = spark.sql(s"select count(*) from clickhouse.`$dataPath`").collect()
      assertResult(600572)(ret.apply(0).get(0))
    }

    withSQLConf(
      "spark.databricks.delta.optimize.maxFileSize" -> "10000000",
      "spark.databricks.delta.optimize.minFileSize" -> "838250") {
      // of the remaing 3 original parts, 2 are less than 838250, 1 is larger (size 838255)
      // the merged part is ~27MB, so after optimize there should be 3 parts:
      // 1 merged part from 2 original parts, 1 merged part from 34 original parts
      // and 1 original part (size 838255)

      val clickhouseTable = ClickhouseTable.forPath(spark, dataPath)
      with_ut_conf(clickhouseTable.optimize().executeCompaction())

      clickhouseTable.vacuum(0.0)
      clickhouseTable.vacuum(0.0)
      if (spark32) {
        assertResult(93)(countFiles(new File(dataPath)))
      } else {
        assertResult(104)(countFiles(new File(dataPath)))
      }

      val ret = spark.sql(s"select count(*) from clickhouse.`$dataPath`").collect()
      assertResult(600572)(ret.apply(0).get(0))
    }

    // now merge all parts (testing merging from merged parts)
    val clickhouseTable = ClickhouseTable.forPath(spark, dataPath)
    with_ut_conf(clickhouseTable.optimize().executeCompaction())

    clickhouseTable.vacuum(0.0)
    clickhouseTable.vacuum(0.0)
    if (spark32) {
      assertResult(77)(countFiles(new File(dataPath)))
    } else {
      assertResult(93)(countFiles(new File(dataPath)))
    }

    val ret = spark.sql(s"select count(*) from clickhouse.`$dataPath`").collect()
    assertResult(600572)(ret.apply(0).get(0))
  }

  test("test mergetree insert with optimize basic") {
    withSQLConf(
      "spark.databricks.delta.optimize.minFileSize" -> "200000000",
      CHConfig.runtimeSettings("mergetree.merge_after_insert") -> "true"
    ) {
      spark.sql(s"""
                   |DROP TABLE IF EXISTS lineitem_mergetree_insert_optimize_basic;
                   |""".stripMargin)

      spark.sql(s"""
                   |CREATE TABLE IF NOT EXISTS lineitem_mergetree_insert_optimize_basic
                   |USING clickhouse
                   |LOCATION '$basePath/lineitem_mergetree_insert_optimize_basic'
                   | as select * from lineitem
                   |""".stripMargin)

      val ret = spark.sql("select count(*) from lineitem_mergetree_insert_optimize_basic").collect()
      assertResult(600572)(ret.apply(0).get(0))
      eventually(timeout(60.seconds), interval(3.seconds)) {
        assertResult(2)(
          new File(s"$basePath/lineitem_mergetree_insert_optimize_basic").listFiles().length
        )
      }
    }
  }
}
