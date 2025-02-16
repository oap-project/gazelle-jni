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
import org.apache.spark.sql.{DataFrame, Row, TestUtils}
import org.apache.spark.sql.execution.InputIteratorTransformer
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanHelper
import org.apache.spark.sql.execution.aggregate.SortAggregateExec

import org.apache.commons.io.FileUtils

import java.io.File

import scala.collection.mutable

// Some sqls' line length exceeds 100
// scalastyle:off line.size.limit

class GlutenClickHouseTPCHBucketSuite
  extends GlutenClickHouseTPCHAbstractSuite
  with AdaptiveSparkPlanHelper {
  override protected val tablesPath: String = basePath + "/tpch-data-ch"
  override protected val tpchQueries: String = rootPath + "queries/tpch-queries-ch"
  override protected val queriesResults: String = rootPath + "bucket-queries-output"

  override protected def sparkConf: SparkConf = {
    import org.apache.gluten.backendsapi.clickhouse.CHConfig._
    super.sparkConf
      .set("spark.shuffle.manager", "org.apache.spark.shuffle.sort.ColumnarShuffleManager")
      .set("spark.io.compression.codec", "LZ4")
      .set("spark.sql.shuffle.partitions", "5")
      .set("spark.sql.autoBroadcastJoinThreshold", "-1") // for test bucket join
      .set("spark.sql.adaptive.enabled", "true")
      .set("spark.gluten.sql.columnar.backend.ch.shuffle.hash.algorithm", "sparkMurmurHash3_32")
      .setCHConfig("enable_pre_projection_for_join_conditions", "false")
      .setCHConfig("enable_grace_aggregate_spill_test", "true")
  }

  override protected val createNullableTables = true

  override protected def createTPCHNullableTables(): Unit = {
    // create parquet data source table
    val parquetSourceDB = "parquet_source"
    spark.sql(s"""
                 |CREATE DATABASE IF NOT EXISTS $parquetSourceDB
                 |""".stripMargin)
    spark.sql(s"use $parquetSourceDB")

    val parquetTablePath = basePath + "/tpch-data"
    val parquetTableDataPath: String =
      "../../../../gluten-core/src/test/resources/tpch-data"
    FileUtils.copyDirectory(new File(rootPath + parquetTableDataPath), new File(parquetTablePath))

    createNotNullTPCHTablesInParquet(parquetTablePath)

    spark.sql(s"""
                 |CREATE DATABASE IF NOT EXISTS tpch_mergetree_bucket
                 |""".stripMargin)
    spark.sql("use tpch_mergetree_bucket")
    val customerData = tablesPath + "/customer"
    spark.sql(s"DROP TABLE IF EXISTS customer")
    // On Spark 3.2, bucket table does not support to create bucket column with sort columns for
    // DS V2
    spark.sql(s"""
                 | CREATE EXTERNAL TABLE IF NOT EXISTS customer (
                 | c_custkey    bigint,
                 | c_name       string,
                 | c_address    string,
                 | c_nationkey  bigint,
                 | c_phone      string,
                 | c_acctbal    double,
                 | c_mktsegment string,
                 | c_comment    string)
                 | USING clickhouse
                 | LOCATION '$customerData'
                 | CLUSTERED BY (c_custkey)
                 | ${if (spark32) "" else "SORTED BY (c_custkey)"} INTO 2 BUCKETS;
                 |""".stripMargin)

    val lineitemData = tablesPath + "/lineitem"
    spark.sql(s"DROP TABLE IF EXISTS lineitem")
    spark.sql(s"""
                 | CREATE EXTERNAL TABLE IF NOT EXISTS lineitem (
                 | l_orderkey      bigint,
                 | l_partkey       bigint,
                 | l_suppkey       bigint,
                 | l_linenumber    bigint,
                 | l_quantity      double,
                 | l_extendedprice double,
                 | l_discount      double,
                 | l_tax           double,
                 | l_returnflag    string,
                 | l_linestatus    string,
                 | l_shipdate      date,
                 | l_commitdate    date,
                 | l_receiptdate   date,
                 | l_shipinstruct  string,
                 | l_shipmode      string,
                 | l_comment       string)
                 | USING clickhouse
                 | LOCATION '$lineitemData'
                 | CLUSTERED BY (l_orderkey)
                 | ${if (spark32) "" else "SORTED BY (l_shipdate, l_orderkey)"} INTO 2 BUCKETS;
                 |""".stripMargin)

    val nationData = tablesPath + "/nation"
    spark.sql(s"DROP TABLE IF EXISTS nation")
    spark.sql(s"""
                 | CREATE EXTERNAL TABLE IF NOT EXISTS nation (
                 | n_nationkey bigint,
                 | n_name      string,
                 | n_regionkey bigint,
                 | n_comment   string)
                 | USING clickhouse
                 | LOCATION '$nationData'
                 | CLUSTERED BY (n_nationkey)
                 | ${if (spark32) "" else "SORTED BY (n_nationkey)"} INTO 1 BUCKETS;
                 |""".stripMargin)

    val regionData = tablesPath + "/region"
    spark.sql(s"DROP TABLE IF EXISTS region")
    spark.sql(s"""
                 | CREATE EXTERNAL TABLE IF NOT EXISTS region (
                 | r_regionkey bigint,
                 | r_name      string,
                 | r_comment   string)
                 | USING clickhouse
                 | LOCATION '$regionData'
                 | CLUSTERED BY (r_regionkey)
                 | ${if (spark32) "" else "SORTED BY (r_regionkey)"} INTO 1 BUCKETS;
                 |""".stripMargin)

    val ordersData = tablesPath + "/orders"
    spark.sql(s"DROP TABLE IF EXISTS orders")
    spark.sql(s"""
                 | CREATE EXTERNAL TABLE IF NOT EXISTS orders (
                 | o_orderkey      bigint,
                 | o_custkey       bigint,
                 | o_orderstatus   string,
                 | o_totalprice    double,
                 | o_orderdate     date,
                 | o_orderpriority string,
                 | o_clerk         string,
                 | o_shippriority  bigint,
                 | o_comment       string)
                 | USING clickhouse
                 | LOCATION '$ordersData'
                 | CLUSTERED BY (o_orderkey)
                 | ${if (spark32) "" else "SORTED BY (o_orderkey, o_orderdate)"} INTO 2 BUCKETS;
                 |""".stripMargin)

    val partData = tablesPath + "/part"
    spark.sql(s"DROP TABLE IF EXISTS part")
    spark.sql(s"""
                 | CREATE EXTERNAL TABLE IF NOT EXISTS part (
                 | p_partkey     bigint,
                 | p_name        string,
                 | p_mfgr        string,
                 | p_brand       string,
                 | p_type        string,
                 | p_size        bigint,
                 | p_container   string,
                 | p_retailprice double,
                 | p_comment     string)
                 | USING clickhouse
                 | LOCATION '$partData'
                 | CLUSTERED BY (p_partkey)
                 | ${if (spark32) "" else "SORTED BY (p_partkey)"} INTO 2 BUCKETS;
                 |""".stripMargin)

    val partsuppData = tablesPath + "/partsupp"
    spark.sql(s"DROP TABLE IF EXISTS partsupp")
    spark.sql(s"""
                 | CREATE EXTERNAL TABLE IF NOT EXISTS partsupp (
                 | ps_partkey    bigint,
                 | ps_suppkey    bigint,
                 | ps_availqty   bigint,
                 | ps_supplycost double,
                 | ps_comment    string)
                 | USING clickhouse
                 | LOCATION '$partsuppData'
                 | CLUSTERED BY (ps_partkey)
                 | ${if (spark32) "" else "SORTED BY (ps_partkey)"} INTO 2 BUCKETS;
                 |""".stripMargin)

    val supplierData = tablesPath + "/supplier"
    spark.sql(s"DROP TABLE IF EXISTS supplier")
    spark.sql(s"""
                 | CREATE EXTERNAL TABLE IF NOT EXISTS supplier (
                 | s_suppkey   bigint,
                 | s_name      string,
                 | s_address   string,
                 | s_nationkey bigint,
                 | s_phone     string,
                 | s_acctbal   double,
                 | s_comment   string)
                 | USING clickhouse
                 | LOCATION '$supplierData'
                 | CLUSTERED BY (s_suppkey)
                 | ${if (spark32) "" else "SORTED BY (s_suppkey)"} INTO 1 BUCKETS;
                 |""".stripMargin)

    val result = spark
      .sql(s"""
              | show tables;
              |""".stripMargin)
      .collect()
    assert(result.length == 8)

    insertIntoMergeTreeTPCHTables(parquetSourceDB)
  }

  test("TPCH Q1") {
    runTPCHQuery(1)(
      df => {
        val plans = collect(df.queryExecution.executedPlan) {
          case scanExec: BasicScanExecTransformer => scanExec
        }
        assert(!plans.head.asInstanceOf[FileSourceScanExecTransformer].bucketedScan)
        assert(plans.head.metrics("numFiles").value === 2)
        assert(plans.head.metrics("pruningTime").value === pruningTimeValueSpark)
        assert(plans.head.metrics("numOutputRows").value === 591673)
      })
  }

  test("TPCH Q2") {
    runTPCHQuery(2)(
      df => {
        val plans = collect(df.queryExecution.executedPlan) {
          case scanExec: BasicScanExecTransformer => scanExec
          case joinExec: HashJoinLikeExecTransformer => joinExec
        }
        assert(
          plans(3)
            .asInstanceOf[HashJoinLikeExecTransformer]
            .left
            .isInstanceOf[InputIteratorTransformer])
        assert(
          plans(3)
            .asInstanceOf[HashJoinLikeExecTransformer]
            .right
            .isInstanceOf[InputIteratorTransformer])

        // Check the bucket join
        assert(
          plans(4)
            .asInstanceOf[HashJoinLikeExecTransformer]
            .left
            .isInstanceOf[ProjectExecTransformer])
        assert(
          plans(4)
            .asInstanceOf[HashJoinLikeExecTransformer]
            .right
            .isInstanceOf[FilterExecTransformerBase])

        assert(
          plans(9)
            .asInstanceOf[HashJoinLikeExecTransformer]
            .left
            .isInstanceOf[InputIteratorTransformer])
        if (spark32) {
          assert(
            plans(9)
              .asInstanceOf[HashJoinLikeExecTransformer]
              .right
              .isInstanceOf[InputIteratorTransformer])
        } else {
          assert(
            plans(9)
              .asInstanceOf[HashJoinLikeExecTransformer]
              .right
              .isInstanceOf[FilterExecTransformerBase])
        }

        if (spark32) {
          assert(!plans(11).asInstanceOf[FileSourceScanExecTransformer].bucketedScan)
        } else {
          assert(plans(11).asInstanceOf[FileSourceScanExecTransformer].bucketedScan)
        }
        assert(plans(11).metrics("numFiles").value === 1)
        assert(plans(11).metrics("numOutputRows").value === 1000)
      })
  }

  test("TPCH Q3") {
    runTPCHQuery(3)(
      df => {
        val plans = collect(df.queryExecution.executedPlan) {
          case scanExec: BasicScanExecTransformer => scanExec
          case joinExec: HashJoinLikeExecTransformer => joinExec
        }
        if (spark32) {
          assert(
            plans(1)
              .asInstanceOf[HashJoinLikeExecTransformer]
              .left
              .isInstanceOf[InputIteratorTransformer])
        } else {
          assert(
            plans(1)
              .asInstanceOf[HashJoinLikeExecTransformer]
              .left
              .isInstanceOf[ProjectExecTransformer])
        }
        assert(
          plans(1)
            .asInstanceOf[HashJoinLikeExecTransformer]
            .right
            .isInstanceOf[InputIteratorTransformer])

        if (spark32) {
          assert(!plans(2).asInstanceOf[FileSourceScanExecTransformer].bucketedScan)
        } else {
          assert(plans(2).asInstanceOf[FileSourceScanExecTransformer].bucketedScan)
        }
        assert(plans(2).metrics("numFiles").value === 2)
        assert(plans(2).metrics("numOutputRows").value === 3111)

        assert(!plans(3).asInstanceOf[FileSourceScanExecTransformer].bucketedScan)
        assert(plans(3).metrics("numFiles").value === 2)
        assert(plans(3).metrics("numOutputRows").value === 72678)
      })

    withSQLConf(
      ("spark.sql.optimizer.runtime.bloomFilter.applicationSideScanSizeThreshold", "1KB"),
      ("spark.sql.optimizer.runtime.bloomFilter.enabled", "true")) {
      runTPCHQuery(3)(
        df => {
          if (spark33) {
            val plans = collectWithSubqueries(df.queryExecution.executedPlan) {
              case aggExec: HashAggregateExecBaseTransformer
                  if aggExec.aggregateExpressions.exists(
                    _.aggregateFunction.getClass.getSimpleName.equals("BloomFilterAggregate")) =>
                aggExec
            }
            assert(plans.size == 8)
          }
        })
    }
  }

  test("TPCH Q4") {
    runTPCHQuery(4)(
      df => {
        val plans = collect(df.queryExecution.executedPlan) {
          case scanExec: BasicScanExecTransformer => scanExec
          case joinExec: HashJoinLikeExecTransformer => joinExec
        }
        // bucket join
        assert(
          plans.head
            .asInstanceOf[HashJoinLikeExecTransformer]
            .left
            .isInstanceOf[ProjectExecTransformer])
        assert(
          plans.head
            .asInstanceOf[HashJoinLikeExecTransformer]
            .right
            .isInstanceOf[ProjectExecTransformer])

        assert(plans(1).asInstanceOf[FileSourceScanExecTransformer].bucketedScan)
        assert(plans(1).metrics("numFiles").value === 2)
        assert(plans(1).metrics("numOutputRows").value === 5552)

        assert(plans(2).asInstanceOf[FileSourceScanExecTransformer].bucketedScan)
        assert(plans(2).metrics("numFiles").value === 2)
        assert(plans(2).metrics("numOutputRows").value === 379809)
      })

    withSQLConf(
      ("spark.sql.optimizer.runtime.bloomFilter.applicationSideScanSizeThreshold", "1KB"),
      ("spark.sql.optimizer.runtime.bloomFilter.enabled", "true")) {
      runTPCHQuery(4)(
        df => {
          if (spark33) {
            val plans = collectWithSubqueries(df.queryExecution.executedPlan) {
              case aggExec: HashAggregateExecBaseTransformer
                  if aggExec.aggregateExpressions.exists(
                    _.aggregateFunction.getClass.getSimpleName.equals("BloomFilterAggregate")) =>
                aggExec
            }
            assert(plans.size == 4)
          }
        })
    }
  }

  test("TPCH Q6") {
    runTPCHQuery(6)(
      df => {
        val plans = collect(df.queryExecution.executedPlan) {
          case scanExec: BasicScanExecTransformer => scanExec
        }
        assert(!plans.head.asInstanceOf[FileSourceScanExecTransformer].bucketedScan)
        assert(plans.head.metrics("numFiles").value === 2)
        assert(plans.head.metrics("pruningTime").value === pruningTimeValueSpark)
        assert(plans.head.metrics("numOutputRows").value === 11618)
      })
  }

  test("TPCH Q12") {
    runTPCHQuery(12)(
      df => {
        val plans = collect(df.queryExecution.executedPlan) {
          case scanExec: BasicScanExecTransformer => scanExec
          case joinExec: HashJoinLikeExecTransformer => joinExec
        }
        // bucket join
        assert(
          plans.head
            .asInstanceOf[HashJoinLikeExecTransformer]
            .left
            .isInstanceOf[FilterExecTransformerBase])
        assert(
          plans.head
            .asInstanceOf[HashJoinLikeExecTransformer]
            .right
            .isInstanceOf[ProjectExecTransformer])

        assert(plans(1).asInstanceOf[FileSourceScanExecTransformer].bucketedScan)
        assert(plans(1).metrics("numFiles").value === 2)
        assert(plans(1).metrics("numOutputRows").value === 150000)

        assert(plans(2).asInstanceOf[FileSourceScanExecTransformer].bucketedScan)
        assert(plans(2).metrics("numFiles").value === 2)
        assert(plans(2).metrics("numOutputRows").value === 3155)
      })

    withSQLConf(
      ("spark.sql.optimizer.runtime.bloomFilter.applicationSideScanSizeThreshold", "1KB"),
      ("spark.sql.optimizer.runtime.bloomFilter.enabled", "true")) {
      runTPCHQuery(12)(
        df => {
          if (spark33) {
            val plans = collectWithSubqueries(df.queryExecution.executedPlan) {
              case aggExec: HashAggregateExecBaseTransformer
                  if aggExec.aggregateExpressions.exists(
                    _.aggregateFunction.getClass.getSimpleName.equals("BloomFilterAggregate")) =>
                aggExec
            }
            assert(plans.size == 4)
          }
        })
    }
  }

  test("TPCH Q18") {
    runTPCHQuery(18)(
      df => {
        val plans = collect(df.queryExecution.executedPlan) {
          case joinExec: HashJoinLikeExecTransformer => joinExec
        }
        // bucket join
        assert(
          plans(2)
            .asInstanceOf[HashJoinLikeExecTransformer]
            .left
            .isInstanceOf[FilterExecTransformerBase])
        assert(
          plans(2)
            .asInstanceOf[HashJoinLikeExecTransformer]
            .right
            .isInstanceOf[ProjectExecTransformer])
        // bucket join
        assert(
          plans(3)
            .asInstanceOf[HashJoinLikeExecTransformer]
            .left
            .isInstanceOf[FilterExecTransformerBase])
        assert(
          plans(3)
            .asInstanceOf[HashJoinLikeExecTransformer]
            .right
            .isInstanceOf[ProjectExecTransformer])
      })
  }

  test("TPCH Q20") {
    runTPCHQuery(20)(
      df => {
        val plans = collect(df.queryExecution.executedPlan) {
          case joinExec: HashJoinLikeExecTransformer => joinExec
        }
        if (spark32) {
          assert(
            plans(1)
              .asInstanceOf[HashJoinLikeExecTransformer]
              .left
              .isInstanceOf[InputIteratorTransformer])
        } else {
          assert(
            plans(1)
              .asInstanceOf[HashJoinLikeExecTransformer]
              .left
              .isInstanceOf[FilterExecTransformerBase])
        }
        assert(
          plans(1)
            .asInstanceOf[HashJoinLikeExecTransformer]
            .right
            .isInstanceOf[InputIteratorTransformer])

        assert(
          plans(2)
            .asInstanceOf[HashJoinLikeExecTransformer]
            .left
            .isInstanceOf[InputIteratorTransformer])
        assert(
          plans(2)
            .asInstanceOf[HashJoinLikeExecTransformer]
            .right
            .isInstanceOf[InputIteratorTransformer])

        assert(
          plans(3)
            .asInstanceOf[HashJoinLikeExecTransformer]
            .left
            .isInstanceOf[FilterExecTransformerBase])
        assert(
          plans(3)
            .asInstanceOf[HashJoinLikeExecTransformer]
            .right
            .isInstanceOf[ProjectExecTransformer])
      })

    withSQLConf(
      ("spark.sql.optimizer.runtime.bloomFilter.applicationSideScanSizeThreshold", "1KB"),
      ("spark.sql.optimizer.runtime.bloomFilter.enabled", "true")) {
      runTPCHQuery(20)(
        df => {
          if (spark33) {
            val plans = collectWithSubqueries(df.queryExecution.executedPlan) {
              case aggExec: HashAggregateExecBaseTransformer
                  if aggExec.aggregateExpressions.exists(
                    _.aggregateFunction.getClass.getSimpleName.equals("BloomFilterAggregate")) =>
                aggExec
            }
            assert(plans.size == 6)
          }
        })
    }
  }

  test("check bucket pruning on filter") {
    // TODO use comparewithvanilla
    val df = spark.sql("select count(*) from lineitem where l_orderkey = 12647")
    val result = df.collect()
    val scanExec = collect(df.queryExecution.executedPlan) {
      case f: FileSourceScanExecTransformer => f
    }
    val touchedParts = scanExec.head.getPartitions
      .flatMap(partition => partition.asInstanceOf[GlutenMergeTreePartition].partList)
      .map(_.name)
      .distinct
    assert(touchedParts.size == 1)
    assert(result.apply(0).apply(0) == 1)
  }

  test("GLUTEN-4668: Merge two phase hash-based aggregate into one aggregate") {
    def checkHashAggregateCount(df: DataFrame, expectedCount: Int): Unit = {
      val plans = collect(df.queryExecution.executedPlan) {
        case agg: HashAggregateExecBaseTransformer => agg
      }
      assert(plans.size == expectedCount)
    }

    def checkResult(df: DataFrame, exceptedResult: Seq[Row]): Unit = {
      // check the result
      val result = df.collect()
      assert(result.length == exceptedResult.size)
      val sortedRes = result.map {
        s =>
          Row.fromSeq(s.toSeq.map {
            case a: mutable.WrappedArray[_] => a.sortBy(_.toString.toInt)
            case o => o
          })
      }
      TestUtils.compareAnswers(sortedRes, exceptedResult)
    }

    val SQL =
      """
        |select l_orderkey, l_returnflag, collect_list(l_linenumber) as t
        |from lineitem group by l_orderkey, l_returnflag
        |order by l_orderkey, l_returnflag, t limit 5
        |""".stripMargin
    runSql(SQL)(
      df => {
        checkResult(
          df,
          Array(
            Row(1, "N", mutable.WrappedArray.make(Array(1, 2, 3, 4, 5, 6))),
            Row(2, "N", mutable.WrappedArray.make(Array(1))),
            Row(3, "A", mutable.WrappedArray.make(Array(3, 4, 6))),
            Row(3, "R", mutable.WrappedArray.make(Array(1, 2, 5))),
            Row(4, "N", mutable.WrappedArray.make(Array(1)))
          )
        )
        checkHashAggregateCount(df, 1)
      })

    val SQL1 =
      """
        |select l_orderkey, l_returnflag,
        |sum(l_linenumber) as t,
        |count(l_linenumber) as t1,
        |min(l_linenumber) as t2
        |from lineitem group by l_orderkey, l_returnflag
        |order by l_orderkey, l_returnflag, t, t1, t2 limit 5
        |""".stripMargin
    runSql(SQL1)(
      df => {
        checkResult(
          df,
          Array(
            Row(1, "N", 21, 6, 1),
            Row(2, "N", 1, 1, 1),
            Row(3, "A", 13, 3, 3),
            Row(3, "R", 8, 3, 1),
            Row(4, "N", 1, 1, 1)
          )
        )
        checkHashAggregateCount(df, 1)
      })

    val SQL2 =
      """
        |select l_returnflag, l_orderkey, collect_list(l_linenumber) as t
        |from lineitem group by l_orderkey, l_returnflag
        |order by l_returnflag, l_orderkey, t limit 5
        |""".stripMargin
    runSql(SQL2)(
      df => {
        checkResult(
          df,
          Array(
            Row("A", 3, mutable.WrappedArray.make(Array(3, 4, 6))),
            Row("A", 5, mutable.WrappedArray.make(Array(3))),
            Row("A", 6, mutable.WrappedArray.make(Array(1))),
            Row("A", 33, mutable.WrappedArray.make(Array(1, 2, 3))),
            Row("A", 37, mutable.WrappedArray.make(Array(1, 2, 3)))
          )
        )
        checkHashAggregateCount(df, 1)
      })

    // will merge four aggregates into two one.
    val SQL3 =
      """
        |select l_returnflag, l_orderkey,
        |count(distinct l_linenumber) as t
        |from lineitem group by l_orderkey, l_returnflag
        |order by l_returnflag, l_orderkey, t limit 5
        |""".stripMargin
    runSql(SQL3)(
      df => {
        checkResult(
          df,
          Array(
            Row("A", 3, 3),
            Row("A", 5, 1),
            Row("A", 6, 1),
            Row("A", 33, 3),
            Row("A", 37, 3)
          )
        )
        checkHashAggregateCount(df, 2)
      })

    // not support when there are more than one count distinct
    val SQL4 =
      """
        |select l_returnflag, l_orderkey,
        |count(distinct l_linenumber) as t,
        |count(distinct l_discount) as t1
        |from lineitem group by l_orderkey, l_returnflag
        |order by l_returnflag, l_orderkey, t, t1 limit 5
        |""".stripMargin
    runSql(SQL4)(
      df => {
        checkResult(
          df,
          Array(
            Row("A", 3, 3, 3),
            Row("A", 5, 1, 1),
            Row("A", 6, 1, 1),
            Row("A", 33, 3, 3),
            Row("A", 37, 3, 2)
          )
        )
        checkHashAggregateCount(df, 4)
      })

    val SQL5 =
      """
        |select l_returnflag, l_orderkey,
        |count(distinct l_linenumber) as t,
        |sum(l_linenumber) as t1
        |from lineitem group by l_orderkey, l_returnflag
        |order by l_returnflag, l_orderkey, t, t1 limit 5
        |""".stripMargin
    runSql(SQL5)(
      df => {
        checkResult(
          df,
          Array(
            Row("A", 3, 3, 13),
            Row("A", 5, 1, 3),
            Row("A", 6, 1, 1),
            Row("A", 33, 3, 6),
            Row("A", 37, 3, 6)
          )
        )
        checkHashAggregateCount(df, 4)
      })

    val SQL6 =
      """
        |select count(1) from lineitem
        |""".stripMargin
    runSql(SQL6)(
      df => {
        checkResult(df, Array(Row(600572)))
        if (spark32) {
          // there is a shuffle between two phase hash aggregate.
          checkHashAggregateCount(df, 2)
        } else {
          // the delta will use the delta log meta to response this sql
          checkHashAggregateCount(df, 0)
        }
      })

    // test sort aggregates
    val SQL7 =
      """
        |select l_orderkey, l_returnflag, max(l_shipinstruct) as t
        |from lineitem
        |group by l_orderkey, l_returnflag
        |order by l_orderkey, l_returnflag, t
        |limit 10
        |""".stripMargin
    runSql(SQL7)(
      df => {
        checkResult(
          df,
          Array(
            Row(1, "N", "TAKE BACK RETURN"),
            Row(2, "N", "TAKE BACK RETURN"),
            Row(3, "A", "TAKE BACK RETURN"),
            Row(3, "R", "TAKE BACK RETURN"),
            Row(4, "N", "DELIVER IN PERSON"),
            Row(5, "A", "DELIVER IN PERSON"),
            Row(5, "R", "NONE"),
            Row(6, "A", "TAKE BACK RETURN"),
            Row(7, "N", "TAKE BACK RETURN"),
            Row(32, "N", "TAKE BACK RETURN")
          )
        )
        checkHashAggregateCount(df, 1)
      })

    withSQLConf(("spark.gluten.sql.columnar.force.hashagg", "false")) {
      val SQL =
        """
          |select l_orderkey, l_returnflag, max(l_shipinstruct) as t
          |from lineitem
          |group by l_orderkey, l_returnflag
          |order by l_orderkey, l_returnflag, t
          |limit 10
          |""".stripMargin
      runSql(SQL7, noFallBack = false)(
        df => {
          checkResult(
            df,
            Array(
              Row(1, "N", "TAKE BACK RETURN"),
              Row(2, "N", "TAKE BACK RETURN"),
              Row(3, "A", "TAKE BACK RETURN"),
              Row(3, "R", "TAKE BACK RETURN"),
              Row(4, "N", "DELIVER IN PERSON"),
              Row(5, "A", "DELIVER IN PERSON"),
              Row(5, "R", "NONE"),
              Row(6, "A", "TAKE BACK RETURN"),
              Row(7, "N", "TAKE BACK RETURN"),
              Row(32, "N", "TAKE BACK RETURN")
            )
          )
          checkHashAggregateCount(df, 0)
          val plans = collect(df.queryExecution.executedPlan) { case agg: SortAggregateExec => agg }
          assert(plans.size == 2)
        })
    }
  }
}
// scalastyle:off line.size.limit
