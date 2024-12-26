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
package org.apache.gluten.execution.tpch

import org.apache.gluten.execution._

import org.apache.spark.SparkConf
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.execution.InputIteratorTransformer
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanHelper
import org.apache.spark.sql.execution.aggregate.SortAggregateExec
import org.apache.spark.sql.execution.datasources.{BucketingUtils, FilePartition}

import org.apache.commons.io.FileUtils
import org.apache.hadoop.fs.Path

import java.io.File

// Some sqls' line length exceeds 100
// scalastyle:off line.size.limit

class GlutenClickHouseTPCHParquetBucketSuite
  extends GlutenClickHouseTPCHAbstractSuite
  with AdaptiveSparkPlanHelper {

  override protected val tablesPath: String = basePath + "/tpch-data-ch"
  override protected val tpchQueries: String =
    rootPath + "../../../../tools/gluten-it/common/src/main/resources/tpch-queries"
  override protected val queriesResults: String = rootPath + "queries-output"

  protected val bucketTableResourcePath: String = rootPath + "tpch-data-bucket/parquet_bucket"
  protected val bucketTableDataPath: String = basePath + "/tpch-parquet-bucket"

  override protected def sparkConf: SparkConf = {
    import org.apache.gluten.backendsapi.clickhouse.CHConf._
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

  override def beforeAll(): Unit = {
    super.beforeAll()
    FileUtils.copyDirectory(new File(bucketTableResourcePath), new File(bucketTableDataPath))
    createTPCHParquetBucketTables()
  }

  protected def createTPCHParquetBucketTables(): Unit = {

    spark.sql(s"""
                 |CREATE DATABASE IF NOT EXISTS tpch_parquet_bucket
                 |""".stripMargin)
    spark.sql("use tpch_parquet_bucket")
    val customerData = bucketTableDataPath + "/customer"
    spark.sql(s"DROP TABLE IF EXISTS customer")
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
                 | USING PARQUET
                 | LOCATION '$customerData'
                 | CLUSTERED BY (c_custkey) SORTED BY (c_custkey) INTO 2 BUCKETS;
                 |""".stripMargin)

    val customerData1 = bucketTableDataPath + "/customer_6_buckets"
    spark.sql(s"DROP TABLE IF EXISTS customer_6_buckets")
    spark.sql(s"""
                 | CREATE EXTERNAL TABLE IF NOT EXISTS customer_6_buckets (
                 | c_custkey    bigint,
                 | c_name       string,
                 | c_address    string,
                 | c_nationkey  bigint,
                 | c_phone      string,
                 | c_acctbal    double,
                 | c_mktsegment string,
                 | c_comment    string)
                 | USING PARQUET
                 | LOCATION '$customerData1'
                 | CLUSTERED BY (c_custkey) SORTED BY (c_custkey) INTO 6 BUCKETS;
                 |""".stripMargin)

    spark.sql(s"""
                 |INSERT INTO customer_6_buckets SELECT * FROM customer;
                 |""".stripMargin)

    val lineitemData = bucketTableDataPath + "/lineitem"
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
                 | USING PARQUET
                 | LOCATION '$lineitemData'
                 | CLUSTERED BY (l_orderkey) SORTED BY (l_shipdate, l_orderkey) INTO 2 BUCKETS;
                 |""".stripMargin)

    val nationData = bucketTableDataPath + "/nation"
    spark.sql(s"DROP TABLE IF EXISTS nation")
    spark.sql(s"""
                 | CREATE EXTERNAL TABLE IF NOT EXISTS nation (
                 | n_nationkey bigint,
                 | n_name      string,
                 | n_regionkey bigint,
                 | n_comment   string)
                 | USING PARQUET
                 | LOCATION '$nationData'
                 | CLUSTERED BY (n_nationkey) SORTED BY (n_nationkey) INTO 1 BUCKETS;
                 |""".stripMargin)

    val regionData = bucketTableDataPath + "/region"
    spark.sql(s"DROP TABLE IF EXISTS region")
    spark.sql(s"""
                 | CREATE EXTERNAL TABLE IF NOT EXISTS region (
                 | r_regionkey bigint,
                 | r_name      string,
                 | r_comment   string)
                 | USING PARQUET
                 | LOCATION '$regionData'
                 | CLUSTERED BY (r_regionkey) SORTED BY (r_regionkey) INTO 1 BUCKETS;
                 |""".stripMargin)

    val ordersData = bucketTableDataPath + "/orders"
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
                 | USING PARQUET
                 | LOCATION '$ordersData'
                 | CLUSTERED BY (o_orderkey) SORTED BY (o_orderkey, o_orderdate) INTO 2 BUCKETS;
                 |""".stripMargin)

    val ordersData1 = bucketTableDataPath + "/orders_6_buckets"
    spark.sql(s"DROP TABLE IF EXISTS orders_6_buckets")
    spark.sql(s"""
                 | CREATE EXTERNAL TABLE IF NOT EXISTS orders_6_buckets (
                 | o_orderkey      bigint,
                 | o_custkey       bigint,
                 | o_orderstatus   string,
                 | o_totalprice    double,
                 | o_orderdate     date,
                 | o_orderpriority string,
                 | o_clerk         string,
                 | o_shippriority  bigint,
                 | o_comment       string)
                 | USING PARQUET
                 | LOCATION '$ordersData1'
                 | CLUSTERED BY (o_orderkey) SORTED BY (o_orderkey, o_orderdate) INTO 6 BUCKETS;
                 |""".stripMargin)

    spark.sql(s"""
                 |INSERT INTO orders_6_buckets SELECT * FROM orders;
                 |""".stripMargin)

    val partData = bucketTableDataPath + "/part"
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
                 | USING PARQUET
                 | LOCATION '$partData'
                 | CLUSTERED BY (p_partkey) SORTED BY (p_partkey) INTO 2 BUCKETS;
                 |""".stripMargin)

    val partsuppData = bucketTableDataPath + "/partsupp"
    spark.sql(s"DROP TABLE IF EXISTS partsupp")
    spark.sql(s"""
                 | CREATE EXTERNAL TABLE IF NOT EXISTS partsupp (
                 | ps_partkey    bigint,
                 | ps_suppkey    bigint,
                 | ps_availqty   bigint,
                 | ps_supplycost double,
                 | ps_comment    string)
                 | USING PARQUET
                 | LOCATION '$partsuppData'
                 | CLUSTERED BY (ps_partkey) SORTED BY (ps_partkey) INTO 2 BUCKETS;
                 |""".stripMargin)

    val supplierData = bucketTableDataPath + "/supplier"
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
                 | USING PARQUET
                 | LOCATION '$supplierData'
                 | CLUSTERED BY (s_suppkey) SORTED BY (s_suppkey) INTO 1 BUCKETS;
                 |""".stripMargin)

    val result = spark
      .sql(s"""
              | show tables;
              |""".stripMargin)
      .collect()
    assert(result.length == 10)
  }

  test("TPCH Q1") {
    compareTPCHQueryAgainstVanillaSpark(
      1,
      tpchQueries,
      df => {
        val plans = collect(df.queryExecution.executedPlan) {
          case scanExec: BasicScanExecTransformer => scanExec
        }
        assert(!plans.head.asInstanceOf[FileSourceScanExecTransformer].bucketedScan)
        assert(plans.head.metrics("numFiles").value === 4)
        assert(plans.head.metrics("pruningTime").value === pruningTimeValueSpark)
        assert(plans.head.metrics("numOutputRows").value === 600572)
      }
    )
  }

  test("TPCH Q2") {
    compareTPCHQueryAgainstVanillaSpark(
      2,
      tpchQueries,
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
      }
    )
  }

  test("TPCH Q3") {
    compareTPCHQueryAgainstVanillaSpark(
      3,
      tpchQueries,
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
        assert(plans(2).metrics("numFiles").value === 4)
        assert(plans(2).metrics("numOutputRows").value === 15000)

        assert(!plans(3).asInstanceOf[FileSourceScanExecTransformer].bucketedScan)
        assert(plans(3).metrics("numFiles").value === 4)
        assert(plans(3).metrics("numOutputRows").value === 150000)
      }
    )

    withSQLConf(
      ("spark.sql.optimizer.runtime.bloomFilter.applicationSideScanSizeThreshold", "1KB"),
      ("spark.sql.optimizer.runtime.bloomFilter.enabled", "true")) {
      compareTPCHQueryAgainstVanillaSpark(
        3,
        tpchQueries,
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
        }
      )
    }
  }

  test("TPCH Q4") {
    compareTPCHQueryAgainstVanillaSpark(
      4,
      tpchQueries,
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
        assert(plans(1).metrics("numFiles").value === 4)
        assert(plans(1).metrics("numOutputRows").value === 150000)

        assert(plans(2).asInstanceOf[FileSourceScanExecTransformer].bucketedScan)
        assert(plans(2).metrics("numFiles").value === 4)
        assert(plans(2).metrics("numOutputRows").value === 600572)
      }
    )

    withSQLConf(
      ("spark.sql.optimizer.runtime.bloomFilter.applicationSideScanSizeThreshold", "1KB"),
      ("spark.sql.optimizer.runtime.bloomFilter.enabled", "true")) {
      compareTPCHQueryAgainstVanillaSpark(
        4,
        tpchQueries,
        df => {
          if (spark33) {
            val plans = collectWithSubqueries(df.queryExecution.executedPlan) {
              case aggExec: HashAggregateExecBaseTransformer
                  if aggExec.aggregateExpressions.exists(
                    _.aggregateFunction.getClass.getSimpleName.equals("BloomFilterAggregate")) =>
                aggExec
            }
            assert(plans.size == 2)
          }
        }
      )
    }
  }

  test("TPCH Q6") {
    compareTPCHQueryAgainstVanillaSpark(
      6,
      tpchQueries,
      df => {
        val plans = collect(df.queryExecution.executedPlan) {
          case scanExec: BasicScanExecTransformer => scanExec
        }
        assert(!plans.head.asInstanceOf[FileSourceScanExecTransformer].bucketedScan)
        assert(plans.head.metrics("numFiles").value === 4)
        assert(plans.head.metrics("pruningTime").value === pruningTimeValueSpark)
        assert(plans.head.metrics("numOutputRows").value === 600572)
      }
    )
  }

  test("TPCH Q12") {
    compareTPCHQueryAgainstVanillaSpark(
      12,
      tpchQueries,
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
        assert(plans(1).metrics("numFiles").value === 4)
        assert(plans(1).metrics("numOutputRows").value === 150000)

        assert(plans(2).asInstanceOf[FileSourceScanExecTransformer].bucketedScan)
        assert(plans(2).metrics("numFiles").value === 4)
        assert(plans(2).metrics("numOutputRows").value === 600572)
      }
    )

    withSQLConf(
      ("spark.sql.optimizer.runtime.bloomFilter.applicationSideScanSizeThreshold", "1KB"),
      ("spark.sql.optimizer.runtime.bloomFilter.enabled", "true")) {
      compareTPCHQueryAgainstVanillaSpark(
        12,
        tpchQueries,
        df => {
          if (spark33) {
            val plans = collectWithSubqueries(df.queryExecution.executedPlan) {
              case aggExec: HashAggregateExecBaseTransformer
                  if aggExec.aggregateExpressions.exists(
                    _.aggregateFunction.getClass.getSimpleName.equals("BloomFilterAggregate")) =>
                aggExec
            }
            assert(plans.size == 2)
          }
        }
      )
    }
  }

  test("TPCH Q18") {
    compareTPCHQueryAgainstVanillaSpark(
      18,
      tpchQueries,
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
      }
    )
  }

  test("TPCH Q20") {
    compareTPCHQueryAgainstVanillaSpark(
      20,
      tpchQueries,
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
      }
    )

    withSQLConf(
      ("spark.sql.optimizer.runtime.bloomFilter.applicationSideScanSizeThreshold", "1KB"),
      ("spark.sql.optimizer.runtime.bloomFilter.enabled", "true")) {
      compareTPCHQueryAgainstVanillaSpark(
        20,
        tpchQueries,
        df => {
          if (spark33) {
            val plans = collectWithSubqueries(df.queryExecution.executedPlan) {
              case aggExec: HashAggregateExecBaseTransformer
                  if aggExec.aggregateExpressions.exists(
                    _.aggregateFunction.getClass.getSimpleName.equals("BloomFilterAggregate")) =>
                aggExec
            }
            assert(plans.size == 3)
          }
        }
      )
    }
  }

  test("check bucket pruning on filter") {
    runQueryAndCompare(" select * from lineitem where l_orderkey = 12647")(
      df => {
        val scanExec = collect(df.queryExecution.executedPlan) {
          case f: FileSourceScanExecTransformer => f
        }
        val touchedBuckets = scanExec.head.getPartitions
          .flatMap(partition => partition.asInstanceOf[FilePartition].files)
          .flatMap(f => BucketingUtils.getBucketId(new Path(f.filePath.toString()).getName))
          .distinct
        // two files from part0-0,part0-1,part1-0,part1-1
        assert(touchedBuckets.size == 1)
      })
  }

  test("GLUTEN-3922: Fix incorrect shuffle hash id value when executing modulo") {
    val SQL =
      """
        |SELECT
        |    c_custkey, o_custkey, hash(o_custkey), pmod(hash(o_custkey), 12),
        |    pmod(hash(o_custkey), 4)
        |FROM
        |    customer_6_buckets,
        |    orders_6_buckets
        |WHERE
        |    c_mktsegment = 'BUILDING'
        |    AND c_custkey = o_custkey
        |    AND o_orderdate < date'1995-03-15'
        |ORDER BY
        |    o_custkey DESC,
        |    c_custkey
        |LIMIT 100;
        |""".stripMargin
    compareResultsAgainstVanillaSpark(
      SQL,
      compareResult = true,
      df => {}
    )
  }

  test("GLUTEN-4668: Merge two phase hash-based aggregate into one aggregate") {
    def checkHashAggregateCount(df: DataFrame, expectedCount: Int): Unit = {
      val plans = collect(df.queryExecution.executedPlan) {
        case agg: HashAggregateExecBaseTransformer => agg
      }
      assert(plans.size == expectedCount)
    }

    val SQL =
      """
        |select l_orderkey, l_returnflag, collect_list(l_linenumber) as t
        |from lineitem group by l_orderkey, l_returnflag
        |order by l_orderkey, l_returnflag, t limit 100
        |""".stripMargin
    compareResultsAgainstVanillaSpark(
      SQL,
      compareResult = true,
      df => { checkHashAggregateCount(df, 1) }
    )

    val SQL1 =
      """
        |select l_orderkey, l_returnflag,
        |sum(l_linenumber) as t,
        |count(l_linenumber) as t1,
        |min(l_linenumber) as t2
        |from lineitem group by l_orderkey, l_returnflag
        |order by l_orderkey, l_returnflag, t, t1, t2 limit 100
        |""".stripMargin
    compareResultsAgainstVanillaSpark(
      SQL1,
      compareResult = true,
      df => { checkHashAggregateCount(df, 1) }
    )

    val SQL2 =
      """
        |select l_returnflag, l_orderkey, collect_list(l_linenumber) as t
        |from lineitem group by l_orderkey, l_returnflag
        |order by l_returnflag, l_orderkey, t limit 100
        |""".stripMargin
    compareResultsAgainstVanillaSpark(
      SQL2,
      compareResult = true,
      df => { checkHashAggregateCount(df, 1) }
    )

    // will merge four aggregates into two one.
    val SQL3 =
      """
        |select l_returnflag, l_orderkey,
        |count(distinct l_linenumber) as t
        |from lineitem group by l_orderkey, l_returnflag
        |order by l_returnflag, l_orderkey, t limit 100
        |""".stripMargin
    compareResultsAgainstVanillaSpark(
      SQL3,
      compareResult = true,
      df => { checkHashAggregateCount(df, 2) }
    )

    // not support when there are more than one count distinct
    val SQL4 =
      """
        |select l_returnflag, l_orderkey,
        |count(distinct l_linenumber) as t,
        |count(distinct l_discount) as t1
        |from lineitem group by l_orderkey, l_returnflag
        |order by l_returnflag, l_orderkey, t, t1 limit 100
        |""".stripMargin
    compareResultsAgainstVanillaSpark(
      SQL4,
      compareResult = true,
      df => { checkHashAggregateCount(df, 4) }
    )

    val SQL5 =
      """
        |select l_returnflag, l_orderkey,
        |count(distinct l_linenumber) as t,
        |sum(l_linenumber) as t1
        |from lineitem group by l_orderkey, l_returnflag
        |order by l_returnflag, l_orderkey, t, t1 limit 100
        |""".stripMargin
    compareResultsAgainstVanillaSpark(
      SQL5,
      compareResult = true,
      df => { checkHashAggregateCount(df, 4) }
    )

    val SQL6 =
      """
        |select count(1) from lineitem
        |""".stripMargin
    compareResultsAgainstVanillaSpark(
      SQL6,
      compareResult = true,
      df => {
        // there is a shuffle between two phase hash aggregate.
        checkHashAggregateCount(df, 2)
      }
    )

    // test sort aggregates
    val SQL7 =
      """
        |select l_orderkey, l_returnflag, max(l_shipinstruct) as t
        |from lineitem
        |group by l_orderkey, l_returnflag
        |order by l_orderkey, l_returnflag, t
        |limit 100
        |""".stripMargin
    compareResultsAgainstVanillaSpark(
      SQL7,
      compareResult = true,
      df => {
        checkHashAggregateCount(df, 1)
      }
    )

    withSQLConf(("spark.gluten.sql.columnar.force.hashagg", "false")) {
      val SQL =
        """
          |select l_orderkey, l_returnflag, max(l_shipinstruct) as t
          |from lineitem
          |group by l_orderkey, l_returnflag
          |order by l_orderkey, l_returnflag, t
          |limit 100
          |""".stripMargin
      compareResultsAgainstVanillaSpark(
        SQL,
        compareResult = true,
        df => {
          checkHashAggregateCount(df, 0)
          val plans = collect(df.queryExecution.executedPlan) { case agg: SortAggregateExec => agg }
          assert(plans.size == 2)
        },
        noFallBack = false
      )
    }
  }
}
// scalastyle:on line.size.limit
