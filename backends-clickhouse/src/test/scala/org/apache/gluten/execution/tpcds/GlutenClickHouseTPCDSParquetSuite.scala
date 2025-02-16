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
package org.apache.gluten.execution.tpcds

import org.apache.gluten.execution._

import org.apache.spark.SparkConf
import org.apache.spark.sql.catalyst.expressions.DynamicPruningExpression
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.exchange.ReusedExchangeExec

// Some sqls' line length exceeds 100
// scalastyle:off line.size.limit

class GlutenClickHouseTPCDSParquetSuite extends GlutenClickHouseTPCDSAbstractSuite {

  /** Run Gluten + ClickHouse Backend with SortShuffleManager */
  override protected def sparkConf: SparkConf = {
    import org.apache.gluten.backendsapi.clickhouse.CHConfig._
    super.sparkConf
      .set("spark.shuffle.manager", "sort")
      .set("spark.io.compression.codec", "snappy")
      .set("spark.sql.shuffle.partitions", "5")
      .set("spark.sql.autoBroadcastJoinThreshold", "10MB")
      .set("spark.memory.offHeap.size", "4g")
      .set("spark.gluten.sql.validation.logLevel", "ERROR")
      .set("spark.gluten.sql.validation.printStackOnFailure", "true")
      .setCHConfig("enable_grace_aggregate_spill_test", "true")
  }

  executeTPCDSTest(false)

  test("test 'select count(*)'") {
    val result = runSql("""
                          |select count(c_customer_sk) from customer
                          |""".stripMargin) { _ => }
    assertResult(100000L)(result.head.getLong(0))
  }

  test("test reading from partitioned table") {
    val result = runSql("""
                          |select count(*)
                          |  from store_sales
                          |  where ss_quantity between 1 and 20
                          |""".stripMargin) { _ => }
    assertResult(550458L)(result.head.getLong(0))
  }

  test("test reading from partitioned table with partition column filter") {
    compareResultsAgainstVanillaSpark(
      """
        |select avg(ss_net_paid_inc_tax)
        |  from store_sales
        |  where ss_quantity between 1 and 20
        |  and ss_sold_date_sk = 2452635
        |""".stripMargin,
      compareResult = true,
      _ => {}
    )
  }

  test("test select avg(int), avg(long)") {
    val result = runSql("""
                          |select avg(cs_item_sk), avg(cs_order_number)
                          |  from catalog_sales
                          |""".stripMargin) { _ => }
    assertResult(8998.463336886734)(result.head.getDouble(0))
    assertResult(80037.12727449503)(result.head.getDouble(1))
  }

  test("test union all operator with two tables") {
    val result = runSql("""
                          |select count(date_sk) from (
                          |  select d_date_sk as date_sk from date_dim
                          |  union all
                          |  select ws_sold_date_sk as date_sk from web_sales
                          |)
                          |""".stripMargin) { _ => }
    assertResult(791809)(result.head.getLong(0))
  }

  test("test union all operator with three tables") {
    val result = runSql("""
                          |select count(date_sk) from (
                          |  select d_date_sk as date_sk from date_dim
                          |  union all
                          |  select ws_sold_date_sk as date_sk from web_sales
                          |  union all (
                          |   select ws_sold_date_sk as date_sk from web_sales limit 100
                          |  )
                          |)
                          |""".stripMargin) { _ => }
    assertResult(791909)(result.head.getLong(0))
  }

  test("test union operator with two tables") {
    val result = runSql("""
                          |select count(date_sk) from (
                          |  select d_date_sk as date_sk from date_dim
                          |  union
                          |  select ws_sold_date_sk as date_sk from web_sales
                          |)
                          |""".stripMargin) { _ => }
    assertResult(73049)(result.head.getLong(0))
  }

  test("Test join with mixed condition 1") {
    val testSql =
      """
        |SELECT  i_brand_id AS brand_id, i_brand AS brand, i_manufact_id, i_manufact,
        |    sum(ss_ext_sales_price) AS ext_price
        | FROM date_dim
        | LEFT JOIN store_sales ON d_date_sk = ss_sold_date_sk
        | LEFT JOIN item ON ss_item_sk = i_item_sk AND i_manager_id = 7
        | LEFT JOIN customer ON ss_customer_sk = c_customer_sk
        | LEFT JOIN customer_address ON c_current_addr_sk = ca_address_sk
        | LEFT JOIN store ON ss_store_sk = s_store_sk AND substr(ca_zip,1,5) <> substr(s_zip,1,5)
        | WHERE d_moy = 11
        |   AND d_year = 1999
        | GROUP BY i_brand_id, i_brand, i_manufact_id, i_manufact
        | ORDER BY ext_price DESC, i_brand, i_brand_id, i_manufact_id, i_manufact
        | LIMIT 100;
        |""".stripMargin
    compareResultsAgainstVanillaSpark(testSql, compareResult = true, _ => {})
  }

  test("Gluten-1235: Fix missing reading from the broadcasted value when executing DPP") {
    val testSql =
      """
        |select dt.d_year
        |       ,sum(ss_ext_sales_price) sum_agg
        | from  date_dim dt
        |      ,store_sales
        | where dt.d_date_sk = store_sales.ss_sold_date_sk
        |   and dt.d_moy=12
        | group by dt.d_year
        | order by dt.d_year
        |         ,sum_agg desc
        |  LIMIT 100 ;
        |""".stripMargin
    compareResultsAgainstVanillaSpark(
      testSql,
      compareResult = true,
      df => {
        val foundDynamicPruningExpr = df.queryExecution.executedPlan.collect {
          case f: FileSourceScanExecTransformer => f
        }
        assert(foundDynamicPruningExpr.size == 2)
        assert(
          foundDynamicPruningExpr(1)
            .asInstanceOf[FileSourceScanExecTransformer]
            .partitionFilters
            .exists(_.isInstanceOf[DynamicPruningExpression]))
        assertResult(1823)(
          foundDynamicPruningExpr(1)
            .asInstanceOf[FileSourceScanExecTransformer]
            .selectedPartitions
            .length)
      }
    )
  }

  test("TPCDS Q9 - ScalarSubquery check") {
    runTPCDSQuery("q9") {
      df =>
        var countSubqueryExec = 0
        df.queryExecution.executedPlan.transformAllExpressions {
          case s @ ScalarSubquery(_: SubqueryExec, _) =>
            countSubqueryExec = countSubqueryExec + 1
            s
          case s @ ScalarSubquery(_: ReusedSubqueryExec, _) =>
            countSubqueryExec = countSubqueryExec + 1
            s
        }
        assert(countSubqueryExec == 15)
    }
  }

  test("TPCDS Q21 - DPP check") {
    runTPCDSQuery("q21") {
      df =>
        val foundDynamicPruningExpr = df.queryExecution.executedPlan.find {
          case f: FileSourceScanExecTransformer =>
            f.partitionFilters.exists {
              case _: DynamicPruningExpression => true
              case _ => false
            }
          case _ => false
        }
        assert(foundDynamicPruningExpr.nonEmpty)

        val reuseExchange = df.queryExecution.executedPlan.find {
          case r: ReusedExchangeExec => true
          case _ => false
        }
        assert(reuseExchange.nonEmpty)
    }
  }

  test("TPCDS Q21 with DPP + SHJ") {
    withSQLConf(
      ("spark.sql.autoBroadcastJoinThreshold", "-1"),
      ("spark.sql.optimizer.dynamicPartitionPruning.reuseBroadcastOnly", "false")) {
      runTPCDSQuery("q21") {
        df =>
          val foundDynamicPruningExpr = df.queryExecution.executedPlan.find {
            case f: FileSourceScanExecTransformer =>
              f.partitionFilters.exists {
                case _: DynamicPruningExpression => true
                case _ => false
              }
            case _ => false
          }
          assert(foundDynamicPruningExpr.nonEmpty)

          val reuseExchange = df.queryExecution.executedPlan.find {
            case r: ReusedExchangeExec => true
            case _ => false
          }
          assert(reuseExchange.isEmpty)
      }
    }
  }

  test("Gluten-1234: Fix error when executing hash agg after union all") {
    val testSql =
      """
        |select channel, COUNT(*) sales_cnt, SUM(ext_sales_price) sales_amt FROM (
        |  SELECT 'store' as channel, ss_ext_sales_price ext_sales_price
        |  FROM store_sales, item, date_dim
        |  WHERE ss_addr_sk IS NULL
        |    AND ss_sold_date_sk=d_date_sk
        |    AND ss_item_sk=i_item_sk
        |  UNION ALL
        |  SELECT 'web' as channel, ws_ext_sales_price ext_sales_price
        |  FROM web_sales, item, date_dim
        |  WHERE ws_web_page_sk IS NULL
        |    AND ws_sold_date_sk=d_date_sk
        |    AND ws_item_sk=i_item_sk
        |  ) foo
        |GROUP BY channel
        |ORDER BY channel
        | LIMIT 100 ;
        |""".stripMargin
    compareResultsAgainstVanillaSpark(testSql, compareResult = true, df => {})
  }

  test("Bug-382 collec_list failure") {
    val sql =
      """
        |select cc_call_center_id, collect_list(cc_call_center_sk) from call_center group by cc_call_center_id
        |order by cc_call_center_id
        |""".stripMargin
    compareResultsAgainstVanillaSpark(sql, compareResult = true, df => {})
  }

  test("collec_set") {
    val sql =
      """
        |select a, b from (
        |select cc_call_center_id as a, collect_set(cc_call_center_sk) as set from call_center group by cc_call_center_id)
        |lateral view explode(set) as b
        |order by a, b
        |""".stripMargin
    compareResultsAgainstVanillaSpark(sql, compareResult = true, _ => {})
  }

  test("GLUTEN-1626: test 'roundHalfup'") {
    val sql0 =
      """
        |select cast(ss_wholesale_cost as Int) a, round(sum(ss_wholesale_cost),2),
        |round(sum(ss_wholesale_cost+0.06),2), round(sum(ss_wholesale_cost-0.04),2)
        |from store_sales
        |group by a order by a
        |""".stripMargin
    compareResultsAgainstVanillaSpark(sql0, compareResult = true, _ => {})

    val sql1 =
      """
        |select cast(ss_sales_price as Int) a, round(sum(ss_sales_price),2),
        |round(sum(ss_sales_price+0.06),2), round(sum(ss_sales_price-0.04),2)
        |from store_sales
        |group by a order by a
        |""".stripMargin
    compareResultsAgainstVanillaSpark(sql1, compareResult = true, _ => {})

    val sql2 =
      """
        |select cast(cs_wholesale_cost as Int) a, round(sum(cs_wholesale_cost),2),
        |round(sum(cs_wholesale_cost+0.06),2), round(sum(cs_wholesale_cost-0.04),2)
        |from catalog_sales
        |group by a order by a
        |""".stripMargin
    compareResultsAgainstVanillaSpark(sql2, compareResult = true, _ => {})

    val sql3 =
      """
        |select cast(cs_sales_price as Int) a, round(sum(cs_sales_price),2),
        |round(sum(cs_sales_price+0.06),2), round(sum(cs_sales_price-0.04),2)
        |from catalog_sales
        |group by a order by a
        |""".stripMargin
    compareResultsAgainstVanillaSpark(sql3, compareResult = true, _ => {})

    val sql4 =
      """
        |select cast(ws_wholesale_cost as Int) a, round(sum(ws_wholesale_cost),2),
        |round(sum(ws_wholesale_cost+0.06),2), round(sum(ws_wholesale_cost-0.04),2)
        |from web_sales
        |group by a order by a
        |""".stripMargin
    compareResultsAgainstVanillaSpark(sql4, compareResult = true, _ => {})

    val sql5 =
      """
        |select cast(ws_sales_price as Int) a, round(sum(ws_sales_price),2),
        |round(sum(ws_sales_price+0.06),2), round(sum(ws_sales_price-0.04),2)
        |from web_sales
        |group by a order by a
        |""".stripMargin
    compareResultsAgainstVanillaSpark(sql5, compareResult = true, _ => {})
  }

  test("TakeOrderedAndProjectExecTransformer in broadcastRelation") {
    val q =
      """
        | with dd as (
        | select d_date_sk, count(*) as cn
        | from date_dim
        | where d_date_sk is not null
        | group by d_date_sk
        | order by cn desc
        | limit 10)
        | select count(ss.ss_sold_date_sk)
        | from store_sales ss, dd
        | where ss_sold_date_sk=dd.d_date_sk+1
        |""".stripMargin
    runQueryAndCompare(q)(checkGlutenOperatorMatch[TakeOrderedAndProjectExecTransformer])
  }

}
// scalastyle:on line.size.limit
