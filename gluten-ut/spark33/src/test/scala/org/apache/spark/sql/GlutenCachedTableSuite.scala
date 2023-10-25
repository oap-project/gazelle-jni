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
package org.apache.spark.sql

import io.glutenproject.GlutenConfig

import org.apache.spark.SparkConf
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanHelper
import org.apache.spark.sql.execution.columnar.InMemoryRelation
import org.apache.spark.sql.internal.StaticSQLConf

class GlutenCachedTableSuite
  extends CachedTableSuite
  with GlutenSQLTestsTrait
  with AdaptiveSparkPlanHelper {

  override def sparkConf: SparkConf = {
    super[GlutenSQLTestsTrait].sparkConf
      .set("spark.sql.shuffle.partitions", "5")
      .set(GlutenConfig.COLUMNAR_TABLE_CACHE_ENABLED.key, "true")
      .set(
        StaticSQLConf.SPARK_CACHE_SERIALIZER.key,
        "org.apache.spark.sql.execution.ColumnarCachedBatchSerializer")
  }

  test("GLUTEN - InMemoryRelation statistics") {
    logWarning(
      "serializer: " +
        s"${spark.sessionState.conf.getConf(StaticSQLConf.SPARK_CACHE_SERIALIZER)}")
    sql("CACHE TABLE testData")
    spark.table("testData").queryExecution.withCachedData.collect {
      case cached: InMemoryRelation =>
        logWarning(s"${cached.cacheBuilder.serializer.getClass.getName}")
        assert(cached.stats.sizeInBytes === 1132)
    }
  }
}
