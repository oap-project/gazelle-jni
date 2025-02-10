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
import org.apache.gluten.exception.GlutenException

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.plans.PlanTest

class GlutenClickHouseNativeLibSuite extends PlanTest {

  private def baseSparkConf: SparkConf = {
    new SparkConf()
      .set("spark.plugins", "org.apache.gluten.GlutenPlugin")
      .set("spark.default.parallelism", "1")
      .set("spark.memory.offHeap.enabled", "true")
      .set("spark.memory.offHeap.size", "1024MB")
      .set("spark.gluten.sql.enable.native.validation", "false")
  }

  test("test columnar lib path not exist") {
    var spark: SparkSession = null
    try {
      spark = SparkSession
        .builder()
        .master("local[1]")
        .config(baseSparkConf)
        .config(GlutenConfig.GLUTEN_LIB_PATH.key, "path/not/exist/libch.so")
        .getOrCreate()
      spark.sql("select 1").show()
    } catch {
      case e: Exception =>
        assert(e.isInstanceOf[GlutenException])
        assert(
          e.getMessage.contains(
            "library at path: path/not/exist/libch.so is not a file or does not exist"))
    } finally {
      if (spark != null) {
        spark.stop()
      }
    }
  }

  test("test CHListenerApi initialize only once") {
    var spark: SparkSession = null
    try {
      spark = SparkSession
        .builder()
        .master("local[1]")
        .config(baseSparkConf)
        .config(GlutenConfig.GLUTEN_EXECUTOR_LIB_PATH.key, "/path/not/exist/libch.so")
        .getOrCreate()
      spark.sql("select 1").show()
    } finally {
      if (spark != null) {
        spark.stop()
      }
    }
  }

}
