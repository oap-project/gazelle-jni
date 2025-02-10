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
package org.apache.spark.sql.execution.benchmarks

import org.apache.gluten.config.GlutenConfig
import org.apache.gluten.jni.JniLibLoader

import org.apache.spark.SparkConf
import org.apache.spark.sql.delta.DeltaLog
import org.apache.spark.sql.execution.benchmark.SqlBasedBenchmark
trait CHSqlBasedBenchmark extends SqlBasedBenchmark {

  protected val appName: String
  protected val thrdNum: String
  protected val memorySize: String
  protected val offheapSize: String
  def getSparkConf: SparkConf = {
    val conf = new SparkConf()
      .setAppName(appName)
      .setIfMissing("spark.master", s"local[$thrdNum]")
      .set("spark.plugins", "org.apache.gluten.GlutenPlugin")
      .set(
        "spark.sql.catalog.spark_catalog",
        "org.apache.spark.sql.execution.datasources.v2.clickhouse.ClickHouseSparkCatalog")
      .set("spark.memory.offHeap.enabled", "true")
      .set("spark.databricks.delta.maxSnapshotLineageLength", "20")
      .set("spark.databricks.delta.snapshotPartitions", "1")
      .set("spark.databricks.delta.properties.defaults.checkpointInterval", "5")
      .set("spark.databricks.delta.stalenessLimit", "3600000")
      .set("spark.gluten.sql.enable.native.validation", "false")
      .set("spark.sql.adaptive.enabled", "false")
      .setIfMissing("spark.memory.offHeap.size", offheapSize)
      .setIfMissing("spark.sql.columnVector.offheap.enabled", "true")
      .setIfMissing("spark.driver.memory", memorySize)
      .setIfMissing("spark.executor.memory", memorySize)
      .setIfMissing("spark.sql.files.maxPartitionBytes", "1G")
      .setIfMissing("spark.sql.files.openCostInBytes", "1073741824")

    conf
  }

  override def afterAll(): Unit = {
    DeltaLog.clearCache()
    JniLibLoader.unloadFromPath(spark.conf.get(GlutenConfig.GLUTEN_LIB_PATH.key))
    // Wait for Ctrl+C, convenient for seeing Spark UI
    // Thread.sleep(600000)
    super.afterAll()
  }
}
