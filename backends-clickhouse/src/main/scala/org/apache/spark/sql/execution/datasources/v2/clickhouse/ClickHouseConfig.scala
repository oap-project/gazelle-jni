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
package org.apache.spark.sql.execution.datasources.v2.clickhouse

import org.apache.gluten.backendsapi.clickhouse.CHConfig

import org.apache.spark.sql.catalyst.catalog.BucketSpec
import org.apache.spark.sql.execution.datasources.mergetree.StorageMeta

import java.util

import scala.collection.JavaConverters.mapAsScalaMapConverter

object ClickHouseConfig {

  // MergeTree DataSource name
  val NAME = "clickhouse"
  val ALT_NAME = "clickhouse"
  val METADATA_DIR = "_delta_log"
  private val FORMAT_ENGINE = "engine"
  private val DEFAULT_ENGINE = "mergetree"
  private val OPT_NAME_PREFIX = "clickhouse."

  val CLICKHOUSE_WORKER_ID: String = CHConfig.prefixOf("worker.id")

  /** Create a mergetree configurations and returns the normalized key -> value map. */
  def createMergeTreeConfigurations(
      allProperties: util.Map[String, String],
      buckets: Option[BucketSpec] = None): Map[String, String] = {
    val configurations = scala.collection.mutable.Map[String, String]()
    allProperties.asScala.foreach(configurations += _)
    if (!configurations.contains("metadata_path")) {
      configurations += ("metadata_path" -> METADATA_DIR)
    }
    if (!configurations.contains(FORMAT_ENGINE)) {
      configurations += (FORMAT_ENGINE -> DEFAULT_ENGINE)
    } else {
      if (
        !configurations
          .get(FORMAT_ENGINE)
          .exists(s => s.equals(DEFAULT_ENGINE) || s.equals("parquet"))
      ) {
        configurations += (FORMAT_ENGINE -> DEFAULT_ENGINE)
      }
    }
    if (!configurations.contains("sampling_key")) {
      configurations += ("sampling_key" -> "")
    }
    if (!configurations.contains(StorageMeta.POLICY)) {
      configurations += (StorageMeta.POLICY -> "default")
    }
    if (!configurations.contains("is_distribute")) {
      configurations += ("is_distribute" -> "true")
    }

    if (buckets.isDefined) {
      val bucketSpec = buckets.get
      configurations += ("numBuckets" -> bucketSpec.numBuckets.toString)
      configurations += ("bucketColumnNames" -> bucketSpec.bucketColumnNames.mkString(","))
      if (bucketSpec.sortColumnNames.nonEmpty) {
        configurations += ("orderByKey" -> bucketSpec.sortColumnNames.mkString(","))
      }
    }
    configurations.toMap
  }

  def isMergeTreeFormatEngine(configuration: Map[String, String]): Boolean = {
    configuration.get(FORMAT_ENGINE).exists(_.equals(DEFAULT_ENGINE))
  }

  /** Get the related clickhouse option when using DataFrameWriter / DataFrameReader */
  def getMergeTreeConfigurations(
      properties: util.Map[String, String]
  ): Map[String, String] = {
    properties.asScala
      .filterKeys(_.startsWith(OPT_NAME_PREFIX))
      .map(x => (x._1.substring(OPT_NAME_PREFIX.length), x._2))
      .toMap
  }
}
