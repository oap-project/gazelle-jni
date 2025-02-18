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
package org.apache.gluten.backendsapi.clickhouse

import org.apache.spark.sql.internal.SQLConf

object RuntimeSettings {

  import CHConfig.runtimeSettings
  import SQLConf._

  /** Clickhouse settings */
  // scalastyle:off line.size.limit
  val MIN_INSERT_BLOCK_SIZE_ROWS =
    buildConf(runtimeSettings("min_insert_block_size_rows"))
      .doc("https://clickhouse.com/docs/en/operations/settings/settings#min_insert_block_size_rows")
      .longConf
      .createWithDefault(1048449)

  val MAX_BYTES_BEFORE_EXTERNAL_SORT =
    buildConf(runtimeSettings("max_bytes_before_external_sort"))
      .doc("https://clickhouse.com/docs/en/operations/settings/query-complexity#settings-max_bytes_before_external_sort")
      .longConf
      .createWithDefault(0)

  // TODO: support check value
  val OUTPUT_FORMAT_COMPRESSION_LEVEL =
    buildConf(runtimeSettings("output_format_compression_level"))
      .doc(s"""https://clickhouse.com/docs/en/operations/settings/settings#output_format_compression_level
              | Notes: we always use Snappy compression, and Snappy doesn't support compression level.
              | Currently, we ONLY set it in UT.
              |""".stripMargin)
      .longConf
      .createWithDefault(Integer.MIN_VALUE & 0xffffffffL)
  // .checkValue(v => v >= 0, "COMPRESSION LEVEL must be greater than 0")
  // scalastyle:on line.size.limit

  /** Gluten Configuration */
  // scalastyle:off line.size.limit
  val COLLECT_METRICS =
    buildConf(runtimeSettings("collect_metrics"))
      .doc(s"""If true, we need disable query_plan_enable_optimizations, otherwise clickhouse optimize the query plan
              |and cause collecting metrics failed.
              |see https://clickhouse.com/docs/en/operations/settings/settings#query_plan_enable_optimizations
              |""".stripMargin)
      .booleanConf
      .createWithDefault(true)
  // scalastyle:on line.size.limit

  val NATIVE_WRITE_RESERVE_PARTITION_COLUMNS =
    buildConf(runtimeSettings("gluten.write.reserve_partition_columns"))
      .doc("Whether reserve partition columns for Native write or not, default is false")
      .booleanConf
      .createWithDefault(false)

  val TASK_WRITE_TMP_DIR =
    buildConf(runtimeSettings("gluten.task_write_tmp_dir"))
      .doc("The temporary directory for writing data")
      .stringConf
      .createWithDefault("")

  val TASK_WRITE_FILENAME_PATTERN =
    buildConf(runtimeSettings("gluten.task_write_filename_pattern"))
      .doc("The pattern to generate file name for writing delta parquet in spark 3.5")
      .stringConf
      .createWithDefault("")

  val PART_NAME_PREFIX =
    buildConf(runtimeSettings("gluten.part_name_prefix"))
      .doc("The part name prefix for writing data")
      .stringConf
      .createWithDefault("")

  val PARTITION_DIR =
    buildConf(runtimeSettings("gluten.partition_dir"))
      .doc("The partition directory for writing data")
      .stringConf
      .createWithDefault("")

  val BUCKET_DIR =
    buildConf(runtimeSettings("gluten.bucket_dir"))
      .doc("The bucket directory for writing data")
      .stringConf
      .createWithDefault("")

  val ENABLE_MEMORY_SPILL_SCHEDULER =
    buildConf(runtimeSettings("enable_adaptive_memory_spill_scheduler"))
      .doc("Enable memory spill scheduler")
      .booleanConf
      .createWithDefault(true)
}
