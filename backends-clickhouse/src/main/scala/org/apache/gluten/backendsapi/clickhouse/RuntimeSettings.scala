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

  import CHConf.runtimeSettings
  import SQLConf._

  /** Clickhouse settings */
  // scalastyle:off line.size.limit
  val MIN_INSERT_BLOCK_SIZE_ROWS =
    buildConf(runtimeSettings("min_insert_block_size_rows"))
      .doc("https://clickhouse.com/docs/en/operations/settings/settings#min_insert_block_size_rows")
      .longConf
      .createWithDefault(1048449)
  // scalastyle:on line.size.limit

  /** Gluten Configuration */
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
}
