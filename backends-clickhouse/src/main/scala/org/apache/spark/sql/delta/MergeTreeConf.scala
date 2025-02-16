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
package org.apache.spark.sql.delta

import org.apache.gluten.backendsapi.clickhouse.CHConfig

import org.apache.spark.internal.config.ConfigBuilder
import org.apache.spark.sql.internal.SQLConf

/** [[CHConfig]] entries for MergeTree features. */
trait MergeTreeConfBase {

  private val CH_PREFIX: String = CHConfig.runtimeSettings("merge_tree")
  private val GLUTEN_PREFIX: String = CHConfig.runtimeSettings("mergetree")

  private def buildCHConf(key: String): ConfigBuilder = SQLConf.buildConf(s"$CH_PREFIX.$key")
  private def buildGLUTENConf(key: String): ConfigBuilder =
    SQLConf.buildConf(s"$GLUTEN_PREFIX.$key")

  // scalastyle:off line.size.limit
  val ASSIGN_PART_UUIDS =
    buildCHConf("assign_part_uuids")
      .doc(s""" Used in UT for compatibility with old compaction algorithm.
              | https://clickhouse.com/docs/en/engines/table-engines/mergetree-family/mergetree#virtual-columns
              |""".stripMargin)
      .booleanConf
      .createWithDefault(false)

  val MIN_ROWS_FOR_WIDE_PART =
    buildCHConf("min_rows_for_wide_part")
      .doc(s""" Used in UT for compatibility with old compaction algorithm.
              | https://clickhouse.com/docs/en/operations/settings/merge-tree-settings#min_bytes_for_wide_part
              |""".stripMargin)
      .longConf
      .createWithDefault(0)
  // scalastyle:on line.size.limit

  val OPTIMIZE_TASK =
    buildGLUTENConf("optimize_task")
      .doc(s""" Used in UT for compatibility with old compaction algorithm.
              | This flag is used to notify pipeline that the current running task
              | is an MergeTree optimize task.
              | """.stripMargin)
      .booleanConf
      .createWithDefault(false)
}

object MergeTreeConf extends MergeTreeConfBase
