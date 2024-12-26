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
package org.apache.spark.sql.delta.rules

import org.apache.gluten.backendsapi.clickhouse.CHBackendSettings

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, V2WriteCommand}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.delta.{OptimisticTransaction, Snapshot, SubqueryTransformerHelper}
import org.apache.spark.sql.delta.files.TahoeLogFileIndex
import org.apache.spark.sql.delta.metering.DeltaLogging
import org.apache.spark.sql.delta.perf.OptimizeMetadataOnlyDeltaQuery
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.delta.stats.DeltaScanGenerator

import org.apache.hadoop.fs.Path

class CHOptimizeMetadataOnlyDeltaQuery(protected val spark: SparkSession)
  extends Rule[LogicalPlan]
  with DeltaLogging
  with SubqueryTransformerHelper
  with OptimizeMetadataOnlyDeltaQuery {

  private val scannedSnapshots =
    new java.util.concurrent.ConcurrentHashMap[(String, Path), Snapshot]

  protected def getDeltaScanGenerator(index: TahoeLogFileIndex): DeltaScanGenerator = {
    // The first case means that we've fixed the table snapshot for time travel
    if (index.isTimeTravelQuery) return index.getSnapshot
    OptimisticTransaction
      .getActive()
      .map(_.getDeltaScanGenerator(index))
      .getOrElse {
        // Will be called only when the log is accessed the first time
        scannedSnapshots.computeIfAbsent(index.deltaLog.compositeId, _ => index.getSnapshot)
      }
  }

  override def apply(plan: LogicalPlan): LogicalPlan = {
    // Should not be applied to subqueries to avoid duplicate delta jobs.
    val isSubquery = isSubqueryRoot(plan)
    // Should not be applied to DataSourceV2 write plans, because they'll be planned later
    // through a V1 fallback and only that later planning takes place within the transaction.
    val isDataSourceV2 = plan.isInstanceOf[V2WriteCommand]
    if (isSubquery || isDataSourceV2) {
      return plan
    }
    // when 'stats.skipping' is off, it still use the metadata to optimize query for count/min/max
    if (
      spark.sessionState.conf
        .getConfString(
          CHBackendSettings.GLUTEN_CLICKHOUSE_DELTA_METADATA_OPTIMIZE,
          CHBackendSettings.GLUTEN_CLICKHOUSE_DELTA_METADATA_OPTIMIZE_DEFAULT_VALUE)
        .toBoolean &&
      !spark.sessionState.conf.getConf(DeltaSQLConf.DELTA_STATS_SKIPPING, true)
    ) {
      optimizeQueryWithMetadata(plan)
    } else {
      plan
    }
  }
}
