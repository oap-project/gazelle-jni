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
package org.apache.gluten.extension

import org.apache.gluten.GlutenConfig

import org.apache.spark.sql.catalyst.expressions.{Alias, BitwiseAnd, Expression, HiveHash, Literal, Pmod}
import org.apache.spark.sql.catalyst.plans.physical.HashPartitioning
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.{ProjectExec, SparkPlan}
import org.apache.spark.sql.execution.datasources.WriteFilesExec

/**
 * Wrap with bucket value to specify the bucket file name in native write. Native writer will remove
 * this value in the final output.
 */
object WriteFilesWithBucketValue extends Rule[SparkPlan] {

  val optionForHiveCompatibleBucketWrite = "__hive_compatible_bucketed_table_insertion__"

  override def apply(plan: SparkPlan): SparkPlan = {
    if (
      GlutenConfig.getConf.enableGluten
      && GlutenConfig.getConf.enableNativeWriter.getOrElse(false)
    ) {
      plan.transformDown {
        case writeFiles: WriteFilesExec if writeFiles.bucketSpec.isDefined =>
          val bucketIdExp = getWriterBucketIdExp(writeFiles)
          val wrapBucketValue = ProjectExec(
            writeFiles.child.output :+ Alias(bucketIdExp, "__bucket_value__")(),
            writeFiles.child)
          writeFiles.copy(child = wrapBucketValue)
      }
    } else {
      plan
    }
  }

  private def getWriterBucketIdExp(writeFilesExec: WriteFilesExec): Expression = {
    val partitionColumns = writeFilesExec.partitionColumns
    val outputColumns = writeFilesExec.child.output
    val dataColumns = outputColumns.filterNot(partitionColumns.contains)
    val bucketSpec = writeFilesExec.bucketSpec.get
    val bucketColumns = bucketSpec.bucketColumnNames.map(c => dataColumns.find(_.name == c).get)
    if (writeFilesExec.options.getOrElse(optionForHiveCompatibleBucketWrite, "false") == "true") {
      val hashId = BitwiseAnd(HiveHash(bucketColumns), Literal(Int.MaxValue))
      Pmod(hashId, Literal(bucketSpec.numBuckets))
      // The bucket file name prefix is following Hive, Presto and Trino conversion, so this
      // makes sure Hive bucketed table written by Spark, can be read by other SQL engines.
      //
      // Hive: `org.apache.hadoop.hive.ql.exec.Utilities#getBucketIdFromFile()`.
      // Trino: `io.trino.plugin.hive.BackgroundHiveSplitLoader#BUCKET_PATTERNS`.

    } else {
      // Spark bucketed table: use `HashPartitioning.partitionIdExpression` as bucket id
      // expression, so that we can guarantee the data distribution is same between shuffle and
      // bucketed data source, which enables us to only shuffle one side when join a bucketed
      // table and a normal one.
      HashPartitioning(bucketColumns, bucketSpec.numBuckets).partitionIdExpression
    }
  }
}
