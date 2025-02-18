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

import org.apache.gluten.backendsapi.BackendsApiManager
import org.apache.gluten.config.GlutenConfig
import org.apache.gluten.extension.ValidationResult
import org.apache.gluten.sql.shims.SparkShimLoader

import org.apache.spark.sql.catalyst.plans.physical.{Partitioning, SinglePartition}
import org.apache.spark.sql.execution.{CollectLimitExec, LimitExec, SparkPlan}

abstract class ColumnarCollectLimitBaseExec(
    limit: Int,
    childPlan: SparkPlan
) extends LimitExec
  with ValidatablePlan {

  override def outputPartitioning: Partitioning = SinglePartition

  override protected def doValidateInternal(): ValidationResult = {
    val isSupported = BackendsApiManager.getSettings.supportCollectLimitExec()

    if (!isSupported) {
      return ValidationResult.failed(
        s"CollectLimitExec is not supported by the current backend."
      )
    }

    if (
      (childPlan.supportsColumnar && GlutenConfig.get.enablePreferColumnar) &&
      BackendsApiManager.getSettings.supportColumnarShuffleExec() &&
      SparkShimLoader.getSparkShims.isColumnarLimitExecSupported()
    ) {
      return ValidationResult.succeeded
    }
    ValidationResult.failed("Columnar shuffle not enabled or child does not support columnar.")
  }

  override protected def doExecute()
      : org.apache.spark.rdd.RDD[org.apache.spark.sql.catalyst.InternalRow] = {
    throw new UnsupportedOperationException(s"This operator doesn't support doExecute().")
  }

}
object ColumnarCollectLimitBaseExec {
  def from(collectLimitExec: CollectLimitExec): ColumnarCollectLimitBaseExec = {
    BackendsApiManager.getSparkPlanExecApiInstance
      .genColumnarCollectLimitExec(
        collectLimitExec.limit,
        collectLimitExec.child
      )
  }
}
