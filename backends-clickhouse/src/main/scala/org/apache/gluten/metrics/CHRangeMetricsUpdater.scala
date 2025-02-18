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
package org.apache.gluten.metrics

import org.apache.spark.sql.execution.metric.SQLMetric

class CHRangeMetricsUpdater(val metrics: Map[String, SQLMetric]) extends MetricsUpdater {
  override def updateNativeMetrics(opMetrics: IOperatorMetrics): Unit = {
    if (opMetrics != null) {
      val operatorMetrics = opMetrics.asInstanceOf[OperatorMetrics]
      if (!operatorMetrics.metricsList.isEmpty) {
        val metricsData = operatorMetrics.metricsList.get(0)
        metrics("totalTime") += (metricsData.time / 1000L).toLong
        metrics("inputWaitTime") += (metricsData.inputWaitTime / 1000L).toLong
        metrics("outputWaitTime") += (metricsData.outputWaitTime / 1000L).toLong
        metrics("outputVectors") += metricsData.outputVectors

        MetricsUtil.updateExtraTimeMetric(
          metricsData,
          metrics("extraTime"),
          metrics("numOutputRows"),
          metrics("outputBytes"),
          metrics("numInputRows"),
          metrics("inputBytes"),
          GenerateMetricsUpdater.INCLUDING_PROCESSORS,
          GenerateMetricsUpdater.CH_PLAN_NODE_NAME
        )
      }
    }
  }

}

object CHRangeMetricsUpdater {
  val INCLUDING_PROCESSORS: Array[String] = Array("SourceFromRange")
  val CH_PLAN_NODE_NAME: Array[String] = Array("SourceFromRange")
}
