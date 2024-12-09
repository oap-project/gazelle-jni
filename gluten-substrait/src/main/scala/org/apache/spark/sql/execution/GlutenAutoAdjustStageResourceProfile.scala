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
package org.apache.spark.sql.execution

import org.apache.gluten.GlutenConfig
import org.apache.gluten.execution.{ColumnarToRowExecBase, GlutenPlan}
import org.apache.gluten.logging.LogLevelUtil

import org.apache.spark.annotation.Experimental
import org.apache.spark.internal.Logging
import org.apache.spark.resource.{ExecutorResourceRequest, ResourceProfile, ResourceProfileManager, TaskResourceRequest}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.GlutenAutoAdjustStageResourceProfile.{applyNewResourceProfileIfPossible, collectStagePlan}
import org.apache.spark.sql.execution.adaptive.QueryStageExec
import org.apache.spark.sql.execution.command.{DataWritingCommandExec, ExecutedCommandExec}
import org.apache.spark.sql.execution.exchange.Exchange

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

/**
 * This rule is used to dynamic adjust stage resource profile for following purposes:
 *   1. swap offheap and onheap memory size when whole stage fallback happened 2. increase executor
 *      heap memory if stage contains gluten operator and spark operator at the same time. Note: we
 *      don't support set resource profile for final stage now. Todo: support set resource profile
 *      for final stage.
 */
@Experimental
case class GlutenAutoAdjustStageResourceProfile(glutenConf: GlutenConfig, spark: SparkSession)
  extends Rule[SparkPlan]
  with LogLevelUtil {

  override def apply(plan: SparkPlan): SparkPlan = {
    if (!glutenConf.enableAutoAdjustStageResourceProfile) {
      return plan
    }
    if (!plan.isInstanceOf[Exchange]) {
      // todo: support set resource profile for final stage
      return plan
    }
    val planNodes = collectStagePlan(plan)
    if (planNodes.isEmpty) {
      return plan
    }
    log.info(s"detailPlanNodes ${planNodes.map(_.nodeName).mkString("Array(", ", ", ")")}")

    // one stage is considered as fallback if all node is not GlutenPlan
    // or all GlutenPlan node is C2R node.
    val wholeStageFallback = planNodes
      .filter(_.isInstanceOf[GlutenPlan])
      .count(!_.isInstanceOf[ColumnarToRowExecBase]) == 0

    val rpManager = spark.sparkContext.resourceProfileManager
    val defaultRP = rpManager.defaultResourceProfile

    // initial resource profile config as default resource profile
    val taskResource = mutable.Map.empty[String, TaskResourceRequest] ++= defaultRP.taskResources
    val executorResource =
      mutable.Map.empty[String, ExecutorResourceRequest] ++= defaultRP.executorResources
    val memoryRequest = executorResource.get(ResourceProfile.MEMORY)
    val offheapRequest = executorResource.get(ResourceProfile.OFFHEAP_MEM)
    logInfo(s"default memory request $memoryRequest")
    logInfo(s"default offheap request $offheapRequest")

    // case 1: whole stage fallback to vanilla spark in such case we swap the heap
    // and offheap amount.
    if (wholeStageFallback) {
      val newMemoryAmount = offheapRequest.get.amount
      val newOffheapAmount = memoryRequest.get.amount
      val newExecutorMemory =
        new ExecutorResourceRequest(ResourceProfile.MEMORY, newMemoryAmount)
      val newExecutorOffheap =
        new ExecutorResourceRequest(ResourceProfile.OFFHEAP_MEM, newOffheapAmount)
      executorResource.put(ResourceProfile.MEMORY, newExecutorMemory)
      executorResource.put(ResourceProfile.OFFHEAP_MEM, newExecutorOffheap)
      val newRP = new ResourceProfile(executorResource.toMap, taskResource.toMap)
      return applyNewResourceProfileIfPossible(plan, newRP, rpManager)
    }

    // case 1: check whether fallback exists and decide whether increase heap memory
    val c2RorR2CCnt = planNodes.count(
      p => p.isInstanceOf[ColumnarToRowTransition] || p.isInstanceOf[RowToColumnarTransition])

    if (c2RorR2CCnt >= glutenConf.autoAdjustStageC2RorR2CThreshold) {
      val newMemoryAmount = memoryRequest.get.amount * glutenConf.autoAdjustStageRPHeapRatio;
      val newExecutorMemory =
        new ExecutorResourceRequest(ResourceProfile.MEMORY, newMemoryAmount.toLong)
      executorResource.put(ResourceProfile.MEMORY, newExecutorMemory)
      val newRP = new ResourceProfile(executorResource.toMap, taskResource.toMap)
      return applyNewResourceProfileIfPossible(plan, newRP, rpManager)
    }
    plan
  }
}

object GlutenAutoAdjustStageResourceProfile extends Logging {
  // collect all plan nodes belong to this stage including child query stage
  // but exclude query stage child
  def collectStagePlan(plan: SparkPlan): ArrayBuffer[SparkPlan] = {

    def collectStagePlan(plan: SparkPlan, planNodes: ArrayBuffer[SparkPlan]): Unit = {
      if (plan.isInstanceOf[DataWritingCommandExec] || plan.isInstanceOf[ExecutedCommandExec]) {
        // todo: support set final stage's resource profile
        return
      }
      planNodes += plan
      if (plan.isInstanceOf[QueryStageExec]) {
        return
      }
      plan.children.foreach(collectStagePlan(_, planNodes))
    }

    val planNodes = new ArrayBuffer[SparkPlan]()
    collectStagePlan(plan, planNodes)
    planNodes
  }

  private def getFinalResourceProfile(
      rpManager: ResourceProfileManager,
      newRP: ResourceProfile): ResourceProfile = {
    val maybeEqProfile = rpManager.getEquivalentProfile(newRP)
    if (maybeEqProfile.isDefined) {
      maybeEqProfile.get
    } else {
      // register new resource profile here
      rpManager.addResourceProfile(newRP)
      newRP
    }
  }

  def applyNewResourceProfileIfPossible(
      plan: SparkPlan,
      rp: ResourceProfile,
      rpManager: ResourceProfileManager): SparkPlan = {
    val finalRP = getFinalResourceProfile(rpManager, rp)
    if (plan.isInstanceOf[Exchange]) {
      // Wrap the plan with ApplyResourceProfileExec so that we can apply new ResourceProfile
      val wrapperPlan = ApplyResourceProfileExec(plan.children.head, finalRP)
      logInfo(s"Apply resource profile $finalRP for plan ${wrapperPlan.nodeName}")
      plan.withNewChildren(IndexedSeq(wrapperPlan))
    } else {
      logInfo(s"Ignore apply resource profile for plan ${plan.nodeName}")
      // todo: support set InsertInto stage's resource profile
      plan
    }
  }
}
