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
import org.apache.gluten.extension.ValidationResult

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.optimizer.BuildSide
import org.apache.spark.sql.catalyst.plans.{ExistenceJoin, JoinType}
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.joins.BuildSideRelation
import org.apache.spark.sql.vectorized.ColumnarBatch

import com.google.protobuf.{Any, StringValue}

case class VeloxBroadcastNestedLoopJoinExecTransformer(
    left: SparkPlan,
    right: SparkPlan,
    buildSide: BuildSide,
    joinType: JoinType,
    condition: Option[Expression])
  extends BroadcastNestedLoopJoinExecTransformer(
    left,
    right,
    buildSide,
    joinType,
    condition
  ) {

  override def columnarInputRDDs: Seq[RDD[ColumnarBatch]] = {
    val streamedRDD = getColumnarInputRDDs(streamedPlan)
    val broadcast = buildPlan.executeBroadcast[BuildSideRelation]()
    val broadcastRDD = VeloxBroadcastBuildSideRDD(sparkContext, broadcast)
    // FIXME: Do we have to make build side a RDD?
    streamedRDD :+ broadcastRDD
  }

  override protected def withNewChildrenInternal(
      newLeft: SparkPlan,
      newRight: SparkPlan): VeloxBroadcastNestedLoopJoinExecTransformer =
    copy(left = newLeft, right = newRight)

  override def genJoinParameters(): Any = {
    val joinParametersStr = new StringBuffer("JoinParameters:")
    joinParametersStr
      .append("isExistenceJoin=")
      .append(if (joinType.isInstanceOf[ExistenceJoin]) 1 else 0)
      .append("\n")
    val message = StringValue
      .newBuilder()
      .setValue(joinParametersStr.toString)
      .build()
    BackendsApiManager.getTransformerApiInstance.packPBMessage(message)
  }

  override def backendSpecificJoinValidation(): Option[ValidationResult] = {
    joinType match {
      case ExistenceJoin(_) => Some(ValidationResult.succeeded)
      case _ => None
    }
  }
}
