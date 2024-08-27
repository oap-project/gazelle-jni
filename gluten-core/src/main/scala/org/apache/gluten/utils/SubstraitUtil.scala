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
package org.apache.gluten.utils

import org.apache.gluten.backendsapi.BackendsApiManager
import org.apache.gluten.expression.{ConverterUtil, ExpressionConverter}
import org.apache.gluten.substrait.`type`.TypeBuilder
import org.apache.gluten.substrait.SubstraitContext
import org.apache.gluten.substrait.expression.ExpressionNode

import org.apache.spark.sql.catalyst.expressions.{Attribute, Expression}
import org.apache.spark.sql.catalyst.plans.{FullOuter, InnerLike, JoinType, LeftAnti, LeftOuter, LeftSemi, RightOuter}

import io.substrait.proto.{CrossRel, JoinRel}

import scala.collection.JavaConverters._

object SubstraitUtil {
  def toSubstrait(sparkJoin: JoinType): JoinRel.JoinType = sparkJoin match {
    case _: InnerLike =>
      JoinRel.JoinType.JOIN_TYPE_INNER
    case FullOuter =>
      JoinRel.JoinType.JOIN_TYPE_OUTER
    case LeftOuter | RightOuter =>
      // The right side is required to be used for building hash table in Substrait plan.
      // Therefore, for RightOuter Join, the left and right relations are exchanged and the
      // join type is reverted.
      JoinRel.JoinType.JOIN_TYPE_LEFT
    case LeftSemi =>
      JoinRel.JoinType.JOIN_TYPE_LEFT_SEMI
    case LeftAnti =>
      JoinRel.JoinType.JOIN_TYPE_ANTI
    case _ =>
      // TODO: Support existence join
      JoinRel.JoinType.UNRECOGNIZED
  }

  def toCrossRelSubstrait(sparkJoin: JoinType): CrossRel.JoinType = sparkJoin match {
    case _: InnerLike =>
      CrossRel.JoinType.JOIN_TYPE_INNER
    case LeftOuter | RightOuter =>
      // since we always assume build right side in substrait,
      // the left and right relations are exchanged and the
      // join type is reverted.
      CrossRel.JoinType.JOIN_TYPE_LEFT
    case LeftSemi =>
      CrossRel.JoinType.JOIN_TYPE_LEFT_SEMI
    case FullOuter =>
      CrossRel.JoinType.JOIN_TYPE_OUTER
    case _ =>
      CrossRel.JoinType.UNRECOGNIZED
  }

  def createEnhancement(output: Seq[Attribute]): com.google.protobuf.Any = {
    val inputTypeNodes = output.map {
      attr => ConverterUtil.getTypeNode(attr.dataType, attr.nullable)
    }
    // Normally the enhancement node is only used for plan validation. But here the enhancement
    // is also used in execution phase. In this case an empty typeUrlPrefix need to be passed,
    // so that it can be correctly parsed into json string on the cpp side.
    BackendsApiManager.getTransformerApiInstance.packPBMessage(
      TypeBuilder.makeStruct(false, inputTypeNodes.asJava).toProtobuf)
  }

  def toSubstraitExpression(
      expr: Expression,
      attributeSeq: Seq[Attribute],
      context: SubstraitContext): ExpressionNode = {
    ExpressionConverter
      .replaceWithExpressionTransformer(expr, attributeSeq)
      .doTransform(context.registeredFunction)
  }
}
