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
import org.apache.gluten.expression._
import org.apache.gluten.extension.ValidationResult
import org.apache.gluten.metrics.MetricsUpdater
import org.apache.gluten.substrait.SubstraitContext
import org.apache.gluten.substrait.expression.WindowFunctionNode
import org.apache.gluten.substrait.extensions.ExtensionBuilder
import org.apache.gluten.substrait.rel.{RelBuilder, RelNode}

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.physical.{AllTuples, ClusteredDistribution, Distribution, Partitioning}
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.window.WindowExecBase

import com.google.protobuf.{Any, StringValue}
import io.substrait.proto.SortField

import java.util.{ArrayList => JArrayList}

import scala.collection.JavaConverters._

case class WindowExecTransformer(
    windowExpression: Seq[NamedExpression],
    partitionSpec: Seq[Expression],
    orderSpec: Seq[SortOrder],
    child: SparkPlan)
  extends WindowExecBase
  with UnaryTransformSupport {

  // Note: "metrics" is made transient to avoid sending driver-side metrics to tasks.
  @transient override lazy val metrics =
    BackendsApiManager.getMetricsApiInstance.genWindowTransformerMetrics(sparkContext)

  override def isNoop: Boolean = windowExpression == null || windowExpression.isEmpty

  override def metricsUpdater(): MetricsUpdater = if (isNoop) {
    MetricsUpdater.None
  } else {
    BackendsApiManager.getMetricsApiInstance.genWindowTransformerMetricsUpdater(metrics)
  }

  override def output: Seq[Attribute] = child.output ++ windowExpression.map(_.toAttribute)

  override def requiredChildDistribution: Seq[Distribution] = {
    if (partitionSpec.isEmpty) {
      // Only show warning when the number of bytes is larger than 100 MiB?
      logWarning(
        "No Partition Defined for Window operation! Moving all data to a single "
          + "partition, this can cause serious performance degradation.")
      AllTuples :: Nil
    } else ClusteredDistribution(partitionSpec) :: Nil
  }

  override def requiredChildOrdering: Seq[Seq[SortOrder]] = {
    if (BackendsApiManager.getSettings.requiredChildOrderingForWindow()) {
      Seq(partitionSpec.map(SortOrder(_, Ascending)) ++ orderSpec)
    } else {
      Seq(Nil)
    }
  }

  override def outputOrdering: Seq[SortOrder] = child.outputOrdering

  override def outputPartitioning: Partitioning = child.outputPartitioning

  def genWindowParameters(): Any = {
    // Start with "WindowParameters:"
    val windowParametersStr = new StringBuffer("WindowParameters:")
    // isStreaming: 1 for streaming, 0 for sort
    val isStreaming: Int =
      if (BackendsApiManager.getSettings.requiredChildOrderingForWindow()) 1 else 0

    windowParametersStr
      .append("isStreaming=")
      .append(isStreaming)
      .append("\n")
    val message = StringValue
      .newBuilder()
      .setValue(windowParametersStr.toString)
      .build()
    BackendsApiManager.getTransformerApiInstance.packPBMessage(message)
  }

  def getWindowRel(
      context: SubstraitContext,
      originalInputAttributes: Seq[Attribute],
      operatorId: Long,
      input: RelNode,
      validation: Boolean): RelNode = {
    val args = context.registeredFunction
    // WindowFunction Expressions
    val windowExpressions = new JArrayList[WindowFunctionNode]()
    BackendsApiManager.getSparkPlanExecApiInstance.genWindowFunctionsNode(
      windowExpression,
      windowExpressions,
      originalInputAttributes,
      args
    )

    // Partition By Expressions
    val partitionsExpressions = partitionSpec
      .map(
        ExpressionConverter
          .replaceWithExpressionTransformer(_, attributeSeq = child.output)
          .doTransform(args))
      .asJava

    // Sort By Expressions
    val sortFieldList =
      orderSpec.map {
        order =>
          val builder = SortField.newBuilder()
          val exprNode = ExpressionConverter
            .replaceWithExpressionTransformer(order.child, attributeSeq = child.output)
            .doTransform(args)
          builder.setExpr(exprNode.toProtobuf)
          builder.setDirectionValue(SortExecTransformer.transformSortDirection(order))
          builder.build()
      }.asJava
    if (!validation) {
      val extensionNode = ExtensionBuilder.makeAdvancedExtension(genWindowParameters(), null)
      RelBuilder.makeWindowRel(
        input,
        windowExpressions,
        partitionsExpressions,
        sortFieldList,
        extensionNode,
        context,
        operatorId)
    } else {
      RelBuilder.makeWindowRel(
        input,
        windowExpressions,
        partitionsExpressions,
        sortFieldList,
        RelBuilder.createExtensionNode(originalInputAttributes.asJava),
        context,
        operatorId)
    }
  }

  override protected def doValidateInternal(): ValidationResult = {
    if (!BackendsApiManager.getSettings.supportWindowExec(windowExpression)) {
      return ValidationResult
        .failed(s"Found unsupported window expression: ${windowExpression.mkString(", ")}")
    }
    val substraitContext = new SubstraitContext
    val operatorId = substraitContext.nextOperatorId(this.nodeName)

    val relNode = getWindowRel(substraitContext, child.output, operatorId, null, validation = true)

    doNativeValidation(substraitContext, relNode)
  }

  override protected def doTransform(context: SubstraitContext): TransformContext = {
    val childCtx = child.asInstanceOf[TransformSupport].transform(context)
    if (isNoop) {
      // The computing for this operator is not needed.
      return childCtx
    }

    val operatorId = context.nextOperatorId(this.nodeName)
    val currRel =
      getWindowRel(context, child.output, operatorId, childCtx.root, validation = false)
    assert(currRel != null, "Window Rel should be valid")
    TransformContext(output, currRel)
  }

  override protected def withNewChildInternal(newChild: SparkPlan): WindowExecTransformer =
    copy(child = newChild)
}
