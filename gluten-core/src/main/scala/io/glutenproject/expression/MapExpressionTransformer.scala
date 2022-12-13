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

package io.glutenproject.expression

import io.glutenproject.expression.ConverterUtils.FunctionConfig
import io.glutenproject.substrait.`type`.TypeBuilder
import io.glutenproject.substrait.expression.{ExpressionBuilder, ExpressionNode}

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.types.DataType
import io.glutenproject.backendsapi.BackendsApiManager

import scala.collection.mutable.ArrayBuffer
import com.google.common.collect.Lists
import io.glutenproject.GlutenConfig

class CreateMapTransformer(substraitExprName: String, children: Seq[ExpressionTransformer],
  useStringTypeWhenEmpty: Boolean, original: CreateMap)
  extends ExpressionTransformer
  with Logging {

  override def doTransform(args: java.lang.Object): ExpressionNode = {
    // If children is empty,
    // transformation is only supported when useStringTypeWhenEmpty is false
    // because ClickHouse and Velox currently doesn't support this config.
    if (children.isEmpty && useStringTypeWhenEmpty) {
      throw new UnsupportedOperationException(s"not supported yet.")
    }

    val childNodes = new java.util.ArrayList[ExpressionNode]()
    children.foreach(child => {
      val childNode = child.doTransform(args)
      childNodes.add(childNode)
    })

    val functionMap = args.asInstanceOf[java.util.HashMap[String, java.lang.Long]]
    val functionName = ConverterUtils.makeFuncName(substraitExprName,
      original.children.map(_.dataType), FunctionConfig.OPT)
    val functionId = ExpressionBuilder.newScalarFunction(functionMap, functionName)
    val typeNode = ConverterUtils.getTypeNode(original.dataType, original.nullable)
    ExpressionBuilder.makeScalarFunction(functionId, childNodes, typeNode)
  }
}

class GetMapValueTransformer(substraitExprName: String, child: ExpressionTransformer,
  key: ExpressionTransformer, failOnError: Boolean, original: GetMapValue)
  extends ExpressionTransformer
  with Logging {

  override def doTransform(args: java.lang.Object): ExpressionNode = {
    // ClickHouse backend doesn't support fail on error
    if (BackendsApiManager.getBackendName.equalsIgnoreCase(
      GlutenConfig.GLUTEN_CLICKHOUSE_BACKEND) && failOnError) {
      throw new UnsupportedOperationException(s"not supported yet.")
    }

    // Velox backend always fails on error
    if (BackendsApiManager.getBackendName.equalsIgnoreCase(
      GlutenConfig.GLUTEN_VELOX_BACKEND) && !failOnError) {
      throw new UnsupportedOperationException(s"not supported yet.")
    }

    val childNode = child.doTransform(args)
    val keyNode = key.doTransform(args)

    val functionMap = args.asInstanceOf[java.util.HashMap[String, java.lang.Long]]
    val functionName = ConverterUtils.makeFuncName(substraitExprName,
      Seq(original.child.dataType, original.key.dataType), FunctionConfig.OPT)
    val functionId = ExpressionBuilder.newScalarFunction(functionMap, functionName)
    val exprNodes = Lists.newArrayList(
      childNode.asInstanceOf[ExpressionNode],
      keyNode.asInstanceOf[ExpressionNode])
    val typeNode = ConverterUtils.getTypeNode(original.dataType, original.nullable)
    ExpressionBuilder.makeScalarFunction(functionId, exprNodes, typeNode)
  }
}
