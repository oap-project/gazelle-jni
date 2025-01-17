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
package org.apache.gluten.expression

import org.apache.gluten.expression.ConverterUtils.FunctionConfig
import org.apache.gluten.expression.ExpressionConverter.replaceWithExpressionTransformer
import org.apache.gluten.substrait.`type`.StructNode
import org.apache.gluten.substrait.expression._

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.types.{IntegerType, LongType}

import java.lang.{Integer => JInteger, Long => JLong}
import java.util.{ArrayList => JArrayList, HashMap => JHashMap}

import scala.language.existentials

case class VeloxAliasTransformer(
    substraitExprName: String,
    child: ExpressionTransformer,
    original: Expression)
  extends UnaryExpressionTransformer {

  override def doTransform(args: java.lang.Object): ExpressionNode = {
    child.doTransform(args)
  }
}

case class VeloxNamedStructTransformer(
    substraitExprName: String,
    original: CreateNamedStruct,
    attributeSeq: Seq[Attribute])
  extends ExpressionTransformer {
  override def children: Seq[ExpressionTransformer] = {
    original.valExprs.map(replaceWithExpressionTransformer(_, attributeSeq))
  }
}

case class VeloxGetStructFieldTransformer(
    substraitExprName: String,
    child: ExpressionTransformer,
    ordinal: Int,
    original: GetStructField)
  extends BinaryExpressionTransformer {
  override def left: ExpressionTransformer = child
  override def right: ExpressionTransformer = LiteralTransformer(ordinal)
  override def doTransform(args: Object): ExpressionNode = {
    val childNode = child.doTransform(args)
    childNode match {
      case node: StructLiteralNode =>
        node.getFieldLiteral(ordinal)
      case node: SelectionNode =>
        // Append the nested index to selection node.
        node.addNestedChildIdx(JInteger.valueOf(ordinal))
      case node: NullLiteralNode =>
        val nodeType =
          node.getTypeNode.asInstanceOf[StructNode].getFieldTypes.get(ordinal)
        ExpressionBuilder.makeNullLiteral(nodeType)
      case _ =>
        super.doTransform(args)
    }
  }
}

case class VeloxHashExpressionTransformer(
    substraitExprName: String,
    children: Seq[ExpressionTransformer],
    original: HashExpression[_])
  extends ExpressionTransformer {

  override def doTransform(args: java.lang.Object): ExpressionNode = {
    // As of Spark 3.3, there are 3 kinds of HashExpression.
    // HiveHash is not supported in native backend and will fail native validation.
    val (seedNode, seedType) = original match {
      case XxHash64(_, seed) =>
        (ExpressionBuilder.makeLongLiteral(seed), LongType)
      case Murmur3Hash(_, seed) =>
        (ExpressionBuilder.makeIntLiteral(seed), IntegerType)
      case HiveHash(_) =>
        (ExpressionBuilder.makeIntLiteral(0), IntegerType)
    }
    val nodes = new JArrayList[ExpressionNode]()
    // Seed as the first argument
    nodes.add(seedNode)
    children.foreach(
      expression => {
        nodes.add(expression.doTransform(args))
      })
    val childrenTypes = seedType +: original.children.map(child => child.dataType)
    val functionMap = args.asInstanceOf[JHashMap[String, JLong]]
    val functionName =
      ConverterUtils.makeFuncName(substraitExprName, childrenTypes, FunctionConfig.OPT)
    val functionId = ExpressionBuilder.newScalarFunction(functionMap, functionName)
    val typeNode = ConverterUtils.getTypeNode(original.dataType, original.nullable)
    ExpressionBuilder.makeScalarFunction(functionId, nodes, typeNode)
  }
}
