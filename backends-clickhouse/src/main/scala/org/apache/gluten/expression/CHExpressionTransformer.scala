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

import org.apache.gluten.backendsapi.clickhouse.CHConfig
import org.apache.gluten.exception.GlutenNotSupportException
import org.apache.gluten.expression.ConverterUtils.FunctionConfig
import org.apache.gluten.substrait.expression._

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

import com.google.common.collect.Lists

import java.util.Locale

case class CHTruncTimestampTransformer(
    substraitExprName: String,
    format: ExpressionTransformer,
    timestamp: ExpressionTransformer,
    timeZoneId: Option[String] = None,
    original: TruncTimestamp)
  extends ExpressionTransformer {
  override def children: Seq[ExpressionTransformer] = format :: timestamp :: Nil

  override def doTransform(args: java.lang.Object): ExpressionNode = {
    // The format must be constant string in the function date_trunc of ch.
    if (!original.format.foldable) {
      throw new GlutenNotSupportException(s"The format ${original.format} must be constant string.")
    }

    val formatStr = original.format.eval().asInstanceOf[UTF8String]
    if (formatStr == null) {
      throw new GlutenNotSupportException("The format is null.")
    }

    val (newFormatStr, timeZoneIgnore) = formatStr.toString.toLowerCase(Locale.ROOT) match {
      case "second" => ("second", false)
      case "minute" => ("minute", false)
      case "hour" => ("hour", false)
      case "day" | "dd" => ("day", false)
      case "week" => ("week", true)
      case "mon" | "month" | "mm" => ("month", true)
      case "quarter" => ("quarter", true)
      case "year" | "yyyy" | "yy" => ("year", true)
      // Can not support now.
      // case "microsecond" => "microsecond"
      // case "millisecond" => "millisecond"
      case _ => throw new GlutenNotSupportException(s"The format $formatStr is invalidate.")
    }

    // Currently, data_trunc function can not support to set the specified timezone,
    // which is different with session_time_zone.
    if (
      timeZoneIgnore && timeZoneId.nonEmpty &&
      !timeZoneId.get.equalsIgnoreCase(
        SQLConf.get.getConfString(s"${CHConfig.runtimeConfig("timezone")}")
      )
    ) {
      throw new GlutenNotSupportException(
        s"It doesn't support trunc the format $newFormatStr with the specified timezone " +
          s"${timeZoneId.get}.")
    }

    val timestampNode = timestamp.doTransform(args)
    val lowerFormatNode = ExpressionBuilder.makeStringLiteral(newFormatStr)

    val functionMap = args.asInstanceOf[java.util.HashMap[String, java.lang.Long]]

    val dataTypes = if (timeZoneId.nonEmpty) {
      Seq(original.format.dataType, original.timestamp.dataType, StringType)
    } else {
      Seq(original.format.dataType, original.timestamp.dataType)
    }

    val functionId = ExpressionBuilder.newScalarFunction(
      functionMap,
      ConverterUtils.makeFuncName(substraitExprName, dataTypes))

    val expressionNodes = new java.util.ArrayList[ExpressionNode]()
    expressionNodes.add(lowerFormatNode)
    expressionNodes.add(timestampNode)
    if (timeZoneId.isDefined) {
      expressionNodes.add(ExpressionBuilder.makeStringLiteral(timeZoneId.get))
    }

    val typeNode = ConverterUtils.getTypeNode(original.dataType, original.nullable)
    ExpressionBuilder.makeScalarFunction(functionId, expressionNodes, typeNode)
  }
}

case class CHStringTranslateTransformer(
    substraitExprName: String,
    srcExpr: ExpressionTransformer,
    matchingExpr: ExpressionTransformer,
    replaceExpr: ExpressionTransformer,
    original: StringTranslate)
  extends ExpressionTransformer {
  override def children: Seq[ExpressionTransformer] = srcExpr :: matchingExpr :: replaceExpr :: Nil

  override def doTransform(args: java.lang.Object): ExpressionNode = {
    // In CH, translateUTF8 requires matchingExpr and replaceExpr argument have the same length
    val matchingNode = matchingExpr.doTransform(args)
    val replaceNode = replaceExpr.doTransform(args)
    if (
      !matchingNode.isInstanceOf[StringLiteralNode] ||
      !replaceNode.isInstanceOf[StringLiteralNode]
    ) {
      throw new GlutenNotSupportException(s"$original not supported yet.")
    }

    super.doTransform(args)
  }
}

case class CHPosExplodeTransformer(
    substraitExprName: String,
    child: ExpressionTransformer,
    original: PosExplode,
    attributeSeq: Seq[Attribute])
  extends UnaryExpressionTransformer {

  override def doTransform(args: java.lang.Object): ExpressionNode = {
    val childNode: ExpressionNode = child.doTransform(args)
    val funcMap = args.asInstanceOf[java.util.HashMap[String, java.lang.Long]]
    val funcId = ExpressionBuilder.newScalarFunction(
      funcMap,
      ConverterUtils.makeFuncName(
        ExpressionNames.POSEXPLODE,
        Seq(original.child.dataType),
        FunctionConfig.OPT))
    val childType = original.child.dataType
    childType match {
      case a: ArrayType =>
        // Output (pos, col) when input is array type
        val structType = StructType(
          Array(
            StructField("pos", IntegerType, nullable = false),
            StructField("col", a.elementType, a.containsNull)))
        ExpressionBuilder.makeScalarFunction(
          funcId,
          Lists.newArrayList(childNode),
          ConverterUtils.getTypeNode(structType, nullable = false))
      case m: MapType =>
        // Output (pos, key, value) when input is map type
        val structType = StructType(
          Array(
            StructField("pos", IntegerType, nullable = false),
            StructField("key", m.keyType, nullable = false),
            StructField("value", m.valueType, m.valueContainsNull)))
        ExpressionBuilder.makeScalarFunction(
          funcId,
          Lists.newArrayList(childNode),
          ConverterUtils.getTypeNode(structType, nullable = false))
      case _ =>
        throw new GlutenNotSupportException(s"posexplode($childType) not supported yet.")
    }
  }
}

case class CHRegExpReplaceTransformer(
    substraitExprName: String,
    childrenWithPos: Seq[ExpressionTransformer],
    original: RegExpReplace)
  extends ExpressionTransformer {
  override def children: Seq[ExpressionTransformer] = childrenWithPos.dropRight(1)

  override def doTransform(args: java.lang.Object): ExpressionNode = {
    // In CH: replaceRegexpAll(subject, regexp, rep), which is equivalent
    // In Spark: regexp_replace(subject, regexp, rep, pos=1)
    val posNode = childrenWithPos(3).doTransform(args)
    if (
      !posNode.isInstanceOf[IntLiteralNode] ||
      posNode.asInstanceOf[IntLiteralNode].getValue != 1
    ) {
      throw new UnsupportedOperationException(s"$original dose not supported position yet.")
    }
    // Replace $num in rep with \num used in CH
    val repNode = childrenWithPos(2).doTransform(args)
    repNode match {
      case node: StringLiteralNode =>
        val strValue = node.getValue
        val replacedValue = strValue.replaceAll("\\$(\\d+)", "\\\\$1")
        if (replacedValue != strValue) {
          val functionName = ConverterUtils.makeFuncName(
            substraitExprName,
            Seq(original.subject.dataType, original.regexp.dataType, original.rep.dataType),
            FunctionConfig.OPT)
          val replacedRepNode = ExpressionBuilder.makeLiteral(replacedValue, StringType, false)
          val exprNodes = Lists.newArrayList(
            childrenWithPos(0).doTransform(args),
            childrenWithPos(1).doTransform(args),
            replacedRepNode)
          val functionMap = args.asInstanceOf[java.util.HashMap[String, java.lang.Long]]
          return ExpressionBuilder.makeScalarFunction(
            ExpressionBuilder.newScalarFunction(functionMap, functionName),
            exprNodes,
            ConverterUtils.getTypeNode(original.dataType, original.nullable))
        }
      case _ =>
    }

    super.doTransform(args)
  }
}

case class GetArrayItemTransformer(
    substraitExprName: String,
    left: ExpressionTransformer,
    right: ExpressionTransformer,
    original: Expression)
  extends BinaryExpressionTransformer {

  override def doTransform(args: java.lang.Object): ExpressionNode = {
    // Ignore failOnError for clickhouse backend
    val functionMap = args.asInstanceOf[java.util.HashMap[String, java.lang.Long]]
    val leftNode = left.doTransform(args)
    var rightNode = right.doTransform(args)

    val getArrayItem = original.asInstanceOf[GetArrayItem]

    // In Spark, the index of getarrayitem starts from 0
    // But in CH, the index of arrayElement starts from 1, besides index argument must
    // So we need to do transform: rightNode = add(rightNode, 1)
    val addFunctionName = ConverterUtils.makeFuncName(
      ExpressionNames.ADD,
      Seq(IntegerType, getArrayItem.right.dataType),
      FunctionConfig.OPT)
    val addFunctionId = ExpressionBuilder.newScalarFunction(functionMap, addFunctionName)
    val literalNode = ExpressionBuilder.makeLiteral(1, IntegerType, false)
    rightNode = ExpressionBuilder.makeScalarFunction(
      addFunctionId,
      Lists.newArrayList(literalNode, rightNode),
      ConverterUtils.getTypeNode(getArrayItem.right.dataType, getArrayItem.right.nullable))

    val functionName = ConverterUtils.makeFuncName(
      substraitExprName,
      Seq(getArrayItem.left.dataType, getArrayItem.right.dataType),
      FunctionConfig.OPT)
    val exprNodes = Lists.newArrayList(leftNode, rightNode)
    ExpressionBuilder.makeScalarFunction(
      ExpressionBuilder.newScalarFunction(functionMap, functionName),
      exprNodes,
      ConverterUtils.getTypeNode(getArrayItem.dataType, getArrayItem.nullable))
  }
}

case class CHStringSplitTransformer(
    substraitExprName: String,
    children: Seq[ExpressionTransformer],
    original: Expression,
    override val dataType: DataType = ArrayType(StringType, containsNull = true))
  extends ExpressionTransformer {
  // In Spark: split return Array(String), while Array is nullable
  // In CH: splitByXXX return Array(Nullable(String))
}
