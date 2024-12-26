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

import org.apache.gluten.backendsapi.BackendsApiManager
import org.apache.gluten.exception.GlutenNotSupportException
import org.apache.gluten.sql.shims.SparkShimLoader
import org.apache.gluten.test.TestStats
import org.apache.gluten.utils.DecimalArithmeticUtil

import org.apache.spark.{SPARK_REVISION, SPARK_VERSION_SHORT}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.SQLConfHelper
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.objects.StaticInvoke
import org.apache.spark.sql.catalyst.optimizer.NormalizeNaNAndZero
import org.apache.spark.sql.execution.ScalarSubquery
import org.apache.spark.sql.hive.HiveUDFTransformer
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._

import scala.collection.mutable.ArrayBuffer

trait Transformable {
  def getTransformer(childrenTransformers: Seq[ExpressionTransformer]): ExpressionTransformer
}

object ExpressionConverter extends SQLConfHelper with Logging {

  def replaceWithExpressionTransformer(
      exprs: Seq[Expression],
      attributeSeq: Seq[Attribute]): Seq[ExpressionTransformer] = {
    val expressionsMap = ExpressionMappings.expressionsMap
    exprs.map(expr => replaceWithExpressionTransformer0(expr, attributeSeq, expressionsMap))
  }

  def replaceWithExpressionTransformer(
      expr: Expression,
      attributeSeq: Seq[Attribute]): ExpressionTransformer = {
    val expressionsMap = ExpressionMappings.expressionsMap
    replaceWithExpressionTransformer0(expr, attributeSeq, expressionsMap)
  }

  def canReplaceWithExpressionTransformer(
      expr: Expression,
      attributeSeq: Seq[Attribute]): Boolean = {
    try {
      replaceWithExpressionTransformer(expr, attributeSeq)
      true
    } catch {
      case e: Exception =>
        logInfo(e.getMessage)
        false
    }
  }

  def replaceAttributeReference(expr: Expression): Expression = expr match {
    case ar: AttributeReference if ar.dataType == BooleanType =>
      EqualNullSafe(ar, Literal.TrueLiteral)
    case e => e
  }

  private def replacePythonUDFWithExpressionTransformer(
      udf: PythonUDF,
      attributeSeq: Seq[Attribute],
      expressionsMap: Map[Class[_], String]): ExpressionTransformer = {
    val substraitExprName = UDFMappings.pythonUDFMap.get(udf.name)
    substraitExprName match {
      case Some(name) =>
        GenericExpressionTransformer(
          name,
          udf.children.map(replaceWithExpressionTransformer0(_, attributeSeq, expressionsMap)),
          udf)
      case _ =>
        throw new GlutenNotSupportException(s"Not supported python udf: $udf.")
    }
  }

  private def replaceScalaUDFWithExpressionTransformer(
      udf: ScalaUDF,
      attributeSeq: Seq[Attribute],
      expressionsMap: Map[Class[_], String]): ExpressionTransformer = {
    if (udf.udfName.isEmpty) {
      throw new GlutenNotSupportException("UDF name is not found!")
    }
    val substraitExprName = UDFMappings.scalaUDFMap.get(udf.udfName.get)
    substraitExprName match {
      case Some(name) =>
        GenericExpressionTransformer(
          name,
          udf.children.map(replaceWithExpressionTransformer0(_, attributeSeq, expressionsMap)),
          udf)
      case _ =>
        throw new GlutenNotSupportException(s"Not supported scala udf: $udf.")
    }
  }

  private def genRescaleDecimalTransformer(
      substraitName: String,
      b: BinaryArithmetic,
      attributeSeq: Seq[Attribute],
      expressionsMap: Map[Class[_], String]): DecimalArithmeticExpressionTransformer = {
    val rescaleBinary = DecimalArithmeticUtil.rescaleLiteral(b)
    val (left, right) = DecimalArithmeticUtil.rescaleCastForDecimal(
      DecimalArithmeticUtil.removeCastForDecimal(rescaleBinary.left),
      DecimalArithmeticUtil.removeCastForDecimal(rescaleBinary.right))
    val resultType = DecimalArithmeticUtil.getResultType(
      b,
      left.dataType.asInstanceOf[DecimalType],
      right.dataType.asInstanceOf[DecimalType]
    )

    val leftChild =
      replaceWithExpressionTransformer0(left, attributeSeq, expressionsMap)
    val rightChild =
      replaceWithExpressionTransformer0(right, attributeSeq, expressionsMap)
    DecimalArithmeticExpressionTransformer(substraitName, leftChild, rightChild, resultType, b)
  }

  private def replaceWithExpressionTransformer0(
      expr: Expression,
      attributeSeq: Seq[Attribute],
      expressionsMap: Map[Class[_], String]): ExpressionTransformer = {
    logDebug(
      s"replaceWithExpressionTransformer expr: $expr class: ${expr.getClass} " +
        s"name: ${expr.prettyName}")

    expr match {
      case p: PythonUDF =>
        return replacePythonUDFWithExpressionTransformer(p, attributeSeq, expressionsMap)
      case s: ScalaUDF =>
        return replaceScalaUDFWithExpressionTransformer(s, attributeSeq, expressionsMap)
      case _ if HiveUDFTransformer.isHiveUDF(expr) =>
        return BackendsApiManager.getSparkPlanExecApiInstance.genHiveUDFTransformer(
          expr,
          attributeSeq)
      case i: StaticInvoke =>
        val objectName = i.staticObject.getName.stripSuffix("$")
        if (objectName.endsWith("UrlCodec")) {
          val child = i.arguments.head
          i.functionName match {
            case "decode" =>
              return GenericExpressionTransformer(
                ExpressionNames.URL_DECODE,
                child.map(replaceWithExpressionTransformer0(_, attributeSeq, expressionsMap)),
                i)
            case "encode" =>
              return GenericExpressionTransformer(
                ExpressionNames.URL_ENCODE,
                child.map(replaceWithExpressionTransformer0(_, attributeSeq, expressionsMap)),
                i)
          }
        }
      case _ =>
    }

    val substraitExprName: String = getAndCheckSubstraitName(expr, expressionsMap)
    val backendConverted = BackendsApiManager.getSparkPlanExecApiInstance.extraExpressionConverter(
      substraitExprName,
      expr,
      attributeSeq)
    if (backendConverted.isDefined) {
      return backendConverted.get
    }
    expr match {
      case c: CreateArray =>
        val children =
          c.children.map(replaceWithExpressionTransformer0(_, attributeSeq, expressionsMap))
        CreateArrayTransformer(substraitExprName, children, c)
      case g: GetArrayItem =>
        BackendsApiManager.getSparkPlanExecApiInstance.genGetArrayItemTransformer(
          substraitExprName,
          replaceWithExpressionTransformer0(g.left, attributeSeq, expressionsMap),
          replaceWithExpressionTransformer0(g.right, attributeSeq, expressionsMap),
          g
        )
      case c: CreateMap =>
        val children =
          c.children.map(replaceWithExpressionTransformer0(_, attributeSeq, expressionsMap))
        CreateMapTransformer(substraitExprName, children, c)
      case g: GetMapValue =>
        BackendsApiManager.getSparkPlanExecApiInstance.genGetMapValueTransformer(
          substraitExprName,
          replaceWithExpressionTransformer0(g.child, attributeSeq, expressionsMap),
          replaceWithExpressionTransformer0(g.key, attributeSeq, expressionsMap),
          g
        )
      case m: MapEntries =>
        BackendsApiManager.getSparkPlanExecApiInstance.genMapEntriesTransformer(
          substraitExprName,
          replaceWithExpressionTransformer0(m.child, attributeSeq, expressionsMap),
          m)
      case e: Explode =>
        ExplodeTransformer(
          substraitExprName,
          replaceWithExpressionTransformer0(e.child, attributeSeq, expressionsMap),
          e)
      case p: PosExplode =>
        BackendsApiManager.getSparkPlanExecApiInstance.genPosExplodeTransformer(
          substraitExprName,
          replaceWithExpressionTransformer0(p.child, attributeSeq, expressionsMap),
          p,
          attributeSeq)
      case i: Inline =>
        BackendsApiManager.getSparkPlanExecApiInstance.genInlineTransformer(
          substraitExprName,
          replaceWithExpressionTransformer0(i.child, attributeSeq, expressionsMap),
          i)
      case a: Alias =>
        BackendsApiManager.getSparkPlanExecApiInstance.genAliasTransformer(
          substraitExprName,
          replaceWithExpressionTransformer0(a.child, attributeSeq, expressionsMap),
          a)
      case a: AttributeReference =>
        if (attributeSeq == null) {
          throw new UnsupportedOperationException(s"attributeSeq should not be null.")
        }
        try {
          val bindReference =
            BindReferences.bindReference(expr, attributeSeq, allowFailures = false)
          val b = bindReference.asInstanceOf[BoundReference]
          AttributeReferenceTransformer(substraitExprName, a, b)
        } catch {
          case e: IllegalStateException =>
            // This situation may need developers to fix, although we just throw the below
            // exception to let the corresponding operator fall back.
            throw new UnsupportedOperationException(
              s"Failed to bind reference for $expr: ${e.getMessage}")
        }
      case b: BoundReference =>
        BoundReferenceTransformer(substraitExprName, b)
      case l: Literal =>
        LiteralTransformer(l)
      case d: DateDiff =>
        BackendsApiManager.getSparkPlanExecApiInstance.genDateDiffTransformer(
          substraitExprName,
          replaceWithExpressionTransformer0(d.endDate, attributeSeq, expressionsMap),
          replaceWithExpressionTransformer0(d.startDate, attributeSeq, expressionsMap),
          d
        )
      case r: Round if r.child.dataType.isInstanceOf[DecimalType] =>
        DecimalRoundTransformer(
          substraitExprName,
          replaceWithExpressionTransformer0(r.child, attributeSeq, expressionsMap),
          r)
      case t: ToUnixTimestamp =>
        // The failOnError depends on the config for ANSI. ANSI is not supported currently.
        // And timeZoneId is passed to backend config.
        GenericExpressionTransformer(
          substraitExprName,
          Seq(
            replaceWithExpressionTransformer0(t.timeExp, attributeSeq, expressionsMap),
            replaceWithExpressionTransformer0(t.format, attributeSeq, expressionsMap)
          ),
          t
        )
      case u: UnixTimestamp =>
        GenericExpressionTransformer(
          substraitExprName,
          Seq(
            replaceWithExpressionTransformer0(u.timeExp, attributeSeq, expressionsMap),
            replaceWithExpressionTransformer0(u.format, attributeSeq, expressionsMap)
          ),
          ToUnixTimestamp(u.timeExp, u.format, u.timeZoneId, u.failOnError)
        )
      case t: TruncTimestamp =>
        BackendsApiManager.getSparkPlanExecApiInstance.genTruncTimestampTransformer(
          substraitExprName,
          replaceWithExpressionTransformer0(t.format, attributeSeq, expressionsMap),
          replaceWithExpressionTransformer0(t.timestamp, attributeSeq, expressionsMap),
          t.timeZoneId,
          t
        )
      case m: MonthsBetween =>
        MonthsBetweenTransformer(
          substraitExprName,
          replaceWithExpressionTransformer0(m.date1, attributeSeq, expressionsMap),
          replaceWithExpressionTransformer0(m.date2, attributeSeq, expressionsMap),
          replaceWithExpressionTransformer0(m.roundOff, attributeSeq, expressionsMap),
          m
        )
      case i: If =>
        IfTransformer(
          substraitExprName,
          replaceWithExpressionTransformer0(i.predicate, attributeSeq, expressionsMap),
          replaceWithExpressionTransformer0(i.trueValue, attributeSeq, expressionsMap),
          replaceWithExpressionTransformer0(i.falseValue, attributeSeq, expressionsMap),
          i
        )
      case cw: CaseWhen =>
        CaseWhenTransformer(
          substraitExprName,
          cw.branches.map {
            expr =>
              {
                (
                  replaceWithExpressionTransformer0(expr._1, attributeSeq, expressionsMap),
                  replaceWithExpressionTransformer0(expr._2, attributeSeq, expressionsMap))
              }
          },
          cw.elseValue.map {
            expr =>
              {
                replaceWithExpressionTransformer0(expr, attributeSeq, expressionsMap)
              }
          },
          cw
        )
      case i: In =>
        if (i.list.exists(!_.foldable)) {
          throw new GlutenNotSupportException(
            s"In list option does not support non-foldable expression, ${i.list.map(_.sql)}")
        }
        InTransformer(
          substraitExprName,
          replaceWithExpressionTransformer0(i.value, attributeSeq, expressionsMap),
          i)
      case i: InSet =>
        InSetTransformer(
          substraitExprName,
          replaceWithExpressionTransformer0(i.child, attributeSeq, expressionsMap),
          i)
      case s: ScalarSubquery =>
        ScalarSubqueryTransformer(substraitExprName, s)
      case c: Cast =>
        // Add trim node, as necessary.
        val newCast =
          BackendsApiManager.getSparkPlanExecApiInstance.genCastWithNewChild(c)
        CastTransformer(
          substraitExprName,
          replaceWithExpressionTransformer0(newCast.child, attributeSeq, expressionsMap),
          newCast)
      case s: String2TrimExpression =>
        val (srcStr, trimStr) = s match {
          case StringTrim(srcStr, trimStr) => (srcStr, trimStr)
          case StringTrimLeft(srcStr, trimStr) => (srcStr, trimStr)
          case StringTrimRight(srcStr, trimStr) => (srcStr, trimStr)
        }
        val children = trimStr
          .map(replaceWithExpressionTransformer0(_, attributeSeq, expressionsMap))
          .toSeq ++
          Seq(replaceWithExpressionTransformer0(srcStr, attributeSeq, expressionsMap))
        GenericExpressionTransformer(
          substraitExprName,
          children,
          s
        )
      case m: HashExpression[_] =>
        BackendsApiManager.getSparkPlanExecApiInstance.genHashExpressionTransformer(
          substraitExprName,
          m.children.map(
            expr => replaceWithExpressionTransformer0(expr, attributeSeq, expressionsMap)),
          m)
      case getStructField: GetStructField =>
        try {
          val bindRef =
            bindGetStructField(getStructField, attributeSeq)
          // Different backends may have different result.
          BackendsApiManager.getSparkPlanExecApiInstance.genGetStructFieldTransformer(
            substraitExprName,
            replaceWithExpressionTransformer0(getStructField.child, attributeSeq, expressionsMap),
            bindRef.ordinal,
            getStructField)
        } catch {
          case e: IllegalStateException =>
            // This situation may need developers to fix, although we just throw the below
            // exception to let the corresponding operator fall back.
            throw new UnsupportedOperationException(
              s"Failed to bind reference for $getStructField: ${e.getMessage}")
        }

      case getArrayStructFields: GetArrayStructFields =>
        GenericExpressionTransformer(
          substraitExprName,
          Seq(
            replaceWithExpressionTransformer0(
              getArrayStructFields.child,
              attributeSeq,
              expressionsMap),
            LiteralTransformer(getArrayStructFields.ordinal)),
          getArrayStructFields
        )
      case t: StringTranslate =>
        BackendsApiManager.getSparkPlanExecApiInstance.genStringTranslateTransformer(
          substraitExprName,
          replaceWithExpressionTransformer0(t.srcExpr, attributeSeq, expressionsMap),
          replaceWithExpressionTransformer0(t.matchingExpr, attributeSeq, expressionsMap),
          replaceWithExpressionTransformer0(t.replaceExpr, attributeSeq, expressionsMap),
          t
        )
      case r: RegExpReplace =>
        BackendsApiManager.getSparkPlanExecApiInstance.genRegexpReplaceTransformer(
          substraitExprName,
          Seq(
            replaceWithExpressionTransformer0(r.subject, attributeSeq, expressionsMap),
            replaceWithExpressionTransformer0(r.regexp, attributeSeq, expressionsMap),
            replaceWithExpressionTransformer0(r.rep, attributeSeq, expressionsMap),
            replaceWithExpressionTransformer0(r.pos, attributeSeq, expressionsMap)
          ),
          r
        )
      case size: Size =>
        // Covers Spark ArraySize which is replaced by Size(child, false).
        val child =
          replaceWithExpressionTransformer0(size.child, attributeSeq, expressionsMap)
        GenericExpressionTransformer(
          substraitExprName,
          Seq(child, LiteralTransformer(size.legacySizeOfNull)),
          size)
      case namedStruct: CreateNamedStruct =>
        BackendsApiManager.getSparkPlanExecApiInstance.genNamedStructTransformer(
          substraitExprName,
          namedStruct.children.map(
            replaceWithExpressionTransformer0(_, attributeSeq, expressionsMap)),
          namedStruct,
          attributeSeq)
      case namedLambdaVariable: NamedLambdaVariable =>
        // namedlambdavariable('acc')-> <Integer, notnull>
        GenericExpressionTransformer(
          substraitExprName,
          LiteralTransformer(namedLambdaVariable.name),
          namedLambdaVariable
        )
      case lambdaFunction: LambdaFunction =>
        LambdaFunctionTransformer(
          substraitExprName,
          function = replaceWithExpressionTransformer0(
            lambdaFunction.function,
            attributeSeq,
            expressionsMap),
          arguments = lambdaFunction.arguments.map(
            replaceWithExpressionTransformer0(_, attributeSeq, expressionsMap)),
          original = lambdaFunction
        )
      case j: JsonTuple =>
        val children =
          j.children.map(replaceWithExpressionTransformer0(_, attributeSeq, expressionsMap))
        JsonTupleExpressionTransformer(substraitExprName, children, j)
      case l: Like =>
        BackendsApiManager.getSparkPlanExecApiInstance.genLikeTransformer(
          substraitExprName,
          replaceWithExpressionTransformer0(l.left, attributeSeq, expressionsMap),
          replaceWithExpressionTransformer0(l.right, attributeSeq, expressionsMap),
          l
        )
      case m: MakeDecimal =>
        GenericExpressionTransformer(
          substraitExprName,
          Seq(
            replaceWithExpressionTransformer0(m.child, attributeSeq, expressionsMap),
            LiteralTransformer(m.nullOnOverflow)),
          m
        )
      case PromotePrecision(_ @Cast(child, _: DecimalType, _, _))
          if child.dataType
            .isInstanceOf[DecimalType] && !BackendsApiManager.getSettings.transformCheckOverflow =>
        replaceWithExpressionTransformer0(child, attributeSeq, expressionsMap)
      case _: NormalizeNaNAndZero | _: PromotePrecision | _: TaggingExpression |
          _: DynamicPruningExpression =>
        ChildTransformer(
          substraitExprName,
          replaceWithExpressionTransformer0(expr.children.head, attributeSeq, expressionsMap),
          expr
        )
      case _: GetDateField | _: GetTimeField =>
        ExtractDateTransformer(
          substraitExprName,
          replaceWithExpressionTransformer0(expr.children.head, attributeSeq, expressionsMap),
          expr)
      case _: StringToMap =>
        BackendsApiManager.getSparkPlanExecApiInstance.genStringToMapTransformer(
          substraitExprName,
          expr.children.map(replaceWithExpressionTransformer0(_, attributeSeq, expressionsMap)),
          expr)
      case CheckOverflow(b: BinaryArithmetic, decimalType, _)
          if !BackendsApiManager.getSettings.transformCheckOverflow &&
            DecimalArithmeticUtil.isDecimalArithmetic(b) =>
        DecimalArithmeticUtil.checkAllowDecimalArithmetic()
        val arithmeticExprName = getAndCheckSubstraitName(b, expressionsMap)
        val left =
          replaceWithExpressionTransformer0(b.left, attributeSeq, expressionsMap)
        val right =
          replaceWithExpressionTransformer0(b.right, attributeSeq, expressionsMap)
        DecimalArithmeticExpressionTransformer(arithmeticExprName, left, right, decimalType, b)
      case c: CheckOverflow =>
        CheckOverflowTransformer(
          substraitExprName,
          replaceWithExpressionTransformer0(c.child, attributeSeq, expressionsMap),
          c)
      case c if c.getClass.getSimpleName.equals("CheckOverflowInTableInsert") =>
        throw new GlutenNotSupportException(
          "CheckOverflowInTableInsert is used in ANSI mode, but Gluten does not support ANSI mode."
        )
      case b: BinaryArithmetic if DecimalArithmeticUtil.isDecimalArithmetic(b) =>
        DecimalArithmeticUtil.checkAllowDecimalArithmetic()
        if (!BackendsApiManager.getSettings.transformCheckOverflow) {
          GenericExpressionTransformer(
            substraitExprName,
            expr.children.map(replaceWithExpressionTransformer0(_, attributeSeq, expressionsMap)),
            expr
          )
        } else {
          // Without the rescale and remove cast, result is right for high version Spark,
          // but performance regression in velox
          genRescaleDecimalTransformer(substraitExprName, b, attributeSeq, expressionsMap)
        }
      case n: NaNvl =>
        BackendsApiManager.getSparkPlanExecApiInstance.genNaNvlTransformer(
          substraitExprName,
          replaceWithExpressionTransformer0(n.left, attributeSeq, expressionsMap),
          replaceWithExpressionTransformer0(n.right, attributeSeq, expressionsMap),
          n
        )
      case a: AtLeastNNonNulls =>
        BackendsApiManager.getSparkPlanExecApiInstance.genAtLeastNNonNullsTransformer(
          substraitExprName,
          a.children.map(replaceWithExpressionTransformer0(_, attributeSeq, expressionsMap)),
          a
        )
      case m: MakeTimestamp =>
        BackendsApiManager.getSparkPlanExecApiInstance.genMakeTimestampTransformer(
          substraitExprName,
          m.children.map(replaceWithExpressionTransformer0(_, attributeSeq, expressionsMap)),
          m)
      case timestampAdd if timestampAdd.getClass.getSimpleName.equals("TimestampAdd") =>
        // for spark3.3
        val extract = SparkShimLoader.getSparkShims.extractExpressionTimestampAddUnit(timestampAdd)
        if (extract.isEmpty) {
          throw new UnsupportedOperationException(s"Not support expression TimestampAdd.")
        }
        val add = timestampAdd.asInstanceOf[BinaryExpression]
        TimestampAddTransformer(
          substraitExprName,
          extract.get.head,
          replaceWithExpressionTransformer0(add.left, attributeSeq, expressionsMap),
          replaceWithExpressionTransformer0(add.right, attributeSeq, expressionsMap),
          extract.get.last,
          add
        )
      case e: Transformable =>
        val childrenTransformers =
          e.children.map(replaceWithExpressionTransformer0(_, attributeSeq, expressionsMap))
        e.getTransformer(childrenTransformers)
      case u: Uuid =>
        BackendsApiManager.getSparkPlanExecApiInstance.genUuidTransformer(substraitExprName, u)
      case f: ArrayFilter =>
        BackendsApiManager.getSparkPlanExecApiInstance.genArrayFilterTransformer(
          substraitExprName,
          replaceWithExpressionTransformer0(f.argument, attributeSeq, expressionsMap),
          replaceWithExpressionTransformer0(f.function, attributeSeq, expressionsMap),
          f
        )
      case arrayTransform: ArrayTransform =>
        BackendsApiManager.getSparkPlanExecApiInstance.genArrayTransformTransformer(
          substraitExprName,
          replaceWithExpressionTransformer0(arrayTransform.argument, attributeSeq, expressionsMap),
          replaceWithExpressionTransformer0(arrayTransform.function, attributeSeq, expressionsMap),
          arrayTransform
        )
      case arraySort: ArraySort =>
        BackendsApiManager.getSparkPlanExecApiInstance.genArraySortTransformer(
          substraitExprName,
          replaceWithExpressionTransformer0(arraySort.argument, attributeSeq, expressionsMap),
          replaceWithExpressionTransformer0(arraySort.function, attributeSeq, expressionsMap),
          arraySort
        )
      case tryEval @ TryEval(a: Add) =>
        BackendsApiManager.getSparkPlanExecApiInstance.genTryArithmeticTransformer(
          substraitExprName,
          replaceWithExpressionTransformer0(a.left, attributeSeq, expressionsMap),
          replaceWithExpressionTransformer0(a.right, attributeSeq, expressionsMap),
          tryEval,
          ExpressionNames.CHECKED_ADD
        )
      case tryEval @ TryEval(a: Subtract) =>
        BackendsApiManager.getSparkPlanExecApiInstance.genTryArithmeticTransformer(
          substraitExprName,
          replaceWithExpressionTransformer0(a.left, attributeSeq, expressionsMap),
          replaceWithExpressionTransformer0(a.right, attributeSeq, expressionsMap),
          tryEval,
          ExpressionNames.CHECKED_SUBTRACT
        )
      case tryEval @ TryEval(a: Divide) =>
        BackendsApiManager.getSparkPlanExecApiInstance.genTryArithmeticTransformer(
          substraitExprName,
          replaceWithExpressionTransformer0(a.left, attributeSeq, expressionsMap),
          replaceWithExpressionTransformer0(a.right, attributeSeq, expressionsMap),
          tryEval,
          ExpressionNames.CHECKED_DIVIDE
        )
      case tryEval @ TryEval(a: Multiply) =>
        BackendsApiManager.getSparkPlanExecApiInstance.genTryArithmeticTransformer(
          substraitExprName,
          replaceWithExpressionTransformer0(a.left, attributeSeq, expressionsMap),
          replaceWithExpressionTransformer0(a.right, attributeSeq, expressionsMap),
          tryEval,
          ExpressionNames.CHECKED_MULTIPLY
        )
      case a: Add =>
        BackendsApiManager.getSparkPlanExecApiInstance.genArithmeticTransformer(
          substraitExprName,
          replaceWithExpressionTransformer0(a.left, attributeSeq, expressionsMap),
          replaceWithExpressionTransformer0(a.right, attributeSeq, expressionsMap),
          a,
          ExpressionNames.CHECKED_ADD
        )
      case a: Subtract =>
        BackendsApiManager.getSparkPlanExecApiInstance.genArithmeticTransformer(
          substraitExprName,
          replaceWithExpressionTransformer0(a.left, attributeSeq, expressionsMap),
          replaceWithExpressionTransformer0(a.right, attributeSeq, expressionsMap),
          a,
          ExpressionNames.CHECKED_SUBTRACT
        )
      case a: Multiply =>
        BackendsApiManager.getSparkPlanExecApiInstance.genArithmeticTransformer(
          substraitExprName,
          replaceWithExpressionTransformer0(a.left, attributeSeq, expressionsMap),
          replaceWithExpressionTransformer0(a.right, attributeSeq, expressionsMap),
          a,
          ExpressionNames.CHECKED_MULTIPLY
        )
      case a: Divide =>
        BackendsApiManager.getSparkPlanExecApiInstance.genArithmeticTransformer(
          substraitExprName,
          replaceWithExpressionTransformer0(a.left, attributeSeq, expressionsMap),
          replaceWithExpressionTransformer0(a.right, attributeSeq, expressionsMap),
          a,
          ExpressionNames.CHECKED_DIVIDE
        )
      case tryEval: TryEval =>
        // This is a placeholder to handle try_eval(other expressions).
        BackendsApiManager.getSparkPlanExecApiInstance.genTryEvalTransformer(
          substraitExprName,
          replaceWithExpressionTransformer0(tryEval.child, attributeSeq, expressionsMap),
          tryEval
        )
      case a: ArrayForAll =>
        BackendsApiManager.getSparkPlanExecApiInstance.genArrayForAllTransformer(
          substraitExprName,
          replaceWithExpressionTransformer0(a.argument, attributeSeq, expressionsMap),
          replaceWithExpressionTransformer0(a.function, attributeSeq, expressionsMap),
          a
        )
      case a: ArrayExists =>
        BackendsApiManager.getSparkPlanExecApiInstance.genArrayExistsTransformer(
          substraitExprName,
          replaceWithExpressionTransformer0(a.argument, attributeSeq, expressionsMap),
          replaceWithExpressionTransformer0(a.function, attributeSeq, expressionsMap),
          a
        )
      case arrayInsert if arrayInsert.getClass.getSimpleName.equals("ArrayInsert") =>
        // Since spark 3.4.0
        val children = SparkShimLoader.getSparkShims.extractExpressionArrayInsert(arrayInsert)
        GenericExpressionTransformer(
          substraitExprName,
          children.map(replaceWithExpressionTransformer0(_, attributeSeq, expressionsMap)),
          arrayInsert
        )
      case s: Shuffle =>
        GenericExpressionTransformer(
          substraitExprName,
          Seq(
            replaceWithExpressionTransformer0(s.child, attributeSeq, expressionsMap),
            LiteralTransformer(Literal(s.randomSeed.get))),
          s)
      case c: PreciseTimestampConversion =>
        BackendsApiManager.getSparkPlanExecApiInstance.genPreciseTimestampConversionTransformer(
          substraitExprName,
          Seq(replaceWithExpressionTransformer0(c.child, attributeSeq, expressionsMap)),
          c
        )
      case t: TransformKeys =>
        // default is `EXCEPTION`
        val mapKeyDedupPolicy = SQLConf.get.getConf(SQLConf.MAP_KEY_DEDUP_POLICY)
        if (mapKeyDedupPolicy == SQLConf.MapKeyDedupPolicy.LAST_WIN.toString) {
          // TODO: Remove after fix ready for
          //  https://github.com/facebookincubator/velox/issues/10219
          throw new GlutenNotSupportException(
            "LAST_WIN policy is not supported yet in native to deduplicate map keys"
          )
        }
        GenericExpressionTransformer(
          substraitExprName,
          t.children.map(replaceWithExpressionTransformer0(_, attributeSeq, expressionsMap)),
          t
        )
      case e: EulerNumber =>
        LiteralTransformer(Literal(Math.E))
      case p: Pi =>
        LiteralTransformer(Literal(Math.PI))
      case v: SparkVersion =>
        LiteralTransformer(SPARK_VERSION_SHORT + " " + SPARK_REVISION)
      case dateAdd: DateAdd =>
        BackendsApiManager.getSparkPlanExecApiInstance.genDateAddTransformer(
          attributeSeq,
          substraitExprName,
          dateAdd.children,
          dateAdd
        )
      case timeAdd: TimeAdd =>
        BackendsApiManager.getSparkPlanExecApiInstance.genDateAddTransformer(
          attributeSeq,
          substraitExprName,
          timeAdd.children,
          timeAdd
        )
      case ss: StringSplit =>
        BackendsApiManager.getSparkPlanExecApiInstance.genStringSplitTransformer(
          substraitExprName,
          replaceWithExpressionTransformer0(ss.str, attributeSeq, expressionsMap),
          replaceWithExpressionTransformer0(ss.regex, attributeSeq, expressionsMap),
          replaceWithExpressionTransformer0(ss.limit, attributeSeq, expressionsMap),
          ss
        )
      case expr =>
        GenericExpressionTransformer(
          substraitExprName,
          expr.children.map(replaceWithExpressionTransformer0(_, attributeSeq, expressionsMap)),
          expr
        )
    }
  }

  private def getAndCheckSubstraitName(expr: Expression, expressionsMap: Map[Class[_], String]) = {
    TestStats.addExpressionClassName(expr.getClass.getName)
    // Check whether Gluten supports this expression
    val substraitExprNameOpt = expressionsMap.get(expr.getClass)
    if (substraitExprNameOpt.isEmpty) {
      throw new GlutenNotSupportException(
        s"Not supported to map spark function name" +
          s" to substrait function name: $expr, class name: ${expr.getClass.getSimpleName}.")
    }
    val substraitExprName = substraitExprNameOpt.get
    // Check whether each backend supports this expression
    if (!BackendsApiManager.getValidatorApiInstance.doExprValidate(substraitExprName, expr)) {
      throw new GlutenNotSupportException(s"Not supported: $expr.")
    }
    substraitExprName
  }

  private def bindGetStructField(
      structField: GetStructField,
      input: AttributeSeq): BoundReference = {
    // get the new ordinal base input
    var newOrdinal: Int = -1
    val names = new ArrayBuffer[String]
    var root: Expression = structField
    while (root.isInstanceOf[GetStructField]) {
      val curField = root.asInstanceOf[GetStructField]
      val name = curField.childSchema.fields(curField.ordinal).name
      names += name
      root = root.asInstanceOf[GetStructField].child
    }
    // For map/array type, the reference is correct no matter NESTED_SCHEMA_PRUNING_ENABLED or not
    if (!root.isInstanceOf[AttributeReference]) {
      return BoundReference(structField.ordinal, structField.dataType, structField.nullable)
    }
    names += root.asInstanceOf[AttributeReference].name
    input.attrs.foreach(
      attribute => {
        var level = names.size - 1
        if (names(level) == attribute.name) {
          var candidateFields: Array[StructField] = null
          var dtType = attribute.dataType
          while (dtType.isInstanceOf[StructType] && level >= 1) {
            candidateFields = dtType.asInstanceOf[StructType].fields
            level -= 1
            val curName = names(level)
            for (i <- 0 until candidateFields.length) {
              if (candidateFields(i).name == curName) {
                dtType = candidateFields(i).dataType
                newOrdinal = i
              }
            }
          }
        }
      })
    if (newOrdinal == -1) {
      throw new IllegalStateException(
        s"Couldn't find $structField in ${input.attrs.mkString("[", ",", "]")}")
    } else {
      BoundReference(newOrdinal, structField.dataType, structField.nullable)
    }
  }
}
