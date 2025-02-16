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
package org.apache.spark.sql

import org.apache.gluten.execution.{ProjectExecTransformer, TransformSupport, WholeStageTransformerSuite}
import org.apache.gluten.utils.BackendTestUtils

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.execution.LeafExecNode
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.sql.types._

import scala.collection.mutable.Buffer

class GlutenExpressionDataTypesValidation extends WholeStageTransformerSuite {
  protected val resourcePath: String = null
  protected val fileFormat: String = null
  override protected val logLevel: String = "INFO"

  override protected def sparkConf: SparkConf = {
    val conf = super.sparkConf
      .set("spark.plugins", "org.apache.gluten.GlutenPlugin")
      .set("spark.default.parallelism", "1")
      .set("spark.memory.offHeap.enabled", "true")
      .set("spark.memory.offHeap.size", "1024MB")
      .set("spark.ui.enabled", "false")
      .set("spark.gluten.ui.enabled", "false")
    if (BackendTestUtils.isCHBackendLoaded()) {
      conf
        .set("spark.gluten.sql.enable.native.validation", "false")
    }
    conf
  }

  private case class DummyPlan() extends LeafExecNode {
    override def output: Seq[Attribute] = Seq()

    override val metrics: Map[String, SQLMetric] = Map.empty

    override def doExecute(): RDD[InternalRow] = throw new UnsupportedOperationException(
      "Just a dummy plan.")
  }

  private val allPrimitiveDataTypes: Seq[DataType] =
    Seq(
      BooleanType,
      ByteType,
      ShortType,
      IntegerType,
      LongType,
      FloatType,
      DoubleType,
      DecimalType(5, 1),
      StringType,
      BinaryType,
      DateType,
      TimestampType,
      NullType)

  private val allComplexDataTypes: Seq[DataType] = Seq(
    // Currently, only check certain inner types, assuming they are representative
    // for checking most expressions.
    ArrayType(IntegerType),
    MapType(StringType, IntegerType),
    StructType(Seq(StructField("a", StringType), StructField("b", IntegerType)))
  )

  def generateChildExpression(t: DataType): Expression = {
    t match {
      case _: BooleanType => Literal(true, t)
      case _: IntegralType => Literal(null, t)
      case _: FractionalType => Literal(null, t)
      case StringType | BinaryType => Literal("123")
      case DateType => Literal(null, t)
      case TimestampType => Literal(null, t)
      case ArrayType(_, _) => Literal(null, t)
      case MapType(_, _, _) => Literal(null, t)
      case StructType(_) => Literal(null, t)
      case NullType => Literal(null, t)
      case _ => throw new UnsupportedOperationException("Not supported type: " + t)
    }
  }

  def generateGlutenProjectPlan(expr: Expression): TransformSupport = {
    val namedExpr = Seq(Alias(expr, "r")())
    ProjectExecTransformer(namedExpr, DummyPlan())
  }

  def validateExpr(targetExpr: Expression): Unit = {
    val glutenProject = generateGlutenProjectPlan(targetExpr)
    if (targetExpr.resolved && glutenProject.doValidate().ok()) {
      logInfo(
        "## validation passes: " + targetExpr.getClass.getSimpleName + "(" +
          targetExpr.children.map(_.dataType.toString).mkString(", ") + ")")
    } else {
      logInfo(
        "!! validation fails: " + targetExpr.getClass.getSimpleName + "(" +
          targetExpr.children.map(_.dataType.toString).mkString(", ") + ")")
    }
  }

  test("cast") {
    for (from <- allPrimitiveDataTypes ++ allComplexDataTypes) {
      for (to <- allPrimitiveDataTypes ++ allComplexDataTypes) {
        if (to != from) {
          val castExpr = Cast(generateChildExpression(from), to)
          if (castExpr.checkInputDataTypes().isSuccess) {
            val glutenProject = generateGlutenProjectPlan(castExpr)
            if (castExpr.resolved && glutenProject.doValidate().ok()) {
              logInfo("## cast validation passes: cast from " + from + " to " + to)
            } else {
              logInfo("!! cast validation fails: cast from " + from + " to " + to)
            }
          }
        }
      }
    }
  }

  test("unary expressions with expected input types") {
    val functionRegistry = spark.sessionState.functionRegistry
    val sparkBuiltInFunctions = functionRegistry.listFunction()
    val exceptionalList: Buffer[Expression] = Buffer()

    for (func <- sparkBuiltInFunctions) {
      val builder = functionRegistry.lookupFunctionBuilder(func).get
      val expr: Expression = {
        try {
          // Instantiate an expression with null input. Just for obtaining the instance for checking
          // its allowed input types.
          builder(Seq(null))
        } catch {
          // Ignore the exception as some expression builders require more than one input.
          case _: Throwable => null
        }
      }
      val needsValidation = if (expr == null) {
        false
      } else {
        expr match {
          // Validated separately.
          case _: Cast => false
          case _: ExpectsInputTypes if expr.isInstanceOf[UnaryExpression] => true
          case _ =>
            exceptionalList += expr
            false
        }
      }
      if (needsValidation) {
        val acceptedTypes = allPrimitiveDataTypes ++ allComplexDataTypes.filter(
          expr.asInstanceOf[ExpectsInputTypes].inputTypes.head.acceptsType(_))
        if (acceptedTypes.isEmpty) {
          logWarning("Any given type is not accepted for " + expr.getClass.getSimpleName)
        }
        acceptedTypes.foreach(
          t => {
            val child = generateChildExpression(t)
            // Builds an expression whose child's type is really accepted in Spark.
            val targetExpr = builder(Seq(child))
            validateExpr(targetExpr)
          })
      }
    }

    logWarning("Exceptional list:\n" + exceptionalList.mkString(", "))
  }

  def hasImplicitCast(expr: Expression): Boolean = expr match {
    case _: ImplicitCastInputTypes => true
    case _: BinaryOperator => true
    case _ => false
  }

  test("binary expressions with expected input types") {
    val functionRegistry = spark.sessionState.functionRegistry
    val exceptionalList: Buffer[Expression] = Buffer()

    val sparkBuiltInFunctions = functionRegistry.listFunction()
    sparkBuiltInFunctions.foreach(
      func => {
        val builder = functionRegistry.lookupFunctionBuilder(func).get
        val expr: Expression = {
          try {
            // Instantiate an expression with null input. Just for obtaining the instance for
            // checking its allowed input types.
            builder(Seq(null, null))
          } catch {
            // Ignore the exception as some expression builders that don't require exact two input.
            case _: Throwable => null
          }
        }
        val needsValidation = if (expr == null) {
          false
        } else {
          expr match {
            // Requires left/right child's DataType to determine inputTypes.
            case _: BinaryArrayExpressionWithImplicitCast =>
              exceptionalList += expr
              false
            case _: ExpectsInputTypes if expr.isInstanceOf[BinaryExpression] => true
            case _ =>
              exceptionalList += expr
              false
          }
        }

        if (needsValidation) {
          var acceptedLeftTypes: Seq[DataType] = Seq.empty
          var acceptedRightTypes: Seq[DataType] = Seq.empty
          try {
            acceptedLeftTypes = allPrimitiveDataTypes ++ allComplexDataTypes.filter(
              expr.asInstanceOf[ExpectsInputTypes].inputTypes(0).acceptsType(_))
            acceptedRightTypes = allPrimitiveDataTypes ++ allComplexDataTypes.filter(
              expr.asInstanceOf[ExpectsInputTypes].inputTypes(1).acceptsType(_))
          } catch {
            case _: java.lang.NullPointerException =>
          }

          if (acceptedLeftTypes.isEmpty || acceptedRightTypes.isEmpty) {
            logWarning("Any given type is not accepted for " + expr.getClass.getSimpleName)
          }
          val leftChildList = acceptedLeftTypes.map(
            t => {
              generateChildExpression(t)
            })
          if (hasImplicitCast(expr)) {
            leftChildList.foreach(
              left => {
                // Spark's implicit cast makes same input types.
                val targetExpr = builder(Seq(left, left))
                validateExpr(targetExpr)
              })
          } else {
            val rightChildList = acceptedRightTypes.map(
              t => {
                generateChildExpression(t)
              })
            leftChildList.foreach(
              left => {
                rightChildList.foreach(
                  right => {
                    // Builds an expression whose child's type is really accepted in Spark.
                    val targetExpr = builder(Seq(left, right))
                    validateExpr(targetExpr)
                  })
              })
          }
        }
      })

    logWarning("Exceptional list:\n" + exceptionalList.mkString(", "))
  }
}
