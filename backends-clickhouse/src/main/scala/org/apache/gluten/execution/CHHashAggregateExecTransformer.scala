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
import org.apache.gluten.exception.GlutenNotSupportException
import org.apache.gluten.execution.CHHashAggregateExecTransformer.getAggregateResultAttributes
import org.apache.gluten.expression._
import org.apache.gluten.extension.ExpressionExtensionTrait
import org.apache.gluten.substrait.`type`.{TypeBuilder, TypeNode}
import org.apache.gluten.substrait.{AggregationParams, SubstraitContext}
import org.apache.gluten.substrait.expression.{AggregateFunctionNode, ExpressionBuilder, ExpressionNode}
import org.apache.gluten.substrait.extensions.{AdvancedExtensionNode, ExtensionBuilder}
import org.apache.gluten.substrait.rel.{RelBuilder, RelNode}

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate._
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.adaptive._
import org.apache.spark.sql.execution.aggregate.BaseAggregateExec
import org.apache.spark.sql.execution.exchange._
import org.apache.spark.sql.types._

import com.google.protobuf.{Any, StringValue}

import java.util

import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer

object CHHashAggregateExecTransformer {
  // The result attributes of aggregate expressions from vanilla may be different from CH native.
  // For example, the result attributes of `avg(x)` are `sum(x)` and `count(x)`. This could bring
  // some unexpected issues. So we need to make the result attributes consistent with CH native.
  def getCHAggregateResultExpressions(
      groupingExpressions: Seq[NamedExpression],
      aggregateExpressions: Seq[AggregateExpression],
      resultExpressions: Seq[NamedExpression]): Seq[NamedExpression] = {
    var adjustedResultExpressions = resultExpressions.slice(0, groupingExpressions.length)
    var resultExpressionIndex = groupingExpressions.length
    adjustedResultExpressions ++ aggregateExpressions.flatMap {
      aggExpr =>
        aggExpr.mode match {
          case Partial | PartialMerge =>
            // For partial aggregate, the size of the result expressions of an aggregate expression
            // is the same as aggBufferAttributes' length
            val aggBufferAttributesCount = aggExpr.aggregateFunction.aggBufferAttributes.length
            aggExpr.aggregateFunction match {
              case avg: Average =>
                val res = Seq(aggExpr.resultAttribute)
                resultExpressionIndex += aggBufferAttributesCount
                res
              case sum: Sum if (sum.dataType.isInstanceOf[DecimalType]) =>
                val res = Seq(resultExpressions(resultExpressionIndex))
                resultExpressionIndex += aggBufferAttributesCount
                res
              case _ =>
                val res = resultExpressions
                  .slice(resultExpressionIndex, resultExpressionIndex + aggBufferAttributesCount)
                resultExpressionIndex += aggBufferAttributesCount
                res
            }
          case _ =>
            val res = Seq(resultExpressions(resultExpressionIndex))
            resultExpressionIndex += 1
            res
        }
    }
  }

  def getAggregateResultAttributes(
      groupingExpressions: Seq[NamedExpression],
      aggregateExpressions: Seq[AggregateExpression]): Seq[Attribute] = {
    groupingExpressions.map(ConverterUtils.getAttrFromExpr(_).toAttribute) ++ aggregateExpressions
      .map(_.resultAttribute)
  }

  private val curId = new java.util.concurrent.atomic.AtomicLong()

  def newStructFieldId(): Long = curId.getAndIncrement()
}

/**
 * About aggregate modes. In general, all the modes of aggregate expressions in the same
 * HashAggregateExec are the same. And the aggregation will be divided into two stages, partial
 * aggregate and final merge aggregated. But there are some exceptions.
 *   - f(distinct x). This will be divided into four stages (without stages merged by
 *     `MergeTwoPhasesHashBaseAggregate`). The first two stages use `x` as a grouping key and
 *     without aggregate functions. The last two stages aggregate without `x` as a grouping key and
 *     with aggregate function `f`.
 *   - f1(distinct x), f(2). This will be divided into four stages. The first two stages use `x` as
 *     a grouping key and with partial aggregate function `f2`. The last two stages aggregate
 *     without `x` as a grouping key and with aggregate function `f1` and `f2`. The 3rd stages hase
 *     different modes at the same time. `f2` is partial merge but `f1` is partial.
 */
case class CHHashAggregateExecTransformer(
    requiredChildDistributionExpressions: Option[Seq[Expression]],
    groupingExpressions: Seq[NamedExpression],
    aggregateExpressions: Seq[AggregateExpression],
    aggregateAttributes: Seq[Attribute],
    initialInputBufferOffset: Int,
    resultExpressions: Seq[NamedExpression],
    child: SparkPlan)
  extends HashAggregateExecBaseTransformer(
    requiredChildDistributionExpressions,
    groupingExpressions,
    aggregateExpressions,
    aggregateAttributes,
    initialInputBufferOffset,
    resultExpressions,
    child) {

  lazy val aggregateResultAttributes =
    getAggregateResultAttributes(groupingExpressions, aggregateExpressions)

  protected val modes: Seq[AggregateMode] = aggregateExpressions.map(_.mode).distinct

  override protected def checkType(dataType: DataType): Boolean = {
    dataType match {
      case _: StructType => true
      case other => super.checkType(other)
    }
  }

  // CH does not support duplicate columns in a block. So there should not be duplicate attributes
  // in child's output.
  // There is an exception case, when a shuffle result is reused, the child's output may contain
  // duplicate columns. It's mismatched with the the real output of CH.
  protected lazy val childOutput: Seq[Attribute] = {
    child.output
  }

  override protected def doTransform(context: SubstraitContext): TransformContext = {
    val childCtx = child.asInstanceOf[TransformSupport].transform(context)
    val operatorId = context.nextOperatorId(this.nodeName)

    val aggParams = new AggregationParams
    val isChildTransformSupported = !child.isInstanceOf[InputIteratorTransformer]
    val (relNode, inputAttributes, outputAttributes) = if (isChildTransformSupported) {
      // The final HashAggregateExecTransformer and partial HashAggregateExecTransformer
      // are in the one WholeStageTransformer.
      if (modes.isEmpty || !modes.contains(Partial)) {
        (
          getAggRel(context, operatorId, aggParams, childCtx.root),
          childCtx.outputAttributes,
          output)
      } else {
        (
          getAggRel(context, operatorId, aggParams, childCtx.root),
          childCtx.outputAttributes,
          aggregateResultAttributes)
      }
    } else {
      // This means the input is just an iterator, so an ReadRel will be created as child.
      // Prepare the input schema.
      // Notes: Currently, ClickHouse backend uses the output attributes of
      // aggregateResultAttributes as Shuffle output,
      // which is different from Velox backend.
      val typeList = new util.ArrayList[TypeNode]()
      val nameList = new util.ArrayList[String]()
      val (inputAttrs, outputAttrs) = {
        if (modes.isEmpty || modes.forall(_ == Complete)) {
          // When there is no aggregate function or there is complete mode, it does not need
          // to handle outputs according to the AggregateMode
          for (attr <- childOutput) {
            typeList.add(ConverterUtils.getTypeNode(attr.dataType, attr.nullable))
            nameList.add(ConverterUtils.genColumnNameWithExprId(attr))
            nameList.addAll(ConverterUtils.collectStructFieldNames(attr.dataType))
          }
          (childOutput, output)
        } else if (!modes.contains(Partial)) {
          // non-partial mode
          var resultAttrIndex = 0
          for (attr <- aggregateResultAttributes) {
            val colName = getIntermediateAggregateResultColumnName(
              resultAttrIndex,
              aggregateResultAttributes,
              groupingExpressions,
              aggregateExpressions)
            nameList.add(colName)
            val (dataType, nullable) =
              getIntermediateAggregateResultType(attr, aggregateExpressions)
            nameList.addAll(ConverterUtils.collectStructFieldNames(dataType))
            typeList.add(ConverterUtils.getTypeNode(dataType, nullable))
            resultAttrIndex += 1
          }
          (aggregateResultAttributes, output)
        } else {
          // partial mode
          for (attr <- childOutput) {
            typeList.add(ConverterUtils.getTypeNode(attr.dataType, attr.nullable))
            nameList.add(ConverterUtils.genColumnNameWithExprId(attr))
            nameList.addAll(ConverterUtils.collectStructFieldNames(attr.dataType))
          }

          (childOutput, aggregateResultAttributes)
        }
      }

      // The output is different with child.output, so we can not use `childCtx.root` as the
      // `ReadRel`. Here we re-generate the `ReadRel` with the special output list.
      val readRel =
        RelBuilder.makeReadRelForInputIteratorWithoutRegister(typeList, nameList, context)
      (getAggRel(context, operatorId, aggParams, readRel), inputAttrs, outputAttrs)
    }
    TransformContext(outputAttributes, relNode)
  }

  override def getAggRel(
      context: SubstraitContext,
      operatorId: Long,
      aggParams: AggregationParams,
      input: RelNode = null,
      validation: Boolean = false): RelNode = {
    val aggRel =
      getAggRelInternal(context, aggregateResultAttributes, operatorId, input, validation)
    context.registerAggregationParam(operatorId, aggParams)
    aggRel
  }

  private def getAggRelInternal(
      context: SubstraitContext,
      originalInputAttributes: Seq[Attribute],
      operatorId: Long,
      input: RelNode = null,
      validation: Boolean): RelNode = {
    val args = context.registeredFunction
    // Get the grouping nodes.
    val groupingList = new util.ArrayList[ExpressionNode]()
    groupingExpressions.foreach(
      expr => {
        // Use 'child.output' as based Seq[Attribute], the originalInputAttributes
        // may be different for each backend.
        val exprNode = ExpressionConverter
          .replaceWithExpressionTransformer(expr, childOutput)
          .doTransform(args)
        groupingList.add(exprNode)
      })
    // Get the aggregate function nodes.
    val aggregateFunctionList = new util.ArrayList[AggregateFunctionNode]()

    val distinct_modes = aggregateExpressions.map(_.mode).distinct
    // quick check
    if (distinct_modes.contains(PartialMerge)) {
      if (distinct_modes.contains(Final)) {
        throw new IllegalStateException("PartialMerge co-exists with Final")
      }
    }

    val aggFilterList = new util.ArrayList[ExpressionNode]()
    aggregateExpressions.foreach(
      aggExpr => {
        if (aggExpr.filter.isDefined) {
          val exprNode = ExpressionConverter
            .replaceWithExpressionTransformer(aggExpr.filter.get, childOutput)
            .doTransform(args)
          aggFilterList.add(exprNode)
        } else {
          aggFilterList.add(null)
        }

        val aggregateFunc = aggExpr.aggregateFunction
        val childrenNodeList = new util.ArrayList[ExpressionNode]()
        val childrenNodes = aggExpr.mode match {
          case Partial | Complete =>
            val nodes = aggregateFunc.children.toList.map(
              expr => {
                ExpressionConverter
                  .replaceWithExpressionTransformer(expr, childOutput)
                  .doTransform(args)
              })

            val extraNodes = aggregateFunc match {
              case hll: HyperLogLogPlusPlus =>
                val relativeSDLiteral = Literal(hll.relativeSD)
                Seq(
                  ExpressionConverter
                    .replaceWithExpressionTransformer(relativeSDLiteral, child.output)
                    .doTransform(args))
              case _ => Seq.empty
            }

            nodes ++ extraNodes
          case PartialMerge if distinct_modes.contains(Partial) =>
            // this is the case where PartialMerge co-exists with Partial
            // so far, it only happens in a three-stage count distinct case
            // e.g. select sum(a), count(distinct b) from f
            if (!child.isInstanceOf[BaseAggregateExec]) {
              throw new GlutenNotSupportException(
                "PartialMerge's child not being HashAggregateExecBaseTransformer" +
                  " is unsupported yet")
            }
            val aggTypesExpr = ExpressionConverter
              .replaceWithExpressionTransformer(
                aggExpr.resultAttribute,
                CHHashAggregateExecTransformer.getAggregateResultAttributes(
                  child.asInstanceOf[BaseAggregateExec].groupingExpressions,
                  child.asInstanceOf[BaseAggregateExec].aggregateExpressions)
              )
            Seq(aggTypesExpr.doTransform(args))
          case Final | PartialMerge =>
            Seq(
              ExpressionConverter
                .replaceWithExpressionTransformer(aggExpr.resultAttribute, originalInputAttributes)
                .doTransform(args))
          case other =>
            throw new GlutenNotSupportException(s"$other not supported.")
        }
        for (node <- childrenNodes) {
          childrenNodeList.add(node)
        }
        val aggFunctionNode = ExpressionBuilder.makeAggregateFunction(
          CHExpressions.createAggregateFunction(args, aggregateFunc),
          childrenNodeList,
          modeToKeyWord(aggExpr.mode),
          ConverterUtils.getTypeNode(aggregateFunc.dataType, aggregateFunc.nullable)
        )
        aggregateFunctionList.add(aggFunctionNode)
      })

    val extensionNode = getAdvancedExtension(validation, originalInputAttributes)
    RelBuilder.makeAggregateRel(
      input,
      groupingList,
      aggregateFunctionList,
      aggFilterList,
      extensionNode,
      context,
      operatorId)
  }

  override def isStreaming: Boolean = false

  def numShufflePartitions: Option[Int] = Some(0)

  // aggResultAttributes is groupkeys ++ aggregate expressions
  def getIntermediateAggregateResultColumnName(
      columnIndex: Int,
      aggResultAttributes: Seq[Attribute],
      groupingExprs: Seq[NamedExpression],
      aggExpressions: Seq[AggregateExpression]): String = {
    val resultAttr = aggResultAttributes(columnIndex)
    if (columnIndex < groupingExprs.length) {
      ConverterUtils.genColumnNameWithExprId(resultAttr)
    } else {
      val aggExpr = aggExpressions(columnIndex - groupingExprs.length)
      val aggregateFunc = aggExpr.aggregateFunction
      var aggFunctionName =
        if (
          ExpressionExtensionTrait.expressionExtensionTransformer.extensionExpressionsMapping
            .contains(aggregateFunc.getClass)
        ) {
          ExpressionExtensionTrait.expressionExtensionTransformer
            .buildCustomAggregateFunction(aggregateFunc)
            ._1
            .get
        } else {
          AggregateFunctionsBuilder.getSubstraitFunctionName(aggregateFunc)
        }
      ConverterUtils.genColumnNameWithExprId(resultAttr) + "#Partial#" + aggFunctionName
    }
  }

  def getIntermediateAggregateResultType(
      attr: Attribute,
      inputAggregateExpressions: Seq[AggregateExpression]): (DataType, Boolean) = {

    def makeStructType(types: Seq[(DataType, Boolean)]): StructType = {
      val fields = new Array[StructField](types.length)
      var i = 0
      types.foreach {
        case (dataType, nullable) =>
          fields.update(
            i,
            StructField(
              s"anonymousField${CHHashAggregateExecTransformer.newStructFieldId()}",
              dataType,
              nullable))
          i += 1
      }
      StructType(fields)
    }

    def makeStructTypeSingleOne(dataType: DataType, nullable: Boolean): StructType = {
      makeStructType(Seq((dataType, nullable)))
    }

    val aggregateExpression = inputAggregateExpressions.find(_.resultAttribute == attr)
    // We need a way to represent the intermediate result's type of the aggregate function.
    // substrait only contains basic types, we make a trick here.
    //   1. the intermediate result column will has a special format name,
    //     see genPartialAggregateResultColumnName
    //   2. Use a struct type to wrap the arguments' types of the aggregate function.
    //      the arguments' types will be useful later in TypeParser::buildBlockFromNamedStruct
    val (dataType, nullable) = if (aggregateExpression.isEmpty) {
      (attr.dataType, attr.nullable)
    } else {
      aggregateExpression.get match {
        case aggExpr: AggregateExpression =>
          aggExpr.aggregateFunction match {
            case avg: Average =>
              // why using attr.nullable instead of child.nullable?
              // because some aggregate operator's input's nullability is force changed
              // in AggregateFunctionParser::parseFunctionArguments
              (makeStructTypeSingleOne(avg.child.dataType, attr.nullable), attr.nullable)
            case collect @ (_: CollectList | _: CollectSet) =>
              // Be careful with the nullable. We must keep the nullable the same as the column
              // otherwise it will cause a parsing exception on partial aggregated data.
              val child = collect.children.head
              (makeStructTypeSingleOne(child.dataType, child.nullable), child.nullable)
            case covar: Covariance =>
              var fields = Seq[(DataType, Boolean)]()
              fields = fields :+ (covar.left.dataType, covar.left.nullable)
              fields = fields :+ (covar.right.dataType, covar.right.nullable)
              (makeStructType(fields), attr.nullable)
            case corr: PearsonCorrelation =>
              var fields = Seq[(DataType, Boolean)]()
              fields = fields :+ (corr.left.dataType, corr.left.nullable)
              fields = fields :+ (corr.right.dataType, corr.right.nullable)
              (makeStructType(fields), attr.nullable)
            case expr if "bloom_filter_agg".equals(expr.prettyName) =>
              (makeStructTypeSingleOne(expr.children.head.dataType, attr.nullable), attr.nullable)
            case cd: CountDistinct =>
              var fields = Seq[(DataType, Boolean)]()
              for (child <- cd.children) {
                fields = fields :+ (child.dataType, child.nullable)
              }
              (makeStructType(fields), false)
            case approxPercentile: ApproximatePercentile =>
              var fields = Seq[(DataType, Boolean)]()
              // Use approxPercentile.nullable as the nullable of the struct type
              // to make sure it returns null when input is empty
              fields = fields :+ (approxPercentile.child.dataType, approxPercentile.nullable)
              fields = fields :+ (
                approxPercentile.percentageExpression.dataType,
                approxPercentile.percentageExpression.nullable)
              (makeStructType(fields), attr.nullable)
            case percentile: Percentile =>
              var fields = Seq[(DataType, Boolean)]()
              // Use percentile.nullable as the nullable of the struct type
              // to make sure it returns null when input is empty
              fields = fields :+ (percentile.child.dataType, percentile.nullable)
              fields = fields :+ (
                percentile.percentageExpression.dataType,
                percentile.percentageExpression.nullable)
              (makeStructType(fields), attr.nullable)
            case hllpp: HyperLogLogPlusPlus =>
              var fields = Seq[(DataType, Boolean)]()
              fields = fields :+ (hllpp.child.dataType, hllpp.child.nullable)
              val relativeSDLiteral = Literal(hllpp.relativeSD)
              fields = fields :+ (relativeSDLiteral.dataType, false)
              (makeStructType(fields), attr.nullable)
            case _ =>
              (makeStructTypeSingleOne(attr.dataType, attr.nullable), attr.nullable)
          }
        case _ =>
          (attr.dataType, attr.nullable)
      }
    }
    (dataType, nullable)
  }

  override protected def withNewChildInternal(
      newChild: SparkPlan): CHHashAggregateExecTransformer = {
    copy(child = newChild)
  }

  private def getAdvancedExtension(
      validation: Boolean = false,
      originalInputAttributes: Seq[Attribute] = Seq.empty): AdvancedExtensionNode = {
    val enhancement = if (validation) {
      // Use a extension node to send the input types through Substrait plan for validation.
      val inputTypeNodeList = originalInputAttributes
        .map(attr => ConverterUtils.getTypeNode(attr.dataType, attr.nullable))
        .asJava
      Any.pack(TypeBuilder.makeStruct(false, inputTypeNodeList).toProtobuf)
    } else {
      null
    }
    val parametersStrBuf = new StringBuffer("AggregateParams:")
    parametersStrBuf
      .append(s"hasPrePartialAggregate=$hasPrePartialAggregate")
      .append("\n")
      .append(s"hasRequiredChildDistributionExpressions=" +
        s"${requiredChildDistributionExpressions.isDefined}")
      .append("\n")
    val optimization =
      BackendsApiManager.getTransformerApiInstance.packPBMessage(
        StringValue.newBuilder.setValue(parametersStrBuf.toString).build)
    ExtensionBuilder.makeAdvancedExtension(optimization, enhancement)
  }

  // Check that there is a partial aggregation ahead this HashAggregateExec.
  // This is useful when aggregate expressions are empty.
  private def hasPrePartialAggregate(): Boolean = {
    def isSameAggregation(agg1: BaseAggregateExec, agg2: BaseAggregateExec): Boolean = {
      val res = agg1.groupingExpressions.length == agg2.groupingExpressions.length &&
        agg1.groupingExpressions.zip(agg2.groupingExpressions).forall {
          case (e1, e2) =>
            e1.toAttribute == e2.toAttribute
        }
      res
    }

    def checkChild(exec: SparkPlan): Boolean = {
      exec match {
        case agg: BaseAggregateExec =>
          isSameAggregation(this, agg)
        case shuffle: ShuffleExchangeLike =>
          checkChild(shuffle.child)
        case iter: InputIteratorTransformer =>
          checkChild(iter.child)
        case inputAdapter: ColumnarInputAdapter =>
          checkChild(inputAdapter.child)
        case wholeStage: WholeStageTransformer =>
          checkChild(wholeStage.child)
        case aqeShuffleRead: AQEShuffleReadExec =>
          checkChild(aqeShuffleRead.child)
        case shuffle: ShuffleQueryStageExec =>
          checkChild(shuffle.plan)
        case _ =>
          false
      }
    }

    // It's more complex when the aggregate expressions are empty. We need to iterate the plan
    // to find whether there is a partial aggregation. `count(distinct x)` is one of these cases.
    if (aggregateExpressions.length > 0) {
      modes.exists(mode => mode == PartialMerge || mode == Final)
    } else {
      checkChild(child)
    }
  }
}

case class CHHashAggregateExecPullOutHelper(
    aggregateExpressions: Seq[AggregateExpression],
    aggregateAttributes: Seq[Attribute])
  extends HashAggregateExecPullOutBaseHelper {

  /** This method calculates the output attributes of Aggregation. */
  override protected def getAttrForAggregateExprs: List[Attribute] = {
    val aggregateAttr = new ListBuffer[Attribute]()
    val size = aggregateExpressions.size
    var resIndex = 0
    for (expIdx <- 0 until size) {
      val exp: AggregateExpression = aggregateExpressions(expIdx)
      resIndex = getAttrForAggregateExpr(exp, aggregateAttributes, aggregateAttr, resIndex)
    }
    aggregateAttr.toList
  }

  protected def getAttrForAggregateExpr(
      exp: AggregateExpression,
      aggregateAttributeList: Seq[Attribute],
      aggregateAttr: ListBuffer[Attribute],
      index: Int): Int = {
    var resIndex = index
    val aggregateFunc = exp.aggregateFunction
    // First handle the custom aggregate functions
    if (
      ExpressionExtensionTrait.expressionExtensionTransformer.extensionExpressionsMapping.contains(
        aggregateFunc.getClass)
    ) {
      ExpressionExtensionTrait.expressionExtensionTransformer
        .getAttrsIndexForExtensionAggregateExpr(
          aggregateFunc,
          exp.mode,
          exp,
          aggregateAttributeList,
          aggregateAttr,
          index)
    } else {
      exp.mode match {
        case Partial | PartialMerge =>
          val aggBufferAttr = aggregateFunc.inputAggBufferAttributes
          for (index <- aggBufferAttr.indices) {
            val attr = ConverterUtils.getAttrFromExpr(aggBufferAttr(index))
            aggregateAttr += attr
          }
          resIndex += aggBufferAttr.size
          resIndex
        case Final | Complete =>
          aggregateAttr += aggregateAttributeList(resIndex)
          resIndex += 1
          resIndex
        case other =>
          throw new GlutenNotSupportException(s"Unsupported aggregate mode: $other.")
      }
    }
  }
}
