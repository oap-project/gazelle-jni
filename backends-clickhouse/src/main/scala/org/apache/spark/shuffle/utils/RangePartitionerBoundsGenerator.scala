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
package org.apache.spark.shuffle.utils

import org.apache.gluten.backendsapi.clickhouse.CHValidatorApi
import org.apache.gluten.execution.SortExecTransformer
import org.apache.gluten.expression.ExpressionConverter
import org.apache.gluten.substrait.SubstraitContext
import org.apache.gluten.substrait.expression.ExpressionNode
import org.apache.gluten.substrait.plan.{PlanBuilder, PlanNode}
import org.apache.gluten.substrait.rel.{RelBuilder, RelNode}

import org.apache.spark.RangePartitioner
import org.apache.spark.rdd.{PartitionPruningRDD, RDD}
import org.apache.spark.sql.catalyst.expressions.{Attribute, NamedExpression, SortOrder, UnsafeRow}
import org.apache.spark.sql.catalyst.plans.physical.RangePartitioning
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.types._

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.node.ArrayNode

import java.util

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag
import scala.util.control.Breaks.{break, breakable}
import scala.util.hashing.byteswap32

/**
 * In spark RangePartitioner, the rangeBounds is private, so we make a copied-implementation here.
 * It is based on the fact that, there has been a pre-projection before the range partition and
 * remove all function expressions in the sort ordering expressions.
 */
class RangePartitionerBoundsGenerator[K: Ordering: ClassTag, V](
    partitions: Int,
    rdd: RDD[_ <: Product2[K, V]],
    ordering: Seq[SortOrder],
    inputAttributes: Seq[Attribute],
    private var ascending: Boolean = true,
    val samplePointsPerPartitionHint: Int = 20
) {

  private def getRangeBounds: Array[K] = {
    if (partitions <= 1) {
      Array.empty
    } else {
      val sampleSize = math.min(samplePointsPerPartitionHint.toDouble * partitions, 1e6)
      val sampleSizePerPartition = math.ceil(3.0 * sampleSize / rdd.partitions.length).toInt
      val (numItems, sketched) = RangePartitioner.sketch(rdd.map(_._1), sampleSizePerPartition)
      if (numItems == 0L) {
        Array.empty
      } else {
        val fraction = math.min(sampleSize / math.max(numItems, 1L), 1.0)
        val candidates = ArrayBuffer.empty[(K, Float)]
        val imbalancedPartitions = mutable.Set.empty[Int]
        sketched.foreach {
          case (idx, n, sample) =>
            if (fraction * n > sampleSizePerPartition) {
              imbalancedPartitions += idx
            } else {
              val weight = (n.toDouble / sample.length).toFloat
              for (key <- sample) {
                candidates += ((key, weight))
              }
            }
        }
        if (imbalancedPartitions.nonEmpty) {
          val imbalanced = new PartitionPruningRDD(rdd.map(_._1), imbalancedPartitions.contains)
          val seed = byteswap32(-rdd.id - 1)
          val reSampled = imbalanced.sample(withReplacement = false, fraction, seed).collect()
          val weight = (1.0 / fraction).toFloat
          candidates ++= reSampled.map(x => (x, weight))
        }
        RangePartitioner.determineBounds(candidates, math.min(partitions, candidates.size))
      }
    }
  }

  /*
   return json structure
   {
    "ordering":[
      {
       "column_ref":0,
        "data_type":"xxx",
        "is_nullable":true,
        "direction":0
      },
        ...
    ],
    "range_bounds":[
     {
         "is_null":false,
         "value": ...
       },
       {
         "is_null":true
       },
       ...
    ]
    }
   */
  private def getExpressionFieldReference(
      context: SubstraitContext,
      ordering: SortOrder,
      attributes: Seq[Attribute]): Int = {
    val funcs = context.registeredFunction
    val projExprNode = ExpressionConverter
      .replaceWithExpressionTransformer(ordering.child, attributes)
      .doTransform(funcs)
    val pb = projExprNode.toProtobuf
    if (!pb.hasSelection) {
      throw new IllegalArgumentException(s"A sorting field should be an attribute")
    } else {
      pb.getSelection.getDirectReference.getStructField.getField()
    }
  }

  private def buildProjectionPlan(
      context: SubstraitContext,
      sortExpressions: Seq[NamedExpression]): PlanNode = {
    val args = context.registeredFunction
    val columnarProjExprs = sortExpressions.map(
      expr => {
        ExpressionConverter
          .replaceWithExpressionTransformer(expr, attributeSeq = inputAttributes)
      })
    val projExprNodeList = new java.util.ArrayList[ExpressionNode]()
    for (expr <- columnarProjExprs) {
      projExprNodeList.add(expr.doTransform(args))
    }
    val projectRel = RelBuilder.makeProjectRel(null, projExprNodeList, context, 0)
    val outNames = new util.ArrayList[String]
    val relNodes = new util.ArrayList[RelNode]()
    relNodes.add(projectRel)
    PlanBuilder.makePlan(context, relNodes, outNames)
  }

  private def buildOrderingJson(
      context: SubstraitContext,
      orderings: Seq[SortOrder],
      attributes: Seq[Attribute],
      jsonMapper: ObjectMapper,
      arrayNode: ArrayNode): Unit = {
    orderings.foreach {
      ordering =>
        val node = jsonMapper.createObjectNode()
        val index = getExpressionFieldReference(context, ordering, attributes)
        node.put("column_name", attributes(index).name)
        node.put("column_ref", index)
        node.put("data_type", ordering.dataType.toString)
        node.put("is_nullable", ordering.nullable)
        node.put("direction", SortExecTransformer.transformSortDirection(ordering))
        arrayNode.add(node)
    }
  }

  private def buildRangeBoundJson(
      row: UnsafeRow,
      orderings: Seq[SortOrder],
      jsonMapper: ObjectMapper): ArrayNode = {
    val arrayNode = jsonMapper.createArrayNode()
    (0 until row.numFields).foreach {
      i =>
        if (row.isNullAt(i)) {
          val node = jsonMapper.createObjectNode()
          node.put("is_null", true)
          arrayNode.add(node)
        } else {
          val ordering = orderings(i)
          val node = jsonMapper.createObjectNode()
          node.put("is_null", false)
          ordering.dataType match {
            case _: BooleanType => node.put("value", row.getBoolean(i))
            case _: ByteType => node.put("value", row.getByte(i))
            case _: ShortType => node.put("value", row.getShort(i))
            case _: IntegerType => node.put("value", row.getInt(i))
            case _: LongType => node.put("value", row.getLong(i))
            case _: FloatType => node.put("value", row.getFloat(i))
            case _: DoubleType => node.put("value", row.getDouble(i))
            case _: StringType => node.put("value", row.getString(i))
            case _: DateType => node.put("value", row.getInt(i))
            case d: DecimalType =>
              val decimal = row.getDecimal(i, d.precision, d.scale).toString()
              node.put("value", decimal)
            case _: TimestampType => node.put("value", row.getLong(i))
            case _ =>
              throw new IllegalArgumentException(
                s"Unsupported data type ${ordering.dataType.toString}")
          }
          arrayNode.add(node)
        }
    }
    arrayNode
  }

  private def buildRangeBoundsJson(jsonMapper: ObjectMapper, arrayNode: ArrayNode): Int = {
    val bounds = getRangeBounds
    bounds.foreach {
      bound =>
        val row = bound.asInstanceOf[UnsafeRow]
        arrayNode.add(buildRangeBoundJson(row, ordering, jsonMapper))
    }
    bounds.length
  }

  // Make a json structure that can be passed to native engine
  def getRangeBoundsJsonString: RangeBoundsInfo = {
    val context = new SubstraitContext()
    val mapper = new ObjectMapper
    val rootNode = mapper.createObjectNode
    val orderingArray = rootNode.putArray("ordering")
    buildOrderingJson(context, ordering, inputAttributes, mapper, orderingArray)
    val boundArray = rootNode.putArray("range_bounds")
    val boundLength = buildRangeBoundsJson(mapper, boundArray)
    RangeBoundsInfo(mapper.writeValueAsString(rootNode), boundLength)
  }
}

case class RangeBoundsInfo(json: String, boundsSize: Int)

object RangePartitionerBoundsGenerator {
  def supportedFieldType(dataType: DataType): Boolean = {
    dataType match {
      case _: BooleanType => true
      case _: ByteType => true
      case _: ShortType => true
      case _: IntegerType => true
      case _: LongType => true
      case _: FloatType => true
      case _: DoubleType => true
      case _: StringType => true
      case _: DateType => true
      case _: DecimalType => true
      case _: TimestampType => true
      case _ => false
    }
  }

  def supportedOrderings(rangePartitioning: RangePartitioning, child: SparkPlan): Boolean = {
    val orderings = rangePartitioning.ordering
    var enableRangePartitioning = true
    // TODO. support complex data type in orderings
    breakable {
      for (ordering <- orderings) {
        if (!RangePartitionerBoundsGenerator.supportedFieldType(ordering.dataType)) {
          enableRangePartitioning = false
          break
        }
        if (
          !ordering.child.isInstanceOf[Attribute] && !CHValidatorApi.supportShuffleWithProject(
            rangePartitioning,
            child)
        ) {
          enableRangePartitioning = false
          break
        }
      }
    }
    enableRangePartitioning
  }
}
