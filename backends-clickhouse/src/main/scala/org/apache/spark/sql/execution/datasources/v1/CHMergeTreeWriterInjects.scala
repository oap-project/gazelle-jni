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
package org.apache.spark.sql.execution.datasources.v1

import org.apache.gluten.backendsapi.clickhouse.CHConfig
import org.apache.gluten.execution.ColumnarToRowExecBase
import org.apache.gluten.expression.ConverterUtils
import org.apache.gluten.substrait.`type`.ColumnTypeNode
import org.apache.gluten.substrait.SubstraitContext
import org.apache.gluten.substrait.extensions.ExtensionBuilder
import org.apache.gluten.substrait.plan.PlanBuilder
import org.apache.gluten.substrait.rel.RelBuilder
import org.apache.gluten.utils.ConfigUtil

import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.delta.MergeTreeFileFormat
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanExec
import org.apache.spark.sql.execution.datasources.{CHDatasourceJniWrapper, FakeRowAdaptor, OutputWriter}
import org.apache.spark.sql.execution.datasources.clickhouse.ExtensionTableNode
import org.apache.spark.sql.execution.datasources.mergetree.{MetaSerializer, PartSerializer, StorageConfigProvider, StorageMeta}
import org.apache.spark.sql.types.StructType

import com.google.common.collect.Lists
import com.google.protobuf.{Any, StringValue}
import io.substrait.proto.NamedStruct
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.mapreduce.TaskAttemptContext

import java.{util => ju}

import scala.collection.JavaConverters._

case class PlanWithSplitInfo(plan: Array[Byte], splitInfo: Array[Byte])

case class HadoopConfReader(conf: Configuration) extends StorageConfigProvider {
  lazy val storageConf: Map[String, String] = {
    conf
      .iterator()
      .asScala
      .filter(_.getKey.startsWith(StorageMeta.STORAGE_PREFIX))
      .map(entry => entry.getKey -> entry.getValue)
      .toMap
  }
}

class CHMergeTreeWriterInjects extends CHFormatWriterInjects {

  override def nativeConf(
      options: Map[String, String],
      compressionCodec: String): ju.Map[String, String] = {
    options.asJava
  }

  override def createNativeWrite(outputPath: String, context: TaskAttemptContext): Write = {
    val conf = HadoopConfReader(context.getConfiguration).storageConf
    val jobID = context.getJobID.toString
    val taskAttemptID = context.getTaskAttemptID.toString
    Write
      .newBuilder()
      .setCommon(
        Write.Common
          .newBuilder()
          .setFormat(formatName)
          .setJobTaskAttemptId(s"$jobID/$taskAttemptID")
          .build())
      .setMergetree(MergeTreeFileFormat.createWrite(Some(outputPath), conf))
      .build()
  }

  override def createOutputWriter(
      outputPath: String,
      dataSchema: StructType,
      context: TaskAttemptContext,
      nativeConf: ju.Map[String, String]): OutputWriter = {

    val wrapper = if (CHConfig.get.enableOnePipelineMergeTreeWrite) {

      /**
       * In pipeline mode, CHColumnarWriteFilesRDD.writeFilesForEmptyIterator will create a JNI
       * wrapper which is not needed in this case.
       *
       * TODO: We should refactor the code to avoid creating the JNI wrapper in this case.
       */
      None
    } else {
      val datasourceJniWrapper = new CHDatasourceJniWrapper(
        createWriteRel(outputPath, dataSchema, context),
        ConfigUtil.serialize(nativeConf)
      )
      Some(datasourceJniWrapper)
    }

    new FakeRowOutputWriter(wrapper, outputPath)
  }

  override val formatName: String = "mergetree"
}

object MergeTreeWriterInjects {
  def insertFakeRowAdaptor(
      queryPlan: SparkPlan,
      output: Seq[Attribute]): (SparkPlan, Seq[Attribute]) = {
    var result = queryPlan match {
      // if the child is columnar, we can just wrap&transfer the columnar data
      case c2r: ColumnarToRowExecBase =>
        FakeRowAdaptor(c2r.child)
      // If the child is aqe, we make aqe "support columnar",
      // then aqe itself will guarantee to generate columnar outputs.
      // So FakeRowAdaptor will always consumes columnar data,
      // thus avoiding the case of c2r->aqe->r2c->writer
      case aqe: AdaptiveSparkPlanExec =>
        FakeRowAdaptor(
          AdaptiveSparkPlanExec(
            aqe.inputPlan,
            aqe.context,
            aqe.preprocessingRules,
            aqe.isSubquery,
            supportsColumnar = true
          ))
      case other => FakeRowAdaptor(other)
    }
    assert(output.size == result.output.size)
    val newOutput = result.output.zip(output).map {
      case (newAttr, oldAttr) =>
        oldAttr.withExprId(newAttr.exprId)
    }
    (result, newOutput)
  }
}
object CHMergeTreeWriterInjects {

  // scalastyle:off argcount
  def genMergeTreeWriteRel(
      path: String,
      database: String,
      tableName: String,
      snapshotId: String,
      orderByKey: String,
      lowCardKey: String,
      minmaxIndexKey: String,
      bfIndexKey: String,
      setIndexKey: String,
      primaryKey: String,
      partitionColumns: Seq[String],
      partList: Seq[String],
      tableSchema: StructType,
      clickhouseTableConfigs: Map[String, String],
      output: Seq[Attribute]): PlanWithSplitInfo = {
    // scalastyle:on argcount
    val typeNodes = ConverterUtils.collectAttributeTypeNodes(output)
    val nameList = ConverterUtils.collectAttributeNamesWithoutExprId(output)
    val columnTypeNodes = output.map {
      attr =>
        if (partitionColumns.exists(_.equals(attr.name))) {
          new ColumnTypeNode(NamedStruct.ColumnType.PARTITION_COL)
        } else {
          new ColumnTypeNode(NamedStruct.ColumnType.NORMAL_COL)
        }
    }.asJava

    val substraitContext = new SubstraitContext

    val result = MetaSerializer.apply(
      database,
      tableName,
      snapshotId,
      path,
      "",
      orderByKey,
      lowCardKey,
      minmaxIndexKey,
      bfIndexKey,
      setIndexKey,
      primaryKey,
      PartSerializer.fromPartNames(partList),
      tableSchema,
      clickhouseTableConfigs.asJava
    )

    val optimizationContent = "isMergeTree=1\n"
    val optimization = Any.pack(StringValue.newBuilder.setValue(optimizationContent).build)
    val extensionNode = ExtensionBuilder.makeAdvancedExtension(optimization, null)

    val relNode = RelBuilder.makeReadRel(
      typeNodes,
      nameList,
      columnTypeNodes,
      null,
      extensionNode,
      substraitContext,
      substraitContext.nextOperatorId("readRel"))

    val plan =
      PlanBuilder.makePlan(substraitContext, Lists.newArrayList(relNode), nameList).toProtobuf

    PlanWithSplitInfo(plan.toByteArray, ExtensionTableNode.toProtobuf(result).toByteArray)
  }
}
