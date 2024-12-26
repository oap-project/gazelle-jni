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

import org.apache.gluten.GlutenConfig
import org.apache.gluten.backendsapi.BackendsApiManager
import org.apache.gluten.expression.ConverterUtils
import org.apache.gluten.extension.ValidationResult
import org.apache.gluten.metrics.MetricsUpdater
import org.apache.gluten.substrait.`type`.ColumnTypeNode
import org.apache.gluten.substrait.SubstraitContext
import org.apache.gluten.substrait.extensions.ExtensionBuilder
import org.apache.gluten.substrait.rel.{RelBuilder, RelNode}
import org.apache.gluten.utils.SubstraitUtil

import org.apache.spark.sql.catalyst.catalog.BucketSpec
import org.apache.spark.sql.catalyst.catalog.CatalogTypes.TablePartitionSpec
import org.apache.spark.sql.catalyst.expressions.{Attribute, Expression, Literal}
import org.apache.spark.sql.catalyst.plans.logical.Project
import org.apache.spark.sql.catalyst.util.CaseInsensitiveMap
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.datasources.FileFormat
import org.apache.spark.sql.execution.datasources.parquet.ParquetFileFormat
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{ArrayType, MapType, MetadataBuilder}

import io.substrait.proto.NamedStruct
import org.apache.parquet.hadoop.ParquetOutputFormat

import java.util.Locale

import scala.collection.convert.ImplicitConversions.`collection AsScalaIterable`

/**
 * Note that, the output staging path is set by `ColumnarWriteFilesExec`, each task should have its
 * own staging path.
 */
case class WriteFilesExecTransformer(
    child: SparkPlan,
    fileFormat: FileFormat,
    partitionColumns: Seq[Attribute],
    bucketSpec: Option[BucketSpec],
    options: Map[String, String],
    staticPartitions: TablePartitionSpec)
  extends UnaryTransformSupport {
  // Note: "metrics" is made transient to avoid sending driver-side metrics to tasks.
  @transient override lazy val metrics: Map[String, SQLMetric] =
    BackendsApiManager.getMetricsApiInstance.genWriteFilesTransformerMetrics(sparkContext)

  override def metricsUpdater(): MetricsUpdater =
    BackendsApiManager.getMetricsApiInstance.genWriteFilesTransformerMetricsUpdater(metrics)

  override def output: Seq[Attribute] = Seq.empty

  val caseInsensitiveOptions: CaseInsensitiveMap[String] = CaseInsensitiveMap(options)

  def getRelNode(
      context: SubstraitContext,
      originalInputAttributes: Seq[Attribute],
      operatorId: Long,
      input: RelNode,
      validation: Boolean): RelNode = {
    val typeNodes = ConverterUtils.collectAttributeTypeNodes(originalInputAttributes)

    val columnTypeNodes = new java.util.ArrayList[ColumnTypeNode]()
    val inputAttributes = new java.util.ArrayList[Attribute]()
    val childSize = this.child.output.size
    val childOutput = this.child.output
    for (i <- 0 until childSize) {
      val partitionCol = partitionColumns.find(_.exprId == childOutput(i).exprId)
      if (partitionCol.nonEmpty) {
        columnTypeNodes.add(new ColumnTypeNode(NamedStruct.ColumnType.PARTITION_COL))
        // "aggregate with partition group by can be pushed down"
        // test partitionKey("p") is different with
        // data columns("P").
        inputAttributes.add(partitionCol.get)
      } else {
        columnTypeNodes.add(new ColumnTypeNode(NamedStruct.ColumnType.NORMAL_COL))
        inputAttributes.add(originalInputAttributes(i))
      }
    }

    val nameList =
      ConverterUtils.collectAttributeNames(inputAttributes.toSeq)
    val extensionNode = if (!validation) {
      ExtensionBuilder.makeAdvancedExtension(
        BackendsApiManager.getTransformerApiInstance.genWriteParameters(this),
        SubstraitUtil.createEnhancement(originalInputAttributes)
      )
    } else {
      // Use an extension node to send the input types through Substrait plan for validation.
      ExtensionBuilder.makeAdvancedExtension(
        SubstraitUtil.createEnhancement(originalInputAttributes))
    }
    RelBuilder.makeWriteRel(
      input,
      typeNodes,
      nameList,
      columnTypeNodes,
      extensionNode,
      context,
      operatorId)
  }

  private def getFinalChildOutput: Seq[Attribute] = {
    val metadataExclusionList = conf
      .getConf(GlutenConfig.NATIVE_WRITE_FILES_COLUMN_METADATA_EXCLUSION_LIST)
      .split(",")
      .map(_.trim)
      .toSeq
    child.output.map(attr => WriteFilesExecTransformer.removeMetadata(attr, metadataExclusionList))
  }

  override def doValidateInternal(): ValidationResult = {
    val finalChildOutput = getFinalChildOutput

    def isConstantComplexType(e: Expression): Boolean = {
      e match {
        case Literal(_, _: ArrayType | _: MapType) => true
        case _ => e.children.exists(isConstantComplexType)
      }
    }

    def hasConstantComplexType = child.logicalLink.collectFirst {
      case p: Project if p.projectList.exists(isConstantComplexType) => true
    }.isDefined

    // TODO: Currently Velox doesn't support Parquet write of constant with complex data type.
    if (fileFormat.isInstanceOf[ParquetFileFormat] && hasConstantComplexType) {
      return ValidationResult.failed(
        "Unsupported native parquet write: " +
          "complex data type with constant")
    }

    val validationResult =
      BackendsApiManager.getSettings.supportWriteFilesExec(
        fileFormat,
        finalChildOutput.toStructType.fields,
        bucketSpec,
        caseInsensitiveOptions)
    if (!validationResult.ok()) {
      return ValidationResult.failed("Unsupported native write: " + validationResult.reason())
    }

    val substraitContext = new SubstraitContext
    val operatorId = substraitContext.nextOperatorId(this.nodeName)
    val relNode =
      getRelNode(substraitContext, finalChildOutput, operatorId, null, validation = true)
    doNativeValidation(substraitContext, relNode)
  }

  override protected def doTransform(context: SubstraitContext): TransformContext = {
    val childCtx = child.asInstanceOf[TransformSupport].transform(context)
    val operatorId = context.nextOperatorId(this.nodeName)
    val currRel =
      getRelNode(context, getFinalChildOutput, operatorId, childCtx.root, validation = false)
    assert(currRel != null, "Write Rel should be valid")
    TransformContext(output, currRel)
  }

  override protected def withNewChildInternal(newChild: SparkPlan): WriteFilesExecTransformer =
    copy(child = newChild)
}

object WriteFilesExecTransformer {
  def getCompressionCodec(options: Map[String, String]): String = {
    // From `ParquetOptions`
    val parquetCompressionConf = options.get(ParquetOutputFormat.COMPRESSION)
    options
      .get("compression")
      .orElse(parquetCompressionConf)
      .getOrElse(SQLConf.get.parquetCompressionCodec)
      .toLowerCase(Locale.ROOT)
  }

  // To be compatible with Spark3.2/3.3/3.4, we do cleanup spark internal metadata manually.
  // See https://github.com/apache/spark/pull/40776
  private val INTERNAL_METADATA_KEYS = Seq(
    "__autoGeneratedAlias",
    "__qualified_access_only",
    "__metadata_col",
    "__file_source_metadata_col",
    "__file_source_constant_metadata_col",
    "__file_source_generated_metadata_col"
  )

  private def removeMetadata(attr: Attribute, metadataExclusionList: Seq[String]): Attribute = {
    val metadataKeys = INTERNAL_METADATA_KEYS ++ metadataExclusionList
    attr.withMetadata {
      var builder = new MetadataBuilder().withMetadata(attr.metadata)
      metadataKeys.foreach(key => builder = builder.remove(key))
      builder.build()
    }
  }
}
