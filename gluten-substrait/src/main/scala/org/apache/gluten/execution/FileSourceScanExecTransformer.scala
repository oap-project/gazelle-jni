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
import org.apache.gluten.expression.ExpressionConverter
import org.apache.gluten.extension.ValidationResult
import org.apache.gluten.metrics.MetricsUpdater
import org.apache.gluten.sql.shims.SparkShimLoader
import org.apache.gluten.substrait.rel.LocalFilesNode.ReadFileFormat
import org.apache.gluten.utils.FileIndexUtil

import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference, Expression, PlanExpression}
import org.apache.spark.sql.catalyst.plans.QueryPlan
import org.apache.spark.sql.catalyst.util.truncatedString
import org.apache.spark.sql.connector.read.InputPartition
import org.apache.spark.sql.execution.FileSourceScanExecShim
import org.apache.spark.sql.execution.datasources.HadoopFsRelation
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.sql.types.StructType
import org.apache.spark.util.collection.BitSet

import org.apache.commons.lang3.StringUtils

case class FileSourceScanExecTransformer(
    @transient override val relation: HadoopFsRelation,
    override val output: Seq[Attribute],
    override val requiredSchema: StructType,
    override val partitionFilters: Seq[Expression],
    override val optionalBucketSet: Option[BitSet],
    override val optionalNumCoalescedBuckets: Option[Int],
    override val dataFilters: Seq[Expression],
    override val tableIdentifier: Option[TableIdentifier],
    override val disableBucketedScan: Boolean = false)
  extends FileSourceScanExecTransformerBase(
    relation,
    output,
    requiredSchema,
    partitionFilters,
    optionalBucketSet,
    optionalNumCoalescedBuckets,
    dataFilters,
    tableIdentifier,
    disableBucketedScan) {

  override def doCanonicalize(): FileSourceScanExecTransformer = {
    FileSourceScanExecTransformer(
      relation,
      output.map(QueryPlan.normalizeExpressions(_, output)),
      requiredSchema,
      QueryPlan.normalizePredicates(
        filterUnusedDynamicPruningExpressions(partitionFilters),
        output),
      optionalBucketSet,
      optionalNumCoalescedBuckets,
      QueryPlan.normalizePredicates(dataFilters, output),
      None,
      disableBucketedScan
    )
  }
}

abstract class FileSourceScanExecTransformerBase(
    @transient override val relation: HadoopFsRelation,
    override val output: Seq[Attribute],
    requiredSchema: StructType,
    partitionFilters: Seq[Expression],
    optionalBucketSet: Option[BitSet],
    optionalNumCoalescedBuckets: Option[Int],
    dataFilters: Seq[Expression],
    tableIdentifier: Option[TableIdentifier],
    disableBucketedScan: Boolean = false)
  extends FileSourceScanExecShim(
    relation,
    output,
    requiredSchema,
    partitionFilters,
    optionalBucketSet,
    optionalNumCoalescedBuckets,
    dataFilters,
    tableIdentifier,
    disableBucketedScan)
  with DatasourceScanTransformer {

  // Note: "metrics" is made transient to avoid sending driver-side metrics to tasks.
  @transient override lazy val metrics: Map[String, SQLMetric] =
    BackendsApiManager.getMetricsApiInstance
      .genFileSourceScanTransformerMetrics(sparkContext)
      .filter(m => !driverMetricsAlias.contains(m._1)) ++ driverMetricsAlias

  override def filterExprs(): Seq[Expression] = dataFiltersInScan.filter {
    expr =>
      ExpressionConverter.canReplaceWithExpressionTransformer(
        ExpressionConverter.replaceAttributeReference(expr),
        output)
  }

  override def getMetadataColumns(): Seq[AttributeReference] = metadataColumns

  override def outputAttributes(): Seq[Attribute] = output

  override def getPartitions: Seq[InputPartition] = {
    BackendsApiManager.getTransformerApiInstance.genInputPartitionSeq(
      relation,
      requiredSchema,
      dynamicallySelectedPartitions,
      output,
      bucketedScan,
      optionalBucketSet,
      optionalNumCoalescedBuckets,
      disableBucketedScan,
      filterExprs()
    )
  }

  override def getPartitionSchema: StructType = relation.partitionSchema

  override def getDataSchema: StructType = relation.dataSchema

  override def getRootPathsInternal: Seq[String] = {
    FileIndexUtil.getRootPath(relation.location)
  }

  override protected def doValidateInternal(): ValidationResult = {
    if (
      !metadataColumns.isEmpty && !BackendsApiManager.getSettings.supportNativeMetadataColumns()
    ) {
      return ValidationResult.failed(s"Unsupported metadata columns scan in native.")
    }

    if (
      SparkShimLoader.getSparkShims.findRowIndexColumnIndexInSchema(schema) > 0 &&
      !BackendsApiManager.getSettings.supportNativeRowIndexColumn()
    ) {
      return ValidationResult.failed("Unsupported row index column scan in native.")
    }

    if (hasUnsupportedColumns) {
      return ValidationResult.failed(s"Unsupported columns scan in native.")
    }

    if (hasFieldIds) {
      // Spark read schema expects field Ids , the case didn't support yet by native.
      return ValidationResult.failed(
        s"Unsupported matching schema column names " +
          s"by field ids in native scan.")
    }
    super.doValidateInternal()
  }

  override def metricsUpdater(): MetricsUpdater =
    BackendsApiManager.getMetricsApiInstance.genFileSourceScanTransformerMetricsUpdater(metrics)

  override val nodeName: String = {
    s"ScanTransformer $relation ${tableIdentifier.map(_.unquotedString).getOrElse("")}"
  }

  override def getProperties: Map[String, String] = {
    this.fileFormat match {
      case ReadFileFormat.TextReadFormat =>
        var options: Map[String, String] = Map()
        relation.options.foreach {
          case ("delimiter", v) => options += ("field_delimiter" -> v)
          case ("quote", v) => options += ("quote" -> v)
          case ("header", v) =>
            val cnt = if (v == "true") 1 else 0
            options += ("header" -> cnt.toString)
          case ("escape", v) => options += ("escape" -> v)
          case ("nullvalue", v) => options += ("nullValue" -> v)
          case (_, _) =>
        }
        options
      case _ => Map.empty
    }
  }

  @transient override lazy val fileFormat: ReadFileFormat =
    BackendsApiManager.getSettings.getSubstraitReadFileFormatV1(relation.fileFormat)

  override def simpleString(maxFields: Int): String = {
    val metadataEntries = metadata.toSeq.sorted.map {
      case (key, value) =>
        key + ": " + StringUtils.abbreviate(redact(value), maxMetadataValueLength)
    }
    val metadataStr = truncatedString(metadataEntries, " ", ", ", "", maxFields)
    val nativeFiltersString = s"NativeFilters: ${filterExprs().mkString("[", ",", "]")}"
    redact(
      s"$nodeNamePrefix$nodeName${truncatedString(output, "[", ",", "]", maxFields)}$metadataStr" +
        s" $nativeFiltersString")
  }
}

object FileSourceScanExecTransformerBase {
  private def isDynamicPruningFilter(e: Expression): Boolean =
    e.find(_.isInstanceOf[PlanExpression[_]]).isDefined
}
