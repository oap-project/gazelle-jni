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

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.QueryPlan
import org.apache.spark.sql.catalyst.util.truncatedString
import org.apache.spark.sql.connector.catalog.Table
import org.apache.spark.sql.connector.read.{InputPartition, Scan}
import org.apache.spark.sql.execution.datasources.v2.{BatchScanExecShim, FileScan}
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.sql.types.StructType

/** Columnar Based BatchScanExec. */
case class BatchScanExecTransformer(
    override val output: Seq[AttributeReference],
    @transient override val scan: Scan,
    override val runtimeFilters: Seq[Expression],
    override val keyGroupedPartitioning: Option[Seq[Expression]] = None,
    override val ordering: Option[Seq[SortOrder]] = None,
    @transient override val table: Table,
    override val commonPartitionValues: Option[Seq[(InternalRow, Int)]] = None,
    override val applyPartialClustering: Boolean = false,
    override val replicatePartitions: Boolean = false)
  extends BatchScanExecTransformerBase(
    output,
    scan,
    runtimeFilters,
    keyGroupedPartitioning,
    ordering,
    table,
    commonPartitionValues,
    applyPartialClustering,
    replicatePartitions) {

  protected[this] def supportsBatchScan(scan: Scan): Boolean = {
    scan.isInstanceOf[FileScan]
  }

  override def doCanonicalize(): BatchScanExecTransformer = {
    this.copy(
      output = output.map(QueryPlan.normalizeExpressions(_, output)),
      runtimeFilters = QueryPlan.normalizePredicates(
        runtimeFilters.filterNot(_ == DynamicPruningExpression(Literal.TrueLiteral)),
        output)
    )
  }
}

abstract class BatchScanExecTransformerBase(
    override val output: Seq[AttributeReference],
    @transient override val scan: Scan,
    override val runtimeFilters: Seq[Expression],
    override val keyGroupedPartitioning: Option[Seq[Expression]] = None,
    override val ordering: Option[Seq[SortOrder]] = None,
    @transient override val table: Table,
    override val commonPartitionValues: Option[Seq[(InternalRow, Int)]] = None,
    override val applyPartialClustering: Boolean = false,
    override val replicatePartitions: Boolean = false)
  extends BatchScanExecShim(
    output,
    scan,
    runtimeFilters,
    keyGroupedPartitioning,
    ordering,
    table,
    commonPartitionValues,
    applyPartialClustering,
    replicatePartitions)
  with BasicScanExecTransformer {

  // Note: "metrics" is made transient to avoid sending driver-side metrics to tasks.
  @transient override lazy val metrics: Map[String, SQLMetric] =
    BackendsApiManager.getMetricsApiInstance.genBatchScanTransformerMetrics(sparkContext)

  // Similar to the problem encountered in https://github.com/oap-project/gluten/pull/3184,
  // we cannot add member variables to BatchScanExecTransformerBase, which inherits from case
  // class. Otherwise, we will encounter an issue where makeCopy cannot find a constructor
  // with the corresponding number of parameters.
  // The workaround is to add a mutable list to pass in pushdownFilters.
  protected var pushdownFilters: Seq[Expression] = scan match {
    case fileScan: FileScan =>
      fileScan.dataFilters.filter {
        expr =>
          ExpressionConverter.canReplaceWithExpressionTransformer(
            ExpressionConverter.replaceAttributeReference(expr),
            output)
      }
    case _ =>
      logInfo(s"${scan.getClass.toString} does not support push down filters")
      Seq.empty
  }

  def setPushDownFilters(filters: Seq[Expression]): Unit = {
    pushdownFilters = filters
  }

  override def filterExprs(): Seq[Expression] = pushdownFilters

  override def getMetadataColumns(): Seq[AttributeReference] = Seq.empty

  override def outputAttributes(): Seq[Attribute] = output

  override def getPartitions: Seq[InputPartition] = filteredFlattenPartitions

  override def getPartitionSchema: StructType = scan match {
    case fileScan: FileScan => fileScan.readPartitionSchema
    case _ => new StructType()
  }

  override def getDataSchema: StructType = scan match {
    case fileScan: FileScan => fileScan.readDataSchema
    case _ => new StructType()
  }

  override def getRootPathsInternal: Seq[String] = {
    scan match {
      case fileScan: FileScan =>
        FileIndexUtil.getRootPath(fileScan.fileIndex)
      case _ => Seq.empty
    }
  }

  protected[this] def supportsBatchScan(scan: Scan): Boolean

  override def doValidateInternal(): ValidationResult = {
    if (!supportsBatchScan(scan)) {
      return ValidationResult.failed(s"Unsupported scan $scan")
    }

    if (pushedAggregate.nonEmpty) {
      return ValidationResult.failed(s"Unsupported aggregation push down for $scan.")
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

    super.doValidateInternal()
  }

  override def metricsUpdater(): MetricsUpdater =
    BackendsApiManager.getMetricsApiInstance.genBatchScanTransformerMetricsUpdater(metrics)

  @transient protected lazy val filteredFlattenPartitions: Seq[InputPartition] =
    filteredPartitions.flatten

  @transient override lazy val fileFormat: ReadFileFormat =
    BackendsApiManager.getSettings.getSubstraitReadFileFormatV2(scan)

  override def simpleString(maxFields: Int): String = {
    val truncatedOutputString = truncatedString(output, "[", ", ", "]", maxFields)
    val runtimeFiltersString = s"RuntimeFilters: ${runtimeFilters.mkString("[", ",", "]")}"
    val nativeFiltersString = s"NativeFilters: ${filterExprs().mkString("[", ",", "]")}"
    val result = s"$nodeName$truncatedOutputString ${scan.description()}" +
      s" $runtimeFiltersString $nativeFiltersString"
    redact(result)
  }
}
