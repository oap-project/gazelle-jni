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
package org.apache.spark.sql.delta.catalog

import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.analysis.{ResolvedTable, UnresolvedTable}
import org.apache.spark.sql.catalyst.catalog.CatalogTable
import org.apache.spark.sql.catalyst.expressions.{Attribute, Expression}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.connector.catalog.V1Table
import org.apache.spark.sql.connector.read.InputPartition
import org.apache.spark.sql.delta.{ClickhouseSnapshot, DeltaErrors, DeltaLog, DeltaTableUtils, DeltaTimeTravelSpec, Snapshot, UnresolvedPathBasedDeltaTable}
import org.apache.spark.sql.delta.actions.{Metadata, Protocol}
import org.apache.spark.sql.delta.sources.DeltaDataSource
import org.apache.spark.sql.execution.datasources.{HadoopFsRelation, PartitionDirectory}
import org.apache.spark.sql.execution.datasources.clickhouse.utils.MergeTreePartsPartitionsUtil
import org.apache.spark.sql.execution.datasources.mergetree.StorageMeta
import org.apache.spark.sql.execution.datasources.v2.clickhouse.source.DeltaMergeTreeFileFormat
import org.apache.spark.sql.execution.datasources.v2.clickhouse.utils.CHDataSourceUtils
import org.apache.spark.sql.types.StructType
import org.apache.spark.util.collection.BitSet

import org.apache.hadoop.fs.Path

import java.{util => ju}

import scala.collection.JavaConverters._

@SuppressWarnings(Array("io.github.zhztheplayer.scalawarts.InheritFromCaseClass"))
class ClickHouseTableV2(
    override val spark: SparkSession,
    override val path: Path,
    override val catalogTable: Option[CatalogTable] = None,
    override val tableIdentifier: Option[String] = None,
    override val timeTravelOpt: Option[DeltaTimeTravelSpec] = None,
    override val options: Map[String, String] = Map.empty,
    val clickhouseExtensionOptions: Map[String, String] = Map.empty)
  extends DeltaTableV2(spark, path, catalogTable, tableIdentifier, timeTravelOpt, options)
  with ClickHouseTableV2Base {

  lazy val (rootPath, partitionFilters, timeTravelByPath) = {
    if (catalogTable.isDefined) {
      // Fast path for reducing path munging overhead
      (new Path(catalogTable.get.location), Nil, None)
    } else {
      DeltaDataSource.parsePathIdentifier(spark, path.toString, options)
    }
  }

  override protected lazy val tableSchema: StructType = schema()

  override def name(): String =
    catalogTable
      .map(_.identifier.unquotedString)
      .orElse(tableIdentifier)
      .getOrElse(s"clickhouse.`${deltaLog.dataPath}`")

  override def properties(): ju.Map[String, String] = {
    val ret = super.properties()

    // for file path based write
    if (initialSnapshot.version < 0 && clickhouseExtensionOptions.nonEmpty) {
      ret.putAll(clickhouseExtensionOptions.asJava)
    }
    ret
  }

  def getFileFormat(protocol: Protocol, meta: Metadata): DeltaMergeTreeFileFormat = {
    new DeltaMergeTreeFileFormat(
      protocol,
      StorageMeta.withStorageID(
        meta,
        dataBaseName,
        tableName,
        ClickhouseSnapshot.genSnapshotId(initialSnapshot)))
  }

  override def deltaProperties: Map[String, String] = properties().asScala.toMap

  override def deltaCatalog: Option[CatalogTable] = catalogTable

  override def deltaPath: Path = path

  override def deltaSnapshot: Snapshot = initialSnapshot

  def cacheThis(): Unit = {
    ClickHouseTableV2.deltaLog2Table.put(deltaLog, this)
  }

  cacheThis()
}

@SuppressWarnings(Array("io.github.zhztheplayer.scalawarts.InheritFromCaseClass"))
class TempClickHouseTableV2(
    override val spark: SparkSession,
    override val catalogTable: Option[CatalogTable] = None)
  extends ClickHouseTableV2(spark, null, catalogTable) {
  override def properties(): ju.Map[String, String] = catalogTable.get.properties.asJava
  override protected def rawPartitionColumns: Seq[String] = catalogTable.get.partitionColumnNames
  override def cacheThis(): Unit = {}
}

object ClickHouseTableV2 extends Logging {
  private val deltaLog2Table =
    new scala.collection.concurrent.TrieMap[DeltaLog, ClickHouseTableV2]()
  // for CTAS use
  val temporalThreadLocalCHTable = new ThreadLocal[ClickHouseTableV2]()

  def getTable(deltaLog: DeltaLog): ClickHouseTableV2 = {
    if (deltaLog2Table.contains(deltaLog)) {
      deltaLog2Table(deltaLog)
    } else if (temporalThreadLocalCHTable.get() != null) {
      temporalThreadLocalCHTable.get()
    } else {
      throw new IllegalStateException(
        s"Can not find ClickHouseTableV2 for deltalog ${deltaLog.dataPath}")
    }
  }

  def clearCache(): Unit = {
    deltaLog2Table.clear()
    temporalThreadLocalCHTable.remove()
  }

  def partsPartitions(
      deltaLog: DeltaLog,
      relation: HadoopFsRelation,
      selectedPartitions: Array[PartitionDirectory],
      output: Seq[Attribute],
      bucketedScan: Boolean,
      optionalBucketSet: Option[BitSet],
      optionalNumCoalescedBuckets: Option[Int],
      disableBucketedScan: Boolean,
      filterExprs: Seq[Expression]): Seq[InputPartition] = {
    val tableV2 = ClickHouseTableV2.getTable(deltaLog)

    MergeTreePartsPartitionsUtil.getMergeTreePartsPartitions(
      relation,
      selectedPartitions,
      output,
      bucketedScan,
      tableV2.spark,
      tableV2,
      optionalBucketSet,
      optionalNumCoalescedBuckets,
      disableBucketedScan,
      filterExprs)
  }

  /** Resolves a path into a DeltaTableV2, leveraging standard v2 table resolution. */
  def apply(
      spark: SparkSession,
      tablePath: Path,
      options: Map[String, String],
      cmd: String): DeltaTableV2 =
    resolve(spark, UnresolvedPathBasedDeltaTable(tablePath.toString, options, cmd), cmd)

  /** Resolves a table identifier into a DeltaTableV2, leveraging standard v2 table resolution. */
  def apply(spark: SparkSession, tableId: TableIdentifier, cmd: String): DeltaTableV2 = {
    resolve(spark, UnresolvedTable(tableId.nameParts, cmd, None), cmd)
  }

  /** Applies standard v2 table resolution to an unresolved Delta table plan node */
  def resolve(spark: SparkSession, unresolved: LogicalPlan, cmd: String): DeltaTableV2 =
    extractFrom(spark.sessionState.analyzer.ResolveRelations(unresolved), cmd)

  /**
   * Extracts the DeltaTableV2 from a resolved Delta table plan node, throwing "table not found" if
   * the node does not actually represent a resolved Delta table.
   */
  def extractFrom(plan: LogicalPlan, cmd: String): DeltaTableV2 = plan match {
    case ResolvedTable(_, _, d: ClickHouseTableV2, _) => d
    case ResolvedTable(_, _, d: DeltaTableV2, _) => d
    case ResolvedTable(_, _, t: V1Table, _)
        if CHDataSourceUtils.isClickHouseTable(t.catalogTable) =>
      new ClickHouseTableV2(SparkSession.active, new Path(t.v1Table.location), Some(t.v1Table))
    case ResolvedTable(_, _, t: V1Table, _) if DeltaTableUtils.isDeltaTable(t.catalogTable) =>
      DeltaTableV2(SparkSession.active, new Path(t.v1Table.location), Some(t.v1Table))
    case _ => throw DeltaErrors.notADeltaTableException(cmd)
  }
}
