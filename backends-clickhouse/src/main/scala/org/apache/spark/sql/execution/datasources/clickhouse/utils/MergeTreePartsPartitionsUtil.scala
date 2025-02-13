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
package org.apache.spark.sql.execution.datasources.clickhouse.utils

import org.apache.gluten.backendsapi.clickhouse.{CHBackendSettings, CHConfig}
import org.apache.gluten.execution.{GlutenMergeTreePartition, MergeTreePartRange, MergeTreePartSplit}
import org.apache.gluten.expression.{ConverterUtils, ExpressionConverter}
import org.apache.gluten.softaffinity.SoftAffinityManager
import org.apache.gluten.substrait.`type`.ColumnTypeNode
import org.apache.gluten.substrait.SubstraitContext
import org.apache.gluten.substrait.extensions.ExtensionBuilder
import org.apache.gluten.substrait.rel.RelBuilder

import org.apache.spark.affinity.CHAffinity
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.catalog.BucketSpec
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.connector.read.InputPartition
import org.apache.spark.sql.delta.ClickhouseSnapshot
import org.apache.spark.sql.delta.catalog.ClickHouseTableV2
import org.apache.spark.sql.delta.files.TahoeFileIndex
import org.apache.spark.sql.execution.datasources.{CHDatasourceJniWrapper, HadoopFsRelation, PartitionDirectory}
import org.apache.spark.sql.execution.datasources.clickhouse.{ExtensionTableBuilder, MergeTreePartFilterReturnedRange}
import org.apache.spark.sql.execution.datasources.mergetree.{PartSerializer, StorageMeta}
import org.apache.spark.sql.execution.datasources.v2.clickhouse.metadata.AddMergeTreeParts
import org.apache.spark.sql.execution.datasources.v2.clickhouse.source.DeltaMergeTreeFileFormat
import org.apache.spark.sql.types.{BooleanType, StructType}
import org.apache.spark.util.SparkResourceUtil
import org.apache.spark.util.collection.BitSet

import com.fasterxml.jackson.core.`type`.TypeReference
import com.fasterxml.jackson.databind.ObjectMapper
import com.google.protobuf.{Any, StringValue}
import io.substrait.proto.NamedStruct
import io.substrait.proto.Plan

import java.util
import java.util.{ArrayList => JArrayList}

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer

// scalastyle:off argcount
object MergeTreePartsPartitionsUtil extends Logging {

  def getMergeTreePartsPartitions(
      relation: HadoopFsRelation,
      selectedPartitions: Array[PartitionDirectory],
      output: Seq[Attribute],
      bucketedScan: Boolean,
      sparkSession: SparkSession,
      table: ClickHouseTableV2,
      optionalBucketSet: Option[BitSet],
      optionalNumCoalescedBuckets: Option[Int],
      disableBucketedScan: Boolean,
      filterExprs: Seq[Expression]): Seq[InputPartition] = {
    if (
      !relation.location.isInstanceOf[TahoeFileIndex] || !relation.fileFormat
        .isInstanceOf[DeltaMergeTreeFileFormat]
    ) {
      throw new IllegalStateException()
    }
    val fileIndex = relation.location.asInstanceOf[TahoeFileIndex]

    // when querying, use deltaLog.update(true) to get the staleness acceptable snapshot
    val snapshotId =
      ClickhouseSnapshot.genSnapshotId(table.deltaLog.update(stalenessAcceptable = true))

    val partitions = new ArrayBuffer[InputPartition]
    val (database, tableName) = if (table.catalogTable.isDefined) {
      (table.catalogTable.get.identifier.database.get, table.catalogTable.get.identifier.table)
    } else {
      // for file_format.`file_path`
      (StorageMeta.DEFAULT_PATH_BASED_DATABASE, table.deltaPath.toUri.getPath)
    }
    val engine = "MergeTree"
    val relativeTablePath = fileIndex.deltaLog.dataPath.toUri.getPath.substring(1)
    val absoluteTablePath = fileIndex.deltaLog.dataPath.toUri.toString

    // bucket table
    if (table.bucketOption.isDefined && bucketedScan) {
      genBucketedInputPartitionSeq(
        engine,
        database,
        tableName,
        snapshotId,
        relativeTablePath,
        absoluteTablePath,
        table.bucketOption.get,
        optionalBucketSet,
        optionalNumCoalescedBuckets,
        selectedPartitions,
        table.schema(),
        partitions,
        table,
        table.clickhouseTableConfigs,
        output,
        filterExprs,
        sparkSession
      )
    } else {
      genInputPartitionSeq(
        relation,
        engine,
        database,
        tableName,
        snapshotId,
        relativeTablePath,
        absoluteTablePath,
        optionalBucketSet,
        selectedPartitions,
        table.schema(),
        partitions,
        table,
        table.clickhouseTableConfigs,
        output,
        filterExprs,
        sparkSession
      )
    }
    partitions.toSeq
  }

  def genInputPartitionSeq(
      relation: HadoopFsRelation,
      engine: String,
      database: String,
      tableName: String,
      snapshotId: String,
      relativeTablePath: String,
      absoluteTablePath: String,
      optionalBucketSet: Option[BitSet],
      selectedPartitions: Array[PartitionDirectory],
      tableSchema: StructType,
      partitions: ArrayBuffer[InputPartition],
      table: ClickHouseTableV2,
      clickhouseTableConfigs: Map[String, String],
      output: Seq[Attribute],
      filterExprs: Seq[Expression],
      sparkSession: SparkSession): Unit = {

    val bucketingEnabled = sparkSession.sessionState.conf.bucketingEnabled
    val shouldProcess: String => Boolean = optionalBucketSet match {
      case Some(bucketSet) if bucketingEnabled =>
        name =>
          // find bucket it in name pattern of:
          // "partition_col=1/00001/373c9386-92a4-44ef-baaf-a67e1530b602_0_006"
          name.split("/").dropRight(1).filterNot(_.contains("=")).map(_.toInt).forall(bucketSet.get)
      case _ =>
        _ => true
    }

    val selectPartsFiles = selectedPartitions
      .flatMap(
        partition =>
          partition.files.map(
            fs => {
              val path = fs.getPath.toUri.toString

              val ret = ClickhouseSnapshot.pathToAddMTPCache.getIfPresent(path)
              if (ret == null) {
                val keys = ClickhouseSnapshot.pathToAddMTPCache.asMap().keySet()
                val keySample = if (keys.isEmpty) {
                  "<empty>"
                } else {
                  keys.iterator().next()
                }
                throw new IllegalStateException(
                  "Can't find AddMergeTreeParts from cache pathToAddMTPCache for key: " +
                    path + ". This happens when too many new entries are added to " +
                    "pathToAddMTPCache during current query. " +
                    "Try rerun current query. Existing KeySample: " + keySample
                )
              }
              ret
            }))
      .filter(part => shouldProcess(part.name))
      .toSeq
    if (selectPartsFiles.isEmpty) {
      return
    }

    val selectRanges: Seq[MergeTreePartRange] =
      getMergeTreePartRange(
        selectPartsFiles,
        snapshotId,
        database,
        tableName,
        relativeTablePath,
        absoluteTablePath,
        tableSchema,
        table,
        clickhouseTableConfigs,
        filterExprs,
        output,
        sparkSession
      )

    if (selectRanges.isEmpty) {
      return
    }

    val maxSplitBytes = getMaxSplitBytes(sparkSession, selectRanges)
    val totalCores = SparkResourceUtil.getTotalCores(relation.sparkSession.sessionState.conf)
    val isAllSmallFiles = selectRanges.forall(_.size < maxSplitBytes)
    val fileCntThreshold = relation.sparkSession.sessionState.conf
      .getConfString(
        CHBackendSettings.GLUTEN_CLICKHOUSE_FILES_PER_PARTITION_THRESHOLD,
        CHBackendSettings.GLUTEN_CLICKHOUSE_FILES_PER_PARTITION_THRESHOLD_DEFAULT
      )
      .toInt
    val totalMarksThreshold = totalCores * fileCntThreshold
    if (fileCntThreshold > 0 && isAllSmallFiles && selectRanges.size <= totalMarksThreshold) {
      var fileCnt = math.round((selectRanges.size * 1.0) / totalCores).toInt
      if (fileCnt < 1) fileCnt = 1
      val splitFiles = selectRanges
        .map {
          part =>
            MergeTreePartSplit(
              part.name,
              part.dirName,
              part.targetNode,
              part.start,
              part.marks,
              part.size
            )
        }
      genInputPartitionSeqByFileCnt(
        engine,
        database,
        tableName,
        snapshotId,
        relativeTablePath,
        absoluteTablePath,
        tableSchema,
        partitions,
        table,
        clickhouseTableConfigs,
        splitFiles,
        fileCnt
      )
    } else {
      val openCostInBytes = sparkSession.sessionState.conf.filesOpenCostInBytes
      val totalMarks = selectRanges.map(p => p.marks).sum
      val totalBytes = selectRanges.map(p => p.size).sum
      // maxSplitBytes / (total_Bytes / total_marks) + 1
      val markCntPerPartition = maxSplitBytes * totalMarks / totalBytes + 1

      logInfo(s"Planning scan with bin packing, max mark: $markCntPerPartition")
      val splitFiles = selectRanges
        .flatMap {
          part =>
            val end = part.marks + part.start
            (part.start until end by markCntPerPartition).map {
              offset =>
                val remaining = end - offset
                val size = if (remaining > markCntPerPartition) markCntPerPartition else remaining
                MergeTreePartSplit(
                  part.name,
                  part.dirName,
                  part.targetNode,
                  offset,
                  size,
                  size * part.size / part.marks)
            }
        }

      val (partNameWithLocation, locationDistinct) =
        calculatedLocationForSoftAffinity(splitFiles, relativeTablePath)

      genInputPartitionSeqBySplitFiles(
        engine,
        database,
        tableName,
        snapshotId,
        relativeTablePath,
        absoluteTablePath,
        tableSchema,
        partitions,
        table,
        clickhouseTableConfigs,
        splitFiles,
        openCostInBytes,
        maxSplitBytes,
        partNameWithLocation,
        locationDistinct
      )
    }
  }

  def genInputPartitionSeqByFileCnt(
      engine: String,
      database: String,
      tableName: String,
      snapshotId: String,
      relativeTablePath: String,
      absoluteTablePath: String,
      tableSchema: StructType,
      partitions: ArrayBuffer[InputPartition],
      table: ClickHouseTableV2,
      clickhouseTableConfigs: Map[String, String],
      splitFiles: Seq[MergeTreePartSplit],
      fileCnt: Int): Unit = {
    val currentFiles = new ArrayBuffer[MergeTreePartSplit]
    var currentFileCnt = 0L

    /** Close the current partition and move to the next. */
    def closePartition(): Unit = {
      if (currentFiles.nonEmpty) {
        // Copy to a new Array.
        val newPartition = GlutenMergeTreePartition(
          partitions.size,
          engine,
          database,
          tableName,
          snapshotId,
          relativeTablePath,
          absoluteTablePath,
          table.orderByKey,
          table.lowCardKey,
          table.minmaxIndexKey,
          table.bfIndexKey,
          table.setIndexKey,
          table.primaryKey,
          currentFiles.toArray,
          tableSchema,
          clickhouseTableConfigs
        )
        partitions += newPartition
      }
      currentFiles.clear()
      currentFileCnt = 0L
    }

    splitFiles.foreach {
      file =>
        if (currentFileCnt >= fileCnt) {
          closePartition()
        }
        // Add the given file to the current partition.
        currentFileCnt += 1L
        currentFiles += file
    }
    closePartition()
    partitions.toSeq
  }
  def genInputPartitionSeqBySplitFiles(
      engine: String,
      database: String,
      tableName: String,
      snapshotId: String,
      relativeTablePath: String,
      absoluteTablePath: String,
      tableSchema: StructType,
      partitions: ArrayBuffer[InputPartition],
      table: ClickHouseTableV2,
      clickhouseTableConfigs: Map[String, String],
      splitFiles: Seq[MergeTreePartSplit],
      openCostInBytes: Long,
      maxSplitBytes: Long,
      partNameWithLocation: util.HashMap[String, String],
      locationDistinct: util.HashSet[String]): Unit = {

    val currentSizeByLocation = new util.HashMap[String, Long]
    val currentFilesByLocation = new util.HashMap[String, ArrayBuffer[MergeTreePartSplit]]

    /** Close the current partition and move to the next. */
    def closePartition(location: String): Unit = {
      val currentFiles: ArrayBuffer[MergeTreePartSplit] = currentFilesByLocation.get(location)
      if (currentFiles.nonEmpty) {
        val newPartition = GlutenMergeTreePartition(
          partitions.size,
          engine,
          database,
          tableName,
          snapshotId,
          relativeTablePath,
          absoluteTablePath,
          table.orderByKey,
          table.lowCardKey,
          table.minmaxIndexKey,
          table.bfIndexKey,
          table.setIndexKey,
          table.primaryKey,
          currentFiles.toArray,
          tableSchema,
          clickhouseTableConfigs
        )
        partitions += newPartition
      }
      currentFiles.clear()
      currentSizeByLocation.put(location, 0)
    }

    // generate `Seq[InputPartition]` by file size
    // Assign files to partitions using "Next Fit Decreasing"
    locationDistinct.forEach(
      location => {
        currentSizeByLocation.put(location, 0L)
        currentFilesByLocation.put(location, new ArrayBuffer[MergeTreePartSplit])
      })

    splitFiles.foreach {
      parts =>
        {
          val location = partNameWithLocation.get(parts.name)
          var currentSize = currentSizeByLocation.get(location)
          val currentFiles = currentFilesByLocation.get(location)

          if (currentSize + parts.bytesOnDisk > maxSplitBytes) {
            closePartition(location)
            currentSize = 0L
          }
          // Add the given file to the current partition.
          currentSizeByLocation.put(location, currentSize + parts.bytesOnDisk + openCostInBytes)
          currentFiles += parts
        }
    }

    locationDistinct.forEach(closePartition)
  }

  /** Generate bucket partition */
  def genBucketedInputPartitionSeq(
      engine: String,
      database: String,
      tableName: String,
      snapshotId: String,
      relativeTablePath: String,
      absoluteTablePath: String,
      bucketSpec: BucketSpec,
      optionalBucketSet: Option[BitSet],
      optionalNumCoalescedBuckets: Option[Int],
      selectedPartitions: Array[PartitionDirectory],
      tableSchema: StructType,
      partitions: ArrayBuffer[InputPartition],
      table: ClickHouseTableV2,
      clickhouseTableConfigs: Map[String, String],
      output: Seq[Attribute],
      filterExprs: Seq[Expression],
      sparkSession: SparkSession): Unit = {

    val selectPartsFiles = selectedPartitions
      .flatMap(
        partition =>
          partition.files.map(
            fs => {
              val path = fs.getPath.toString
              val ret = ClickhouseSnapshot.pathToAddMTPCache.getIfPresent(path)
              if (ret == null) {
                val keys = ClickhouseSnapshot.pathToAddMTPCache.asMap().keySet()
                val keySample = if (keys.isEmpty) {
                  "<empty>"
                } else {
                  keys.iterator().next()
                }
                throw new IllegalStateException(
                  "Can't find AddMergeTreeParts from cache pathToAddMTPCache for key: " +
                    path + ". This happens when too many new entries are added to " +
                    "pathToAddMTPCache during current query. " +
                    "Try rerun current query. Existing KeySample: " + keySample)
              }
              ret
            }))
      .toSeq

    if (selectPartsFiles.isEmpty) {
      return
    }

    val selectRanges: Seq[MergeTreePartRange] =
      getMergeTreePartRange(
        selectPartsFiles,
        snapshotId,
        database,
        tableName,
        relativeTablePath,
        absoluteTablePath,
        tableSchema,
        table,
        clickhouseTableConfigs,
        filterExprs,
        output,
        sparkSession
      )

    if (selectRanges.isEmpty) {
      return
    }

    val bucketGroupParts = selectRanges.groupBy(p => Integer.parseInt(p.bucketNum))

    val prunedFilesGroupedToBuckets = if (optionalBucketSet.isDefined) {
      val bucketSet = optionalBucketSet.get
      bucketGroupParts.filter(f => bucketSet.get(f._1))
    } else {
      bucketGroupParts
    }

    if (optionalNumCoalescedBuckets.isDefined) {
      throw new UnsupportedOperationException(
        "Currently CH backend can't support coalesced buckets.")
    }
    Seq.tabulate(bucketSpec.numBuckets) {
      bucketId =>
        val currBucketParts: Seq[MergeTreePartRange] =
          prunedFilesGroupedToBuckets.getOrElse(bucketId, Seq.empty)
        if (currBucketParts.nonEmpty) {
          val currentFiles = currBucketParts.map {
            part =>
              MergeTreePartSplit(
                part.name,
                part.dirName,
                part.targetNode,
                part.start,
                part.marks,
                part.size)
          }
          val newPartition = GlutenMergeTreePartition(
            partitions.size,
            engine,
            database,
            tableName,
            snapshotId,
            relativeTablePath,
            absoluteTablePath,
            table.orderByKey,
            table.lowCardKey,
            table.minmaxIndexKey,
            table.bfIndexKey,
            table.setIndexKey,
            table.primaryKey,
            currentFiles.toArray,
            tableSchema,
            clickhouseTableConfigs
          )
          partitions += newPartition
        }
    }
  }

  private def useDriverFilter(filterExprs: Seq[Expression], sparkSession: SparkSession): Boolean = {
    val enableDriverFilterKey = CHConfig.runtimeSettings("enabled_driver_filter_mergetree_index")

    // When using soft affinity, disable driver filter
    filterExprs.nonEmpty && sparkSession.sessionState.conf.getConfString(
      enableDriverFilterKey,
      "false") == "true" && !SoftAffinityManager.usingSoftAffinity
  }

  private def getMergeTreePartRange(
      selectPartsFiles: Seq[AddMergeTreeParts],
      snapshotId: String,
      database: String,
      tableName: String,
      relativeTablePath: String,
      absoluteTablePath: String,
      tableSchema: StructType,
      table: ClickHouseTableV2,
      clickhouseTableConfigs: Map[String, String],
      filterExprs: Seq[Expression],
      output: Seq[Attribute],
      sparkSession: SparkSession): Seq[MergeTreePartRange] = {

    if (useDriverFilter(filterExprs, sparkSession)) {
      val size_per_mark = selectPartsFiles.map(part => (part.size, part.marks)).unzip match {
        case (l1, l2) => l1.sum / l2.sum
      }

      val extensionTableNode = ExtensionTableBuilder
        .makeExtensionTable(
          database,
          tableName,
          snapshotId,
          relativeTablePath,
          absoluteTablePath,
          table.orderByKey,
          table.lowCardKey,
          table.minmaxIndexKey,
          table.bfIndexKey,
          table.setIndexKey,
          table.primaryKey,
          PartSerializer.fromAddMergeTreeParts(selectPartsFiles),
          tableSchema,
          clickhouseTableConfigs.asJava,
          new JArrayList[String]()
        )

      val transformer = filterExprs
        .map {
          case ar: AttributeReference if ar.dataType == BooleanType =>
            EqualNullSafe(ar, Literal.TrueLiteral)
          case e => e
        }
        .reduceLeftOption(And)
        .map(ExpressionConverter.replaceWithExpressionTransformer(_, output))

      val typeNodes = ConverterUtils.collectAttributeTypeNodes(output)
      val nameList = ConverterUtils.collectAttributeNamesWithoutExprId(output)
      val columnTypeNodes = output.map {
        attr =>
          if (table.partitionColumns.exists(_.equals(attr.name))) {
            new ColumnTypeNode(NamedStruct.ColumnType.PARTITION_COL)
          } else {
            new ColumnTypeNode(NamedStruct.ColumnType.NORMAL_COL)
          }
      }.asJava
      val substraitContext = new SubstraitContext
      val enhancement =
        Any.pack(StringValue.newBuilder.setValue(extensionTableNode.getExtensionTableStr).build)
      val extensionNode = ExtensionBuilder.makeAdvancedExtension(enhancement)
      val readNode = RelBuilder.makeReadRel(
        typeNodes,
        nameList,
        columnTypeNodes,
        transformer.map(_.doTransform(substraitContext.registeredFunction)).orNull,
        extensionNode,
        substraitContext,
        substraitContext.nextOperatorId("readRel")
      )

      val planBuilder = Plan.newBuilder
      substraitContext.registeredFunction.forEach(
        (k, v) => planBuilder.addExtensions(ExtensionBuilder.makeFunctionMapping(k, v).toProtobuf))

      val filter_ranges = CHDatasourceJniWrapper.filterRangesOnDriver(
        planBuilder.build().toByteArray,
        readNode.toProtobuf.toByteArray
      )

      val mapper: ObjectMapper = new ObjectMapper()
      val values: JArrayList[MergeTreePartFilterReturnedRange] =
        mapper.readValue(
          filter_ranges,
          new TypeReference[JArrayList[MergeTreePartFilterReturnedRange]]() {})

      val partMap = selectPartsFiles.map(part => (part.name, part)).toMap
      values.asScala
        .map(
          range => {
            val part = partMap.get(range.getPartName).orNull
            val marks = range.getEnd - range.getBegin
            MergeTreePartRange(
              part.name,
              part.dirName,
              part.targetNode,
              part.bucketNum,
              range.getBegin,
              marks,
              marks * size_per_mark)
          })
        .toSeq
    } else {
      selectPartsFiles
        .map(
          part =>
            MergeTreePartRange(
              part.name,
              part.dirName,
              part.targetNode,
              part.bucketNum,
              0,
              part.marks,
              part.size))
        .toSeq
    }
  }

  private def calculatedLocationForSoftAffinity(
      splits: Seq[MergeTreePartSplit],
      relativeTablePath: String): (util.HashMap[String, String], util.HashSet[String]) = {
    val partNameWithLocation = new util.HashMap[String, String]()
    val locationDistinct = new util.HashSet[String]()

    splits.foreach(
      part => {
        if (!partNameWithLocation.containsKey(part.name)) {
          val locations = CHAffinity.getNativeMergeTreePartLocations(part.name, relativeTablePath)
          val localtionKey = locations.sorted.mkString(",")
          locationDistinct.add(localtionKey)
          partNameWithLocation.put(part.name, localtionKey)
        }
      })
    (partNameWithLocation, locationDistinct)
  }

  def getMaxSplitBytes(
      sparkSession: SparkSession,
      selectedRanges: Seq[MergeTreePartRange]): Long = {
    val defaultMaxSplitBytes = sparkSession.sessionState.conf.filesMaxPartitionBytes
    val openCostInBytes = sparkSession.sessionState.conf.filesOpenCostInBytes
    val minPartitionNum = sparkSession.sessionState.conf.filesMinPartitionNum
      .getOrElse(sparkSession.leafNodeDefaultParallelism)
    val totalBytes = selectedRanges.map(_.size + openCostInBytes).sum
    val bytesPerCore = totalBytes / minPartitionNum

    Math.min(defaultMaxSplitBytes, Math.max(openCostInBytes, bytesPerCore))
  }
}
// scalastyle:on argcount
