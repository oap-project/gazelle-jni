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
package org.apache.spark.sql.execution.datasources.v2.clickhouse

import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.util.CaseInsensitiveMap
import org.apache.spark.sql.connector.catalog.Table
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.delta._
import org.apache.spark.sql.delta.catalog.ClickHouseTableV2
import org.apache.spark.sql.delta.commands.WriteIntoDelta
import org.apache.spark.sql.delta.commands.cdc.CDCReader
import org.apache.spark.sql.delta.sources.{DeltaDataSource, DeltaSourceUtils, DeltaSQLConf}
import org.apache.spark.sql.sources.BaseRelation
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import org.apache.hadoop.fs.Path

import scala.collection.JavaConverters._
import scala.collection.mutable

/** A DataSource V1 for integrating Delta into Spark SQL batch and Streaming APIs. */
class ClickHouseDataSource extends DeltaDataSource {

  override def shortName(): String = {
    ClickHouseConfig.NAME
  }

  override def getTable(
      schema: StructType,
      partitioning: Array[Transform],
      properties: java.util.Map[String, String]): Table = {
    val options = new CaseInsensitiveStringMap(properties)
    val path = options.get("path")
    if (path == null) throw DeltaErrors.pathNotSpecifiedException
    new ClickHouseTableV2(
      SparkSession.active,
      new Path(path),
      options = properties.asScala.toMap,
      clickhouseExtensionOptions = ClickHouseConfig
        .createMergeTreeConfigurations(
          ClickHouseConfig
            .getMergeTreeConfigurations(properties)
            .asJava)
    )
  }

  override def createRelation(
      sqlContext: SQLContext,
      mode: SaveMode,
      parameters: Map[String, String],
      data: DataFrame): BaseRelation = {
    val path = parameters.getOrElse("path", throw DeltaErrors.pathNotSpecifiedException)
    val partitionColumns = parameters
      .get(DeltaSourceUtils.PARTITIONING_COLUMNS_KEY)
      .map(DeltaDataSource.decodePartitioningColumns)
      .getOrElse(Nil)

    val deltaLog = DeltaLog.forTable(sqlContext.sparkSession, new Path(path), parameters)
    // need to use the latest snapshot
    val configs = if (deltaLog.update().version < 0) {
      // when creating table, save the clickhouse config to the delta metadata
      val clickHouseTableV2 = ClickHouseTableV2.getTable(deltaLog)
      clickHouseTableV2.properties().asScala.toMap ++ DeltaConfigs
        .validateConfigurations(parameters.filterKeys(_.startsWith("delta.")).toMap)
    } else {
      DeltaConfigs.validateConfigurations(parameters.filterKeys(_.startsWith("delta.")).toMap)
    }
    WriteIntoDelta(
      deltaLog = deltaLog,
      mode = mode,
      new DeltaOptions(parameters, sqlContext.sparkSession.sessionState.conf),
      partitionColumns = partitionColumns,
      configuration = configs,
      data = data
    ).run(sqlContext.sparkSession)

    deltaLog.createRelation()
  }

  override def createRelation(
      sqlContext: SQLContext,
      parameters: Map[String, String]): BaseRelation = {
    recordFrameProfile("Delta", "DeltaDataSource.createRelation") {
      val maybePath = parameters.getOrElse("path", throw DeltaErrors.pathNotSpecifiedException)

      // Log any invalid options that are being passed in
      DeltaOptions.verifyOptions(CaseInsensitiveMap(parameters))

      val timeTravelByParams = DeltaDataSource.getTimeTravelVersion(parameters)
      var cdcOptions: mutable.Map[String, String] = mutable.Map.empty
      val caseInsensitiveParams = new CaseInsensitiveStringMap(parameters.asJava)
      if (CDCReader.isCDCRead(caseInsensitiveParams)) {
        cdcOptions = mutable.Map[String, String](DeltaDataSource.CDC_ENABLED_KEY -> "true")
        if (caseInsensitiveParams.containsKey(DeltaDataSource.CDC_START_VERSION_KEY)) {
          cdcOptions(DeltaDataSource.CDC_START_VERSION_KEY) =
            caseInsensitiveParams.get(DeltaDataSource.CDC_START_VERSION_KEY)
        }
        if (caseInsensitiveParams.containsKey(DeltaDataSource.CDC_START_TIMESTAMP_KEY)) {
          cdcOptions(DeltaDataSource.CDC_START_TIMESTAMP_KEY) =
            caseInsensitiveParams.get(DeltaDataSource.CDC_START_TIMESTAMP_KEY)
        }
        if (caseInsensitiveParams.containsKey(DeltaDataSource.CDC_END_VERSION_KEY)) {
          cdcOptions(DeltaDataSource.CDC_END_VERSION_KEY) =
            caseInsensitiveParams.get(DeltaDataSource.CDC_END_VERSION_KEY)
        }
        if (caseInsensitiveParams.containsKey(DeltaDataSource.CDC_END_TIMESTAMP_KEY)) {
          cdcOptions(DeltaDataSource.CDC_END_TIMESTAMP_KEY) =
            caseInsensitiveParams.get(DeltaDataSource.CDC_END_TIMESTAMP_KEY)
        }
      }
      val dfOptions: Map[String, String] =
        if (
          sqlContext.sparkSession.sessionState.conf.getConf(
            DeltaSQLConf.LOAD_FILE_SYSTEM_CONFIGS_FROM_DATAFRAME_OPTIONS)
        ) {
          parameters ++ cdcOptions
        } else {
          cdcOptions.toMap
        }
      (new ClickHouseTableV2(
        sqlContext.sparkSession,
        new Path(maybePath),
        timeTravelOpt = timeTravelByParams,
        options = dfOptions
      )).toBaseRelation
    }
  }
}
