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
package org.apache.spark.gluten.delta

import org.apache.spark.sql.{wrapper, DataFrame, SparkSession}
import org.apache.spark.sql.delta.{DeltaColumnMappingMode, DeltaLog, DeltaLogFileIndex, NoMapping}
import org.apache.spark.sql.delta.actions.Protocol
import org.apache.spark.sql.delta.actions.TableFeatureProtocolUtils.TABLE_FEATURES_MIN_WRITER_VERSION
import org.apache.spark.sql.delta.stats.{DeltaStatsColumnSpec, StatisticsCollection}
import org.apache.spark.sql.functions.{col, from_json, regexp_replace}
import org.apache.spark.sql.types._

import org.apache.hadoop.fs.{FileSystem, Path}

import java.util.Locale

case class Statistics(override val tableSchema: StructType) extends StatisticsCollection {
  override val outputAttributeSchema: StructType = tableSchema
  // [[outputTableStatsSchema]] is the candidate schema to find statistics columns.
  override val outputTableStatsSchema: StructType = tableSchema
  override val statsColumnSpec = DeltaStatsColumnSpec(None, Some(32))
  override val columnMappingMode: DeltaColumnMappingMode = NoMapping
  override val protocol: Protocol = Protocol(
    minReaderVersion = 1,
    minWriterVersion = TABLE_FEATURES_MIN_WRITER_VERSION,
    readerFeatures = None,
    writerFeatures = Some(Set()))

  override def spark: SparkSession = {
    throw new Exception("Method not used in statisticsCollectionFromMetadata")
  }
}

object DeltaStatsUtils {

  private def stringToDataType(dataType: String): DataType =
    dataType.toLowerCase(Locale.ROOT) match {
      case "bigint" => LongType
      case "double" => DoubleType
      case "string" => StringType
      case "date" => DateType
      case _ => throw new IllegalArgumentException(s"Unsupported data type: $dataType")
    }

  /**
   * Parse a schema string as follows into a [[StructType]].
   * {{{
   *  l_orderkey      bigint,
   *  l_partkey       bigint,
   *  l_suppkey       bigint,
   *  l_linenumber    bigint
   * }}}
   */
  private def stringToSchema(schemaString: String): StructType = {
    val fields = schemaString.trim.split(",\\s*").map {
      fieldString =>
        val parts = fieldString.trim.split("\\s+")
        require(parts.length == 2, s"Invalid field definition: $fieldString")
        val fieldName = parts(0).trim
        val fieldType = stringToDataType(parts(1).trim)
        StructField(fieldName, fieldType, nullable = true)
    }
    StructType(fields)
  }

  def statsDF(
      sparkSession: SparkSession,
      deltaJson: String,
      schema: String
  ): DataFrame = {

    val statistics = Statistics(stringToSchema(schema))

    val hadoopConf = sparkSession.sessionState.newHadoopConfWithOptions(Map.empty)
    wrapper
      .ofRows(
        sparkSession,
        DeltaLog.indexToRelation(
          sparkSession,
          DeltaLogFileIndex(
            DeltaLogFileIndex.COMMIT_FILE_FORMAT,
            FileSystem.get(hadoopConf),
            Seq(new Path(deltaJson))),
          Map.empty)
      )
      .select("add")
      .filter("add is not null")
      .withColumns(Map(
        "path" -> regexp_replace(
          col("add.path"),
          "-[0-9A-Fa-f]{8}-[0-9A-Fa-f]{4}-[0-9A-Fa-f]{4}-[0-9A-Fa-f]{4}-[0-9A-Fa-f]{12}",
          ""
        ), // normalize file name
        "stats" -> from_json(col("add.stats"), statistics.statsSchema)
      ))
      .select(
        "path",
        "stats.numRecords",
        "stats.minValues.*",
        "stats.maxValues.*",
        "stats.nullCount.*")
  }

}
