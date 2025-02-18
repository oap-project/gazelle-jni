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
package org.apache.spark.sql.hive.execution

import org.apache.gluten.execution.datasource.GlutenFormatFactory

import org.apache.spark.internal.Logging
import org.apache.spark.internal.config.SPECULATION_ENABLED
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.errors.QueryExecutionErrors
import org.apache.spark.sql.execution.datasources.{FileFormat, OutputWriter, OutputWriterFactory}
import org.apache.spark.sql.execution.datasources.orc.OrcOptions
import org.apache.spark.sql.execution.datasources.parquet.ParquetOptions
import org.apache.spark.sql.hive.{HiveInspectors, HiveTableUtil}
import org.apache.spark.sql.hive.HiveShim.{ShimFileSinkDesc => FileSinkDesc}
import org.apache.spark.sql.sources.DataSourceRegister
import org.apache.spark.sql.types.StructType
import org.apache.spark.util.SerializableJobConf

import org.apache.hadoop.fs.{FileStatus, Path}
import org.apache.hadoop.hive.ql.exec.Utilities
import org.apache.hadoop.hive.ql.io.{HiveFileFormatUtils, HiveOutputFormat}
import org.apache.hadoop.hive.serde2.Serializer
import org.apache.hadoop.hive.serde2.objectinspector.{ObjectInspectorUtils, StructObjectInspector}
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils.ObjectInspectorCopyOption
import org.apache.hadoop.io.Writable
import org.apache.hadoop.mapred.{JobConf, Reporter}
import org.apache.hadoop.mapreduce.{Job, TaskAttemptContext}
import org.apache.parquet.hadoop.ParquetOutputFormat
import org.apache.parquet.hadoop.metadata.CompressionCodecName

import scala.collection.JavaConverters._

/* -
 * This class is copied from Spark 3.2 and modified for Gluten. \n
 * Gluten should make sure this class is loaded before the original class.
 * If new Spark releases accepts changes and make this class incompatible,
 * we can move this class to shims-spark32,
 * shims-spark33, etc.
 */

/** Offload the parquet write of InsertIntoHiveDirCommand to backend when enable gluten plugin. */
class HiveFileFormat(fileSinkConf: FileSinkDesc)
  extends FileFormat
  with DataSourceRegister
  with Logging {

  def this() = this(null)

  override def shortName(): String = "hive"

  override def inferSchema(
      sparkSession: SparkSession,
      options: Map[String, String],
      files: Seq[FileStatus]): Option[StructType] = {
    throw QueryExecutionErrors.inferSchemaUnsupportedForHiveError()
  }

  override def prepareWrite(
      sparkSession: SparkSession,
      job: Job,
      options: Map[String, String],
      dataSchema: StructType): OutputWriterFactory = {

    val conf = job.getConfiguration
    val tableDesc = fileSinkConf.getTableInfo
    conf.set("mapred.output.format.class", tableDesc.getOutputFileFormatClassName)

    // When speculation is on and output committer class name contains "Direct", we should warn
    // users that they may loss data if they are using a direct output committer.
    val speculationEnabled = sparkSession.sparkContext.conf.get(SPECULATION_ENABLED)
    val outputCommitterClass = conf.get("mapred.output.committer.class", "")
    if (speculationEnabled && outputCommitterClass.contains("Direct")) {
      val warningMessage =
        s"$outputCommitterClass may be an output committer that writes data directly to " +
          "the final location. Because speculation is enabled, this output committer may " +
          "cause data loss (see the case in SPARK-10063). If possible, please use an output " +
          "committer that does not have this behavior (e.g. FileOutputCommitter)."
      logWarning(warningMessage)
    }

    // Add table properties from storage handler to hadoopConf, so any custom storage
    // handler settings can be set to hadoopConf
    HiveTableUtil.configureJobPropertiesForStorageHandler(tableDesc, conf, false)
    Utilities.copyTableJobPropertiesToConf(tableDesc, conf)

    // Avoid referencing the outer object.
    val fileSinkConfSer = fileSinkConf
    val outputFormat = fileSinkConf.tableInfo.getOutputFileFormatClassName
    if ("true" == sparkSession.sparkContext.getLocalProperty("isNativeApplicable")) {
      val nativeFormat = sparkSession.sparkContext.getLocalProperty("nativeFormat")
      val tableOptions = tableDesc.getProperties.asScala.toMap
      val compressionCodec = nativeFormat match {
        case "parquet" if fileSinkConf.compressed =>
          // MapredParquetOutputFormat use the `ParquetOutputFormat.COMPRESSION` as
          // the compression codec. Currently, we only support the parquet native
          // writer for Spark3.2/3.3 code path.
          tableOptions.getOrElse(
            ParquetOutputFormat.COMPRESSION,
            conf.get(ParquetOutputFormat.COMPRESSION, CompressionCodecName.UNCOMPRESSED.name))
        case "parquet" =>
          val parquetOptions =
            new ParquetOptions(tableOptions, sparkSession.sessionState.conf)
          parquetOptions.compressionCodecClassName
        case _ =>
          if (fileSinkConf.compressed) {
            fileSinkConf.compressCodec
          } else {
            val orcOptions = new OrcOptions(tableOptions, sparkSession.sessionState.conf)
            orcOptions.compressionCodec
          }
      }

      val nativeConf =
        GlutenFormatFactory(nativeFormat).nativeConf(tableOptions, compressionCodec)

      new OutputWriterFactory {
        private val jobConf = new SerializableJobConf(new JobConf(conf))
        @transient private lazy val outputFormat =
          jobConf.value.getOutputFormat.asInstanceOf[HiveOutputFormat[AnyRef, Writable]]

        override def getFileExtension(context: TaskAttemptContext): String = {
          Utilities.getFileExtension(jobConf.value, fileSinkConfSer.getCompressed, outputFormat)
        }

        override def newInstance(
            path: String,
            dataSchema: StructType,
            context: TaskAttemptContext): OutputWriter = {
          GlutenFormatFactory(nativeFormat)
            .createOutputWriter(path, dataSchema, context, nativeConf)
        }
      }
    } else {
      new OutputWriterFactory {
        private val jobConf = new SerializableJobConf(new JobConf(conf))
        @transient private lazy val outputFormat =
          jobConf.value.getOutputFormat.asInstanceOf[HiveOutputFormat[AnyRef, Writable]]

        override def getFileExtension(context: TaskAttemptContext): String = {
          Utilities.getFileExtension(jobConf.value, fileSinkConfSer.getCompressed, outputFormat)
        }

        override def newInstance(
            path: String,
            dataSchema: StructType,
            context: TaskAttemptContext): OutputWriter = {
          new HiveOutputWriter(path, fileSinkConfSer, jobConf.value, dataSchema)
        }
      }
    }
  }
}

class HiveOutputWriter(
    val path: String,
    fileSinkConf: FileSinkDesc,
    jobConf: JobConf,
    dataSchema: StructType)
  extends OutputWriter
  with HiveInspectors {

  private def tableDesc = fileSinkConf.getTableInfo

  private val serializer = {
    val serializer =
      tableDesc.getDeserializerClass.getConstructor().newInstance().asInstanceOf[Serializer]
    serializer.initialize(jobConf, tableDesc.getProperties)
    serializer
  }

  private val hiveWriter = HiveFileFormatUtils.getHiveRecordWriter(
    jobConf,
    tableDesc,
    serializer.getSerializedClass,
    fileSinkConf,
    new Path(path),
    Reporter.NULL)

  /**
   * Since SPARK-30201 ObjectInspectorCopyOption.JAVA change to ObjectInspectorCopyOption.DEFAULT.
   * The reason is DEFAULT option can convert `UTF8String` to `Text` with bytes and we can
   * compatible with non UTF-8 code bytes during write.
   */
  private val standardOI = ObjectInspectorUtils
    .getStandardObjectInspector(
      tableDesc.getDeserializer(jobConf).getObjectInspector,
      ObjectInspectorCopyOption.DEFAULT)
    .asInstanceOf[StructObjectInspector]

  private val fieldOIs =
    standardOI.getAllStructFieldRefs.asScala.map(_.getFieldObjectInspector).toArray
  private val dataTypes = dataSchema.map(_.dataType).toArray
  private val wrappers = fieldOIs.zip(dataTypes).map { case (f, dt) => wrapperFor(f, dt) }
  private val outputData = new Array[Any](fieldOIs.length)

  override def write(row: InternalRow): Unit = {
    var i = 0
    while (i < fieldOIs.length) {
      outputData(i) = if (row.isNullAt(i)) null else wrappers(i)(row.get(i, dataTypes(i)))
      i += 1
    }
    hiveWriter.write(serializer.serialize(outputData, standardOI))
  }

  override def close(): Unit = {
    // Seems the boolean value passed into close does not matter.
    hiveWriter.close(false)
  }
}
