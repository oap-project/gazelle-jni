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
package org.apache.spark.sql.execution.commands

import org.apache.gluten.backendsapi.clickhouse.CHConfig
import org.apache.gluten.substrait.rel.LocalFilesBuilder
import org.apache.gluten.substrait.rel.LocalFilesNode.ReadFileFormat

import org.apache.spark.affinity.CHAffinity
import org.apache.spark.rpc.GlutenDriverEndpoint
import org.apache.spark.rpc.GlutenRpcMessages.{CacheJobInfo, GlutenFilesCacheLoad}
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference}
import org.apache.spark.sql.execution.command.LeafRunnableCommand
import org.apache.spark.sql.execution.commands.GlutenCacheBase._
import org.apache.spark.sql.types.{BooleanType, StringType}

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, FileSystem, Path}

import java.io.FileNotFoundException
import java.lang.{Long => JLong}
import java.util.{ArrayList => JArrayList, HashMap => JHashMap, Map => JMap}

import scala.collection.mutable.ArrayBuffer
import scala.concurrent.Future

case class GlutenCacheFilesCommand(
    async: Boolean,
    selectedColumn: Option[Seq[String]],
    filePath: String,
    propertyOverrides: Map[String, String]
) extends LeafRunnableCommand {

  override def output: Seq[Attribute] = Seq(
    AttributeReference("result", BooleanType, nullable = false)(),
    AttributeReference("reason", StringType, nullable = false)())

  override def run(session: SparkSession): Seq[Row] = {
    if (
      !session.sparkContext.getConf.getBoolean(
        CHConfig.runtimeConfig("gluten_cache.local.enabled"),
        defaultValue = false)
    ) {
      return Seq(Row(false, "Config `gluten_cache.local.enabled` is disabled."))
    }

    val targetFile = new Path(filePath)
    val hadoopConf: Configuration = session.sparkContext.hadoopConfiguration
    val fs = targetFile.getFileSystem(hadoopConf)
    if (!fs.exists(targetFile)) {
      throw new FileNotFoundException(filePath)
    }

    val recursive =
      if ("true".equalsIgnoreCase(propertyOverrides.getOrElse("recursive", "false"))) {
        true
      } else {
        false
      }

    val files: Seq[FileStatus] = listFiles(targetFile, recursive, fs)
    val executorIdsToFiles =
      scala.collection.mutable.Map[String, ArrayBuffer[FileStatus]]()
    executorIdsToFiles.put(ALL_EXECUTORS, new ArrayBuffer[FileStatus]())

    files.foreach(
      fileStatus => {
        val locations = CHAffinity.getHostLocations(fileStatus.getPath.toUri.toASCIIString)
        if (locations.isEmpty) {
          executorIdsToFiles(ALL_EXECUTORS).append(fileStatus)
        } else {
          locations.foreach(
            executor => {
              if (!executorIdsToFiles.contains(executor)) {
                executorIdsToFiles.put(executor, new ArrayBuffer[FileStatus]())
              }
              executorIdsToFiles(executor).append(fileStatus)
            })
        }
      })

    val executorIdsToLocalFiles = executorIdsToFiles
      .filter(_._2.nonEmpty)
      .map {
        case (executorId, fileStatusArray) =>
          val paths = new JArrayList[String]()
          val starts = new JArrayList[JLong]()
          val lengths = new JArrayList[JLong]()
          val modificationTimes = new JArrayList[JLong]()
          val partitionColumns = new JArrayList[JMap[String, String]]

          fileStatusArray.foreach(
            fileStatus => {
              paths.add(fileStatus.getPath.toUri.toASCIIString)
              starts.add(JLong.valueOf(0))
              lengths.add(JLong.valueOf(fileStatus.getLen))
              modificationTimes.add(JLong.valueOf(fileStatus.getModificationTime))
              partitionColumns.add(new JHashMap[String, String]())
            })

          val localFile = LocalFilesBuilder.makeLocalFiles(
            null,
            paths,
            starts,
            lengths,
            lengths, /* fileSizes */
            modificationTimes,
            partitionColumns,
            new JArrayList[JMap[String, String]](),
            ReadFileFormat.ParquetReadFormat, // ignore format in backend
            new JArrayList[String](),
            new JHashMap[String, String]()
          )

          (executorId, localFile)
      }
      .toMap

    val futureList = ArrayBuffer[(String, Future[CacheJobInfo])]()
    val fileNodeOption = executorIdsToLocalFiles.get(ALL_EXECUTORS)
    if (fileNodeOption.isDefined) {
      GlutenDriverEndpoint.executorDataMap.forEach(
        (executorId, executor) => {
          futureList.append(
            (
              executorId,
              executor.executorEndpointRef.ask[CacheJobInfo](
                GlutenFilesCacheLoad(fileNodeOption.get.toProtobuf.toByteArray))))
        })
    } else {
      executorIdsToLocalFiles.foreach {
        case (executorId, fileNode) =>
          checkExecutorId(executorId)
          val executor = GlutenDriverEndpoint.executorDataMap.get(toExecutorId(executorId))
          futureList.append(
            (
              executorId,
              executor.executorEndpointRef.ask[CacheJobInfo](
                GlutenFilesCacheLoad(fileNode.toProtobuf.toByteArray))))
      }
    }

    getResult(futureList, async)
  }

  private def listFiles(targetFile: Path, recursive: Boolean, fs: FileSystem): Seq[FileStatus] = {
    val dirContents = fs
      .listStatus(targetFile)
      .flatMap(f => addInputPathRecursively(fs, f, recursive))
      .filter(isNonEmptyDataFile)
      .toSeq
    dirContents
  }

  private def addInputPathRecursively(
      fs: FileSystem,
      files: FileStatus,
      recursive: Boolean): Seq[FileStatus] = {
    if (files.isFile) {
      Seq(files)
    } else if (recursive) {
      fs.listStatus(files.getPath)
        .flatMap(
          file => {
            if (file.isFile) {
              Seq(file)
            } else {
              addInputPathRecursively(fs, file, recursive)
            }
          })
    } else {
      Seq()
    }
  }

  private def isNonEmptyDataFile(f: FileStatus): Boolean = {
    if (!f.isFile || f.getLen == 0) {
      false
    } else {
      val name = f.getPath.getName
      !((name.startsWith("_") && !name.contains("=")) || name.startsWith("."))
    }
  }
}
