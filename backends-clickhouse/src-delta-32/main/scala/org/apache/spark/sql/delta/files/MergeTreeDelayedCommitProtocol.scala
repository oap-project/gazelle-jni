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
package org.apache.spark.sql.delta.files

import org.apache.hadoop.mapreduce.TaskAttemptContext

class MergeTreeDelayedCommitProtocol(
    val outputPath: String,
    randomPrefixLength: Option[Int],
    subdir: Option[String],
    val database: String,
    val tableName: String)
  extends DelayedCommitProtocol("delta-mergetree", outputPath, randomPrefixLength, subdir)
  with MergeTreeFileCommitProtocol {}

/**
 * A Wrapper of [[DelayedCommitProtocol]] for accessing protected methods and fields in a pipeline
 * write for parquet.
 */
class FileDelayedCommitProtocol(
    jobId: String,
    val outputPath: String,
    randomPrefixLength: Option[Int],
    subdir: Option[String])
  extends DelayedCommitProtocol(jobId, outputPath, randomPrefixLength, subdir) {

  override def getFileName(
      taskContext: TaskAttemptContext,
      ext: String,
      partitionValues: Map[String, String]): String = {
    super.getFileName(taskContext, ext, partitionValues)
  }

  def updateAddedFiles(files: Seq[(Map[String, String], String)]): Unit = {
    assert(addedFiles.isEmpty)
    addedFiles ++= files
  }

  override def parsePartitions(dir: String): Map[String, String] =
    super.parsePartitions(dir)
}

/**
 * A Wrapper of [[DelayedCommitProtocol]] for accessing protected methods and fields in a pipeline
 * write for mergetree.
 */
class MergeTreeDelayedCommitProtocol2(
    val outputPath: String,
    randomPrefixLength: Option[Int],
    subdir: Option[String],
    val database: String,
    val tableName: String)
  extends DelayedCommitProtocol("delta-mergetree", outputPath, randomPrefixLength, subdir) {

  override def newTaskTempFile(
      taskContext: TaskAttemptContext,
      dir: Option[String],
      ext: String): String = outputPath

}
