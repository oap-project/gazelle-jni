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
package org.apache.hadoop.fs.viewfs

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}

import scala.collection.mutable

object ViewFileSystemUtils {

  /**
   * Convert the viewfs path to hdfs path. Similar to ViewFileSystem.resolvePath, but does not make
   * RPC calls.
   */
  def convertViewfsToHdfs(f: String, hadoopConfig: Configuration): String = {
    val path = new Path(f)
    FileSystem.get(path.toUri, hadoopConfig) match {
      case vfs: ViewFileSystem =>
        val fsStateField = vfs.getClass.getDeclaredField("fsState")
        fsStateField.setAccessible(true)
        val fsState = fsStateField.get(vfs).asInstanceOf[InodeTree[FileSystem]]
        val res = fsState.resolve(f, true)
        if (res.isInternalDir) {
          f
        } else {
          Path.mergePaths(new Path(res.targetFileSystem.getUri), res.remainingPath).toString
        }
      case otherFileSystem =>
        otherFileSystem.resolvePath(path).toString
    }
  }

  /**
   * Convert a sequence of viewfs path to a sequence of hdfs path.
   * @param paths
   *   sequence of viewfs path
   * @param viewfsToHdfsCache
   *   A map use to cache converted paths
   * @param hadoopConfig
   *   Hadoop configuration
   * @return
   *   sequence of hdfs path
   */
  def convertViewfsToHdfs(
      paths: Seq[String],
      viewfsToHdfsCache: mutable.Map[String, String],
      hadoopConfig: Configuration): Seq[String] = {
    paths.map {
      path =>
        if (path.startsWith("viewfs")) {
          val pathSplit = path.split(Path.SEPARATOR)
          val prefixIndex = pathSplit.size - 1
          val pathPrefix = pathSplit.take(prefixIndex).mkString(Path.SEPARATOR)
          val hdfsPath = viewfsToHdfsCache.getOrElseUpdate(
            pathPrefix,
            convertViewfsToHdfs(pathPrefix, hadoopConfig))
          hdfsPath + Path.SEPARATOR + pathSplit.drop(prefixIndex).mkString(Path.SEPARATOR)
        } else {
          path
        }
    }
  }
}
