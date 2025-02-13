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
package org.apache.gluten.expression

import org.apache.gluten.config.GlutenConfig

import org.apache.spark.SparkConf
import org.apache.spark.internal.Logging

import org.apache.commons.lang3.StringUtils

import java.util.Locale

import scala.collection.mutable.Map

object UDFMappings extends Logging {
  val hiveUDFMap: Map[String, String] = Map()
  val pythonUDFMap: Map[String, String] = Map()
  val scalaUDFMap: Map[String, String] = Map()

  private def appendKVToMap(key: String, value: String, res: Map[String, String]): Unit = {
    if (key.isEmpty || value.isEmpty()) {
      throw new IllegalArgumentException(s"key:$key or value:$value is empty")
    }

    if (res.contains(key)) {
      logWarning(
        s"key:$key already exists in map:${res.toString}, " +
          s"will be replaced by value:$value")
    }

    res.put(key.toLowerCase(Locale.ROOT), value)
  }

  private def parseStringToMap(input: String, res: Map[String, String]): Unit = {
    input.split(",").map {
      item =>
        val keyValue = item.split(":")
        keyValue.length match {
          case 1 => appendKVToMap(keyValue(0), keyValue(0), res)
          case 2 => appendKVToMap(keyValue(0), keyValue(1), res)
        }
    }
  }

  def loadFromSparkConf(conf: SparkConf): Unit = {
    val strHiveUDFs = conf.get(GlutenConfig.GLUTEN_SUPPORTED_HIVE_UDFS.key, "")
    if (!StringUtils.isBlank(strHiveUDFs)) {
      parseStringToMap(strHiveUDFs, hiveUDFMap)
      logDebug(s"loaded hive udf mappings:${hiveUDFMap.toString}")
    }

    val strPythonUDFs = conf.get(GlutenConfig.GLUTEN_SUPPORTED_PYTHON_UDFS.key, "")
    if (!StringUtils.isBlank(strPythonUDFs)) {
      parseStringToMap(strPythonUDFs, pythonUDFMap)
      logDebug(s"loaded python udf mappings:${pythonUDFMap.toString}")
    }

    val strScalaUDFs = conf.get(GlutenConfig.GLUTEN_SUPPORTED_SCALA_UDFS.key, "")
    if (!StringUtils.isBlank(strScalaUDFs)) {
      parseStringToMap(strScalaUDFs, scalaUDFMap)
      logDebug(s"loaded scala udf mappings:${scalaUDFMap.toString}")
    }
  }
}
