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

package io.glutenproject

import java.util
import java.util.{Collections, Objects}
import scala.language.implicitConversions
import io.glutenproject.GlutenPlugin.{GLUTEN_SESSION_EXTENSION_NAME, SPARK_SESSION_EXTS_KEY}
import io.glutenproject.backendsapi.BackendsApiManager
import io.glutenproject.extension.{ColumnarOverrides, ColumnarQueryStagePrepOverrides, OthersExtensionOverrides, StrategyOverrides}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.api.plugin.{DriverPlugin, ExecutorPlugin, PluginContext, SparkPlugin}
import org.apache.spark.network.util.JavaUtils
import org.apache.spark.sql.SparkSessionExtensions
import org.apache.spark.sql.internal.StaticSQLConf

class GlutenPlugin extends SparkPlugin {
  override def driverPlugin(): DriverPlugin = {
    new GlutenDriverPlugin()
  }

  override def executorPlugin(): ExecutorPlugin = {
    new GlutenExecutorPlugin()
  }
}

private[glutenproject] class GlutenDriverPlugin extends DriverPlugin {
  override def init(sc: SparkContext, pluginContext: PluginContext): util.Map[String, String] = {
    // Set the system properties.
    // Use appending policy for children with the same name in a arrow struct vector.
    System.setProperty("arrow.struct.conflict.policy", "CONFLICT_APPEND")
    val conf = pluginContext.conf()
    setPredefinedConfigs(sc, conf)
    // Initialize Backends API
    BackendsApiManager.initialize()
    BackendsApiManager.getInitializerApiInstance.initialize(conf)
    Collections.emptyMap()
  }

  def setPredefinedConfigs(sc: SparkContext, conf: SparkConf): Unit = {
    val extensions = if (conf.contains(SPARK_SESSION_EXTS_KEY)) {
      s"${conf.get(SPARK_SESSION_EXTS_KEY)},${GLUTEN_SESSION_EXTENSION_NAME}"
    } else {
      s"${GLUTEN_SESSION_EXTENSION_NAME}"
    }
    conf.set(SPARK_SESSION_EXTS_KEY, String.format("%s", extensions))
    if (BackendsApiManager.getSettings.disableVanillaColumnarReaders()) {
      // FIXME Hongze 22/12/06
      //  BatchScan.scala in shim was not always loaded by class loader.
      //  The file should be removed and the "ClassCastException" issue caused by
      //  spark.sql.<format>.enableVectorizedReader=true should be fixed in another way.
      //  Before the issue was fixed we force the use of vanilla row reader by using
      //  the following statement.
      conf.set("spark.sql.parquet.enableVectorizedReader", "false")
      conf.set("spark.sql.orc.enableVectorizedReader", "false")
      conf.set("spark.sql.inMemoryColumnarStorage.enableVectorizedReader", "false")
    }
    val hdfsUri = sc.hadoopConfiguration.get("fs.defaultFS", "hdfs://localhost:9000")
    if (!conf.contains(GlutenConfig.SPARK_HDFS_URI)) {
      conf.set(GlutenConfig.SPARK_HDFS_URI, hdfsUri)
    }
  }
}

private[glutenproject] class GlutenExecutorPlugin extends ExecutorPlugin {
  /**
   * Initialize the executor plugin.
   */
  override def init(ctx: PluginContext, extraConf: util.Map[String, String]): Unit = {
    // Set the system properties.
    // Use appending policy for children with the same name in a arrow struct vector.
    System.setProperty("arrow.struct.conflict.policy", "CONFLICT_APPEND")
    val conf = ctx.conf()
    // Must set the 'spark.memory.offHeap.size' value to native memory malloc
    if (!conf.getBoolean("spark.memory.offHeap.enabled", false) ||
      (JavaUtils.byteStringAsBytes(
        conf.get("spark.memory.offHeap.size").toString) / 1024 / 1024).toInt <= 0) {
      throw new IllegalArgumentException(s"Must set the 'spark.memory.offHeap.enabled' to true" +
        s" and set the off heap memory size of the 'spark.memory.offHeap.size'")
    }
    // Initialize Backends API
    BackendsApiManager.initialize()
    BackendsApiManager.getInitializerApiInstance.initialize(conf)
  }

  /**
   * Clean up and terminate this plugin.
   * For example: close the native engine.
   */
  override def shutdown(): Unit = {
    super.shutdown()
  }
}

private[glutenproject] class GlutenSessionExtensions extends (SparkSessionExtensions => Unit) {
  override def apply(exts: SparkSessionExtensions): Unit = {
    GlutenPlugin.DEFAULT_INJECTORS.foreach(injector => injector.inject(exts))
  }
}

private[glutenproject] class SparkConfImplicits(conf: SparkConf) {
  def enableGlutenPlugin(): SparkConf = {
    if (conf.contains(GlutenPlugin.SPARK_SQL_PLUGINS_KEY)) {
      throw new IllegalArgumentException("A Spark plugin is already specified before enabling " +
        "Gluten plugin: " + conf.get(GlutenPlugin.SPARK_SQL_PLUGINS_KEY))
    }
    conf.set(GlutenPlugin.SPARK_SQL_PLUGINS_KEY, GlutenPlugin.GLUTEN_PLUGIN_NAME)
  }
}

private[glutenproject] trait GlutenSparkExtensionsInjector {
  def inject(extensions: SparkSessionExtensions)
}

private[glutenproject] object GlutenPlugin {
  // To enable GlutenPlugin in production, set "spark.plugins=io.glutenproject.GlutenPlugin"
  val SPARK_SQL_PLUGINS_KEY: String = "spark.plugins"
  val GLUTEN_PLUGIN_NAME: String = Objects.requireNonNull(classOf[GlutenPlugin]
    .getCanonicalName)
  val SPARK_SESSION_EXTS_KEY: String = StaticSQLConf.SPARK_SESSION_EXTENSIONS.key
  val GLUTEN_SESSION_EXTENSION_NAME: String = Objects.requireNonNull(
    classOf[GlutenSessionExtensions].getCanonicalName)

  /**
   * Specify all injectors that Gluten is using in following list.
   */
  val DEFAULT_INJECTORS: List[GlutenSparkExtensionsInjector] = List(
    ColumnarQueryStagePrepOverrides,
    ColumnarOverrides,
    StrategyOverrides,
    OthersExtensionOverrides
  )

  implicit def sparkConfImplicit(conf: SparkConf): SparkConfImplicits = {
    new SparkConfImplicits(conf)
  }
}
