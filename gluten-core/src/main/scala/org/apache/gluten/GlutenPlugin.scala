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
package org.apache.gluten

import org.apache.gluten.GlutenBuildInfo._
import org.apache.gluten.component.Component
import org.apache.gluten.config.GlutenConfig
import org.apache.gluten.config.GlutenConfig._
import org.apache.gluten.events.GlutenBuildInfoEvent
import org.apache.gluten.exception.GlutenException
import org.apache.gluten.extension.GlutenSessionExtensions
import org.apache.gluten.task.TaskListener

import org.apache.spark.{SparkConf, SparkContext, TaskFailedReason}
import org.apache.spark.api.plugin.{DriverPlugin, ExecutorPlugin, PluginContext, SparkPlugin}
import org.apache.spark.internal.Logging
import org.apache.spark.network.util.JavaUtils
import org.apache.spark.softaffinity.SoftAffinityListener
import org.apache.spark.sql.execution.ui.{GlutenEventUtils, GlutenSQLAppStatusListener}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.internal.StaticSQLConf.SPARK_SESSION_EXTENSIONS
import org.apache.spark.task.TaskResources
import org.apache.spark.util.SparkResourceUtil

import java.util
import java.util.Collections

import scala.collection.mutable

class GlutenPlugin extends SparkPlugin {
  override def driverPlugin(): DriverPlugin = {
    new GlutenDriverPlugin()
  }

  override def executorPlugin(): ExecutorPlugin = {
    new GlutenExecutorPlugin()
  }
}

private[gluten] class GlutenDriverPlugin extends DriverPlugin with Logging {
  private var _sc: Option[SparkContext] = None

  override def init(sc: SparkContext, pluginContext: PluginContext): util.Map[String, String] = {
    _sc = Some(sc)
    val conf = pluginContext.conf()

    // Register Gluten listeners
    GlutenSQLAppStatusListener.register(sc)
    if (
      conf.getBoolean(
        GLUTEN_SOFT_AFFINITY_ENABLED.key,
        GLUTEN_SOFT_AFFINITY_ENABLED.defaultValue.get)
    ) {
      SoftAffinityListener.register(sc)
    }

    postBuildInfoEvent(sc)

    setPredefinedConfigs(conf)

    // Initialize Backend.
    Component.sorted().foreach(_.onDriverStart(sc, pluginContext))

    Collections.emptyMap()
  }

  override def registerMetrics(appId: String, pluginContext: PluginContext): Unit = {
    if (
      pluginContext.conf().getBoolean(GLUTEN_UI_ENABLED.key, GLUTEN_UI_ENABLED.defaultValue.get)
    ) {
      _sc.foreach {
        sc =>
          GlutenEventUtils.attachUI(sc)
          logInfo("Gluten SQL Tab has attached.")
      }
    }
  }

  override def shutdown(): Unit = {
    Component.sorted().reverse.foreach(_.onDriverShutdown())
  }

  private def postBuildInfoEvent(sc: SparkContext): Unit = {
    // export gluten version to property to spark
    System.setProperty("gluten.version", VERSION)

    val glutenBuildInfo = new mutable.LinkedHashMap[String, String]()

    val components = Component.sorted()
    glutenBuildInfo.put("Components", components.map(_.buildInfo().name).mkString(", "))
    components.foreach {
      comp =>
        val buildInfo = comp.buildInfo()
        glutenBuildInfo.put(s"Component ${buildInfo.name} Branch", buildInfo.branch)
        glutenBuildInfo.put(s"Component ${buildInfo.name} Revision", buildInfo.revision)
        glutenBuildInfo.put(s"Component ${buildInfo.name} Revision Time", buildInfo.revisionTime)
    }

    glutenBuildInfo.put("Gluten Version", VERSION)
    glutenBuildInfo.put("GCC Version", GCC_VERSION)
    glutenBuildInfo.put("Java Version", JAVA_COMPILE_VERSION)
    glutenBuildInfo.put("Scala Version", SCALA_COMPILE_VERSION)
    glutenBuildInfo.put("Spark Version", SPARK_COMPILE_VERSION)
    glutenBuildInfo.put("Hadoop Version", HADOOP_COMPILE_VERSION)
    glutenBuildInfo.put("Gluten Branch", BRANCH)
    glutenBuildInfo.put("Gluten Revision", REVISION)
    glutenBuildInfo.put("Gluten Revision Time", REVISION_TIME)
    glutenBuildInfo.put("Gluten Build Time", BUILD_DATE)
    glutenBuildInfo.put("Gluten Repo URL", REPO_URL)

    val loggingInfo = glutenBuildInfo
      .map { case (name, value) => s"$name: $value" }
      .mkString(
        "Gluten build info:\n==============================================================\n",
        "\n",
        "\n=============================================================="
      )
    logInfo(loggingInfo)
    val event = GlutenBuildInfoEvent(glutenBuildInfo.toMap)
    GlutenEventUtils.post(sc, event)
  }

  private def setPredefinedConfigs(conf: SparkConf): Unit = {
    // Spark SQL extensions
    val extensions = if (conf.contains(SPARK_SESSION_EXTENSIONS.key)) {
      s"${conf.get(SPARK_SESSION_EXTENSIONS.key)}," +
        s"${GlutenSessionExtensions.GLUTEN_SESSION_EXTENSION_NAME}"
    } else {
      s"${GlutenSessionExtensions.GLUTEN_SESSION_EXTENSION_NAME}"
    }
    conf.set(SPARK_SESSION_EXTENSIONS.key, extensions)

    // adaptive custom cost evaluator class
    val enableGlutenCostEvaluator = conf.getBoolean(
      GlutenConfig.COST_EVALUATOR_ENABLED.key,
      GlutenConfig.COST_EVALUATOR_ENABLED.defaultValue.get)
    if (enableGlutenCostEvaluator) {
      val costEvaluator = "org.apache.spark.sql.execution.adaptive.GlutenCostEvaluator"
      conf.set(SQLConf.ADAPTIVE_CUSTOM_COST_EVALUATOR_CLASS.key, costEvaluator)
    }

    // check memory off-heap enabled and size
    val minOffHeapSize = "1MB"
    if (
      !conf.getBoolean(
        DYNAMIC_OFFHEAP_SIZING_ENABLED.key,
        DYNAMIC_OFFHEAP_SIZING_ENABLED.defaultValue.get) &&
      (!conf.getBoolean(GlutenConfig.SPARK_OFFHEAP_ENABLED, false) ||
        conf.getSizeAsBytes(GlutenConfig.SPARK_OFFHEAP_SIZE_KEY, 0) < JavaUtils.byteStringAsBytes(
          minOffHeapSize))
    ) {
      throw new GlutenException(
        s"Must set '$SPARK_OFFHEAP_ENABLED' to true " +
          s"and set '$SPARK_OFFHEAP_SIZE_KEY' to be greater than $minOffHeapSize")
    }

    // Session's local time zone must be set. If not explicitly set by user, its default
    // value (detected for the platform) is used, consistent with spark.
    conf.set(GLUTEN_DEFAULT_SESSION_TIMEZONE.key, SQLConf.SESSION_LOCAL_TIMEZONE.defaultValueString)

    // Task slots.
    val taskSlots = SparkResourceUtil.getTaskSlots(conf)
    conf.set(NUM_TASK_SLOTS_PER_EXECUTOR.key, taskSlots.toString)

    val onHeapSize: Long = conf.getSizeAsBytes(SPARK_ONHEAP_SIZE_KEY, 1024 * 1024 * 1024)

    // If dynamic off-heap sizing is enabled, the off-heap size is calculated based on the on-heap
    // size. Otherwise, the off-heap size is set to the value specified by the user (if any).
    // Note that this means that we will IGNORE the off-heap size specified by the user if the
    // dynamic off-heap feature is enabled.
    val offHeapSize: Long =
      if (
        conf.getBoolean(
          DYNAMIC_OFFHEAP_SIZING_ENABLED.key,
          DYNAMIC_OFFHEAP_SIZING_ENABLED.defaultValue.get)
      ) {
        // Since when dynamic off-heap sizing is enabled, we commingle on-heap
        // and off-heap memory, we set the off-heap size to the usable on-heap size. We will
        // size it with a memory fraction, which can be aggressively set, but the default
        // is using the same way that Spark sizes on-heap memory:
        //
        // spark.gluten.memory.dynamic.offHeap.sizing.memory.fraction *
        //    (spark.executor.memory - 300MB).
        //
        // We will be careful to use the same configuration settings as Spark to ensure
        // that we are sizing the off-heap memory in the same way as Spark sizes on-heap memory.
        // The 300MB value, unfortunately, is hard-coded in Spark code.
        ((onHeapSize - (300 * 1024 * 1024)) *
          conf.getDouble(DYNAMIC_OFFHEAP_SIZING_MEMORY_FRACTION.key, 0.6d)).toLong
      } else {
        // Optimistic off-heap sizes, assuming all storage memory can be borrowed into execution
        // memory pool, regardless of Spark option spark.memory.storageFraction.
        conf.getSizeAsBytes(SPARK_OFFHEAP_SIZE_KEY, 0L)
      }

    conf.set(COLUMNAR_OFFHEAP_SIZE_IN_BYTES.key, offHeapSize.toString)
    conf.set(SPARK_OFFHEAP_SIZE_KEY, offHeapSize.toString)

    val offHeapPerTask = offHeapSize / taskSlots
    conf.set(COLUMNAR_TASK_OFFHEAP_SIZE_IN_BYTES.key, offHeapPerTask.toString)

    // If we are using dynamic off-heap sizing, we should also enable off-heap memory
    // officially.
    if (
      conf.getBoolean(
        DYNAMIC_OFFHEAP_SIZING_ENABLED.key,
        DYNAMIC_OFFHEAP_SIZING_ENABLED.defaultValue.get)
    ) {
      conf.set(SPARK_OFFHEAP_ENABLED, "true")

      // We already sized the off-heap per task in a conservative manner, so we can just
      // use it.
      conf.set(COLUMNAR_CONSERVATIVE_TASK_OFFHEAP_SIZE_IN_BYTES.key, offHeapPerTask.toString)
    } else {
      // Let's make sure this is set to false explicitly if it is not on as it
      // is looked up when throwing OOF exceptions.
      conf.set(
        DYNAMIC_OFFHEAP_SIZING_ENABLED.key,
        DYNAMIC_OFFHEAP_SIZING_ENABLED.defaultValueString)

      // Pessimistic off-heap sizes, with the assumption that all non-borrowable storage memory
      // determined by spark.memory.storageFraction was used.
      val fraction = 1.0d - conf.getDouble("spark.memory.storageFraction", 0.5d)
      val conservativeOffHeapPerTask = (offHeapSize * fraction).toLong / taskSlots
      conf.set(
        COLUMNAR_CONSERVATIVE_TASK_OFFHEAP_SIZE_IN_BYTES.key,
        conservativeOffHeapPerTask.toString)
    }

    // Disable vanilla columnar readers, to prevent columnar-to-columnar conversions.
    // FIXME: Do we still need this trick since
    //  https://github.com/apache/incubator-gluten/pull/1931 was merged?
    if (
      !conf.getBoolean(
        VANILLA_VECTORIZED_READERS_ENABLED.key,
        VANILLA_VECTORIZED_READERS_ENABLED.defaultValue.get)
    ) {
      // FIXME Hongze 22/12/06
      //  BatchScan.scala in shim was not always loaded by class loader.
      //  The file should be removed and the "ClassCastException" issue caused by
      //  spark.sql.<format>.enableVectorizedReader=true should be fixed in another way.
      //  Before the issue is fixed we force the use of vanilla row reader by using
      //  the following statement.
      conf.set(SQLConf.PARQUET_VECTORIZED_READER_ENABLED.key, "false")
      conf.set(SQLConf.ORC_VECTORIZED_READER_ENABLED.key, "false")
      conf.set(SQLConf.CACHE_VECTORIZED_READER_ENABLED.key, "false")
    }
  }
}

private[gluten] class GlutenExecutorPlugin extends ExecutorPlugin {
  private val taskListeners: Seq[TaskListener] = Seq(TaskResources)

  /** Initialize the executor plugin. */
  override def init(ctx: PluginContext, extraConf: util.Map[String, String]): Unit = {
    // Initialize Backend.
    Component.sorted().foreach(_.onExecutorStart(ctx))
  }

  /** Clean up and terminate this plugin. For example: close the native engine. */
  override def shutdown(): Unit = {
    Component.sorted().reverse.foreach(_.onExecutorShutdown())
    super.shutdown()
  }

  override def onTaskStart(): Unit = {
    taskListeners.foreach(_.onTaskStart())
  }

  override def onTaskSucceeded(): Unit = {
    taskListeners.reverse.foreach(_.onTaskSucceeded())
  }

  override def onTaskFailed(failureReason: TaskFailedReason): Unit = {
    taskListeners.reverse.foreach(_.onTaskFailed(failureReason))
  }
}

private object GlutenPlugin {}
