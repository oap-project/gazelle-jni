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
package org.apache.gluten.backendsapi.velox

import org.apache.gluten.GlutenConfig
import org.apache.gluten.backendsapi.ListenerApi
import org.apache.gluten.columnarbatch.ArrowBatches.{ArrowJavaBatch, ArrowNativeBatch}
import org.apache.gluten.columnarbatch.VeloxBatch
import org.apache.gluten.execution.datasource.GlutenFormatFactory
import org.apache.gluten.expression.UDFMappings
import org.apache.gluten.init.NativeBackendInitializer
import org.apache.gluten.jni.{JniLibLoader, JniWorkspace}
import org.apache.gluten.udf.UdfJniWrapper
import org.apache.gluten.utils._

import org.apache.spark.{HdfsConfGenerator, SparkConf, SparkContext}
import org.apache.spark.api.plugin.PluginContext
import org.apache.spark.internal.Logging
import org.apache.spark.network.util.ByteUnit
import org.apache.spark.sql.execution.ColumnarCachedBatchSerializer
import org.apache.spark.sql.execution.datasources.GlutenWriterColumnarRules
import org.apache.spark.sql.execution.datasources.velox.{VeloxParquetWriterInjects, VeloxRowSplitter}
import org.apache.spark.sql.expression.UDFResolver
import org.apache.spark.sql.internal.{GlutenConfigUtil, StaticSQLConf}
import org.apache.spark.util.{SparkDirectoryUtil, SparkResourceUtil}

import org.apache.commons.lang3.StringUtils

import java.util.concurrent.atomic.AtomicBoolean

class VeloxListenerApi extends ListenerApi with Logging {
  import VeloxListenerApi._

  override def onDriverStart(sc: SparkContext, pc: PluginContext): Unit = {
    val conf = pc.conf()

    // Generate HDFS client configurations.
    HdfsConfGenerator.addHdfsClientToSparkWorkDirectory(sc)

    // Overhead memory limits.
    val offHeapSize = conf.getSizeAsBytes(GlutenConfig.SPARK_OFFHEAP_SIZE_KEY)
    val desiredOverheadSize = (0.3 * offHeapSize).toLong.max(ByteUnit.MiB.toBytes(384))
    if (!SparkResourceUtil.isMemoryOverheadSet(conf)) {
      // If memory overhead is not set by user, automatically set it according to off-heap settings.
      logInfo(
        s"Memory overhead is not set. Setting it to $desiredOverheadSize automatically." +
          " Gluten doesn't follow Spark's calculation on default value of this option because the" +
          " actual required memory overhead will depend on off-heap usage than on on-heap usage.")
      conf.set(
        GlutenConfig.SPARK_OVERHEAD_SIZE_KEY,
        ByteUnit.BYTE.toMiB(desiredOverheadSize).toString)
    }
    val overheadSize: Long = SparkResourceUtil.getMemoryOverheadSize(conf)
    if (overheadSize < desiredOverheadSize) {
      logWarning(
        s"Memory overhead is set to $overheadSize which is smaller than the recommended size" +
          s" $desiredOverheadSize. This may cause OOM.")
    }
    conf.set(GlutenConfig.GLUTEN_OVERHEAD_SIZE_IN_BYTES_KEY, overheadSize.toString)

    // Sql table cache serializer.
    if (conf.getBoolean(GlutenConfig.COLUMNAR_TABLE_CACHE_ENABLED.key, defaultValue = false)) {
      conf.set(
        StaticSQLConf.SPARK_CACHE_SERIALIZER.key,
        classOf[ColumnarCachedBatchSerializer].getName)
    }

    // Static initializers for driver.
    if (!driverInitialized.compareAndSet(false, true)) {
      // Make sure we call the static initializers only once.
      logInfo(
        "Skip rerunning static initializers since they are only supposed to run once." +
          " You see this message probably because you are creating a new SparkSession.")
      return
    }

    SparkDirectoryUtil.init(conf)
    UDFResolver.resolveUdfConf(conf, isDriver = true)
    initialize(conf, isDriver = true)
    UdfJniWrapper.registerFunctionSignatures()
  }

  override def onDriverShutdown(): Unit = shutdown()

  override def onExecutorStart(pc: PluginContext): Unit = {
    val conf = pc.conf()

    // Static initializers for executor.
    if (!executorInitialized.compareAndSet(false, true)) {
      // Make sure we call the static initializers only once.
      logInfo(
        "Skip rerunning static initializers since they are only supposed to run once." +
          " You see this message probably because you are creating a new SparkSession.")
      return
    }
    if (inLocalMode(conf)) {
      // Don't do static initializations from executor side in local mode.
      // Driver already did that.
      logInfo(
        "Gluten is running with Spark local mode. Skip running static initializer for executor.")
      return
    }

    SparkDirectoryUtil.init(conf)
    UDFResolver.resolveUdfConf(conf, isDriver = false)
    initialize(conf, isDriver = false)
  }

  override def onExecutorShutdown(): Unit = shutdown()

  private def initialize(conf: SparkConf, isDriver: Boolean): Unit = {
    // Force batch type initializations.
    VeloxBatch.ensureRegistered()
    ArrowJavaBatch.ensureRegistered()
    ArrowNativeBatch.ensureRegistered()

    // Sets this configuration only once, since not undoable.
    if (conf.getBoolean(GlutenConfig.GLUTEN_DEBUG_KEEP_JNI_WORKSPACE, defaultValue = false)) {
      val debugDir = conf.get(GlutenConfig.GLUTEN_DEBUG_KEEP_JNI_WORKSPACE_DIR)
      JniWorkspace.enableDebug(debugDir)
    }

    // Set the system properties.
    // Use appending policy for children with the same name in a arrow struct vector.
    System.setProperty("arrow.struct.conflict.policy", "CONFLICT_APPEND")

    // Load supported hive/python/scala udfs
    UDFMappings.loadFromSparkConf(conf)

    // Initial library loader.
    val loader = JniWorkspace.getDefault.libLoader

    // Load shared native libraries the backend libraries depend on.
    SharedLibraryLoader.load(conf, loader)

    // Load backend libraries.
    val libPath = conf.get(GlutenConfig.GLUTEN_LIB_PATH, StringUtils.EMPTY)
    if (StringUtils.isNotBlank(libPath)) { // Path based load. Ignore all other loadees.
      JniLibLoader.loadFromPath(libPath, false)
    } else {
      val baseLibName = conf.get(GlutenConfig.GLUTEN_LIB_NAME, "gluten")
      loader.load(s"$platformLibDir/${System.mapLibraryName(baseLibName)}", false)
      loader.load(s"$platformLibDir/${System.mapLibraryName(VeloxBackend.BACKEND_NAME)}", false)
    }

    // Initial native backend with configurations.
    var parsed = GlutenConfigUtil.parseConfig(conf.getAll.toMap)

    // Workaround for https://github.com/apache/incubator-gluten/issues/7837
    if (isDriver && !inLocalMode(conf)) {
      parsed += (GlutenConfig.COLUMNAR_VELOX_CACHE_ENABLED.key -> "false")
    }
    NativeBackendInitializer.forBackend(VeloxBackend.BACKEND_NAME).initialize(parsed)

    // Inject backend-specific implementations to override spark classes.
    GlutenFormatFactory.register(new VeloxParquetWriterInjects)
    GlutenFormatFactory.injectPostRuleFactory(
      session => GlutenWriterColumnarRules.NativeWritePostRule(session))
    GlutenFormatFactory.register(new VeloxRowSplitter())
  }

  private def shutdown(): Unit = {
    // TODO shutdown implementation in velox to release resources
  }
}

object VeloxListenerApi {
  // TODO: Implement graceful shutdown and remove these flags.
  //  As spark conf may change when active Spark session is recreated.
  private val driverInitialized: AtomicBoolean = new AtomicBoolean(false)
  private val executorInitialized: AtomicBoolean = new AtomicBoolean(false)
  private val platformLibDir: String = {
    val osName = System.getProperty("os.name") match {
      case n if n.contains("Linux") => "linux"
      case n if n.contains("Mac") => "darwin"
      case _ =>
        // Default to linux
        "linux"
    }
    val arch = System.getProperty("os.arch")
    s"$osName/$arch"
  }

  private def inLocalMode(conf: SparkConf): Boolean = {
    SparkResourceUtil.isLocalMaster(conf)
  }
}
