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
package org.apache.gluten.execution

import org.apache.gluten.backendsapi.clickhouse.RuntimeConfig
import org.apache.gluten.utils.UTSystemParameters

import org.apache.spark.{SPARK_VERSION_SHORT, SparkConf}

import org.apache.commons.io.FileUtils
import org.scalatest.Tag

import java.io.File

// Some sqls' line length exceeds 100
// scalastyle:off line.size.limit

class GlutenClickHouseWholeStageTransformerSuite extends WholeStageTransformerSuite {

  val DBL_EPSILON = 2.2204460492503131e-16
  val DBL_RELAX_EPSILON: Double = Math.pow(10, -11)
  val FLT_EPSILON = 1.19209290e-07f

  private val sparkVersion: String = {
    val version = SPARK_VERSION_SHORT.split("\\.")
    version(0) + "." + version(1)
  }
  val SPARK_DIR_NAME: String = sparkVersion.replace(".", "-")

  val S3_METADATA_PATH = s"/tmp/metadata/s3/$SPARK_DIR_NAME"
  val S3_CACHE_PATH = s"/tmp/s3_cache/$SPARK_DIR_NAME/"
  val S3_ENDPOINT = "s3://127.0.0.1:9000/"
  val MINIO_ENDPOINT: String = S3_ENDPOINT.replace("s3", "http")
  val BUCKET_NAME: String = SPARK_DIR_NAME
  val WHOLE_PATH: String = MINIO_ENDPOINT + BUCKET_NAME + "/"

  val HDFS_METADATA_PATH = s"/tmp/metadata/hdfs/$SPARK_DIR_NAME"
  val HDFS_CACHE_PATH = s"/tmp/hdfs_cache/$SPARK_DIR_NAME/"
  val HDFS_URL_ENDPOINT = "hdfs://127.0.0.1:8020"
  val HDFS_URL = s"$HDFS_URL_ENDPOINT/$SPARK_DIR_NAME"

  val S3_ACCESS_KEY = "minioadmin"
  val S3_SECRET_KEY = "minioadmin"

  val CH_DEFAULT_STORAGE_DIR = "/data"

  protected def spark32: Boolean = sparkVersion.equals("3.2")
  protected def spark33: Boolean = sparkVersion.equals("3.3")
  protected def spark35: Boolean = sparkVersion.equals("3.5")

  def AlmostEqualsIsRel(expected: Double, actual: Double, EPSILON: Double = DBL_EPSILON): Unit = {
    val diff = Math.abs(expected - actual)
    val epsilon = EPSILON * Math.max(Math.abs(expected), Math.abs(actual))
    if (diff > epsilon) {
      fail(s"""
              |expected: $expected
              |actual:   $actual
              | abs(expected-expected) ~ epsilon = $diff ~ $epsilon
              |""".stripMargin)
    }
  }

  override protected def sparkConf: SparkConf = {
    import org.apache.gluten.backendsapi.clickhouse.CHConf._

    val conf = super.sparkConf
      .set("spark.gluten.sql.enable.native.validation", "false")
      .set("spark.sql.warehouse.dir", warehouse)
      .setCHConfig("user_defined_path", "/tmp/user_defined")
      .set(RuntimeConfig.PATH.key, UTSystemParameters.diskOutputDataPath)
      .set(RuntimeConfig.TMP_PATH.key, s"/tmp/libch/$SPARK_DIR_NAME")
    if (UTSystemParameters.testMergeTreeOnObjectStorage) {
      conf
        .set("spark.hadoop.fs.s3a.access.key", S3_ACCESS_KEY)
        .set("spark.hadoop.fs.s3a.secret.key", S3_SECRET_KEY)
        .set("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .set("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT)
        .set("spark.hadoop.fs.s3a.path.style.access", "true")
        .set("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .setCHConfig(
          "storage_configuration.disks.s3.type" -> "s3_gluten",
          "storage_configuration.disks.s3.endpoint" -> WHOLE_PATH,
          "storage_configuration.disks.s3.access_key_id" -> S3_ACCESS_KEY,
          "storage_configuration.disks.s3.secret_access_key" -> S3_SECRET_KEY,
          "storage_configuration.disks.s3.metadata_path" -> S3_METADATA_PATH,
          "storage_configuration.disks.s3_cache.type" -> "cache",
          "storage_configuration.disks.s3_cache.disk" -> "s3",
          "storage_configuration.disks.s3_cache.path" -> S3_CACHE_PATH,
          "storage_configuration.disks.s3_cache.max_size" -> "10Gi",
          "storage_configuration.policies.__s3_main.volumes" -> "main",
          "storage_configuration.policies.__s3_main.volumes.main.disk" -> "s3_cache"
        )
        .setCHConfig(
          "storage_configuration.disks.hdfs.type" -> "hdfs_gluten",
          "storage_configuration.disks.hdfs.endpoint" -> s"$HDFS_URL_ENDPOINT/",
          "storage_configuration.disks.hdfs.metadata_path" -> HDFS_METADATA_PATH,
          "storage_configuration.disks.hdfs_cache.type" -> "cache",
          "storage_configuration.disks.hdfs_cache.disk" -> "hdfs",
          "storage_configuration.disks.hdfs_cache.path" -> HDFS_CACHE_PATH,
          "storage_configuration.disks.hdfs_cache.max_size" -> "10Gi",
          "storage_configuration.policies.__hdfs_main.volumes" -> "main",
          "storage_configuration.policies.__hdfs_main.volumes.main.disk" -> "hdfs_cache"
        )
        .setCHConfig(
          "storage_configuration.disks.hdfs2.type" -> "hdfs_gluten",
          "storage_configuration.disks.hdfs2.endpoint" -> s"$HDFS_URL_ENDPOINT/",
          "storage_configuration.disks.hdfs2.metadata_path" -> HDFS_METADATA_PATH,
          "storage_configuration.disks.hdfs2.metadata_type" -> "rocksdb",
          "storage_configuration.disks.hdfs_cache2.type" -> "cache",
          "storage_configuration.disks.hdfs_cache2.disk" -> "hdfs2",
          "storage_configuration.disks.hdfs_cache2.path" -> HDFS_CACHE_PATH,
          "storage_configuration.disks.hdfs_cache2.max_size" -> "10Gi",
          "storage_configuration.policies.__hdfs_main_rocksdb.volumes" -> "main",
          "storage_configuration.policies.__hdfs_main_rocksdb.volumes.main.disk" -> "hdfs_cache2"
        )
        .setCHConfig(
          "hdfs.dfs_client_read_shortcircuit" -> "false",
          "hdfs.dfs_default_replica" -> "1"
        )
    } else {
      conf
    }
  }

  def clearDataPath(dataPath: String): Unit = {
    val dataPathDir = new File(dataPath)
    if (dataPathDir.exists()) FileUtils.forceDelete(dataPathDir)
  }

  override def beforeAll(): Unit = {
    // is not exist may cause some ut error
    assert(new File(CH_DEFAULT_STORAGE_DIR).exists())

    // prepare working paths
    val basePathDir = new File(basePath)
    if (basePathDir.exists()) {
      FileUtils.forceDelete(basePathDir)
    }
    FileUtils.forceMkdir(basePathDir)
    FileUtils.forceMkdir(new File(warehouse))
    FileUtils.forceMkdir(new File(metaStorePathAbsolute))
    super.beforeAll()
  }

  final protected val rootPath: String = this.getClass.getResource("/").getPath
  final protected val queryPath: String = s"${rootPath}queries"
  final protected val basePath: String =
    if (UTSystemParameters.diskOutputDataPath.equals("/")) rootPath + "tests-working-home"
    else UTSystemParameters.diskOutputDataPath + "/" + rootPath + "tests-working-home"
  final protected val warehouse: String = basePath + "/spark-warehouse"
  final protected val metaStorePathAbsolute: String = basePath + "/meta"

  protected val hiveMetaStoreDB: String =
    s"$metaStorePathAbsolute/${getClass.getSimpleName}/metastore_db"

  final override protected val resourcePath: String = "" // ch not need this
  override protected val fileFormat: String = "parquet"

  protected def testSparkVersionLE33(testName: String, testTag: Tag*)(testFun: => Any): Unit = {
    if (isSparkVersionLE("3.3")) {
      test(testName, testTag: _*)(testFun)
    } else {
      ignore(s"[$SPARK_VERSION_SHORT]-$testName", testTag: _*)(testFun)
    }
  }

  lazy val pruningTimeValueSpark: Int = if (isSparkVersionLE("3.3")) -1 else 0
}
// scalastyle:off line.size.limit
