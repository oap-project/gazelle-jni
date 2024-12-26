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
package org.apache.execution

import org.apache.gluten.execution.VeloxTPCHSuite
import org.apache.spark.SparkConf

import java.io.File

class VeloxTPCHHudiSuite extends VeloxTPCHSuite {
  protected val tpchBasePath: String =
    getClass.getResource("/").getPath + "../../../src/test/resources"

  override protected val resourcePath: String =
    new File(tpchBasePath, "tpch-data-parquet").getCanonicalPath

  override protected val queriesResults: String =
    new File(tpchBasePath, "queries-output").getCanonicalPath

  override protected def sparkConf: SparkConf = {
    super.sparkConf
      .set("spark.executor.memory", "4g")
      .set("spark.sql.extensions", "org.apache.spark.sql.hudi.HoodieSparkSessionExtension")
      .set("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.hudi.catalog.HoodieCatalog")
      .set("spark.kryo.registrator", "org.apache.spark.HoodieSparkKryoRegistrar")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
  }

  override protected def createTPCHNotNullTables(): Unit = {
    TPCHTables
      .map(_.name)
      .map {
        table =>
          val tablePath = new File(resourcePath, table).getAbsolutePath
          val tableDF = spark.read.format(fileFormat).load(tablePath)
          tableDF.write.format("hudi").mode("append").saveAsTable(table)
          (table, tableDF)
      }
      .toMap
  }

  override protected def afterAll(): Unit = {
    TPCHTables.map(_.name).foreach(table => spark.sql(s"DROP TABLE IF EXISTS $table"))
    super.afterAll()
  }
}
