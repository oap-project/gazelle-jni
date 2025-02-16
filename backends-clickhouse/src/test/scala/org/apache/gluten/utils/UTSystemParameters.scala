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
package org.apache.gluten.utils

object UTSystemParameters {

  private val TEST_DATA_PATH_KEY = "gluten.test.data.path"
  private val TEST_DATA_PATH_DEFAULT_VALUE = "/data"

  private val TEST_DATA_DISK_OUTPUT_KEY = "gluten.test.disk.output.path"
  private val TEST_DATA_DISK_OUTPUT_DEFAULT_VALUE = "/"

  def testDataPath: String = {
    System.getProperty(
      UTSystemParameters.TEST_DATA_PATH_KEY,
      UTSystemParameters.TEST_DATA_PATH_DEFAULT_VALUE)
  }
  def diskOutputDataPath: String = {
    System.getProperty(
      UTSystemParameters.TEST_DATA_DISK_OUTPUT_KEY,
      UTSystemParameters.TEST_DATA_DISK_OUTPUT_DEFAULT_VALUE)
  }
  private val TPCDS_RELATIVE_DATA_PATH = "tpcds-data-sf1"
  private val TPCDS_DECIMAL_RELATIVE_DATA_PATH = "tpcds-data-sf1-decimal"

  def tpcdsDataPath: String = s"$testDataPath/$TPCDS_RELATIVE_DATA_PATH"

  def tpcdsDecimalDataPath: String = s"$testDataPath/$TPCDS_DECIMAL_RELATIVE_DATA_PATH"

  private val TEST_MERGETREE_ON_OBJECT_STORAGE = "gluten.ch.test.mergetree.object.storage"
  private val TEST_MERGETREE_ON_OBJECT_STORAGE_DEFAULT_VALUE = "true"

  def testMergeTreeOnObjectStorage: Boolean = {
    System
      .getProperty(
        UTSystemParameters.TEST_MERGETREE_ON_OBJECT_STORAGE,
        UTSystemParameters.TEST_MERGETREE_ON_OBJECT_STORAGE_DEFAULT_VALUE)
      .toBoolean
  }
}
