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
package org.apache.gluten.sql.shims.spark35

import org.apache.gluten.sql.shims.{SparkShimDescriptor, SparkShims}
import org.apache.gluten.sql.shims.spark35.SparkShimProvider.DESCRIPTOR

object SparkShimProvider {
  val DESCRIPTOR = SparkShimDescriptor(3, 5, 4)
}

class SparkShimProvider extends org.apache.gluten.sql.shims.SparkShimProvider {
  def createShim: SparkShims = {
    new Spark35Shims()
  }

  def matches(version: String): Boolean = {
    val majorMinorVersionMatch = DESCRIPTOR.toMajorMinorVersion ==
      extractMajorAndMinorVersion(version)
    if (majorMinorVersionMatch && DESCRIPTOR.toString() != version) {
      logWarning(
        s"Spark runtime version $version is not matched with Gluten's fully" +
          s" tested version ${DESCRIPTOR.toString()}")
    }
    majorMinorVersionMatch
  }
}
