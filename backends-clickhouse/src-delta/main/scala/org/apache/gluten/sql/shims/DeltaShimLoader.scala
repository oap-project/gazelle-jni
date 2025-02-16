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
package org.apache.gluten.sql.shims

import org.apache.spark.internal.Logging

import java.util.ServiceLoader

import scala.collection.JavaConverters._

object DeltaShimLoader extends Logging {
  private var deltaShims: DeltaShims = null

  def getDeltaShims: DeltaShims = {
    if (deltaShims == null) {
      val provider = getDeltaShimProvider
      deltaShims = provider.createShim
    }
    deltaShims
  }

  private def loadDeltaShimProvider(): DeltaShimProvider = {
    // Load and filter the providers based on version
    val shimProviders =
      ServiceLoader.load(classOf[DeltaShimProvider]).asScala
    if (shimProviders.size > 1) {
      throw new IllegalStateException(s"More than one DeltaShimProvider found: $shimProviders")
    }

    val shimProvider = shimProviders.headOption match {
      case Some(shimProvider) => shimProvider
      case None =>
        throw new IllegalStateException(s"No Delta Shim Provider.")
    }
    logInfo(s"Using Shim provider: $shimProviders")
    shimProvider
  }

  private def getDeltaShimProvider: DeltaShimProvider = {
    loadDeltaShimProvider()
  }
}
