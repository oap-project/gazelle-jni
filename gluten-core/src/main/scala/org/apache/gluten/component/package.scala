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

import org.apache.spark.internal.Logging

import java.util.concurrent.atomic.AtomicBoolean

package object component extends Logging {
  private val allComponentsLoaded: AtomicBoolean = new AtomicBoolean(false)

  private[component] def ensureAllComponentsRegistered(): Unit = {
    if (!allComponentsLoaded.compareAndSet(false, true)) {
      return
    }

    // Load all components in classpath.
    val all = Discovery.discoverAll()

    // Register all components.
    all.foreach(_.ensureRegistered())

    // Output log so user could view the component loading order.
    // Call #sortedUnsafe than on #sorted to avoid unnecessary recursion.
    val components = Component.sortedUnsafe()
    logInfo(s"Components registered within order: ${components.map(_.name()).mkString(", ")}")
  }
}
