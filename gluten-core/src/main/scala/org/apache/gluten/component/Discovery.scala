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

package org.apache.gluten.component

import org.apache.gluten.exception.GlutenException
import org.apache.gluten.utils.ResourceUtil

import org.apache.spark.internal.Logging
import org.apache.spark.util.SparkReflectionUtil

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.util.matching.Regex




// format: off
/**
 * Gluten's global discovery to find all [[Component]] definitions in the classpath.
 *
 * We don't use [[java.util.ServiceLoader]] since it requires all the service files to have
 * the same file name which is the class name of [[Component]], this causes the service files
 * easily be overwritten by each other during Maven build. Typically, See code of
 * `DefaultMavenFileFilter` used by Maven's `maven-resources-plugin`.
 *
 * Instead, Gluten defines its own way to register components. For example, placing the following
 * component files to resource folder:
 *
 *  META-INF
 *  \- gluten-components
 *     |- org.apache.gluten.component.AComponent
 *     \- org.apache.gluten.backend.BBackend
 *
 * Will cause the registration of component `AComponent` and backend `BBackend`.
 *
 * The content in a component file is not read so doesn't matter at the moment.
 */
// format: on
private object Discovery extends Logging {
  private val container: String = "META-INF/gluten-components"
  private val componentFilePattern: Regex = s"^$container/(.+)$$".r

  def discoverAll(): Seq[Component] = {
    logInfo("Start discovering components in the current classpath... ")
    val prev = System.currentTimeMillis()
    val allFiles = ResourceUtil.getResources(componentFilePattern.pattern).asScala
    val duration = System.currentTimeMillis() - prev
    logInfo(s"Discovered component files: ${allFiles.mkString(", ")}. Duration: $duration ms.")
    val deDup = mutable.Set[String]()
    val out = allFiles.flatMap {
      case componentFilePattern(className) =>
        if (!deDup.add(className)) {
          logWarning(s"Found duplicated component class $className in then classpath, ignoring.")
          None
        } else {
          val clazz =
            try {
              SparkReflectionUtil.classForName(className)
            } catch {
              case e: ClassNotFoundException =>
                throw new GlutenException(s"Component class not found: $className", e)
            }
          val instance = clazz.getDeclaredConstructor().newInstance().asInstanceOf[Component]
          Some(instance)
        }
      case _ => None
    }.toSeq
    out
  }
}
