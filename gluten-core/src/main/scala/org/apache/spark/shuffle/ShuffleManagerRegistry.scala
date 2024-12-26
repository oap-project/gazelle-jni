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
package org.apache.spark.shuffle

import org.apache.spark.SparkConf
import org.apache.spark.util.{SparkTestUtil, Utils}

import scala.collection.mutable

class ShuffleManagerRegistry private[ShuffleManagerRegistry] {
  import ShuffleManagerRegistry._
  private val all: mutable.Buffer[(LookupKey, String)] = mutable.Buffer()
  private val routerBuilders: mutable.Buffer[RouterBuilder] = mutable.Buffer()
  private val classDeDup: mutable.Set[String] = mutable.Set()

  def register(lookupKey: LookupKey, shuffleManagerClass: String): Unit = {
    val clazz = Utils.classForName(shuffleManagerClass)
    require(
      !clazz.isAssignableFrom(classOf[GlutenShuffleManager]),
      "It's not allowed to register GlutenShuffleManager recursively")
    require(
      classOf[ShuffleManager].isAssignableFrom(clazz),
      s"Shuffle manager class to register is not an implementation of Spark ShuffleManager: " +
        s"$shuffleManagerClass"
    )
    require(
      !classDeDup.contains(shuffleManagerClass),
      s"Shuffle manager class already registered: $shuffleManagerClass")
    this.synchronized {
      classDeDup += shuffleManagerClass
      all += lookupKey -> shuffleManagerClass
      // Invalidate all shuffle managers cached in each alive router builder instances.
      // Then, once the router builder is accessed, a new router will be forced to create.
      routerBuilders.foreach(_.invalidateCache())
    }
  }

  // Visible for testing.
  private[shuffle] def clear(): Unit = {
    assert(SparkTestUtil.isTesting)
    this.synchronized {
      classDeDup.clear()
      all.clear()
      routerBuilders.foreach(_.invalidateCache())
    }
  }

  private[shuffle] def newRouterBuilder(conf: SparkConf, isDriver: Boolean): RouterBuilder =
    this.synchronized {
      val out = new RouterBuilder(this, conf, isDriver)
      routerBuilders += out
      out
    }
}

object ShuffleManagerRegistry {
  private val instance = new ShuffleManagerRegistry()

  def get(): ShuffleManagerRegistry = instance

  class RouterBuilder(registry: ShuffleManagerRegistry, conf: SparkConf, isDriver: Boolean) {
    private var router: Option[ShuffleManagerRouter] = None

    private[ShuffleManagerRegistry] def invalidateCache(): Unit = synchronized {
      router = None
    }

    private[shuffle] def getOrBuild(): ShuffleManagerRouter = synchronized {
      if (router.isEmpty) {
        val instances = registry.all.map(key => key._1 -> instantiate(key._2, conf, isDriver))
        router = Some(new ShuffleManagerRouter(new ShuffleManagerLookup(instances.toSeq)))
      }
      router.get
    }

    private def instantiate(clazz: String, conf: SparkConf, isDriver: Boolean): ShuffleManager = {
      Utils
        .instantiateSerializerOrShuffleManager[ShuffleManager](clazz, conf, isDriver)
    }
  }
}
