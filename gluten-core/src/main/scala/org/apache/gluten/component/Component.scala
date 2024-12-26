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

import org.apache.gluten.extension.columnar.transition.ConventionFunc
import org.apache.gluten.extension.injector.Injector

import org.apache.spark.SparkContext
import org.apache.spark.annotation.Experimental
import org.apache.spark.api.plugin.PluginContext

import java.util.concurrent.atomic.{AtomicBoolean, AtomicInteger}

import scala.collection.mutable

/**
 * The base API to inject user-defined logic to Gluten. To register a component, its implementations
 * should be placed to Gluten's classpath with a Java service file. Gluten will discover all the
 * component implementations then register them at the booting time.
 *
 * Experimental: This is not expected to be used in production yet. Use
 * [[org.apache.gluten.backend.Backend]] instead.
 */
@Experimental
trait Component {
  import Component._

  private val uid = nextUid.getAndIncrement()
  private val isRegistered = new AtomicBoolean(false)

  def ensureRegistered(): Unit = {
    if (!isRegistered.compareAndSet(false, true)) {
      return
    }
    graph.add(this)
    dependencies().foreach(req => graph.declareDependency(this, req))
  }

  /** Base information. */
  def name(): String
  def buildInfo(): BuildInfo
  def dependencies(): Seq[Class[_ <: Component]]

  /** Spark listeners. */
  def onDriverStart(sc: SparkContext, pc: PluginContext): Unit = {}
  def onDriverShutdown(): Unit = {}
  def onExecutorStart(pc: PluginContext): Unit = {}
  def onExecutorShutdown(): Unit = {}

  /**
   * Overrides [[org.apache.gluten.extension.columnar.transition.ConventionFunc]] Gluten is using to
   * determine the convention (its row-based processing / columnar-batch processing support) of a
   * plan with a user-defined function that accepts a plan then returns convention type it outputs,
   * and input conventions it requires.
   */
  def convFuncOverride(): ConventionFunc.Override = ConventionFunc.Override.Empty

  /** Query planner rules. */
  def injectRules(injector: Injector): Unit
}

object Component {
  private val nextUid = new AtomicInteger()
  private val graph: Graph = new Graph()

  // format: off
  /**
   * Apply topology sort on all registered components in graph to get an ordered list of
   * components. The root nodes will be on the head side of the list, while leaf nodes
   * will be on the tail side of the list.
   *
   * Say if component-A depends on component-B while component-C requires nothing, then the
   * output order will be one of the following:
   *
   *   1. [component-B, component-A, component-C]
   *   2. [component-C, component-B, component-A]
   *   3. [component-B, component-C, component-A]
   *
   * By all means component B will be placed before component A because of the declared
   * dependency from component A to component B.
   *
   * @throws UnsupportedOperationException When cycles in dependency graph are found.
   */
  // format: on
  def sorted(): Seq[Component] = {
    ensureAllComponentsRegistered()
    graph.sorted()
  }

  private[component] def sortedUnsafe(): Seq[Component] = {
    graph.sorted()
  }

  private class Registry {
    private val lookupByUid: mutable.Map[Int, Component] = mutable.Map()
    private val lookupByClass: mutable.Map[Class[_ <: Component], Component] = mutable.Map()

    def register(comp: Component): Unit = synchronized {
      val uid = comp.uid
      val clazz = comp.getClass
      require(!lookupByUid.contains(uid), s"Component UID $uid already registered: ${comp.name()}")
      require(
        !lookupByClass.contains(clazz),
        s"Component class $clazz already registered: ${comp.name()}")
      lookupByUid += uid -> comp
      lookupByClass += clazz -> comp
    }

    def isUidRegistered(uid: Int): Boolean = synchronized {
      lookupByUid.contains(uid)
    }

    def isClassRegistered(clazz: Class[_ <: Component]): Boolean = synchronized {
      lookupByClass.contains(clazz)
    }

    def findByClass(clazz: Class[_ <: Component]): Component = synchronized {
      require(lookupByClass.contains(clazz))
      lookupByClass(clazz)
    }

    def findByUid(uid: Int): Component = synchronized {
      require(lookupByUid.contains(uid))
      lookupByUid(uid)
    }

    def allUids(): Seq[Int] = synchronized {
      return lookupByUid.keys.toSeq
    }
  }

  private class Graph {
    import Graph._
    private val registry: Registry = new Registry()
    private val dependencies: mutable.Buffer[(Int, Class[_ <: Component])] = mutable.Buffer()

    private var sortedComponents: Option[Seq[Component]] = None

    def add(comp: Component): Unit = synchronized {
      require(
        !registry.isUidRegistered(comp.uid),
        s"Component UID ${comp.uid} already registered: ${comp.name()}")
      require(
        !registry.isClassRegistered(comp.getClass),
        s"Component class ${comp.getClass} already registered: ${comp.name()}")
      registry.register(comp)
      sortedComponents = None
    }

    def declareDependency(comp: Component, dependencyCompClass: Class[_ <: Component]): Unit =
      synchronized {
        require(registry.isUidRegistered(comp.uid))
        require(registry.isClassRegistered(comp.getClass))
        dependencies += comp.uid -> dependencyCompClass
        sortedComponents = None
      }

    private def newLookup(): mutable.Map[Int, Node] = {
      val lookup: mutable.Map[Int, Node] = mutable.Map()

      registry.allUids().foreach {
        uid =>
          require(!lookup.contains(uid))
          val n = new Node(uid)
          lookup += uid -> n
      }

      dependencies.foreach {
        case (uid, dependencyCompClass) =>
          require(
            registry.isClassRegistered(dependencyCompClass),
            s"Dependency class not registered yet: ${dependencyCompClass.getName}")
          val dependencyUid = registry.findByClass(dependencyCompClass).uid
          require(uid != dependencyUid)
          require(lookup.contains(uid))
          require(lookup.contains(dependencyUid))
          val n = lookup(uid)
          val r = lookup(dependencyUid)
          require(!n.parents.contains(r.uid))
          require(!r.children.contains(n.uid))
          n.parents += r.uid -> r
          r.children += n.uid -> n
      }

      lookup
    }

    def sorted(): Seq[Component] = synchronized {
      if (sortedComponents.isDefined) {
        return sortedComponents.get
      }

      val lookup: mutable.Map[Int, Node] = newLookup()

      val out = mutable.Buffer[Component]()
      val uidToNumParents = lookup.map { case (uid, node) => uid -> node.parents.size }
      val removalQueue = mutable.Queue[Int]()

      // 1. Find out all nodes with zero parents then enqueue them.
      uidToNumParents.filter(_._2 == 0).foreach(kv => removalQueue.enqueue(kv._1))

      // 2. Loop to dequeue and remove nodes from the uid-to-num-parents map.
      while (removalQueue.nonEmpty) {
        val parentUid = removalQueue.dequeue()
        val node = lookup(parentUid)
        out += registry.findByUid(parentUid)
        node.children.keys.foreach {
          childUid =>
            uidToNumParents += childUid -> (uidToNumParents(childUid) - 1)
            val updatedNumParents = uidToNumParents(childUid)
            assert(updatedNumParents >= 0)
            if (updatedNumParents == 0) {
              removalQueue.enqueue(childUid)
            }
        }
      }

      // 3. If there are still outstanding nodes (those are with more non-zero parents) in the
      // uid-to-num-parents map, then it means at least one cycle is found. Report error if so.
      if (uidToNumParents.exists(_._2 != 0)) {
        val cycleNodes = uidToNumParents.filter(_._2 != 0).keys.map(registry.findByUid)
        val cycleNodeNames = cycleNodes.map(_.name()).mkString(", ")
        throw new UnsupportedOperationException(
          s"Cycle detected in the component graph: $cycleNodeNames")
      }

      // 4. Return the ordered nodes.
      sortedComponents = Some(out.toSeq)
      sortedComponents.get
    }
  }

  private object Graph {
    class Node(val uid: Int) {
      val parents: mutable.Map[Int, Node] = mutable.Map()
      val children: mutable.Map[Int, Node] = mutable.Map()
    }
  }

  case class BuildInfo(name: String, branch: String, revision: String, revisionTime: String)
}
