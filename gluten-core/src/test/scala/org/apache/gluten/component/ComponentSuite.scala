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

import org.apache.gluten.backend.Backend
import org.apache.gluten.extension.injector.Injector

import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

class ComponentSuite extends AnyFunSuite with BeforeAndAfterAll {
  import ComponentSuite._

  private val d = new DummyComponentD()
  d.ensureRegistered()
  private val b = new DummyBackendB()
  b.ensureRegistered()
  private val a = new DummyBackendA()
  a.ensureRegistered()
  private val c = new DummyComponentC()
  c.ensureRegistered()
  private val e = new DummyComponentE()
  e.ensureRegistered()

  test("Load order - sanity") {
    val possibleOrders =
      Set(
        Seq(a, b, c, d, e),
        Seq(a, b, d, c, e),
        Seq(b, a, c, d, e),
        Seq(b, a, d, c, e)
      )

    assert(possibleOrders.contains(Component.sorted()))
  }

  test("Register again") {
    assertThrows[IllegalArgumentException] {
      new DummyBackendA().ensureRegistered()
    }
  }
}

object ComponentSuite {
  private class DummyBackendA extends Backend {
    override def name(): String = "dummy-backend-a"
    override def buildInfo(): Component.BuildInfo =
      Component.BuildInfo("DUMMY_BACKEND_A", "N/A", "N/A", "N/A")
    override def injectRules(injector: Injector): Unit = {}
  }

  private class DummyBackendB extends Backend {
    override def name(): String = "dummy-backend-b"
    override def buildInfo(): Component.BuildInfo =
      Component.BuildInfo("DUMMY_BACKEND_B", "N/A", "N/A", "N/A")
    override def injectRules(injector: Injector): Unit = {}
  }

  private class DummyComponentC extends Component {
    override def dependencies(): Seq[Class[_ <: Component]] = classOf[DummyBackendA] :: Nil

    override def name(): String = "dummy-component-c"
    override def buildInfo(): Component.BuildInfo =
      Component.BuildInfo("DUMMY_COMPONENT_C", "N/A", "N/A", "N/A")
    override def injectRules(injector: Injector): Unit = {}
  }

  private class DummyComponentD extends Component {
    override def dependencies(): Seq[Class[_ <: Component]] =
      Seq(classOf[DummyBackendA], classOf[DummyBackendB])

    override def name(): String = "dummy-component-d"
    override def buildInfo(): Component.BuildInfo =
      Component.BuildInfo("DUMMY_COMPONENT_D", "N/A", "N/A", "N/A")
    override def injectRules(injector: Injector): Unit = {}
  }

  private class DummyComponentE extends Component {
    override def dependencies(): Seq[Class[_ <: Component]] =
      Seq(classOf[DummyBackendA], classOf[DummyComponentD])

    override def name(): String = "dummy-component-e"
    override def buildInfo(): Component.BuildInfo =
      Component.BuildInfo("DUMMY_COMPONENT_E", "N/A", "N/A", "N/A")
    override def injectRules(injector: Injector): Unit = {}
  }
}
