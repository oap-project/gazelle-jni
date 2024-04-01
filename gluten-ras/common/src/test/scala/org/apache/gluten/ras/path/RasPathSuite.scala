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
package org.apache.gluten.ras.path

import org.apache.gluten.ras.Ras
import org.apache.gluten.ras.RasSuiteBase._
import org.apache.gluten.ras.mock.MockRasPath
import org.apache.gluten.ras.rule.RasRule

import org.scalatest.funsuite.AnyFunSuite

class RasPathSuite extends AnyFunSuite {
  import RasPathSuite._

  test("Path aggregate - empty") {
    val ras =
      Ras[TestNode](
        PlanModelImpl,
        CostModelImpl,
        MetadataModelImpl,
        PropertyModelImpl,
        ExplainImpl,
        RasRule.Factory.reuse(List.empty))
    assert(RasPath.aggregate(ras, List.empty) == List.empty)
  }

  test("Path aggregate") {
    val ras =
      Ras[TestNode](
        PlanModelImpl,
        CostModelImpl,
        MetadataModelImpl,
        PropertyModelImpl,
        ExplainImpl,
        RasRule.Factory.reuse(List.empty))

    val n1 = "n1"
    val n2 = "n2"
    val n3 = "n3"
    val n4 = "n4"
    val n5 = "n5"
    val n6 = "n6"
    val path1 = MockRasPath.mock(
      ras,
      Unary(n5, Leaf(n6, 1)),
      PathKeySet(Set(DummyPathKey(1), DummyPathKey(3)))
    )
    val path2 = MockRasPath.mock(
      ras,
      Unary(n1, Unary(n2, Leaf(n3, 1))),
      PathKeySet(Set(DummyPathKey(1)))
    )
    val path3 = MockRasPath.mock(
      ras,
      Unary(n1, Unary(n2, Leaf(n3, 1))),
      PathKeySet(Set(DummyPathKey(1), DummyPathKey(2)))
    )
    val path4 = MockRasPath.mock(
      ras,
      Unary(n1, Unary(n2, Leaf(n3, 1))),
      PathKeySet(Set(DummyPathKey(4)))
    )
    val path5 = MockRasPath.mock(
      ras,
      Unary(n5, Leaf(n6, 1)),
      PathKeySet(Set(DummyPathKey(4)))
    )
    val out = RasPath
      .aggregate(ras, List(path1, path2, path3, path4, path5))
      .toSeq
      .sortBy(_.height())
    assert(out.size == 2)
    assert(out.head.height() == 2)
    assert(out.head.plan() == Unary(n5, Leaf(n6, 1)))
    assert(out.head.keys() == PathKeySet(Set(DummyPathKey(1), DummyPathKey(3), DummyPathKey(4))))

    assert(out(1).height() == 3)
    assert(out(1).plan() == Unary(n1, Unary(n2, Leaf(n3, 1))))
    assert(out(1).keys() == PathKeySet(Set(DummyPathKey(1), DummyPathKey(2), DummyPathKey(4))))
  }
}

object RasPathSuite {
  case class Leaf(name: String, override val selfCost: Long) extends LeafLike {
    override def makeCopy(): LeafLike = this
  }

  case class Unary(name: String, child: TestNode) extends UnaryLike {
    override def selfCost(): Long = 1
    override def withNewChildren(child: TestNode): UnaryLike = copy(child = child)
  }

  case class Binary(name: String, left: TestNode, right: TestNode) extends BinaryLike {
    override def selfCost(): Long = 1
    override def withNewChildren(left: TestNode, right: TestNode): BinaryLike =
      copy(left = left, right = right)
  }

  case class DummyPathKey(value: Int) extends PathKey
}
