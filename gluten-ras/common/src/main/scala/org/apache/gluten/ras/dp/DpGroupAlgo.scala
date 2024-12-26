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
package org.apache.gluten.ras.dp

import org.apache.gluten.ras.{InGroupNode, RasGroup}
import org.apache.gluten.ras.dp.DpZipperAlgo.Solution
import org.apache.gluten.ras.memo.MemoState

// Dynamic programming algorithm to solve problem against a single RAS group that can be
// broken down to sub problems for subgroups.
trait DpGroupAlgoDef[T <: AnyRef, NodeOutput <: AnyRef, GroupOutput <: AnyRef] {
  def solveNode(node: InGroupNode[T], childrenGroupsOutput: RasGroup[T] => GroupOutput): NodeOutput
  def solveGroup(group: RasGroup[T], nodesOutput: InGroupNode[T] => NodeOutput): GroupOutput

  def solveNodeOnCycle(node: InGroupNode[T]): NodeOutput
  def solveGroupOnCycle(cluster: RasGroup[T]): GroupOutput
}

object DpGroupAlgo {

  trait Adjustment[T <: AnyRef] extends DpZipperAlgo.Adjustment[InGroupNode[T], RasGroup[T]]

  object Adjustment {
    private class None[T <: AnyRef] extends Adjustment[T] {
      override def exploreChildX(
          panel: DpZipperAlgo.Adjustment.Panel[InGroupNode[T], RasGroup[T]],
          x: InGroupNode[T]): Unit = {}
      override def exploreParentY(
          panel: DpZipperAlgo.Adjustment.Panel[InGroupNode[T], RasGroup[T]],
          y: RasGroup[T]): Unit = {}
      override def exploreChildY(
          panel: DpZipperAlgo.Adjustment.Panel[InGroupNode[T], RasGroup[T]],
          y: RasGroup[T]): Unit = {}
      override def exploreParentX(
          panel: DpZipperAlgo.Adjustment.Panel[InGroupNode[T], RasGroup[T]],
          x: InGroupNode[T]): Unit = {}
    }

    def none[T <: AnyRef](): Adjustment[T] = new None[T]()
  }

  def resolve[T <: AnyRef, NodeOutput <: AnyRef, GroupOutput <: AnyRef](
      memoState: MemoState[T],
      groupAlgoDef: DpGroupAlgoDef[T, NodeOutput, GroupOutput],
      adjustment: Adjustment[T],
      group: RasGroup[T]): Solution[InGroupNode[T], RasGroup[T], NodeOutput, GroupOutput] = {
    DpZipperAlgo.resolve(new ZipperAlgoDefImpl(memoState, groupAlgoDef), adjustment, group)
  }

  private class ZipperAlgoDefImpl[T <: AnyRef, NodeOutput <: AnyRef, GroupOutput <: AnyRef](
      memoState: MemoState[T],
      groupAlgoDef: DpGroupAlgoDef[T, NodeOutput, GroupOutput])
    extends DpZipperAlgoDef[InGroupNode[T], RasGroup[T], NodeOutput, GroupOutput] {
    override def idOfX(x: InGroupNode[T]): Any = {
      x.toUniqueKey
    }

    override def idOfY(y: RasGroup[T]): Any = {
      y.id()
    }

    override def browseX(x: InGroupNode[T]): Iterable[RasGroup[T]] = {
      val allGroups = memoState.allGroups()
      x.can.getChildrenGroups(allGroups).map(gn => allGroups(gn.groupId()))
    }

    override def browseY(y: RasGroup[T]): Iterable[InGroupNode[T]] = {
      y.nodes(memoState).map(can => InGroupNode(y.id(), can))
    }

    override def solveX(x: InGroupNode[T], yOutput: RasGroup[T] => GroupOutput): NodeOutput =
      groupAlgoDef.solveNode(x, yOutput)

    override def solveY(y: RasGroup[T], xOutput: InGroupNode[T] => NodeOutput): GroupOutput =
      groupAlgoDef.solveGroup(y, xOutput)

    override def solveXOnCycle(x: InGroupNode[T]): NodeOutput = groupAlgoDef.solveNodeOnCycle(x)

    override def solveYOnCycle(y: RasGroup[T]): GroupOutput = groupAlgoDef.solveGroupOnCycle(y)
  }
}
