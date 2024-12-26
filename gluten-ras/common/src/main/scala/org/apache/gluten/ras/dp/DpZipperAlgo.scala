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

import org.apache.gluten.ras.util.CycleDetector

import scala.collection.mutable
// format: off
/**
 * Dynamic programming algorithm to solve problem that can be broken down to sub-problems on 2
 * individual different element types.
 *
 * The element types here are X, Y. Programming starts from Y, respectively traverses down to X, Y,
 * X..., util reaching to a leaf.
 *
 * Two major issues are handled by the base algo internally:
 *
 * 1. Cycle exclusion:
 *
 * The algo will withdraw the recursive call when found a cycle. Cycle is detected via the
 * comparison function passed by DpZipperAlgoDef#idOfX and DpZipperAlgoDef#idOfY. When a cycle is
 * found, the element that just created cycle (assume it is A) will be forced to return a
 * CycleMemory(A), then nodes on the whole recursive tree will therefore return their results with
 * CycleMemory(A). This means their results are incomplete by having the cyclic paths excluded.
 * Whether a path is "cyclic" is subjective: a child path can be cyclic for some parent nodes, but
 * not for some other parent nodes. So the incomplete results will not be memorized to solution
 * builder.
 *
 * However, once CycleMemory(A) is returned back to element A, A could be safely removed from the
 * cycle memory. This means the cycle is successfully enclosed and when the call tree continues
 * returning, there will be no cycles. Then the further results can be cached to solution builder.
 *
 * The above is a simplified example. The real cycle memory consists of a set for all cyclic nodes.
 * Only when the set gets cleared, then the current call can be considered cycle-free.
 *
 * 2. Branch invalidation:
 *
 * Since it can be required that the algo implementation tends to re-compute the already solved
 * elements, a #invalidate API is added in the adjustment panel.
 *
 * The invalidation is implemented in this way: each element would log its parent as its
 * back-dependency after it gets solved. For example, A has 3 children (B, C, D), after B, C were
 * solved respectively, A is added to B and C's back-dependency list. Then solution builder would be
 * aware of that A depends on B, as well as A depends on C. After this operation, Algorithm would
 * call the user-defined adjustment to allow caller invalidate some elements. If B is getting
 * invalidated, the algo will remove the saved solution of B, then find all back-dependencies of B,
 * then remove the saved results (if exist) of them, then find all back-dependencies of all the
 * back-dependencies of B, ... In this case, we just have 1 layer of recursive so only relation (B
 * -> A) gets removed. After A successfully solved D, the algo will backtrack all the already solved
 * children (B, C, D) to see if the previously registered back-dependencies (B -> A, C -> A, D -> A)
 * are still alive. In the case we only have (C -> A, D -> A) remaining, thus the algo will try to
 * recompute B. If during the procedure C or D or some of their children get invalidated again, then
 * keep looping until all the children are successfully solved and all the back-dependencies
 * survived.
 *
 * One of the possible corner cases is, for example, when B just gets solved, and is getting
 * adjusted, during which one of B's subtree gets invalidated. Since we apply the adjustment right
 * after the back-dependency (B -> A) is established, algo can still recognize (B -> A)'s removal
 * and recompute B. So this corner case is also handled correctly. The above is a simplified example
 * either. The real program will handle the invalidation for any depth of recursions.
 */
// format: on
trait DpZipperAlgoDef[X <: AnyRef, Y <: AnyRef, XOutput <: AnyRef, YOutput <: AnyRef] {
  def idOfX(x: X): Any
  def idOfY(y: Y): Any

  def browseX(x: X): Iterable[Y]
  def browseY(y: Y): Iterable[X]

  def solveX(x: X, yOutput: Y => YOutput): XOutput
  def solveY(y: Y, xOutput: X => XOutput): YOutput

  def solveXOnCycle(x: X): XOutput
  def solveYOnCycle(y: Y): YOutput
}

object DpZipperAlgo {
  def resolve[X <: AnyRef, Y <: AnyRef, XOutput <: AnyRef, YOutput <: AnyRef](
      algoDef: DpZipperAlgoDef[X, Y, XOutput, YOutput],
      adjustment: Adjustment[X, Y],
      root: Y): Solution[X, Y, XOutput, YOutput] = {
    val algo = new DpZipperAlgoResolver(algoDef, adjustment)
    algo.resolve(root)
  }

  trait Adjustment[X <: AnyRef, Y <: AnyRef] {
    import Adjustment._
    def exploreChildX(panel: Panel[X, Y], x: X): Unit
    def exploreParentY(panel: Panel[X, Y], y: Y): Unit
    def exploreChildY(panel: Panel[X, Y], y: Y): Unit
    def exploreParentX(panel: Panel[X, Y], x: X): Unit
  }

  object Adjustment {
    def none[X <: AnyRef, Y <: AnyRef](): Adjustment[X, Y] = new None()

    private class None[X <: AnyRef, Y <: AnyRef] extends Adjustment[X, Y] {
      // IDEA complains if simply using `panel: Panel[X, Y]` as parameter. Not sure why.
      override def exploreChildX(panel: Adjustment.Panel[X, Y], x: X): Unit = {}
      override def exploreParentY(panel: Adjustment.Panel[X, Y], y: Y): Unit = {}
      override def exploreChildY(panel: Adjustment.Panel[X, Y], y: Y): Unit = {}
      override def exploreParentX(panel: Adjustment.Panel[X, Y], x: X): Unit = {}
    }

    trait Panel[X <: AnyRef, Y <: AnyRef] {
      def invalidateXSolution(x: X): Unit
      def invalidateYSolution(y: Y): Unit
    }

    object Panel {
      def apply[X <: AnyRef, Y <: AnyRef, XOutput <: AnyRef, YOutput <: AnyRef](
          sBuilder: Solution.Builder[X, Y, XOutput, YOutput]): Panel[X, Y] =
        new PanelImpl[X, Y, XOutput, YOutput](sBuilder)

      private class PanelImpl[X <: AnyRef, Y <: AnyRef, XOutput <: AnyRef, YOutput <: AnyRef](
          sBuilder: Solution.Builder[X, Y, XOutput, YOutput])
        extends Panel[X, Y] {
        override def invalidateXSolution(x: X): Unit = {
          if (!sBuilder.isXResolved(x)) {
            return
          }
          sBuilder.invalidateXSolution(x)
        }

        override def invalidateYSolution(y: Y): Unit = {
          if (!sBuilder.isYResolved(y)) {
            return
          }
          sBuilder.invalidateYSolution(y)
        }
      }
    }
  }

  private class DpZipperAlgoResolver[
      X <: AnyRef,
      Y <: AnyRef,
      XOutput <: AnyRef,
      YOutput <: AnyRef](
      algoDef: DpZipperAlgoDef[X, Y, XOutput, YOutput],
      adjustment: Adjustment[X, Y]) {
    import DpZipperAlgoResolver._

    private val sBuilder: Solution.Builder[X, Y, XOutput, YOutput] =
      Solution.builder[X, Y, XOutput, YOutput](algoDef)
    private val adjustmentPanel = Adjustment.Panel[X, Y, XOutput, YOutput](sBuilder)

    def resolve(root: Y): Solution[X, Y, XOutput, YOutput] = {
      val xCycleDetector =
        CycleDetector[X]((one, other) => algoDef.idOfX(one) == algoDef.idOfX(other))
      val yCycleDetector =
        CycleDetector[Y]((one, other) => algoDef.idOfY(one) == algoDef.idOfY(other))
      solveYRec(root, xCycleDetector, yCycleDetector)
      sBuilder.build()
    }

    private def solveYRec(
        thisY: Y,
        xCycleDetector: CycleDetector[X],
        yCycleDetector: CycleDetector[Y]): CycleAwareYOutput[X, Y, XOutput, YOutput] = {
      if (yCycleDetector.contains(thisY)) {
        return CycleAwareYOutput(algoDef.solveYOnCycle(thisY), CycleMemory(algoDef).addY(thisY))
      }
      val newYCycleDetector = yCycleDetector.append(thisY)
      if (sBuilder.isYResolved(thisY)) {
        // The same Y was already solved by previous traversals before bumping into
        // this position.
        return CycleAwareYOutput(sBuilder.getYSolution(thisY), CycleMemory(algoDef))
      }

      val cyclicXs: mutable.Set[XKey[X, Y, XOutput, YOutput]] = mutable.Set()
      val cyclicYs: mutable.Set[YKey[X, Y, XOutput, YOutput]] = mutable.Set()

      val xSolutions: mutable.Map[XKey[X, Y, XOutput, YOutput], XOutput] = mutable.Map()

      def loop(): Unit = {
        while (true) {
          val xKeys: Set[XKey[X, Y, XOutput, YOutput]] =
            algoDef.browseY(thisY).map(algoDef.keyOfX(_)).toSet

          val xCount = xKeys.size
          if (xCount == xSolutions.size) {
            // We got enough children solutions.
            return
          }

          xKeys.filterNot(xKey => xSolutions.contains(xKey)).foreach {
            childXKey =>
              val xOutputs = solveXRec(childXKey.x, xCycleDetector, newYCycleDetector)
              val cm = xOutputs.cycleMemory()
              cyclicXs ++= cm.cyclicXs
              cyclicYs ++= cm.cyclicYs
              sBuilder.addYAsBackDependencyOfX(thisY, childXKey.x)
              xSolutions += childXKey -> xOutputs.output()
              // Try applying adjustment
              // to see if algo caller likes to add some Xs or to invalidate
              // some of the registered solutions.
              adjustment.exploreChildX(adjustmentPanel, childXKey.x)
          }
          adjustment.exploreParentY(adjustmentPanel, thisY)
          // If an adjustment (this adjustment or children's) just invalidated one or more
          // children of this element's solutions, the children's keys would be removed from
          // back-dependency list. We do a test here to trigger re-computation if some children
          // do get invalidated.
          xSolutions.keySet.foreach {
            childXKey =>
              if (!sBuilder.yHasDependency(thisY, childXKey.x)) {
                xSolutions -= childXKey
              }
          }
        }
      }

      loop()

      // Remove this element from cycle memory, if it's in it.
      cyclicYs -= algoDef.keyOfY(thisY)

      val cycleMemory = CycleMemory(algoDef, cyclicXs.toSet, cyclicYs.toSet)

      val ySolution =
        algoDef.solveY(thisY, x => xSolutions(XKey(algoDef, x)))

      val cycleAware = CycleAwareYOutput(ySolution, cycleMemory)
      if (!cycleMemory.isOnCycle()) {
        // We only cache the solution if this element is not on any cycles.
        sBuilder.addYSolution(thisY, ySolution)
      }
      cycleAware
    }

    private def solveXRec(
        thisX: X,
        xCycleDetector: CycleDetector[X],
        yCycleDetector: CycleDetector[Y]): CycleAwareXOutput[X, Y, XOutput, YOutput] = {
      if (xCycleDetector.contains(thisX)) {
        return CycleAwareXOutput(algoDef.solveXOnCycle(thisX), CycleMemory(algoDef).addX(thisX))
      }
      val newXCycleDetector = xCycleDetector.append(thisX)
      if (sBuilder.isXResolved(thisX)) {
        // The same X was already solved by previous traversals before bumping into
        // this position.
        return CycleAwareXOutput(sBuilder.getXSolution(thisX), CycleMemory(algoDef))
      }

      val cyclicXs: mutable.Set[XKey[X, Y, XOutput, YOutput]] = mutable.Set()
      val cyclicYs: mutable.Set[YKey[X, Y, XOutput, YOutput]] = mutable.Set()

      val ySolutions: mutable.Map[YKey[X, Y, XOutput, YOutput], YOutput] = mutable.Map()

      def loop(): Unit = {
        while (true) {
          val yKeys: Set[YKey[X, Y, XOutput, YOutput]] =
            algoDef.browseX(thisX).map(algoDef.keyOfY(_)).toSet

          val yCount = yKeys.size
          if (yCount == ySolutions.size) {
            // We got enough children solutions.
            return
          }

          yKeys.filterNot(yKey => ySolutions.contains(yKey)).foreach {
            childYKey =>
              val yOutputs = solveYRec(childYKey.y, newXCycleDetector, yCycleDetector)
              val cm = yOutputs.cycleMemory()
              cyclicXs ++= cm.cyclicXs
              cyclicYs ++= cm.cyclicYs
              sBuilder.addXAsBackDependencyOfY(thisX, childYKey.y)
              ySolutions += childYKey -> yOutputs.output()
              // Try applying adjustment
              // to see if algo caller likes to add some Ys or to invalidate
              // some of the registered solutions.
              adjustment.exploreChildY(adjustmentPanel, childYKey.y)
          }
          adjustment.exploreParentX(adjustmentPanel, thisX)
          // If an adjustment (this adjustment or children's) just invalidated one or more
          // children of this element's solutions, the children's keys would be removed from
          // back-dependency list. We do a test here to trigger re-computation if some children
          // do get invalidated.
          ySolutions.keySet.foreach {
            childYKey =>
              if (!sBuilder.xHasDependency(thisX, childYKey.y)) {
                ySolutions -= childYKey
              }
          }
        }
      }

      loop()

      // Remove this element from cycle memory, if it's in it.
      cyclicXs -= algoDef.keyOfX(thisX)

      val cycleMemory = CycleMemory(algoDef, cyclicXs.toSet, cyclicYs.toSet)

      val xSolution =
        algoDef.solveX(thisX, y => ySolutions(YKey(algoDef, y)))

      val cycleAware = CycleAwareXOutput(xSolution, cycleMemory)
      if (!cycleMemory.isOnCycle()) {
        // We only cache the solution if this element is not on any cycles.
        sBuilder.addXSolution(thisX, xSolution)
      }
      cycleAware
    }

  }

  private object DpZipperAlgoResolver {
    private trait CycleAwareXOutput[
        X <: AnyRef,
        Y <: AnyRef,
        XOutput <: AnyRef,
        YOutput <: AnyRef] {
      def output(): XOutput
      def cycleMemory(): CycleMemory[X, Y, XOutput, YOutput]
    }

    private object CycleAwareXOutput {
      def apply[X <: AnyRef, Y <: AnyRef, XOutput <: AnyRef, YOutput <: AnyRef](
          output: XOutput,
          cycleMemory: CycleMemory[X, Y, XOutput, YOutput])
          : CycleAwareXOutput[X, Y, XOutput, YOutput] = {
        new CycleAwareXOutputImpl(output, cycleMemory)
      }

      private class CycleAwareXOutputImpl[
          X <: AnyRef,
          Y <: AnyRef,
          XOutput <: AnyRef,
          YOutput <: AnyRef](
          override val output: XOutput,
          override val cycleMemory: CycleMemory[X, Y, XOutput, YOutput])
        extends CycleAwareXOutput[X, Y, XOutput, YOutput]
    }

    private trait CycleAwareYOutput[
        X <: AnyRef,
        Y <: AnyRef,
        XOutput <: AnyRef,
        YOutput <: AnyRef] {
      def output(): YOutput
      def cycleMemory(): CycleMemory[X, Y, XOutput, YOutput]
    }

    private object CycleAwareYOutput {
      def apply[X <: AnyRef, Y <: AnyRef, XOutput <: AnyRef, YOutput <: AnyRef](
          output: YOutput,
          cycleMemory: CycleMemory[X, Y, XOutput, YOutput])
          : CycleAwareYOutput[X, Y, XOutput, YOutput] = {
        new CycleAwareYOutputImpl(output, cycleMemory)
      }

      private class CycleAwareYOutputImpl[
          X <: AnyRef,
          Y <: AnyRef,
          XOutput <: AnyRef,
          YOutput <: AnyRef](
          override val output: YOutput,
          override val cycleMemory: CycleMemory[X, Y, XOutput, YOutput])
        extends CycleAwareYOutput[X, Y, XOutput, YOutput]
    }

    private trait CycleMemory[X <: AnyRef, Y <: AnyRef, XOutput <: AnyRef, YOutput <: AnyRef] {
      def cyclicXs: Set[XKey[X, Y, XOutput, YOutput]]
      def cyclicYs: Set[YKey[X, Y, XOutput, YOutput]]
      def addX(x: X): CycleMemory[X, Y, XOutput, YOutput]
      def addY(y: Y): CycleMemory[X, Y, XOutput, YOutput]
      def removeX(x: X): CycleMemory[X, Y, XOutput, YOutput]
      def removeY(y: Y): CycleMemory[X, Y, XOutput, YOutput]
      def isOnCycle(): Boolean
    }

    private object CycleMemory {
      def apply[X <: AnyRef, Y <: AnyRef, XOutput <: AnyRef, YOutput <: AnyRef](
          algoDef: DpZipperAlgoDef[X, Y, XOutput, YOutput]): CycleMemory[X, Y, XOutput, YOutput] = {
        new CycleMemoryImpl(algoDef, Set(), Set())
      }

      def apply[X <: AnyRef, Y <: AnyRef, XOutput <: AnyRef, YOutput <: AnyRef](
          algoDef: DpZipperAlgoDef[X, Y, XOutput, YOutput],
          cyclicXs: Set[XKey[X, Y, XOutput, YOutput]],
          cyclicYs: Set[YKey[X, Y, XOutput, YOutput]]): CycleMemory[X, Y, XOutput, YOutput] = {
        new CycleMemoryImpl(algoDef, cyclicXs, cyclicYs)
      }

      private class CycleMemoryImpl[X <: AnyRef, Y <: AnyRef, XOutput <: AnyRef, YOutput <: AnyRef](
          algoDef: DpZipperAlgoDef[X, Y, XOutput, YOutput],
          override val cyclicXs: Set[XKey[X, Y, XOutput, YOutput]],
          override val cyclicYs: Set[YKey[X, Y, XOutput, YOutput]])
        extends CycleMemory[X, Y, XOutput, YOutput] {
        override def addX(x: X): CycleMemory[X, Y, XOutput, YOutput] = new CycleMemoryImpl(
          algoDef,
          cyclicXs + algoDef.keyOfX(x),
          cyclicYs
        )
        override def addY(y: Y): CycleMemory[X, Y, XOutput, YOutput] = new CycleMemoryImpl(
          algoDef,
          cyclicXs,
          cyclicYs + algoDef.keyOfY(y)
        )
        override def removeX(x: X): CycleMemory[X, Y, XOutput, YOutput] = new CycleMemoryImpl(
          algoDef,
          cyclicXs - algoDef.keyOfX(x),
          cyclicYs
        )
        override def removeY(y: Y): CycleMemory[X, Y, XOutput, YOutput] = new CycleMemoryImpl(
          algoDef,
          cyclicXs,
          cyclicYs - algoDef.keyOfY(y)
        )
        override def isOnCycle(): Boolean = cyclicXs.nonEmpty || cyclicYs.nonEmpty
      }
    }
  }

  trait Solution[X <: AnyRef, Y <: AnyRef, XOutput <: AnyRef, YOutput <: AnyRef] {
    def isXSolved(x: X): Boolean
    def isYSolved(y: Y): Boolean
    def solutionOfX(x: X): XOutput
    def solutionOfY(y: Y): YOutput
  }

  private object Solution {
    private case class SolutionImpl[X <: AnyRef, Y <: AnyRef, XOutput <: AnyRef, YOutput <: AnyRef](
        algoDef: DpZipperAlgoDef[X, Y, XOutput, YOutput],
        xSolutions: Map[XKey[X, Y, XOutput, YOutput], XOutput],
        ySolutions: Map[YKey[X, Y, XOutput, YOutput], YOutput])
      extends Solution[X, Y, XOutput, YOutput] {
      override def isXSolved(x: X): Boolean = xSolutions.contains(algoDef.keyOfX(x))
      override def isYSolved(y: Y): Boolean = ySolutions.contains(algoDef.keyOfY(y))
      override def solutionOfX(x: X): XOutput = xSolutions(algoDef.keyOfX(x))
      override def solutionOfY(y: Y): YOutput = ySolutions(algoDef.keyOfY(y))
    }

    def builder[X <: AnyRef, Y <: AnyRef, XOutput <: AnyRef, YOutput <: AnyRef](
        algoDef: DpZipperAlgoDef[X, Y, XOutput, YOutput]): Builder[X, Y, XOutput, YOutput] = {
      Builder[X, Y, XOutput, YOutput](algoDef)
    }

    class Builder[X <: AnyRef, Y <: AnyRef, XOutput <: AnyRef, YOutput <: AnyRef] private (
        algoDef: DpZipperAlgoDef[X, Y, XOutput, YOutput]) {

      // Store the persisted solved elements. Only if a solution doesn't pertain to
      // any cycles then it will be stored there.
      private val xSolutions = mutable.Map[XKey[X, Y, XOutput, YOutput], XOutput]()
      private val ySolutions = mutable.Map[YKey[X, Y, XOutput, YOutput], YOutput]()

      private val xBackDependencies =
        mutable.Map[XKey[X, Y, XOutput, YOutput], mutable.Set[YKey[X, Y, XOutput, YOutput]]]()
      private val yBackDependencies =
        mutable.Map[YKey[X, Y, XOutput, YOutput], mutable.Set[XKey[X, Y, XOutput, YOutput]]]()

      def invalidateXSolution(x: X): Unit = {
        val xKey = algoDef.keyOfX(x)
        invalidateXSolution0(xKey)
      }

      private def invalidateXSolution0(xKey: XKey[X, Y, XOutput, YOutput]): Unit = {
        assert(xSolutions.contains(xKey))
        xSolutions -= xKey
        if (!xBackDependencies.contains(xKey)) {
          return
        }
        val backYs = xBackDependencies(xKey)
        backYs.toList.foreach {
          y =>
            if (isYResolved0(y)) {
              invalidateYSolution0(y)
            }
            backYs -= y
        }
        // Clear x-key from the back dependency table. This will help the algorithm control
        // re-computation after this x gets invalidated.
        xBackDependencies -= xKey
      }

      def invalidateYSolution(y: Y): Unit = {
        val yKey = algoDef.keyOfY(y)
        invalidateYSolution0(yKey)
      }

      private def invalidateYSolution0(yKey: YKey[X, Y, XOutput, YOutput]): Unit = {
        assert(ySolutions.contains(yKey))
        ySolutions -= yKey
        if (!yBackDependencies.contains(yKey)) {
          return
        }
        val backXs = yBackDependencies(yKey)
        backXs.toList.foreach {
          x =>
            if (isXResolved0(x)) {
              invalidateXSolution0(x)
            }
            backXs -= x
        }
        // Clear y-key from the back dependency table. This will help the algorithm control
        // re-computation after this y gets invalidated.
        yBackDependencies -= yKey
      }

      def isXResolved(x: X): Boolean = {
        val xKey = algoDef.keyOfX(x)
        isXResolved0(xKey)
      }

      private def isXResolved0(xKey: XKey[X, Y, XOutput, YOutput]): Boolean = {
        xSolutions.contains(xKey)
      }

      def isYResolved(y: Y): Boolean = {
        val yKey = algoDef.keyOfY(y)
        ySolutions.contains(yKey)
      }

      private def isYResolved0(yKey: YKey[X, Y, XOutput, YOutput]): Boolean = {
        ySolutions.contains(yKey)
      }

      def getXSolution(x: X): XOutput = {
        val xKey = algoDef.keyOfX(x)
        assert(xSolutions.contains(xKey))
        xSolutions(xKey)
      }

      def getYSolution(y: Y): YOutput = {
        val yKey = algoDef.keyOfY(y)
        assert(ySolutions.contains(yKey))
        ySolutions(yKey)
      }

      def addXSolution(x: X, xSolution: XOutput): Unit = {
        val xKey = algoDef.keyOfX(x)
        assert(!xSolutions.contains(xKey))
        xSolutions += xKey -> xSolution
      }

      def addYSolution(y: Y, ySolution: YOutput): Unit = {
        val yKey = algoDef.keyOfY(y)
        assert(!ySolutions.contains(yKey))
        ySolutions += yKey -> ySolution
      }

      def addXAsBackDependencyOfY(x: X, dependency: Y): Unit = {
        val xKey = algoDef.keyOfX(x)
        val yKey = algoDef.keyOfY(dependency)
        yBackDependencies.getOrElseUpdate(yKey, mutable.Set()) += xKey
      }

      def addYAsBackDependencyOfX(y: Y, dependency: X): Unit = {
        val yKey = algoDef.keyOfY(y)
        val xKey = algoDef.keyOfX(dependency)
        xBackDependencies.getOrElseUpdate(xKey, mutable.Set()) += yKey
      }

      def xHasDependency(x: X, y: Y): Boolean = {
        val xKey = algoDef.keyOfX(x)
        val yKey = algoDef.keyOfY(y)
        yBackDependencies.get(yKey).exists(_.contains(xKey))
      }

      def yHasDependency(y: Y, x: X): Boolean = {
        val yKey = algoDef.keyOfY(y)
        val xKey = algoDef.keyOfX(x)
        xBackDependencies.get(xKey).exists(_.contains(yKey))
      }

      def build(): Solution[X, Y, XOutput, YOutput] = {
        SolutionImpl(
          algoDef,
          xSolutions.toMap,
          ySolutions.toMap
        )
      }
    }

    private object Builder {
      def apply[X <: AnyRef, Y <: AnyRef, XOutput <: AnyRef, YOutput <: AnyRef](
          algoDef: DpZipperAlgoDef[X, Y, XOutput, YOutput]): Builder[X, Y, XOutput, YOutput] = {
        new Builder[X, Y, XOutput, YOutput](algoDef)
      }
    }
  }

  class XKey[X <: AnyRef, Y <: AnyRef, XOutput <: AnyRef, YOutput <: AnyRef] private (
      algoDef: DpZipperAlgoDef[X, Y, XOutput, YOutput],
      val x: X) {
    private val id = algoDef.idOfX(x)
    override def hashCode(): Int = id.hashCode()
    override def equals(obj: Any): Boolean = {
      obj match {
        case other: XKey[X, Y, XOutput, YOutput] => id == other.id
        case _ => false
      }
    }
    override def toString: String = x.toString
  }

  private object XKey {
    def apply[X <: AnyRef, Y <: AnyRef, XOutput <: AnyRef, YOutput <: AnyRef](
        algoDef: DpZipperAlgoDef[X, Y, XOutput, YOutput],
        x: X): XKey[X, Y, XOutput, YOutput] = {
      new XKey[X, Y, XOutput, YOutput](algoDef, x)
    }
  }

  class YKey[X <: AnyRef, Y <: AnyRef, XOutput <: AnyRef, YOutput <: AnyRef] private (
      algoDef: DpZipperAlgoDef[X, Y, XOutput, YOutput],
      val y: Y) {
    private val id = algoDef.idOfY(y)
    override def hashCode(): Int = id.hashCode()
    override def equals(obj: Any): Boolean = {
      obj match {
        case other: YKey[X, Y, XOutput, YOutput] => id == other.id
        case _ => false
      }
    }
    override def toString: String = y.toString
  }

  private object YKey {
    def apply[X <: AnyRef, Y <: AnyRef, XOutput <: AnyRef, YOutput <: AnyRef](
        algoDef: DpZipperAlgoDef[X, Y, XOutput, YOutput],
        y: Y): YKey[X, Y, XOutput, YOutput] = {
      new YKey[X, Y, XOutput, YOutput](algoDef, y)
    }
  }

  implicit class DpZipperAlgoDefImplicits[
      X <: AnyRef,
      Y <: AnyRef,
      XOutput <: AnyRef,
      YOutput <: AnyRef](algoDef: DpZipperAlgoDef[X, Y, XOutput, YOutput]) {

    def keyOfX(x: X): XKey[X, Y, XOutput, YOutput] = {
      XKey(algoDef, x)
    }

    def keyOfY(y: Y): YKey[X, Y, XOutput, YOutput] = {
      YKey(algoDef, y)
    }
  }
}
