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

import org.apache.gluten.ras._
import org.apache.gluten.ras.Best.KnownCostPath
import org.apache.gluten.ras.best.BestFinder
import org.apache.gluten.ras.dp.DpZipperAlgo.Adjustment.Panel
import org.apache.gluten.ras.memo.{Memo, MemoTable}
import org.apache.gluten.ras.path._
import org.apache.gluten.ras.property.PropertySet
import org.apache.gluten.ras.rule.{EnforcerRuleSet, RuleApplier, Shape}

// TODO: Branch and bound pruning.
private class DpPlanner[T <: AnyRef] private (ras: Ras[T], constraintSet: PropertySet[T], plan: T)
  extends RasPlanner[T] {
  import DpPlanner._

  private val memo = Memo.unsafe(ras)
  private val rules = ras.ruleFactory.create().map(rule => RuleApplier(ras, memo, rule))
  private val enforcerRuleSet = EnforcerRuleSet[T](ras, memo)

  private lazy val rootGroupId: Int = {
    memo.memorize(plan, constraintSet).id()
  }

  private lazy val best: (Best[T], KnownCostPath[T]) = {
    val groupId = rootGroupId
    val memoTable = memo.table()
    val best = findBest(memoTable, groupId)
    (best, best.path())
  }

  override def plan(): T = {
    best._2.rasPath.plan()
  }

  override def newState(): PlannerState[T] = {
    val foundBest = best._1
    PlannerState(ras, memo.newState(), rootGroupId, foundBest)
  }

  private def findBest(memoTable: MemoTable[T], groupId: Int): Best[T] = {
    val cKey = memoTable.asGroupSupplier()(groupId).clusterKey()
    val algoDef = new DpExploreAlgoDef[T]
    val adjustment = new ExploreAdjustment(ras, memoTable, rules, enforcerRuleSet)
    DpClusterAlgo.resolve(memoTable, algoDef, adjustment, cKey)
    val finder = BestFinder(ras, memoTable.newState())
    finder.bestOf(groupId)
  }
}

object DpPlanner {
  def apply[T <: AnyRef](ras: Ras[T], constraintSet: PropertySet[T], plan: T): RasPlanner[T] = {
    new DpPlanner(ras, constraintSet, plan)
  }

  // Visited flag.
  sealed private trait SolvedFlag
  private case object Solved extends SolvedFlag

  private class DpExploreAlgoDef[T <: AnyRef] extends DpClusterAlgoDef[T, SolvedFlag, SolvedFlag] {
    override def solveNode(
        node: InClusterNode[T],
        childrenClustersOutput: RasClusterKey => SolvedFlag): SolvedFlag = Solved
    override def solveCluster(
        group: RasClusterKey,
        nodesOutput: InClusterNode[T] => SolvedFlag): SolvedFlag = Solved
    override def solveNodeOnCycle(node: InClusterNode[T]): SolvedFlag = Solved
    override def solveClusterOnCycle(cluster: RasClusterKey): SolvedFlag = Solved
  }

  private class ExploreAdjustment[T <: AnyRef](
      ras: Ras[T],
      memoTable: MemoTable[T],
      rules: Seq[RuleApplier[T]],
      enforcerRuleSet: EnforcerRuleSet[T])
    extends DpClusterAlgo.Adjustment[T] {
    import ExploreAdjustment._

    private val ruleShapes: Seq[Shape[T]] = rules.map(_.shape())

    override def exploreChildX(
        panel: Panel[InClusterNode[T], RasClusterKey],
        x: InClusterNode[T]): Unit = {
      applyRulesOnNode(panel, x.clusterKey, x.can)
    }

    override def exploreChildY(
        panel: Panel[InClusterNode[T], RasClusterKey],
        y: RasClusterKey): Unit = {}
    override def exploreParentX(
        panel: Panel[InClusterNode[T], RasClusterKey],
        x: InClusterNode[T]): Unit = {}

    override def exploreParentY(
        panel: Panel[InClusterNode[T], RasClusterKey],
        cKey: RasClusterKey): Unit = {
      memoTable.doExhaustively {
        applyEnforcerRules(panel, cKey)
      }
    }

    private def applyRulesOnNode(
        panel: Panel[InClusterNode[T], RasClusterKey],
        cKey: RasClusterKey,
        can: CanonicalNode[T]): Unit = {
      if (rules.isEmpty) {
        return
      }
      val dummyGroup = memoTable.getDummyGroup(cKey)
      findPaths(GroupNode(ras, dummyGroup), ruleShapes, List(new FromSingleNode[T](can))) {
        path =>
          val rootNode = path.node().self()
          if (rootNode.isCanonical) {
            assert(rootNode.asCanonical() eq can)
          }
          rules.foreach(rule => applyRule(panel, cKey, rule, path))
      }
    }

    private def applyEnforcerRules(
        panel: Panel[InClusterNode[T], RasClusterKey],
        cKey: RasClusterKey): Unit = {
      val dummyGroup = memoTable.getDummyGroup(cKey)
      cKey.propSets(memoTable).foreach {
        constraintSet: PropertySet[T] =>
          val enforcerRules = enforcerRuleSet.rulesOf(constraintSet)
          if (enforcerRules.nonEmpty) {
            val shapes = enforcerRuleSet.ruleShapesOf(constraintSet)
            findPaths(GroupNode(ras, dummyGroup), shapes, List.empty) {
              path => enforcerRules.foreach(rule => applyRule(panel, cKey, rule, path))
            }
          }
      }
    }

    private def findPaths(gn: GroupNode[T], shapes: Seq[Shape[T]], filters: Seq[FilterWizard[T]])(
        onFound: RasPath[T] => Unit): Unit = {
      val finderBuilder = shapes
        .foldLeft(
          PathFinder
            .builder(ras, memoTable)) {
          case (builder, shape) =>
            builder.output(shape.wizard())
        }

      val finder = filters
        .foldLeft(finderBuilder) {
          case (builder, filter) =>
            builder.filter(filter)
        }
        .build()

      finder.find(gn).foreach(path => onFound(path))
    }

    private def applyRule(
        panel: Panel[InClusterNode[T], RasClusterKey],
        thisClusterKey: RasClusterKey,
        rule: RuleApplier[T],
        path: RasPath[T]): Unit = {
      val probe = memoTable.probe()
      rule.apply(InClusterPath(thisClusterKey, path))
      val diff = probe.toDiff()
      val changedClusters = diff.changedClusters()
      if (changedClusters.isEmpty) {
        return
      }

      // One or more cluster changed. If they're not the current cluster, we should
      // withdraw the DP results for them to trigger re-computation. Since
      // changed cluster (may create new groups, may add new nodes) could expand the
      // search spaces again.

      changedClusters.foreach {
        case cKey if cKey == thisClusterKey =>
        // This cluster has been changed. This cluster is being solved so we
        // don't have to invalidate.
        case cKey =>
          // Changes happened on another cluster. Invalidate solution for the cluster
          // To trigger re-computation.
          panel.invalidateYSolution(cKey)
      }
    }
  }

  private object ExploreAdjustment {
    private class FromSingleNode[T <: AnyRef](from: CanonicalNode[T]) extends FilterWizard[T] {
      override def omit(can: CanonicalNode[T]): FilterWizard.FilterAction[T] = {
        if (can eq from) {
          return FilterWizard.FilterAction.Continue(this)
        }
        FilterWizard.FilterAction.omit
      }

      override def omit(group: GroupNode[T]): FilterWizard.FilterAction[T] =
        FilterWizard.FilterAction.Continue(this)

      override def advance(offset: Int, count: Int): FilterWizard.FilterAdvanceAction[T] = {
        // We only filter on nodes from the root group. So continue with a noop filter.
        FilterWizard.FilterAdvanceAction.Continue(FilterWizards.none())
      }
    }
  }
}
