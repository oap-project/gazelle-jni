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
package org.apache.gluten.backendsapi.velox

import org.apache.gluten.GlutenConfig
import org.apache.gluten.backendsapi.RuleApi
import org.apache.gluten.columnarbatch.VeloxBatch
import org.apache.gluten.extension._
import org.apache.gluten.extension.columnar._
import org.apache.gluten.extension.columnar.MiscColumnarRules.{RemoveGlutenTableCacheColumnarToRow, RemoveTopmostColumnarToRow, RewriteSubqueryBroadcast}
import org.apache.gluten.extension.columnar.enumerated.{RasOffload, RemoveSort}
import org.apache.gluten.extension.columnar.enumerated.planner.cost.{LegacyCoster, RoughCoster, RoughCoster2}
import org.apache.gluten.extension.columnar.heuristic.{ExpandFallbackPolicy, HeuristicTransform}
import org.apache.gluten.extension.columnar.offload.{OffloadExchange, OffloadJoin, OffloadOthers}
import org.apache.gluten.extension.columnar.rewrite._
import org.apache.gluten.extension.columnar.transition.{InsertTransitions, RemoveTransitions}
import org.apache.gluten.extension.columnar.validator.{Validator, Validators}
import org.apache.gluten.extension.injector.{Injector, SparkInjector}
import org.apache.gluten.extension.injector.GlutenInjector.{LegacyInjector, RasInjector}
import org.apache.gluten.sql.shims.SparkShimLoader

import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.aggregate.{HashAggregateExec, ObjectHashAggregateExec, SortAggregateExec}
import org.apache.spark.sql.execution.datasources.WriteFilesExec
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2ScanExecBase
import org.apache.spark.sql.execution.exchange.Exchange
import org.apache.spark.sql.execution.joins.BaseJoinExec
import org.apache.spark.sql.execution.python.EvalPythonExec
import org.apache.spark.sql.execution.window.WindowExec
import org.apache.spark.sql.hive.HiveTableScanExecTransformer

class VeloxRuleApi extends RuleApi {
  import VeloxRuleApi._

  override def injectRules(injector: Injector): Unit = {
    injectSpark(injector.spark)
    injectLegacy(injector.gluten.legacy)
    injectRas(injector.gluten.ras)
  }
}

object VeloxRuleApi {
  private def injectSpark(injector: SparkInjector): Unit = {
    // Inject the regular Spark rules directly.
    injector.injectOptimizerRule(CollectRewriteRule.apply)
    injector.injectOptimizerRule(HLLRewriteRule.apply)
    injector.injectPostHocResolutionRule(ArrowConvertorRule.apply)
  }

  private def injectLegacy(injector: LegacyInjector): Unit = {
    // Legacy: Pre-transform rules.
    injector.injectPreTransform(_ => RemoveTransitions)
    injector.injectPreTransform(_ => PushDownInputFileExpression.PreOffload)
    injector.injectPreTransform(c => FallbackOnANSIMode.apply(c.session))
    injector.injectPreTransform(c => FallbackMultiCodegens.apply(c.session))
    injector.injectPreTransform(c => MergeTwoPhasesHashBaseAggregate(c.session))
    injector.injectPreTransform(_ => RewriteSubqueryBroadcast())
    injector.injectPreTransform(c => BloomFilterMightContainJointRewriteRule.apply(c.session))
    injector.injectPreTransform(c => ArrowScanReplaceRule.apply(c.session))

    // Legacy: The legacy transform rule.
    val offloads = Seq(OffloadOthers(), OffloadExchange(), OffloadJoin())
    val validatorBuilder: GlutenConfig => Validator = conf =>
      Validators.newValidator(conf, offloads)
    val rewrites =
      Seq(RewriteIn, RewriteMultiChildrenCount, RewriteJoin, PullOutPreProject, PullOutPostProject)
    injector.injectTransform(
      c => HeuristicTransform.WithRewrites(validatorBuilder(c.glutenConf), rewrites, offloads))

    // Legacy: Post-transform rules.
    injector.injectPostTransform(_ => UnionTransformerRule())
    injector.injectPostTransform(c => PartialProjectRule.apply(c.session))
    injector.injectPostTransform(_ => RemoveNativeWriteFilesSortAndProject())
    injector.injectPostTransform(_ => PushDownFilterToScan)
    injector.injectPostTransform(_ => PushDownInputFileExpression.PostOffload)
    injector.injectPostTransform(_ => EnsureLocalSortRequirements)
    injector.injectPostTransform(_ => EliminateLocalSort)
    injector.injectPostTransform(_ => CollapseProjectExecTransformer)
    injector.injectPostTransform(c => FlushableHashAggregateRule.apply(c.session))
    injector.injectPostTransform(c => InsertTransitions.create(c.outputsColumnar, VeloxBatch))

    // Gluten columnar: Fallback policies.
    injector.injectFallbackPolicy(
      c => ExpandFallbackPolicy(c.ac.isAdaptiveContext(), c.ac.originalPlan()))

    // Gluten columnar: Post rules.
    injector.injectPost(c => RemoveTopmostColumnarToRow(c.session, c.ac.isAdaptiveContext()))
    SparkShimLoader.getSparkShims
      .getExtendedColumnarPostRules()
      .foreach(each => injector.injectPost(c => each(c.session)))
    injector.injectPost(c => ColumnarCollapseTransformStages(c.glutenConf))

    // Gluten columnar: Final rules.
    injector.injectFinal(c => RemoveGlutenTableCacheColumnarToRow(c.session))
    injector.injectFinal(c => GlutenFallbackReporter(c.glutenConf, c.session))
    injector.injectFinal(_ => RemoveFallbackTagRule())
  }

  private def injectRas(injector: RasInjector): Unit = {
    // Gluten RAS: Pre rules.
    injector.injectPreTransform(_ => RemoveTransitions)
    injector.injectPreTransform(_ => PushDownInputFileExpression.PreOffload)
    injector.injectPreTransform(c => FallbackOnANSIMode.apply(c.session))
    injector.injectPreTransform(c => MergeTwoPhasesHashBaseAggregate(c.session))
    injector.injectPreTransform(_ => RewriteSubqueryBroadcast())
    injector.injectPreTransform(c => BloomFilterMightContainJointRewriteRule.apply(c.session))
    injector.injectPreTransform(c => ArrowScanReplaceRule.apply(c.session))

    // Gluten RAS: The RAS rule.
    val validatorBuilder: GlutenConfig => Validator = conf => Validators.newValidator(conf)
    val rewrites =
      Seq(RewriteIn, RewriteMultiChildrenCount, RewriteJoin, PullOutPreProject, PullOutPostProject)
    injector.injectCoster(_ => LegacyCoster)
    injector.injectCoster(_ => RoughCoster)
    injector.injectCoster(_ => RoughCoster2)
    injector.injectRasRule(_ => RemoveSort)
    val offloads: Seq[RasOffload] = Seq(
      RasOffload.from[Exchange](OffloadExchange()),
      RasOffload.from[BaseJoinExec](OffloadJoin()),
      RasOffload.from[FilterExec](OffloadOthers()),
      RasOffload.from[ProjectExec](OffloadOthers()),
      RasOffload.from[DataSourceV2ScanExecBase](OffloadOthers()),
      RasOffload.from[DataSourceScanExec](OffloadOthers()),
      RasOffload.from(HiveTableScanExecTransformer.isHiveTableScan(_))(OffloadOthers()),
      RasOffload.from[CoalesceExec](OffloadOthers()),
      RasOffload.from[HashAggregateExec](OffloadOthers()),
      RasOffload.from[SortAggregateExec](OffloadOthers()),
      RasOffload.from[ObjectHashAggregateExec](OffloadOthers()),
      RasOffload.from[UnionExec](OffloadOthers()),
      RasOffload.from[ExpandExec](OffloadOthers()),
      RasOffload.from[WriteFilesExec](OffloadOthers()),
      RasOffload.from[SortExec](OffloadOthers()),
      RasOffload.from[TakeOrderedAndProjectExec](OffloadOthers()),
      RasOffload.from[WindowExec](OffloadOthers()),
      RasOffload.from(SparkShimLoader.getSparkShims.isWindowGroupLimitExec(_))(OffloadOthers()),
      RasOffload.from[LimitExec](OffloadOthers()),
      RasOffload.from[GenerateExec](OffloadOthers()),
      RasOffload.from[EvalPythonExec](OffloadOthers()),
      RasOffload.from[SampleExec](OffloadOthers())
    )
    offloads.foreach(
      offload =>
        injector.injectRasRule(
          c => RasOffload.Rule(offload, validatorBuilder(c.glutenConf), rewrites)))

    // Gluten RAS: Post rules.
    injector.injectPostTransform(_ => RemoveTransitions)
    injector.injectPostTransform(_ => UnionTransformerRule())
    injector.injectPostTransform(c => PartialProjectRule.apply(c.session))
    injector.injectPostTransform(_ => RemoveNativeWriteFilesSortAndProject())
    injector.injectPostTransform(_ => PushDownFilterToScan)
    injector.injectPostTransform(_ => PushDownInputFileExpression.PostOffload)
    injector.injectPostTransform(_ => EnsureLocalSortRequirements)
    injector.injectPostTransform(_ => EliminateLocalSort)
    injector.injectPostTransform(_ => CollapseProjectExecTransformer)
    injector.injectPostTransform(c => FlushableHashAggregateRule.apply(c.session))
    injector.injectPostTransform(c => InsertTransitions.create(c.outputsColumnar, VeloxBatch))
    injector.injectPostTransform(
      c => RemoveTopmostColumnarToRow(c.session, c.ac.isAdaptiveContext()))
    SparkShimLoader.getSparkShims
      .getExtendedColumnarPostRules()
      .foreach(each => injector.injectPostTransform(c => each(c.session)))
    injector.injectPostTransform(c => ColumnarCollapseTransformStages(c.glutenConf))
    injector.injectPostTransform(c => RemoveGlutenTableCacheColumnarToRow(c.session))
    injector.injectPostTransform(c => GlutenFallbackReporter(c.glutenConf, c.session))
    injector.injectPostTransform(_ => RemoveFallbackTagRule())
  }
}
