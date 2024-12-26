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
package org.apache.gluten.extension.columnar.enumerated

import org.apache.gluten.component.Component
import org.apache.gluten.exception.GlutenException
import org.apache.gluten.extension.columnar.ColumnarRuleApplier.ColumnarRuleCall
import org.apache.gluten.extension.columnar.enumerated.planner.GlutenOptimization
import org.apache.gluten.extension.columnar.enumerated.planner.property.Conv
import org.apache.gluten.extension.injector.Injector
import org.apache.gluten.extension.util.AdaptiveContext
import org.apache.gluten.logging.LogLevelUtil
import org.apache.gluten.ras.CostModel
import org.apache.gluten.ras.property.PropertySet
import org.apache.gluten.ras.rule.RasRule

import org.apache.spark.sql.{SparkSession, SparkSessionExtensions}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution._

/**
 * Rule to offload Spark query plan to Gluten query plan using a search algorithm and a defined cost
 * model.
 *
 * The effect of this rule is similar to
 * [[org.apache.gluten.extension.columnar.heuristic.HeuristicTransform]], except that the 3 stages
 * in the heuristic version, known as rewrite, validate, offload, will take place together
 * individually for each Spark query plan node in RAS rule
 * [[org.apache.gluten.extension.columnar.enumerated.RasOffload]].
 *
 * The feature requires enabling RAS to function.
 */
case class EnumeratedTransform(costModel: CostModel[SparkPlan], rules: Seq[RasRule[SparkPlan]])
  extends Rule[SparkPlan]
  with LogLevelUtil {

  private val optimization = {
    GlutenOptimization
      .builder()
      .costModel(costModel)
      .addRules(rules)
      .create()
  }

  private val convReq = Conv.any

  override def apply(plan: SparkPlan): SparkPlan = {
    val constraintSet = PropertySet(Seq(convReq))
    val planner = optimization.newPlanner(plan, constraintSet)
    val out = planner.plan()
    out
  }
}

object EnumeratedTransform {
  // Creates a static EnumeratedTransform rule for use in certain
  // places that requires to emulate the offloading of a Spark query plan.
  //
  // TODO: Avoid using this and eventually remove the API.
  def static(): EnumeratedTransform = {
    val exts = new SparkSessionExtensions()
    val dummyInjector = new Injector(exts)
    // Components should override Backend's rules. Hence, reversed injection order is applied.
    Component.sorted().reverse.foreach(_.injectRules(dummyInjector))
    val session = SparkSession.getActiveSession.getOrElse(
      throw new GlutenException(
        "HeuristicTransform#static can only be called when an active Spark session exists"))
    val call = new ColumnarRuleCall(session, AdaptiveContext(session), false)
    dummyInjector.gluten.ras.createEnumeratedTransform(call)
  }
}
