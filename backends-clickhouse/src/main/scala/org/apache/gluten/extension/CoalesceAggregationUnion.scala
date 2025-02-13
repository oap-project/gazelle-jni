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
package org.apache.gluten.extension

import org.apache.gluten.backendsapi.clickhouse.CHBackendSettings
import org.apache.gluten.exception.GlutenNotSupportException

import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.analysis.MultiInstanceRelation
import org.apache.spark.sql.catalyst.catalog.HiveTableRelation
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate._
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.datasources._
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.util.{Failure, Success, Try}

/*
 * Example:
 * Rewrite query
 *  SELECT a, b, sum(c) FROM t WHERE d = 1 GROUP BY a,b
 *  UNION ALL
 *  SELECT a, b, sum(c) FROM t WHERE d = 2 GROUP BY a,b
 * into
 *  SELECT a, b, sum(c) FROM (
 *   SELECT s.a as a, s.b as b, s.c as c, s.id as group_id FROM (
 *    SELECT explode(s) as s FROM (
 *      SELECT array(
 *        if(d = 1, named_struct('a', a, 'b', b, 'c', c, 'id', 0), null),
 *        if(d = 2, named_struct('a', a, 'b', b, 'c', c, 'id', 1), null)) as s
 *      FROM t WHERE d = 1 OR d = 2
 *    )
 *   ) WHERE s is not null
 *  ) GROUP BY a,b, group_id
 *
 * The first query need to scan `t` multiply, when the output of scan is large, the query is
 * really slow. The rewritten query only scan `t` once, and the performance is much better.
 */

class CoalesceAggregationUnion(spark: SparkSession) extends Rule[LogicalPlan] with Logging {
  def removeAlias(e: Expression): Expression = {
    e match {
      case alias: Alias => alias.child
      case _ => e
    }
  }

  def hasAggregateExpression(e: Expression): Boolean = {
    if (e.children.isEmpty && !e.isInstanceOf[AggregateExpression]) {
      return false
    }
    e match {
      case _: AggregateExpression => true
      case _ => e.children.exists(hasAggregateExpression(_))
    }
  }

  def isAggregateExpression(e: Expression): Boolean = {
    e match {
      case cast: Cast => isAggregateExpression(cast.child)
      case alias: Alias => isAggregateExpression(alias.child)
      case agg: AggregateExpression => true
      case _ => false
    }
  }

  def hasAggregateExpressionsWithFilter(e: Expression): Boolean = {
    if (e.children.isEmpty && !e.isInstanceOf[AggregateExpression]) {
      return false
    }
    e match {
      case aggExpr: AggregateExpression =>
        aggExpr.filter.isDefined
      case _ => e.children.exists(hasAggregateExpressionsWithFilter(_))
    }
  }

  def buildAttributesMap(
      attributes: Seq[Attribute],
      expressions: Seq[Expression]): Map[ExprId, Expression] = {
    assert(attributes.length == expressions.length)
    val map = new mutable.HashMap[ExprId, Expression]()
    attributes.zip(expressions).foreach {
      case (attr, expr) =>
        map.put(attr.exprId, expr)
    }
    map.toMap
  }

  def replaceAttributes(e: Expression, replaceMap: Map[ExprId, Expression]): Expression = {
    e match {
      case attr: Attribute =>
        replaceMap.get(attr.exprId) match {
          case Some(replaceAttr) => replaceAttr
          case None =>
            throw new GlutenNotSupportException(s"Not found attribute: $attr ${attr.qualifiedName}")
        }
      case _ =>
        e.withNewChildren(e.children.map(replaceAttributes(_, replaceMap)))
    }
  }

  case class AggregateAnalzyInfo(originalAggregate: Aggregate) {
    protected def extractFilter(): Option[Filter] = {
      originalAggregate.child match {
        case filter: Filter => Some(filter)
        case project @ Project(_, filter: Filter) => Some(filter)
        case subquery: SubqueryAlias =>
          subquery.child match {
            case filter: Filter => Some(filter)
            case project @ Project(_, filter: Filter) => Some(filter)
            case relation if isRelation(relation) =>
              Some(Filter(Literal(true, BooleanType), subquery))
            case nestedRelation: SubqueryAlias if (isRelation(nestedRelation.child)) =>
              Some(Filter(Literal(true, BooleanType), nestedRelation))
            case _ => None
          }
        case _ => None
      }
    }

    def isValidSource(plan: LogicalPlan): Boolean = {
      plan match {
        case relation if isRelation(relation) => true
        case _: Project | _: Filter | _: SubqueryAlias =>
          plan.children.forall(isValidSource)
        case _ => false
      }
    }

    // Try to make the plan simple, contain only three steps, source, filter, aggregate.
    lazy val extractedSourcePlan = {
      val filter = extractFilter()
      if (!filter.isDefined) {
        None
      } else {
        filter.get.child match {
          case project: Project if isValidSource(project.child) => Some(project.child)
          case other if isValidSource(other) => Some(other)
          case _ => None
        }
      }
    }

    lazy val constructedFilterPlan = {
      val filter = extractFilter()
      if (!filter.isDefined || !extractedSourcePlan.isDefined) {
        None
      } else {
        val project = filter.get.child match {
          case project: Project => Some(project)
          case other =>
            None
        }
        val newFilter = project match {
          case Some(project) =>
            val replaceMap = buildAttributesMap(
              project.output,
              project.child.output.map(_.asInstanceOf[Expression]))
            val newCondition = replaceAttributes(filter.get.condition, replaceMap)
            Filter(newCondition, extractedSourcePlan.get)
          case None => filter.get.withNewChildren(Seq(extractedSourcePlan.get))
        }
        Some(newFilter)
      }
    }

    lazy val constructedAggregatePlan = {
      if (!constructedFilterPlan.isDefined) {
        None
      } else {
        val project = originalAggregate.child match {
          case p: Project => Some(p)
          case subquery: SubqueryAlias =>
            subquery.child match {
              case p: Project => Some(p)
              case _ => None
            }
          case _ => None
        }

        val newAggregate = project match {
          case Some(innerProject) =>
            val replaceMap = buildAttributesMap(
              innerProject.output,
              innerProject.projectList.map(_.asInstanceOf[Expression]))
            val newGroupExpressions = originalAggregate.groupingExpressions.map {
              e => replaceAttributes(e, replaceMap)
            }
            val newAggregateExpressions = originalAggregate.aggregateExpressions.map {
              e => replaceAttributes(e, replaceMap).asInstanceOf[NamedExpression]
            }
            Aggregate(newGroupExpressions, newAggregateExpressions, constructedFilterPlan.get)
          case None => originalAggregate.withNewChildren(Seq(constructedFilterPlan.get))
        }
        Some(newAggregate)
      }
    }

    lazy val hasAggregateWithFilter = originalAggregate.aggregateExpressions.exists {
      e => hasAggregateExpressionsWithFilter(e)
    }

    // The output results which are not aggregate expressions.
    lazy val resultGroupingExpressions = constructedAggregatePlan match {
      case Some(agg) =>
        agg.asInstanceOf[Aggregate].aggregateExpressions.filter(e => !hasAggregateExpression(e))
      case None => Seq.empty
    }

    lazy val positionInGroupingKeys = {
      var i = 0
      // In most cases, the expressions which are not aggregate result could be matched with one of
      // groupingk keys. There are some exceptions
      // 1. The expression is a literal. The grouping keys do not contain the literal.
      // 2. The expression is an expression withs gruping keys. For example,
      //    `select k1 + k2, count(1) from t group by k1, k2`.
      resultGroupingExpressions.map {
        e =>
          val aggregate = constructedAggregatePlan.get.asInstanceOf[Aggregate]
          e match {
            case literal @ Alias(_: Literal, _) =>
              var idx = aggregate.groupingExpressions.indexOf(e)
              if (idx == -1) {
                idx = aggregate.groupingExpressions.length + i
                i += 1
              }
              idx
            case _ =>
              var idx = aggregate.groupingExpressions.indexOf(removeAlias(e))
              idx = if (idx == -1) {
                aggregate.groupingExpressions.indexOf(e)
              } else {
                idx
              }
              idx
          }
      }
    }
  }

  /*
   * Case class representing an analyzed plan.
   *
   * @param plan The logical plan that to be analyzed.
   * @param analyzedInfo Optional information about the aggregate analysis.
   */
  case class AnalyzedPlan(plan: LogicalPlan, analyzedInfo: Option[AggregateAnalzyInfo])

  def isResolvedPlan(plan: LogicalPlan): Boolean = {
    plan match {
      case isnert: InsertIntoStatement => isnert.query.resolved
      case _ => plan.resolved
    }
  }

  override def apply(plan: LogicalPlan): LogicalPlan = {
    if (
      spark.conf
        .get(CHBackendSettings.GLUTEN_ENABLE_COALESCE_AGGREGATION_UNION, "true")
        .toBoolean && isResolvedPlan(plan)
    ) {
      Try {
        visitPlan(plan)
      } match {
        case Success(res) => res
        case Failure(e) => plan
      }
    } else {
      plan
    }
  }

  def visitPlan(plan: LogicalPlan): LogicalPlan = {
    plan match {
      case union: Union =>
        val planGroups = groupStructureMatchedAggregate(union)
        if (planGroups.forall(group => group.length == 1)) {
          plan.withNewChildren(plan.children.map(visitPlan))
        } else {
          val newUnionClauses = planGroups.map {
            groupedPlans =>
              if (groupedPlans.length == 1) {
                groupedPlans.head.plan
              } else {
                val firstAggregateAnalzyInfo = groupedPlans.head.analyzedInfo.get
                val aggregates = groupedPlans.map(_.analyzedInfo.get.constructedAggregatePlan.get)
                val filterConditions = buildAggregateCasesConditions(groupedPlans)
                val firstAggregateFilter =
                  firstAggregateAnalzyInfo.constructedFilterPlan.get.asInstanceOf[Filter]

                // Add a filter step with condition `cond1 or cond2 or ...`, `cond_i` comes from
                // each union clause. Apply this filter on the source plan.
                val unionFilter = Filter(
                  buildUnionConditionForAggregateSource(filterConditions),
                  firstAggregateAnalzyInfo.extractedSourcePlan.get)

                // Wrap all the attributes into a single structure attribute.
                val wrappedAttributesProject =
                  buildProjectFoldIntoStruct(unionFilter, groupedPlans, filterConditions)

                // Build an array which element are response to each union clause.
                val arrayProject =
                  buildProjectBranchArray(wrappedAttributesProject, filterConditions)

                // Explode the array
                val explode = buildExplodeBranchArray(arrayProject)

                // Null value means that the union clause does not have the corresponding data.
                val notNullFilter = Filter(IsNotNull(explode.output.head), explode)

                // Destruct the struct attribute.
                val destructStructProject = buildProjectUnfoldStruct(notNullFilter)

                buildAggregateWithGroupId(destructStructProject, groupedPlans)
              }
          }
          val coalesePlan = if (newUnionClauses.length == 1) {
            newUnionClauses.head
          } else {
            var firstUnionChild = newUnionClauses.head
            for (i <- 1 until newUnionClauses.length - 1) {
              firstUnionChild = Union(firstUnionChild, newUnionClauses(i))
            }
            Union(firstUnionChild, newUnionClauses.last)
          }

          // We need to keep the output atrributes same as the original plan.
          val outputAttrPairs = coalesePlan.output.zip(union.output)
          if (outputAttrPairs.forall(pair => pair._1.semanticEquals(pair._2))) {
            coalesePlan
          } else {
            val reprejectOutputs = outputAttrPairs.map {
              case (newAttr, oldAttr) =>
                if (newAttr.exprId == oldAttr.exprId) {
                  newAttr
                } else {
                  Alias(newAttr, oldAttr.name)(oldAttr.exprId, oldAttr.qualifier, None, Seq.empty)
                }
            }
            Project(reprejectOutputs, coalesePlan)
          }
        }
      case _ => plan.withNewChildren(plan.children.map(visitPlan))
    }
  }

  def isRelation(plan: LogicalPlan): Boolean = {
    plan.isInstanceOf[MultiInstanceRelation]
  }

  def areSameRelation(l: LogicalPlan, r: LogicalPlan): Boolean = {
    (l, r) match {
      case (lRelation: LogicalRelation, rRelation: LogicalRelation) =>
        val lTable = lRelation.catalogTable.map(_.identifier.unquotedString).getOrElse("")
        val rTable = rRelation.catalogTable.map(_.identifier.unquotedString).getOrElse("")
        lRelation.output.length == rRelation.output.length &&
        lRelation.output.zip(rRelation.output).forall {
          case (lAttr, rAttr) =>
            lAttr.dataType.equals(rAttr.dataType) && lAttr.name.equals(rAttr.name)
        } &&
        lTable.equals(rTable) && lTable.nonEmpty
      case (lCTE: CTERelationRef, rCTE: CTERelationRef) =>
        lCTE.cteId == rCTE.cteId
      case (lHiveTable: HiveTableRelation, rHiveTable: HiveTableRelation) =>
        lHiveTable.tableMeta.identifier.unquotedString
          .equals(rHiveTable.tableMeta.identifier.unquotedString)
      case (_, _) =>
        logInfo(s"xxx unknow relation: ${l.getClass}, ${r.getClass}")
        false
    }
  }

  def isSupportedAggregate(info: AggregateAnalzyInfo): Boolean = {

    !info.hasAggregateWithFilter &&
    info.constructedAggregatePlan.isDefined &&
    info.positionInGroupingKeys.forall(_ >= 0) &&
    info.originalAggregate.aggregateExpressions.forall {
      e =>
        val innerExpr = removeAlias(e)
        // `agg_fun1(x) + agg_fun2(y)` is supported, but `agg_fun1(x) + y` is not supported.
        if (hasAggregateExpression(innerExpr)) {
          innerExpr.isInstanceOf[AggregateExpression] ||
          innerExpr.children.forall(e => isAggregateExpression(e))
        } else {
          true
        }
    } &&
    info.extractedSourcePlan.isDefined
  }

  /**
   * Checks if two AggregateAnalzyInfo instances have the same structure.
   *
   * This method compares the aggregate expressions, grouping expressions, and the source plans of
   * the two AggregateAnalzyInfo instances to determine if they have the same structure.
   *
   * @param l
   *   The first AggregateAnalzyInfo instance.
   * @param r
   *   The second AggregateAnalzyInfo instance.
   * @return
   *   True if the two instances have the same structure, false otherwise.
   */
  def areStructureMatchedAggregate(l: AggregateAnalzyInfo, r: AggregateAnalzyInfo): Boolean = {
    val lAggregate = l.constructedAggregatePlan.get.asInstanceOf[Aggregate]
    val rAggregate = r.constructedAggregatePlan.get.asInstanceOf[Aggregate]
    lAggregate.aggregateExpressions.length == rAggregate.aggregateExpressions.length &&
    lAggregate.aggregateExpressions.zip(rAggregate.aggregateExpressions).forall {
      case (lExpr, rExpr) =>
        if (!lExpr.dataType.equals(rExpr.dataType)) {
          false
        } else {
          (hasAggregateExpression(lExpr), hasAggregateExpression(rExpr)) match {
            case (true, true) => areStructureMatchedExpressions(lExpr, rExpr)
            case (false, true) => false
            case (true, false) => false
            case (false, false) => true
          }
        }
    } &&
    lAggregate.groupingExpressions.length == rAggregate.groupingExpressions.length &&
    l.positionInGroupingKeys.length == r.positionInGroupingKeys.length &&
    l.positionInGroupingKeys.zip(r.positionInGroupingKeys).forall {
      case (lPos, rPos) => lPos == rPos
    } &&
    areSameAggregateSource(l.extractedSourcePlan.get, r.extractedSourcePlan.get)
  }

  /*
   * Finds the index of the first group in `planGroups` that has the same structure as the given
   * `analyzedInfo`.
   *
   * This method iterates over the `planGroups` and checks if the first `AnalyzedPlan` in each group
   * has an `analyzedInfo` that matches the structure of the provided `analyzedInfo`. If a match is
   * found, the index of the group is returned. If no match is found, -1 is returned.
   *
   * @param planGroups
   *   An ArrayBuffer of ArrayBuffers, where each inner ArrayBuffer contains `AnalyzedPlan`
   *   instances.
   * @param analyzedInfo
   *   The `AggregateAnalzyInfo` to match against the groups in `planGroups`.
   * @return
   *   The index of the first group with a matching structure, or -1 if no match is found.
   */
  def findStructureMatchedAggregate(
      planGroups: ArrayBuffer[ArrayBuffer[AnalyzedPlan]],
      analyzedInfo: AggregateAnalzyInfo): Int = {
    planGroups.zipWithIndex.find(
      planWithIndex =>
        planWithIndex._1.head.analyzedInfo.isDefined &&
          areStructureMatchedAggregate(
            planWithIndex._1.head.analyzedInfo.get,
            analyzedInfo)) match {
      case Some((_, i)) => i
      case None => -1
    }

  }

  // Union only has two children. It's children may also be Union.
  def collectAllUnionClauses(union: Union): ArrayBuffer[LogicalPlan] = {
    val unionClauses = ArrayBuffer[LogicalPlan]()
    union.children.foreach {
      case u: Union =>
        unionClauses ++= collectAllUnionClauses(u)
      case other =>
        unionClauses += other
    }
    unionClauses
  }

  def groupStructureMatchedAggregate(union: Union): ArrayBuffer[ArrayBuffer[AnalyzedPlan]] = {

    def tryPutToGroup(
        groupResults: ArrayBuffer[ArrayBuffer[AnalyzedPlan]],
        agg: Aggregate): Unit = {
      val analyzedInfo = AggregateAnalzyInfo(agg)
      if (isSupportedAggregate(analyzedInfo)) {
        if (groupResults.isEmpty) {
          groupResults += ArrayBuffer(
            AnalyzedPlan(analyzedInfo.originalAggregate, Some(analyzedInfo)))
        } else {
          val idx = findStructureMatchedAggregate(groupResults, analyzedInfo)
          if (idx != -1) {
            groupResults(idx) += AnalyzedPlan(
              analyzedInfo.constructedAggregatePlan.get,
              Some(analyzedInfo))
          } else {
            groupResults += ArrayBuffer(
              AnalyzedPlan(analyzedInfo.constructedAggregatePlan.get, Some(analyzedInfo)))
          }
        }
      } else {
        val rewrittenPlan = visitPlan(agg)
        groupResults += ArrayBuffer(AnalyzedPlan(rewrittenPlan, None))
      }
    }

    val groupResults = ArrayBuffer[ArrayBuffer[AnalyzedPlan]]()
    collectAllUnionClauses(union).foreach {
      case project @ Project(projectList, agg: Aggregate) =>
        if (projectList.forall(e => e.isInstanceOf[Alias])) {
          tryPutToGroup(groupResults, agg)
        } else {
          val rewrittenPlan = visitPlan(project)
          groupResults += ArrayBuffer(AnalyzedPlan(rewrittenPlan, None))
        }
      case agg: Aggregate =>
        tryPutToGroup(groupResults, agg)
      case other =>
        val rewrittenPlan = visitPlan(other)
        groupResults += ArrayBuffer(AnalyzedPlan(rewrittenPlan, None))
    }
    groupResults
  }

  def areStructureMatchedExpressions(l: Expression, r: Expression): Boolean = {
    if (l.dataType.equals(r.dataType)) {
      (l, r) match {
        case (lAttr: Attribute, rAttr: Attribute) =>
          // The the qualifier may be overwritten by a subquery alias, and make this check fail.
          lAttr.qualifiedName.equals(rAttr.qualifiedName)
        case (lLiteral: Literal, rLiteral: Literal) =>
          lLiteral.value == rLiteral.value
        case (lagg: AggregateExpression, ragg: AggregateExpression) =>
          lagg.isDistinct == ragg.isDistinct &&
          areStructureMatchedExpressions(lagg.aggregateFunction, ragg.aggregateFunction)
        case _ =>
          l.children.length == r.children.length &&
          l.getClass == r.getClass &&
          l.children.zip(r.children).forall {
            case (lChild, rChild) => areStructureMatchedExpressions(lChild, rChild)
          }
      }
    } else {
      false
    }
  }

  def areSameAggregateSource(lPlan: LogicalPlan, rPlan: LogicalPlan): Boolean = {
    if (lPlan.children.length != rPlan.children.length || lPlan.getClass != rPlan.getClass) {
      false
    } else {
      lPlan.children.zip(rPlan.children).forall {
        case (lRelation, rRelation) if (isRelation(lRelation) && isRelation(rRelation)) =>
          areSameRelation(lRelation, rRelation)
        case (lSubQuery: SubqueryAlias, rSubQuery: SubqueryAlias) =>
          areSameAggregateSource(lSubQuery.child, rSubQuery.child)
        case (lproject: Project, rproject: Project) =>
          lproject.projectList.length == rproject.projectList.length &&
          lproject.projectList.zip(rproject.projectList).forall {
            case (lExpr, rExpr) => areStructureMatchedExpressions(lExpr, rExpr)
          } &&
          areSameAggregateSource(lproject.child, rproject.child)
        case (lFilter: Filter, rFilter: Filter) =>
          areStructureMatchedExpressions(lFilter.condition, rFilter.condition) &&
          areSameAggregateSource(lFilter.child, rFilter.child)
        case (lChild, rChild) => false
      }
    }
  }

  def buildAggregateCasesConditions(
      groupedPlans: ArrayBuffer[AnalyzedPlan]): ArrayBuffer[Expression] = {
    val firstPlanSourceOutputAttrs =
      groupedPlans.head.analyzedInfo.get.extractedSourcePlan.get.output
    groupedPlans.map {
      plan =>
        val attrsMap =
          buildAttributesMap(
            plan.analyzedInfo.get.extractedSourcePlan.get.output,
            firstPlanSourceOutputAttrs)
        val filter = plan.analyzedInfo.get.constructedFilterPlan.get.asInstanceOf[Filter]
        replaceAttributes(filter.condition, attrsMap)
    }
  }

  def buildUnionConditionForAggregateSource(conditions: ArrayBuffer[Expression]): Expression = {
    conditions.reduce(Or);
  }

  def wrapAggregatesAttributesInStructs(
      groupedPlans: ArrayBuffer[AnalyzedPlan]): Seq[NamedExpression] = {
    val structAttributes = ArrayBuffer[NamedExpression]()
    val casePrefix = "case_"
    val structPrefix = "field_"
    val firstSourceAttrs = groupedPlans.head.analyzedInfo.get.extractedSourcePlan.get.output
    groupedPlans.zipWithIndex.foreach {
      case (aggregateCase, case_index) =>
        val analyzedInfo = aggregateCase.analyzedInfo.get
        val aggregate = analyzedInfo.constructedAggregatePlan.get.asInstanceOf[Aggregate]
        val structFields = ArrayBuffer[Expression]()
        var fieldIndex: Int = 0
        val attrReplaceMap = buildAttributesMap(
          aggregateCase.analyzedInfo.get.extractedSourcePlan.get.output,
          firstSourceAttrs)
        aggregate.groupingExpressions.foreach {
          e =>
            structFields += Literal(UTF8String.fromString(s"$structPrefix$fieldIndex"), StringType)
            structFields += replaceAttributes(e, attrReplaceMap)
            fieldIndex += 1
        }
        for (i <- 0 until analyzedInfo.positionInGroupingKeys.length) {
          val position = analyzedInfo.positionInGroupingKeys(i)
          if (position >= fieldIndex) {
            val expr = analyzedInfo.resultGroupingExpressions(i)
            structFields += Literal(UTF8String.fromString(s"$structPrefix$fieldIndex"), StringType)
            structFields += replaceAttributes(
              analyzedInfo.resultGroupingExpressions(i),
              attrReplaceMap)
            fieldIndex += 1
          }
        }

        aggregate.aggregateExpressions
          .filter(e => hasAggregateExpression(e))
          .foreach {
            e =>
              def collectExpressionsInAggregateExpression(aggExpr: Expression): Unit = {
                aggExpr match {
                  case aggExpr: AggregateExpression =>
                    val aggFunction =
                      removeAlias(aggExpr).asInstanceOf[AggregateExpression].aggregateFunction
                    aggFunction.children.foreach {
                      child =>
                        structFields += Literal(
                          UTF8String.fromString(s"$structPrefix$fieldIndex"),
                          StringType)
                        structFields += replaceAttributes(child, attrReplaceMap)
                        fieldIndex += 1
                    }
                  case combineAgg if hasAggregateExpression(combineAgg) =>
                    combineAgg.children.foreach {
                      combindAggchild => collectExpressionsInAggregateExpression(combindAggchild)
                    }
                  case other =>
                    structFields += Literal(
                      UTF8String.fromString(s"$structPrefix$fieldIndex"),
                      StringType)
                    structFields += replaceAttributes(other, attrReplaceMap)
                    fieldIndex += 1
                }
              }
              collectExpressionsInAggregateExpression(e)
          }
        structFields += Literal(UTF8String.fromString(s"$structPrefix$fieldIndex"), StringType)
        structFields += Literal(case_index, IntegerType)
        structAttributes += makeAlias(
          CreateNamedStruct(structFields.toSeq),
          s"$casePrefix$case_index")
    }
    structAttributes.toSeq
  }

  def buildProjectFoldIntoStruct(
      child: LogicalPlan,
      groupedPlans: ArrayBuffer[AnalyzedPlan],
      conditions: ArrayBuffer[Expression]): LogicalPlan = {
    val wrappedAttributes = wrapAggregatesAttributesInStructs(groupedPlans)
    val ifAttributes = wrappedAttributes.zip(conditions).map {
      case (attr, condition) =>
        makeAlias(If(condition, attr, Literal(null, attr.dataType)), attr.name)
          .asInstanceOf[NamedExpression]
    }
    Project(ifAttributes, child)
  }

  def buildProjectBranchArray(
      child: LogicalPlan,
      conditions: ArrayBuffer[Expression]): LogicalPlan = {
    assert(
      child.output.length == conditions.length,
      s"Expected same length of output and conditions")
    val array = makeAlias(CreateArray(child.output), "array")
    Project(Seq(array), child)
  }

  def buildExplodeBranchArray(child: LogicalPlan): LogicalPlan = {
    assert(child.output.length == 1, s"Expected single output from $child")
    val array = child.output.head.asInstanceOf[Expression]
    assert(array.dataType.isInstanceOf[ArrayType], s"Expected ArrayType from $array")
    val explodeExpr = Explode(array)
    val exploadOutput =
      AttributeReference("generate_output", array.dataType.asInstanceOf[ArrayType].elementType)()
    Generate(
      explodeExpr,
      unrequiredChildIndex = Seq(0),
      outer = false,
      qualifier = None,
      generatorOutput = Seq(exploadOutput),
      child)
  }

  def makeAlias(e: Expression, name: String): NamedExpression = {
    Alias(e, name)(
      NamedExpression.newExprId,
      e match {
        case ne: NamedExpression => ne.qualifier
        case _ => Seq.empty
      },
      None,
      Seq.empty)
  }

  def buildProjectUnfoldStruct(child: LogicalPlan): LogicalPlan = {
    assert(child.output.length == 1, s"Expected single output from $child")
    val structedData = child.output.head
    assert(
      structedData.dataType.isInstanceOf[StructType],
      s"Expected StructType from $structedData")
    val structType = structedData.dataType.asInstanceOf[StructType]
    val attributes = ArrayBuffer[NamedExpression]()
    var index = 0
    structType.fields.foreach {
      field =>
        attributes += Alias(GetStructField(structedData, index), field.name)()
        index += 1
    }
    Project(attributes.toSeq, child)
  }

  def buildAggregateWithGroupId(
      child: LogicalPlan,
      groupedPlans: ArrayBuffer[AnalyzedPlan]): LogicalPlan = {
    val attributes = child.output
    val firstAggregateAnalzyInfo = groupedPlans.head.analyzedInfo.get
    val aggregateTemplate =
      firstAggregateAnalzyInfo.constructedAggregatePlan.get.asInstanceOf[Aggregate]
    val analyzedInfo = groupedPlans.head.analyzedInfo.get

    val totalGroupingExpressionsCount =
      math.max(
        aggregateTemplate.groupingExpressions.length,
        analyzedInfo.positionInGroupingKeys.max + 1)

    val groupingExpressions = attributes
      .slice(0, totalGroupingExpressionsCount)
      .map(_.asInstanceOf[Expression]) :+ attributes.last

    val normalExpressionPosition = analyzedInfo.positionInGroupingKeys
    var normalExpressionCount = 0
    var aggregateExpressionIndex = totalGroupingExpressionsCount
    val aggregateExpressions = ArrayBuffer[NamedExpression]()
    aggregateTemplate.aggregateExpressions.foreach {
      e =>
        removeAlias(e) match {
          case aggExpr if hasAggregateExpression(aggExpr) =>
            val (newAggExpr, count) =
              constructAggregateExpression(aggExpr, attributes, aggregateExpressionIndex)
            aggregateExpressions += makeAlias(newAggExpr, e.name).asInstanceOf[NamedExpression]
            aggregateExpressionIndex += count
          case other =>
            val position = normalExpressionPosition(normalExpressionCount)
            val attr = attributes(position)
            normalExpressionCount += 1
            aggregateExpressions += makeAlias(attr, e.name)
              .asInstanceOf[NamedExpression]
        }
    }
    Aggregate(groupingExpressions.toSeq, aggregateExpressions.toSeq, child)
  }

  def constructAggregateExpression(
      aggExpr: Expression,
      attributes: Seq[Attribute],
      index: Int): (Expression, Int) = {
    aggExpr match {
      case singleAggExpr: AggregateExpression =>
        val aggFunc = singleAggExpr.aggregateFunction
        val newAggFuncArgs = aggFunc.children.zipWithIndex.map {
          case (arg, i) =>
            attributes(index + i)
        }
        val newAggFunc =
          aggFunc.withNewChildren(newAggFuncArgs).asInstanceOf[AggregateFunction]
        val res = AggregateExpression(
          newAggFunc,
          singleAggExpr.mode,
          singleAggExpr.isDistinct,
          singleAggExpr.filter,
          singleAggExpr.resultId)
        (res, 1)
      case combineAggExpr if hasAggregateExpression(combineAggExpr) =>
        val childrenExpressions = ArrayBuffer[Expression]()
        var totalCount = 0
        combineAggExpr.children.foreach {
          child =>
            val (expr, count) = constructAggregateExpression(child, attributes, totalCount + index)
            childrenExpressions += expr
            totalCount += count
        }
        (combineAggExpr.withNewChildren(childrenExpressions.toSeq), totalCount)
      case _ => (attributes(index), 1)
    }
  }
}
