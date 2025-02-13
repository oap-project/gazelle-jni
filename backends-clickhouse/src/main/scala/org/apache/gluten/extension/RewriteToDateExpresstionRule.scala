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

import org.apache.gluten.backendsapi.clickhouse.CHConfig
import org.apache.gluten.config.GlutenConfig

import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._

// If users query data through BI tools.
// The BI tools may generate SQL similar to
// `to_date(
//   from_unixtime(
//     unix_timestamp(stringType, 'yyyyMMdd')
//   )
// )`
// to convert string strings to dates.
// Under ch backend, the StringType can be directly converted into DateType,
//     and the functions `from_unixtime` and `unix_timestamp` can be optimized here.
// Optimized result is `to_date(stringType)`
class RewriteToDateExpresstionRule(spark: SparkSession) extends Rule[LogicalPlan] with Logging {

  override def apply(plan: LogicalPlan): LogicalPlan = {
    if (
      plan.resolved &&
      GlutenConfig.get.enableGluten &&
      CHConfig.get.enableCHRewriteDateConversion
    ) {
      visitPlan(plan)
    } else {
      plan
    }
  }

  private def visitPlan(plan: LogicalPlan): LogicalPlan = plan match {
    case project: Project if canRewrite(project) =>
      val newProjectList = project.projectList.map(expr => visitExpression(expr))
      val newProject = Project(newProjectList, project.child)
      newProject
    case other =>
      val children = other.children.map(visitPlan)
      other.withNewChildren(children)
  }

  private def visitExpression(expression: NamedExpression): NamedExpression = expression match {
    case a @ Alias(c, _) if c.isInstanceOf[ParseToDate] =>
      val newToDate = rewriteParseToDate(c.asInstanceOf[ParseToDate])
      if (!newToDate.fastEquals(c)) {
        a.copy(newToDate, a.name)(
          a.exprId,
          a.qualifier,
          a.explicitMetadata,
          a.nonInheritableMetadataKeys)
      } else {
        expression
      }
    case a @ Alias(c, _) if c.isInstanceOf[ParseToTimestamp] =>
      val newToTimestamp = rewriteParseToTimestamp(c.asInstanceOf[ParseToTimestamp])
      if (!newToTimestamp.fastEquals(c)) {
        a.copy(newToTimestamp, a.name)(
          a.exprId,
          a.qualifier,
          a.explicitMetadata,
          a.nonInheritableMetadataKeys)
      } else {
        expression
      }
    case _ => expression
  }

  private def rewriteParseToDate(toDate: ParseToDate): Expression = toDate.left match {
    case fromUnixTime: FromUnixTime
        if fromUnixTime.left.isInstanceOf[UnixTimestamp]
          && fromUnixTime.left.asInstanceOf[UnixTimestamp].left.dataType.isInstanceOf[StringType] =>
      val unixTimestamp = fromUnixTime.left.asInstanceOf[UnixTimestamp]
      val newLeft = unixTimestamp.left
      new ParseToDate(newLeft)
    case date: Expression
        if date.dataType.isInstanceOf[DateType] || date.dataType.isInstanceOf[TimestampType] =>
      // When the data type of the left child in the ParseToDate is the DateType or TimestampType,
      // it will not deal with the format,
      // also CH backend can not support the DateType or TimestampType as input data type
      Cast(date, toDate.dataType, Some(SQLConf.get.sessionLocalTimeZone))
    case _ => toDate
  }

  private def rewriteParseToTimestamp(toTimestamp: ParseToTimestamp): Expression =
    toTimestamp.left match {
      case timestamp: Expression
          if (timestamp.dataType.isInstanceOf[DateType] ||
            timestamp.dataType.isInstanceOf[TimestampType]) =>
        // When the data type of the left child in the ParseToDate is the DateType or TimestampType,
        // it will not deal with the format,
        // also CH backend can not support the DateType or TimestampType as input data type
        Cast(timestamp, toTimestamp.dataType, Some(SQLConf.get.sessionLocalTimeZone))
      case _ => toTimestamp
    }

  private def canRewrite(project: Project): Boolean = {
    project.projectList.exists(
      expr =>
        expr.isInstanceOf[Alias] &&
          (expr.asInstanceOf[Alias].child.isInstanceOf[ParseToDate] ||
            expr.asInstanceOf[Alias].child.isInstanceOf[ParseToTimestamp]))
  }
}
