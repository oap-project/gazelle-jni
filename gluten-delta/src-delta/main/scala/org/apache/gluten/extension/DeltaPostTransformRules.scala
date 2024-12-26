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

import org.apache.gluten.execution.{DeltaFilterExecTransformer, DeltaProjectExecTransformer, DeltaScanTransformer, ProjectExecTransformer}
import org.apache.gluten.extension.columnar.transition.RemoveTransitions

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.{Alias, Attribute, AttributeReference, Expression, InputFileBlockLength, InputFileBlockStart, InputFileName}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.trees.TreeNodeTag
import org.apache.spark.sql.delta.{DeltaColumnMapping, DeltaParquetFileFormat, NoMapping}
import org.apache.spark.sql.execution.{FilterExec, ProjectExec, SparkPlan}
import org.apache.spark.sql.execution.datasources.FileFormat

import scala.collection.mutable.ListBuffer

object DeltaPostTransformRules {
  def rules: Seq[Rule[SparkPlan]] =
    RemoveTransitions :: columnMappingRule :: filterRule :: projectRule ::
      pushDownInputFileExprRule :: Nil

  private val COLUMN_MAPPING_RULE_TAG: TreeNodeTag[String] =
    TreeNodeTag[String]("org.apache.gluten.delta.column.mapping")

  private def notAppliedColumnMappingRule(plan: SparkPlan): Boolean = {
    plan.getTagValue(COLUMN_MAPPING_RULE_TAG).isEmpty
  }

  private def tagColumnMappingRule(plan: SparkPlan): Unit = {
    plan.setTagValue(COLUMN_MAPPING_RULE_TAG, null)
  }

  val columnMappingRule: Rule[SparkPlan] = (plan: SparkPlan) =>
    plan.transformWithSubqueries {
      // If it enables Delta Column Mapping(e.g. nameMapping and idMapping),
      // transform the metadata of Delta into Parquet's,
      // so that gluten can read Delta File using Parquet Reader.
      case p: DeltaScanTransformer
          if isDeltaColumnMappingFileFormat(p.relation.fileFormat) && notAppliedColumnMappingRule(
            p) =>
        transformColumnMappingPlan(p)
    }

  val filterRule: Rule[SparkPlan] = (plan: SparkPlan) =>
    plan.transformUp {
      case FilterExec(condition, child) if containsIncrementMetricExpr(condition) =>
        DeltaFilterExecTransformer(condition, child)
    }

  val projectRule: Rule[SparkPlan] = (plan: SparkPlan) =>
    plan.transformUp {
      case ProjectExec(projectList, child) if projectList.exists(containsIncrementMetricExpr) =>
        DeltaProjectExecTransformer(projectList, child)
    }

  val pushDownInputFileExprRule: Rule[SparkPlan] = (plan: SparkPlan) =>
    plan.transformUp {
      case p @ ProjectExec(projectList, child: DeltaScanTransformer)
          if projectList.exists(containsInputFileRelatedExpr) =>
        child.copy(output = p.output)
    }

  private def isDeltaColumnMappingFileFormat(fileFormat: FileFormat): Boolean = fileFormat match {
    case d: DeltaParquetFileFormat if d.columnMappingMode != NoMapping =>
      true
    case _ =>
      false
  }

  private def containsInputFileRelatedExpr(expr: Expression): Boolean = {
    expr match {
      case _: InputFileName | _: InputFileBlockStart | _: InputFileBlockLength => true
      case _ => expr.children.exists(containsInputFileRelatedExpr)
    }
  }

  private def containsIncrementMetricExpr(expr: Expression): Boolean = {
    expr match {
      case e if e.prettyName == "increment_metric" => true
      case _ => expr.children.exists(containsIncrementMetricExpr)
    }
  }

  /**
   * This method is only used for Delta ColumnMapping FileFormat(e.g. nameMapping and idMapping)
   * transform the metadata of Delta into Parquet's, each plan should only be transformed once.
   */
  private def transformColumnMappingPlan(plan: SparkPlan): SparkPlan = plan match {
    case plan: DeltaScanTransformer =>
      val fmt = plan.relation.fileFormat.asInstanceOf[DeltaParquetFileFormat]

      // transform HadoopFsRelation
      val relation = plan.relation
      val newFsRelation = relation.copy(
        partitionSchema = DeltaColumnMapping.createPhysicalSchema(
          relation.partitionSchema,
          fmt.referenceSchema,
          fmt.columnMappingMode),
        dataSchema = DeltaColumnMapping.createPhysicalSchema(
          relation.dataSchema,
          fmt.referenceSchema,
          fmt.columnMappingMode)
      )(SparkSession.active)
      // transform output's name into physical name so Reader can read data correctly
      // should keep the columns order the same as the origin output
      val originColumnNames = ListBuffer.empty[String]
      val transformedAttrs = ListBuffer.empty[Attribute]
      def mapAttribute(attr: Attribute) = {
        val newAttr = if (!plan.isMetadataColumn(attr)) {
          DeltaColumnMapping
            .createPhysicalAttributes(Seq(attr), fmt.referenceSchema, fmt.columnMappingMode)
            .head
        } else {
          attr
        }
        if (!originColumnNames.contains(attr.name)) {
          transformedAttrs += newAttr
          originColumnNames += attr.name
        }
        newAttr
      }
      val newOutput = plan.output.map(o => mapAttribute(o))
      // transform dataFilters
      val newDataFilters = plan.dataFilters.map {
        e =>
          e.transformDown {
            case attr: AttributeReference =>
              mapAttribute(attr)
          }
      }
      // transform partitionFilters
      val newPartitionFilters = plan.partitionFilters.map {
        e =>
          e.transformDown {
            case attr: AttributeReference =>
              mapAttribute(attr)
          }
      }
      // replace tableName in schema with physicalName
      val scanExecTransformer = new DeltaScanTransformer(
        newFsRelation,
        newOutput,
        DeltaColumnMapping.createPhysicalSchema(
          plan.requiredSchema,
          fmt.referenceSchema,
          fmt.columnMappingMode),
        newPartitionFilters,
        plan.optionalBucketSet,
        plan.optionalNumCoalescedBuckets,
        newDataFilters,
        plan.tableIdentifier,
        plan.disableBucketedScan
      )
      scanExecTransformer.copyTagsFrom(plan)
      tagColumnMappingRule(scanExecTransformer)

      // alias physicalName into tableName
      val expr = (transformedAttrs, originColumnNames).zipped.map {
        (attr, columnName) => Alias(attr, columnName)(exprId = attr.exprId)
      }
      val projectExecTransformer = ProjectExecTransformer(expr.toSeq, scanExecTransformer)
      projectExecTransformer
    case _ => plan
  }
}
