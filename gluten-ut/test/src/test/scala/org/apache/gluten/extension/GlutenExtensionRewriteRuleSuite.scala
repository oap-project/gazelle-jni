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

import org.apache.gluten.execution.{ProjectExecTransformer, WholeStageTransformerSuite}
import org.apache.gluten.utils.BackendTestUtils

import org.apache.spark.SparkConf

class GlutenExtensionRewriteRuleSuite extends WholeStageTransformerSuite {

  protected val resourcePath: String = null
  protected val fileFormat: String = null

  override def sparkConf: SparkConf = {
    val conf = super.sparkConf
      .set("spark.sql.adaptive.enabled", "false")
    if (BackendTestUtils.isCHBackendLoaded()) {
      conf
        .set("spark.gluten.sql.enable.native.validation", "false")
    }
    conf
  }

  test("GLUTEN-8183 - Pruning unused column in project") {
    val query =
      """
        |SELECT
        |  max(n1),
        |  max(n2),
        |  sum(IF(n1 + n2 + n3 % 2 = 0, 1, 0))
        |FROM
        |  (
        |    SELECT
        |      id + 1 AS n1,
        |      id + 2 AS n2,
        |      IF(id % 2 = 0, id + 3, id + 4) AS n3
        |    FROM
        |      RANGE(10)
        |  )
        |""".stripMargin

    val df = sql(query)
    assert(
      getExecutedPlan(df).exists {
        case project: ProjectExecTransformer => project.projectList.length == 3
        case _ => false
      }
    )
  }
}
