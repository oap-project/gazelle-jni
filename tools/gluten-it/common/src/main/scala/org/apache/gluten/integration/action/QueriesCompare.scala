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
package org.apache.gluten.integration.action

import org.apache.commons.lang3.exception.ExceptionUtils
import org.apache.gluten.integration.action.Actions.QuerySelector
import org.apache.gluten.integration.action.TableRender.RowParser.FieldAppender.RowAppender
import org.apache.gluten.integration.stat.RamStat
import org.apache.gluten.integration.{QueryRunner, Suite, TableCreator}
import org.apache.spark.sql.{SparkSessionSwitcher, TestUtils}

case class QueriesCompare(
    scale: Double,
    genPartitionedData: Boolean,
    queries: QuerySelector,
    explain: Boolean,
    iterations: Int)
    extends Action {

  override def execute(suite: Suite): Boolean = {
    val runner: QueryRunner =
      new QueryRunner(suite.queryResource(), suite.dataWritePath(scale, genPartitionedData))
    val runQueryIds = queries.select(suite)
    val results = (0 until iterations).flatMap { iteration =>
      println(s"Running tests (iteration $iteration)...")
      runQueryIds.map { queryId =>
        QueriesCompare.runQuery(
          suite.tableCreator(),
          queryId,
          explain,
          suite.desc(),
          suite.sessionSwitcher,
          runner)
      }
    }.toList

    val passedCount = results.count(l => l.testPassed)
    val count = results.count(_ => true)

    // RAM stats
    println("Performing GC to collect RAM statistics... ")
    System.gc()
    System.gc()
    printf(
      "RAM statistics: JVM Heap size: %d KiB (total %d KiB), Process RSS: %d KiB\n",
      RamStat.getJvmHeapUsed(),
      RamStat.getJvmHeapTotal(),
      RamStat.getProcessRamUsed())

    println("")
    println("Test report: ")
    println("")
    printf("Summary: %d out of %d queries passed. \n", passedCount, count)
    println("")
    val succeed = results.filter(_.testPassed)
    QueriesCompare.printResults(succeed)
    println("")

    if (passedCount == count) {
      println("No failed queries. ")
      println("")
    } else {
      println(
        "Failed queries (a failed query with correct row count indicates value mismatches): ")
      println("")
      QueriesCompare.printResults(results.filter(!_.testPassed))
      println("")
    }

    var all = QueriesCompare.aggregate(results, "all")

    if (passedCount != count) {
      all = QueriesCompare.aggregate(succeed, "succeeded") ::: all
    }

    println("Overall: ")
    println("")
    QueriesCompare.printResults(all)
    println("")

    if (passedCount != count) {
      return false
    }
    true
  }
}

object QueriesCompare {
  case class TestResultLine(
      queryId: String,
      testPassed: Boolean,
      expectedRowCount: Option[Long],
      actualRowCount: Option[Long],
      expectedPlanningTimeMillis: Option[Long],
      actualPlanningTimeMillis: Option[Long],
      expectedExecutionTimeMillis: Option[Long],
      actualExecutionTimeMillis: Option[Long],
      errorMessage: Option[String])

  object TestResultLine {
    implicit object Parser extends TableRender.RowParser[TestResultLine] {
      override def parse(rowAppender: RowAppender, line: TestResultLine): Unit = {
        val inc = rowAppender.incremental()
        val speedUp =
          if (line.expectedExecutionTimeMillis.nonEmpty && line.actualExecutionTimeMillis.nonEmpty) {
            Some(
              ((line.expectedExecutionTimeMillis.get - line.actualExecutionTimeMillis.get).toDouble
                / line.actualExecutionTimeMillis.get.toDouble) * 100)
          } else None
        inc.next().write(line.queryId)
        inc.next().write(line.testPassed)
        inc.next().write(line.expectedRowCount.getOrElse("N/A"))
        inc.next().write(line.actualRowCount.getOrElse("N/A"))
        inc.next().write(line.expectedPlanningTimeMillis.getOrElse("N/A"))
        inc.next().write(line.actualPlanningTimeMillis.getOrElse("N/A"))
        inc.next().write(line.expectedExecutionTimeMillis.getOrElse("N/A"))
        inc.next().write(line.actualExecutionTimeMillis.getOrElse("N/A"))
        inc.next().write(speedUp.map("%.2f%%".format(_)).getOrElse("N/A"))
      }
    }
  }

  private def printResults(results: List[TestResultLine]): Unit = {
    import org.apache.gluten.integration.action.TableRender.Field._

    val render = TableRender.create[TestResultLine](
      Leaf("Query ID"),
      Leaf("Passed"),
      Branch("Row Count", List(Leaf("Vanilla"), Leaf("Gluten"))),
      Branch("Planning Time (Millis)", List(Leaf("Vanilla"), Leaf("Gluten"))),
      Branch("Query Time (Millis)", List(Leaf("Vanilla"), Leaf("Gluten"))),
      Leaf("Speedup"))

    results.foreach { line =>
      render.appendRow(line)
    }

    render.print(System.out)
  }

  private def aggregate(succeed: List[TestResultLine], name: String): List[TestResultLine] = {
    if (succeed.isEmpty) {
      return Nil
    }
    List(
      succeed.reduce((r1, r2) =>
        TestResultLine(
          name,
          testPassed = true,
          if (r1.expectedRowCount.nonEmpty && r2.expectedRowCount.nonEmpty)
            Some(r1.expectedRowCount.get + r2.expectedRowCount.get)
          else None,
          if (r1.actualRowCount.nonEmpty && r2.actualRowCount.nonEmpty)
            Some(r1.actualRowCount.get + r2.actualRowCount.get)
          else None,
          if (r1.expectedPlanningTimeMillis.nonEmpty && r2.expectedPlanningTimeMillis.nonEmpty)
            Some(r1.expectedPlanningTimeMillis.get + r2.expectedPlanningTimeMillis.get)
          else None,
          if (r1.actualPlanningTimeMillis.nonEmpty && r2.actualPlanningTimeMillis.nonEmpty)
            Some(r1.actualPlanningTimeMillis.get + r2.actualPlanningTimeMillis.get)
          else None,
          if (r1.expectedExecutionTimeMillis.nonEmpty && r2.expectedExecutionTimeMillis.nonEmpty)
            Some(r1.expectedExecutionTimeMillis.get + r2.expectedExecutionTimeMillis.get)
          else None,
          if (r1.actualExecutionTimeMillis.nonEmpty && r2.actualExecutionTimeMillis.nonEmpty)
            Some(r1.actualExecutionTimeMillis.get + r2.actualExecutionTimeMillis.get)
          else None,
          None)))
  }

  private[integration] def runQuery(
      creator: TableCreator,
      id: String,
      explain: Boolean,
      desc: String,
      sessionSwitcher: SparkSessionSwitcher,
      runner: QueryRunner): TestResultLine = {
    println(s"Running query: $id...")
    try {
      val baseLineDesc = "Vanilla Spark %s %s".format(desc, id)
      sessionSwitcher.useSession("baseline", baseLineDesc)
      runner.createTables(creator, sessionSwitcher.spark())
      val expected =
        runner.runQuery(sessionSwitcher.spark(), baseLineDesc, id, explain = explain)
      val expectedRows = expected.rows
      val testDesc = "Gluten Spark %s %s".format(desc, id)
      sessionSwitcher.useSession("test", testDesc)
      runner.createTables(creator, sessionSwitcher.spark())
      val result = runner.runQuery(sessionSwitcher.spark(), testDesc, id, explain = explain)
      val resultRows = result.rows
      val error = TestUtils.compareAnswers(resultRows, expectedRows, sort = true)
      if (error.isEmpty) {
        println(
          s"Successfully ran query $id, result check was passed. " +
            s"Returned row count: ${resultRows.length}, expected: ${expectedRows.length}")
        return TestResultLine(
          id,
          testPassed = true,
          Some(expectedRows.length),
          Some(resultRows.length),
          Some(expected.planningTimeMillis),
          Some(result.planningTimeMillis),
          Some(expected.executionTimeMillis),
          Some(result.executionTimeMillis),
          None)
      }
      println(s"Error running query $id, result check was not passed. " +
        s"Returned row count: ${resultRows.length}, expected: ${expectedRows.length}, error: ${error.get}")
      TestResultLine(
        id,
        testPassed = false,
        Some(expectedRows.length),
        Some(resultRows.length),
        Some(expected.planningTimeMillis),
        Some(result.planningTimeMillis),
        Some(expected.executionTimeMillis),
        Some(result.executionTimeMillis),
        error)
    } catch {
      case e: Exception =>
        val error = Some(s"FATAL: ${ExceptionUtils.getStackTrace(e)}")
        println(
          s"Error running query $id. " +
            s" Error: ${error.get}")
        TestResultLine(id, testPassed = false, None, None, None, None, None, None, error)
    }
  }
}
