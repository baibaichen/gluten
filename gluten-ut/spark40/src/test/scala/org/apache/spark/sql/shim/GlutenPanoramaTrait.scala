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
package org.apache.spark.sql.shim

import org.apache.gluten.execution.ProjectExecTransformer

import org.apache.spark.sql
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Expression

import org.scalatest.Args
import org.scalatest.Status

import scala.collection.mutable
import scala.reflect.ClassTag

/**
 * A trait that collects PANORAMA records during test execution and outputs a structured summary in
 * [[afterAll]]. Subclasses must implement [[panoramaMeta]] to return expression-specific metadata.
 *
 * Spark 4.0 version: extends common GlutenTestsTrait directly (no shim GlutenTestsTrait needed).
 */
trait GlutenPanoramaTrait extends sql.GlutenTestsTrait {

  /** Subclasses must implement: return metadata string for the expression. */
  protected def panoramaMeta(expression: Expression): String

  private case class PanoramaRecord(
      method: String,
      expression: String,
      meta: String,
      offload: String)

  private case class TestPanoramaResult(
      testName: String,
      records: Seq[PanoramaRecord],
      status: String)

  private val currentTestRecords = mutable.ArrayBuffer[PanoramaRecord]()
  private val allTestResults = mutable.ArrayBuffer[TestPanoramaResult]()

  private def withPanoramaLog[T](
      method: String,
      expression: Expression,
      resultDF: DataFrame)(body: => T): T = {
    val meta = panoramaMeta(expression)
    try {
      body
    } finally {
      val projectTransformer = resultDF.queryExecution.executedPlan.collect {
        case p: ProjectExecTransformer => p
      }
      val offload = if (projectTransformer.size == 1) "OFFLOAD" else "FALLBACK"
      currentTestRecords += PanoramaRecord(method, expression.toString, meta, offload)
    }
  }

  override def runTest(testName: String, args: Args): Status = {
    currentTestRecords.clear()
    val status = super.runTest(testName, args)
    val result = if (status.succeeds()) "PASS" else "FAIL"
    allTestResults += TestPanoramaResult(testName, currentTestRecords.toSeq, result)
    status
  }

  override protected def doCheckExpression(
      expression: Expression,
      expected: Any,
      inputRow: InternalRow,
      resultDF: DataFrame): Unit = {
    withPanoramaLog("checkExpression", expression, resultDF) {
      super.doCheckExpression(expression, expected, inputRow, resultDF)
    }
  }

  override protected def doCheckExceptionInExpression[T <: Throwable: ClassTag](
      expression: Expression,
      inputRow: InternalRow,
      expectedErrMsg: String,
      resultDF: DataFrame): Unit = {
    withPanoramaLog("checkException", expression, resultDF) {
      super.doCheckExceptionInExpression[T](expression, inputRow, expectedErrMsg, resultDF)
    }
  }

  override def afterAll(): Unit = {
    val suiteName = this.getClass.getSimpleName
    logWarning("ANSI_PANORAMA_SUMMARY_BEGIN")
    logWarning(s"$suiteName:")
    allTestResults.foreach { t =>
      logWarning(s"  ${t.testName}:")
      t.records.zipWithIndex.foreach { case (r, idx) =>
        val methodTag = if (r.method == "checkException") "E" else "N"
        val status = if (idx == t.records.size - 1) t.status else "PASS"
        logWarning(s"    $methodTag|${r.expression}|${r.meta}|${r.offload}|$status")
      }
    }
    logWarning("ANSI_PANORAMA_SUMMARY_END")
    super.afterAll()
  }
}
