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

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Expression

import org.scalatest.Args
import org.scalatest.Status

import scala.collection.mutable
import scala.reflect.ClassTag

trait GlutenExpressionOffloadTracker extends GlutenTestsTrait {

  protected def panoramaMeta(expression: Expression): String =
    s"expr=${expression.getClass.getSimpleName}"

  private case class OffloadRecord(
      method: String,
      expression: String,
      meta: String,
      offload: String)

  private case class TestOffloadResult(
      testName: String,
      records: Seq[OffloadRecord],
      status: String)

  private val currentTestRecords = mutable.ArrayBuffer[OffloadRecord]()
  private val allTestResults = mutable.ArrayBuffer[TestOffloadResult]()

  private def withOffloadLog[T](method: String, expression: Expression, resultDF: DataFrame)(
      body: => T): T = {
    val meta = panoramaMeta(expression)
    try {
      body
    } finally {
      val projectTransformer = resultDF.queryExecution.executedPlan.collect {
        case p: ProjectExecTransformer => p
      }
      val offload = if (projectTransformer.size == 1) "OFFLOAD" else "FALLBACK"
      currentTestRecords += OffloadRecord(method, expression.toString, meta, offload)
    }
  }

  override def runTest(testName: String, args: Args): Status = if (ansiTest) {
    currentTestRecords.clear()
    val status = super.runTest(testName, args)
    val result = if (status.succeeds()) "PASS" else "FAIL"
    allTestResults += TestOffloadResult(testName, currentTestRecords.toSeq, result)
    status
  } else {
    super.runTest(testName, args)
  }

  override protected def doCheckExpression(
      expression: Expression,
      expected: Any,
      inputRow: InternalRow,
      resultDF: DataFrame): Unit = if (ansiTest) {
    withOffloadLog("checkExpression", expression, resultDF) {
      super.doCheckExpression(expression, expected, inputRow, resultDF)
    }
  } else {
    super.doCheckExpression(expression, expected, inputRow, resultDF)
  }

  override protected def doCheckExceptionInExpression[T <: Throwable: ClassTag](
      expression: Expression,
      inputRow: InternalRow,
      expectedErrMsg: String,
      resultDF: DataFrame): Unit = if (ansiTest) {
    withOffloadLog("checkException", expression, resultDF) {
      super.doCheckExceptionInExpression[T](expression, inputRow, expectedErrMsg, resultDF)
    }
  } else {
    super.doCheckExceptionInExpression[T](expression, inputRow, expectedErrMsg, resultDF)
  }

  override def afterAll(): Unit = if (ansiTest) {
    val suiteName = this.getClass.getSimpleName
    logWarning("EXPRESSION_OFFLOAD_SUMMARY_BEGIN")
    logWarning(s"$suiteName:")
    allTestResults.foreach {
      t =>
        logWarning(s"  ${t.testName}:")
        t.records.zipWithIndex.foreach {
          case (r, idx) =>
            val methodTag = if (r.method == "checkException") "E" else "N"
            val status = if (idx == t.records.size - 1) t.status else "PASS"
            logWarning(s"    $methodTag|${r.expression}|${r.meta}|${r.offload}|$status")
        }
    }
    logWarning("EXPRESSION_OFFLOAD_SUMMARY_END")
    super.afterAll()
  } else {
    super.afterAll()
  }
}
