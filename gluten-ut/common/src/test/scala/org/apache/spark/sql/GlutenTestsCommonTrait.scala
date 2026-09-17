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
package org.apache.spark.sql

import org.apache.gluten.test.TestStats

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.types.DataType

import com.fasterxml.jackson.databind.ObjectMapper
import org.scalatest.{Args, Status}

import java.nio.charset.StandardCharsets.UTF_8
import java.nio.file.{Files, Paths, StandardOpenOption}
import java.util
import java.util.concurrent.atomic.AtomicLong

import scala.util.Try
import scala.util.control.NonFatal

trait GlutenTestsCommonTrait
  extends SparkFunSuite
  with ExpressionEvalHelper
  with GlutenTestsBaseTrait {

  protected def defaultOffloadGluten: Boolean = true
  @volatile private var referenceTestName = "<outside-test>"
  private val referenceChecks = new AtomicLong
  private val referenceErrors = new AtomicLong

  override protected def checkEvaluation(
      expression: => Expression,
      expected: Any,
      inputRow: InternalRow = EmptyRow): Unit = {
    var observed: Option[Expression] = None
    def originalExpression: Expression = {
      val expr = expression
      observed = Some(expr)
      expr
    }
    TestStats.offloadGluten = false
    try {
      super[ExpressionEvalHelper].checkEvaluation(originalExpression, expected, inputRow)
      referenceChecks.incrementAndGet()
      recordReference(observed, None)
    } catch {
      case NonFatal(error) =>
        referenceErrors.incrementAndGet()
        recordReference(observed, Some(error))
        throw error
    }
  }

  private def recordReference(expression: Option[Expression], error: Option[Throwable]): Unit = {
    val record = new util.LinkedHashMap[String, Object]()
    record.put("event_type", "evaluation")
    record.put("suite", getClass.getName)
    record.put("test", referenceTestName)
    record.put("route", if (error.isDefined) "baseline-error" else "spark-reference")
    record.put("classification", if (error.isDefined) "baselineerror" else "reference-only")
    record.put("execution_mechanism", "original Spark ExpressionEvalHelper; no query/native route")
    record.put(
      "baseline_only",
      Boolean.box(sys.props.getOrElse("gluten.expression.baselineOnly", "true").toBoolean))
    record.put("legacy_eligible", Boolean.box(false))
    record.put("direct_native_eligible", Boolean.box(false))
    record.put("project_exec_transformer_count", null)
    record.put("spark_project_exec_count", null)
    record.put("expression", expression.map(_.toString).orNull)
    record.put("sql", expression.flatMap(e => Try(e.sql).toOption).orNull)
    record.put("result_type", expression.flatMap(e => Try(e.dataType.sql).toOption).orNull)
    record.put("reason", "Original suite inherited GlutenTestsCommonTrait, not GlutenTestsTrait")
    record.put("plan", "<no query plan: original reference evaluator>")
    record.put("error_type", error.map(_.getClass.getName).orNull)
    record.put("error_message", error.flatMap(e => Option(e.getMessage)).orNull)
    record.put("occurrence_count", Int.box(1))
    GlutenExpressionRouteLedger.append(record)
  }

  final protected def checkEvaluationWithSpark(
      expression: => Expression,
      expected: Any,
      inputRow: InternalRow): Unit = {
    super[ExpressionEvalHelper].checkEvaluation(expression, expected, inputRow)
  }

  final protected def checkResultWithSpark(
      result: Any,
      expected: Any,
      dataType: DataType,
      nullable: Boolean): Boolean = {
    super[ExpressionEvalHelper].checkResult(result, expected, dataType, nullable)
  }

  override def runTest(testName: String, args: Args): Status = {
    referenceTestName = testName
    val referenceBefore = referenceChecks.get()
    val errorBefore = referenceErrors.get()
    TestStats.suiteTestNumber += 1
    TestStats.offloadGluten = defaultOffloadGluten
    TestStats.startCase(testName)
    val status = super.runTest(testName, args)
    if (TestStats.offloadGluten) {
      TestStats.offloadGlutenTestNumber += 1
      print("'" + testName + "'" + " offload to gluten\n")
    } else {
      // you can find the keyword 'Validation failed for' in function doValidate() in log
      // to get the fallback reason
      print("'" + testName + "'" + " NOT use gluten\n")
      TestStats.addFallBackCase()
    }

    TestStats.endCase(status.succeeds())
    if (referenceChecks.get() != referenceBefore || referenceErrors.get() != errorBefore) {
      val summary = new util.LinkedHashMap[String, Object]()
      summary.put("event_type", "test-summary")
      summary.put("suite", getClass.getName)
      summary.put("test", testName)
      summary.put(
        "baseline_only",
        Boolean.box(sys.props.getOrElse("gluten.expression.baselineOnly", "true").toBoolean))
      summary.put("reference_only", Long.box(referenceChecks.get() - referenceBefore))
      summary.put("baselineerror", Long.box(referenceErrors.get() - errorBefore))
      summary.put("new_direct_hook_invocations", Long.box(0L))
      summary.put(
        "mixed_test",
        Boolean.box(
          referenceChecks.get() != referenceBefore && referenceErrors.get() != errorBefore))
      summary.put("test_succeeded", Boolean.box(status.succeeds()))
      GlutenExpressionRouteLedger.append(summary)
    }
    referenceTestName = "<outside-test>"
    status
  }
}

private[sql] object GlutenExpressionRouteLedger {
  private val mapper = new ObjectMapper()

  def append(record: util.Map[String, Object]): Unit = synchronized {
    val path = Paths
      .get(sys.props.getOrElse("gluten.expression.routeLog", "target/expression-routes.jsonl"))
      .toAbsolutePath
    Files.createDirectories(path.getParent)
    Files.write(
      path,
      (mapper.writeValueAsString(record) + "\n").getBytes(UTF_8),
      StandardOpenOption.CREATE,
      StandardOpenOption.APPEND)
  }
}
