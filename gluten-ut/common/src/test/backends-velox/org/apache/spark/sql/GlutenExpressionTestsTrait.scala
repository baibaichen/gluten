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

import org.apache.gluten.backendsapi.SubstraitBackend
import org.apache.gluten.backendsapi.velox.VeloxBackend
import org.apache.gluten.test.TestStats

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{AttributeSeq, BindReferences, BoundReference, EmptyRow, Expression, NativeExpressionRowEvalHelper}
import org.apache.spark.sql.types.DataType

import org.scalatest.{Args, Status}

import java.util
import java.util.concurrent.atomic.AtomicLong

import scala.collection.JavaConverters._
import scala.util.Try

/** The legacy query route decides whether an additional direct native check is eligible. */
trait GlutenExpressionTestsTrait extends GlutenTestsTrait {
  private val queryNativeChecks = new AtomicLong
  private val queryFallbackChecks = new AtomicLong
  private val queryPolicyChecks = new AtomicLong
  private val noEvaluationChecks = new AtomicLong
  private val baselineErrors = new AtomicLong
  private val directHookCalls = new AtomicLong
  private val directValidated = new AtomicLong
  private val directFailed = new AtomicLong
  private val plannerOnlyChecks = new AtomicLong
  private var caseNativeBefore = 0L
  private var caseFallbackBefore = 0L
  private var casePolicyBefore = 0L
  private var caseUncoveredBefore = 0L
  @volatile private var routeTestName = "<outside-test>"

  private def updateOffloadStats(): Unit = {
    TestStats.offloadGluten = queryNativeChecks.get() > caseNativeBefore &&
      queryFallbackChecks.get() == caseFallbackBefore &&
      queryPolicyChecks.get() == casePolicyBefore &&
      noEvaluationChecks.get() == caseUncoveredBefore
  }

  private def baselineOnly: Boolean =
    sys.props.getOrElse("gluten.expression.baselineOnly", "true").toBoolean

  override protected def defaultOffloadGluten: Boolean = false

  private class ConfirmedNativeChecks extends SparkFunSuite with NativeExpressionRowEvalHelper {
    implicit override protected val backendClass: Class[_ <: SubstraitBackend] =
      classOf[VeloxBackend]

    override protected def checkResult(
        result: Any,
        expected: Any,
        dataType: DataType,
        nullable: Boolean): Boolean = {
      GlutenExpressionTestsTrait.this.checkResult(result, expected, dataType, nullable)
    }

    def verify(expression: => Expression, expected: Any, inputRow: InternalRow): Unit = {
      checkEvaluationWithNativeRow(expression, expected, inputRow)
    }
  }

  private lazy val confirmedNativeChecks = new ConfirmedNativeChecks

  override protected def checkEvaluation(
      expression: => Expression,
      expected: Any,
      inputRow: InternalRow = EmptyRow): Unit = {
    lazy val evaluatedExpression = expression
    evaluateHistoricalQuery(evaluatedExpression, expected, inputRow) match {
      case Some((true, projected, attributes)) if !baselineOnly =>
        val bound = BindReferences.bindReference(projected, AttributeSeq(attributes))
        directHookCalls.incrementAndGet()
        checkOffloadedExpression(bound, expected, inputRow)
      case _ =>
    }
  }

  protected def checkOffloadedExpression(
      expression: => Expression,
      expected: Any,
      inputRow: InternalRow): Unit = {
    require(!baselineOnly, "Baseline-only observation must not instantiate direct native checks")
    var succeeded = false
    try {
      confirmedNativeChecks.verify(expression, expected, inputRow)
      directValidated.incrementAndGet()
      succeeded = true
    } finally {
      if (!succeeded) directFailed.incrementAndGet()
    }
  }

  override protected def onQueryEvaluationRoute(
      expression: Expression,
      offloaded: Boolean,
      supportedTypes: Boolean,
      nativeProjectCount: Int,
      sparkProjectCount: Int,
      projectedExpression: Option[Expression],
      plannerOnly: Boolean,
      reason: String,
      plan: => String): Unit = {
    val confirmedSparkFallback = nativeProjectCount == 0 && sparkProjectCount > 0
    val route = if (offloaded && sparkProjectCount == 0) {
      "native"
    } else if (!supportedTypes) {
      "type-policy-excluded"
    } else if (confirmedSparkFallback) {
      "fallback"
    } else {
      "mixed-or-plan-policy-excluded"
    }
    if (route == "native") {
      queryNativeChecks.incrementAndGet()
    } else if (route == "fallback") {
      queryFallbackChecks.incrementAndGet()
    } else {
      queryPolicyChecks.incrementAndGet()
    }
    updateOffloadStats()
    val record = routeRecord(route, reason)
    if (plannerOnly) plannerOnlyChecks.incrementAndGet()
    record.put(
      "classification",
      if (plannerOnly) "planner-only-native-projection"
      else if (route == "native") "native"
      else if (route == "fallback") "confirmed-spark-fallback"
      else "mixed-or-policy-excluded"
    )
    val sql = Try(expression.sql)
    record.put("legacy_eligible", Boolean.box(offloaded))
    record.put(
      "direct_native_eligible",
      Boolean.box(
        nativeProjectCount == 1 && sparkProjectCount == 0 &&
          projectedExpression.isDefined && !plannerOnly))
    record.put("planner_only_native_projection", Boolean.box(plannerOnly))
    record.put(
      "native_operation_evidence",
      if (plannerOnly) "planner emitted a literal; the raw operator was not executed natively"
      else if (projectedExpression.isDefined) "captured native project expression"
      else "no single native-only projection"
    )
    record.put("projected_expression", projectedExpression.map(_.toString).orNull)
    record.put("projected_sql", projectedExpression.flatMap(e => Try(e.sql).toOption).orNull)
    record.put("projected_expression_class", projectedExpression.map(_.getClass.getName).orNull)
    record.put("legacy_type_policy_eligible", Boolean.box(supportedTypes))
    record.put("project_exec_transformer_count", Int.box(nativeProjectCount))
    record.put("spark_project_exec_count", Int.box(sparkProjectCount))
    record.put("confirmed_spark_projection_fallback", Boolean.box(confirmedSparkFallback))
    record.put("expression", expression.toString)
    record.put("sql", sql.toOption.orNull)
    record.put("sql_error", sql.failed.toOption.map(_.getClass.getName).orNull)
    record.put("result_type", expression.dataType.sql)
    record.put("result_type_json", expression.dataType.json)
    record.put("child_types", expression.children.map(_.dataType.sql).asJava)
    record.put(
      "bound_input_types",
      expression
        .collect {
          case ref: BoundReference =>
            s"${ref.ordinal}:${ref.dataType.sql}:nullable=${ref.nullable}"
        }
        .distinct
        .asJava)
    record.put("plan", plan)
    GlutenExpressionRouteLedger.append(record)
  }

  override protected def onQueryNoEvaluation(inputRow: InternalRow): Unit = {
    noEvaluationChecks.incrementAndGet()
    updateOffloadStats()
    val record = routeRecord("uncovered", "legacy canConvertToDataFrame returned false")
    record.put("classification", "old-unconvertible-no-evaluation")
    record.put("legacy_eligible", null)
    record.put("expression", null)
    record.put("sql", null)
    record.put("input_row_class", inputRow.getClass.getName)
    record.put("input_field_count", Int.box(inputRow.numFields))
    record.put("expression_status", "not constructed on the historical no-evaluation path")
    GlutenExpressionRouteLedger.append(record)
  }

  override protected def onQueryBaselineError(
      expression: Option[Expression],
      error: Throwable,
      plan: String): Unit = {
    baselineErrors.incrementAndGet()
    val record = routeRecord("baseline-error", "historical query/preparation/assertion failed")
    record.put("classification", "baselineerror")
    record.put("legacy_eligible", null)
    record.put("expression", expression.map(_.toString).orNull)
    record.put("sql", expression.flatMap(e => Try(e.sql).toOption).orNull)
    record.put("result_type", expression.flatMap(e => Try(e.dataType.sql).toOption).orNull)
    record.put("error_type", error.getClass.getName)
    record.put("error_message", error.getMessage)
    record.put("plan", plan)
    GlutenExpressionRouteLedger.append(record)
  }

  private def routeRecord(route: String, reason: String): util.LinkedHashMap[String, Object] = {
    val record = new util.LinkedHashMap[String, Object]()
    record.put("suite", getClass.getName)
    record.put("test", routeTestName)
    record.put("event_type", "evaluation")
    record.put("baseline_only", Boolean.box(baselineOnly))
    record.put("route", route)
    record.put("reason", reason)
    record.put("occurrence_count", Int.box(1))
    record
  }

  override def runTest(testName: String, args: Args): Status = {
    routeTestName = testName
    caseNativeBefore = queryNativeChecks.get()
    caseFallbackBefore = queryFallbackChecks.get()
    casePolicyBefore = queryPolicyChecks.get()
    caseUncoveredBefore = noEvaluationChecks.get()
    val errorsBefore = baselineErrors.get()
    val directBefore = directHookCalls.get()
    val validatedBefore = directValidated.get()
    val directFailedBefore = directFailed.get()
    val plannerOnlyBefore = plannerOnlyChecks.get()
    val observingBaseline = baselineOnly
    var testSucceeded: Option[Boolean] = None
    try {
      val status = super.runTest(testName, args)
      testSucceeded = Some(status.succeeds())
      status
    } finally {
      val nativeCount = queryNativeChecks.get() - caseNativeBefore
      val fallbackCount = queryFallbackChecks.get() - caseFallbackBefore
      val policyCount = queryPolicyChecks.get() - casePolicyBefore
      val uncoveredCount = noEvaluationChecks.get() - caseUncoveredBefore
      val errorCount = baselineErrors.get() - errorsBefore
      val directCount = directHookCalls.get() - directBefore
      val summary = routeRecord("test-summary", "observed historical routes")
      summary.put("event_type", "test-summary")
      summary.put("baseline_only", Boolean.box(observingBaseline))
      summary.put("test_succeeded", testSucceeded.map(Boolean.box).orNull)
      summary.remove("occurrence_count")
      summary.put("native", Long.box(nativeCount))
      summary.put("confirmed_spark_fallback", Long.box(fallbackCount))
      summary.put("mixed_or_policy_excluded", Long.box(policyCount))
      summary.put("old_unconvertible_no_evaluation", Long.box(uncoveredCount))
      summary.put("baselineerror", Long.box(errorCount))
      summary.put("new_direct_hook_invocations", Long.box(directCount))
      summary.put("new_direct_validated", Long.box(directValidated.get() - validatedBefore))
      summary.put("new_direct_failed", Long.box(directFailed.get() - directFailedBefore))
      summary.put(
        "planner_only_native_projection",
        Long.box(plannerOnlyChecks.get() - plannerOnlyBefore))
      summary.put(
        "mixed_test",
        Boolean.box(
          Seq(nativeCount, fallbackCount, policyCount, uncoveredCount, errorCount)
            .count(_ > 0) > 1))
      GlutenExpressionRouteLedger.append(summary)
      // scalastyle:off println
      println(
        s"[expression-route] $testName: " +
          s"observed-native=$nativeCount, " +
          s"confirmed-spark-fallback=$fallbackCount, " +
          s"mixed-or-policy-excluded=$policyCount, " +
          s"no-evaluation=$uncoveredCount, baseline-error=$errorCount, " +
          s"baseline-only=$observingBaseline, new-direct-hooks=$directCount")
      // scalastyle:on println
      if (observingBaseline) {
        assert(directCount == 0, "Baseline-only observation invoked the new direct hook")
      }
      routeTestName = "<outside-test>"
    }
  }
}
