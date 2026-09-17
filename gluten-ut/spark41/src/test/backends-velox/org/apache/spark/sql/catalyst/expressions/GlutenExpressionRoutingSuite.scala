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
package org.apache.spark.sql.catalyst.expressions

import org.apache.gluten.config.GlutenConfig
import org.apache.gluten.test.TestStats

import org.apache.spark.sql.{GlutenExpressionTestsTrait, Row}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.util.{ArrayBasedMapData, GenericArrayData}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

import com.fasterxml.jackson.databind.ObjectMapper
import org.scalatest.{Args, Reporter}
import org.scalatest.events.{Event, TestFailed}
import org.scalatest.exceptions.TestFailedException

import java.nio.file.{Files, Paths}

import scala.collection.JavaConverters._

class GlutenExpressionRoutingSuite extends GlutenExpressionTestsTrait {
  override protected def shouldRun(testName: String): Boolean = true

  private var directCalls = 0
  private var executeDirect = false
  private var failDirect = false
  private var allowTypePolicy = true

  override protected def beforeEach(): Unit = {
    super.beforeEach()
    directCalls = 0
    executeDirect = false
    failDirect = false
    allowTypePolicy = true
  }

  override def checkDataTypeSupported(expression: Expression): Boolean = {
    allowTypePolicy && super.checkDataTypeSupported(expression)
  }

  override protected def checkOffloadedExpression(
      expression: => Expression,
      expected: Any,
      inputRow: InternalRow): Unit = {
    directCalls += 1
    if (failDirect) {
      throw new IllegalStateException("confirmed native failure")
    }
    if (executeDirect) {
      super.checkOffloadedExpression(expression, expected, inputRow)
    }
  }

  test("legacy fallback never invokes the direct hook") {
    withSQLConf(GlutenConfig.GLUTEN_ENABLED.key -> "false") {
      checkEvaluation(StringTrimRight(Literal("value ")), "value")
    }
    assert(directCalls == 0)
  }

  test("baseline-only native preflight invokes zero new direct hooks") {
    val key = "gluten.expression.baselineOnly"
    val previous = Option(System.getProperty(key))
    System.setProperty(key, "true")
    failDirect = true
    try {
      withSQLConf(GlutenConfig.GLUTEN_ENABLED.key -> "true") {
        val expr = StringTrimRight(Literal("value "))
        assert(checkEvaluationWithQuery(expr, "value", EmptyRow).contains(true))
        checkEvaluation(expr, "value")
        allowTypePolicy = false
        assert(checkEvaluationWithQuery(expr, "value", EmptyRow).contains(true))
        checkEvaluation(expr, "value")
      }
      assert(directCalls == 0)
    } finally {
      previous match {
        case Some(value) => System.setProperty(key, value)
        case None => System.clearProperty(key)
      }
    }
  }

  test("legacy fallback retains its original query comparator") {
    withSQLConf(GlutenConfig.GLUTEN_ENABLED.key -> "false") {
      checkEvaluation(Literal(1.0d), 1.000001d)
    }
    assert(directCalls == 0)
  }

  test("confirmed legacy native route invokes the direct hook once") {
    withSQLConf(GlutenConfig.GLUTEN_ENABLED.key -> "true") {
      checkEvaluation(StringTrimRight(Literal("value ")), "value")
    }
    assert(directCalls == 1)
  }

  test("confirmed native failure propagates without retrying through fallback") {
    failDirect = true
    val error = intercept[IllegalStateException] {
      withSQLConf(GlutenConfig.GLUTEN_ENABLED.key -> "true") {
        checkEvaluation(StringTrimRight(Literal("value ")), "value")
      }
    }
    assert(error.getMessage == "confirmed native failure")
    assert(directCalls == 1)
  }

  test("RTRIM remains eligible for actual direct native evaluation") {
    executeDirect = true
    withSQLConf(GlutenConfig.GLUTEN_ENABLED.key -> "true") {
      checkEvaluation(StringTrimRight(Literal("value ")), "value")
    }
    assert(directCalls == 1)
  }

  test("confirmed native checks retain the original query comparison contract") {
    executeDirect = true
    withSQLConf(GlutenConfig.GLUTEN_ENABLED.key -> "true") {
      checkEvaluation(Literal(1.0d), 1.000001d)
    }
    assert(directCalls == 1)
  }

  test("planner-folded operators remain old-route-only rather than raw native coverage") {
    failDirect = true
    val values = Literal.create(Seq(1L, 2L), ArrayType(LongType))
    withSQLConf(GlutenConfig.GLUTEN_ENABLED.key -> "true") {
      checkEvaluation(GreaterThan(values, values), false)
    }
    assert(directCalls == 0)
  }

  test("native replay uses the observed legacy three-valued-logic normalization") {
    executeDirect = true
    val variable = NamedLambdaVariable("element", IntegerType, nullable = false)
    val predicate = LambdaFunction(Literal.create(null, BooleanType), Seq(variable))
    val expr = ArrayExists(
      Literal.create(Seq(1, 2, 3), ArrayType(IntegerType)),
      predicate,
      followThreeValuedLogic = false)
    withSQLConf(GlutenConfig.GLUTEN_ENABLED.key -> "true") {
      checkEvaluation(expr, false)
    }
    assert(directCalls == 1)
  }

  test("legacy unsupported input remains uncovered without a direct hook") {
    checkEvaluation(
      throw new IllegalStateException("the skipped expression must not be constructed"),
      "unused",
      InternalRow(InternalRow(1)))
    assert(directCalls == 0)
  }

  test("ledger records per-check fallback and uncovered routes separately") {
    val key = "gluten.expression.routeLog"
    val previous = Option(System.getProperty(key))
    val path = Paths.get(basePath, "routing-ledger.jsonl")
    System.setProperty(key, path.toString)
    try {
      withSQLConf(GlutenConfig.GLUTEN_ENABLED.key -> "false") {
        checkEvaluation(StringTrimRight(Literal("value ")), "value")
      }
      withSQLConf(GlutenConfig.GLUTEN_ENABLED.key -> "true") {
        checkEvaluation(StringTrimRight(Literal("value ")), "value")
      }
      allowTypePolicy = false
      withSQLConf(GlutenConfig.GLUTEN_ENABLED.key -> "true") {
        checkEvaluation(StringTrimRight(Literal("value ")), "value")
      }
      checkEvaluation(Literal("unused"), "unused", InternalRow(InternalRow(1)))
      val mapper = new ObjectMapper()
      val records = Files
        .readAllLines(path)
        .asScala
        .map(line => mapper.readTree(line))
        .filter(_.get("event_type").asText() == "evaluation")
      assert(
        records.map(_.get("route").asText()).toSeq ==
          Seq("fallback", "native", "type-policy-excluded", "uncovered"))
      assert(records.forall(_.get("occurrence_count").asInt() == 1))
      assert(records.head.get("suite").asText() == getClass.getName)
      assert(
        records.head.get("test").asText() ==
          "ledger records per-check fallback and uncovered routes separately")
      assert(records.head.get("sql").asText().contains("rtrim"))
      assert(records.head.get("result_type").asText() == "STRING")
      assert(records.head.get("plan").asText().contains("Project"))
      assert(records.head.get("confirmed_spark_projection_fallback").asBoolean())
      assert(!records(2).get("legacy_eligible").asBoolean())
      assert(!records(2).get("legacy_type_policy_eligible").asBoolean())
      assert(records(2).get("project_exec_transformer_count").asInt() == 1)
      assert(!records(2).get("confirmed_spark_projection_fallback").asBoolean())
      assert(records(2).get("direct_native_eligible").asBoolean())
      assert(records.last.get("expression").isNull)
      val baseline = sys.props.getOrElse("gluten.expression.baselineOnly", "true").toBoolean
      assert(directCalls == (if (baseline) 0 else 2))
    } finally {
      previous match {
        case Some(value) => System.setProperty(key, value)
        case None => System.clearProperty(key)
      }
    }
  }

  private def withBaselineMode(value: String)(body: => Unit): Unit = {
    val key = "gluten.expression.baselineOnly"
    val previous = Option(System.getProperty(key))
    if (value == null) System.clearProperty(key) else System.setProperty(key, value)
    try {
      body
    } finally {
      previous match {
        case Some(original) => System.setProperty(key, original)
        case None => System.clearProperty(key)
      }
    }
  }

  // Exercise the actual routing hooks and per-test accounting without constructing a query plan.
  private class RouteStatsProbe(scenarios: Seq[(String, Seq[String])])
    extends GlutenExpressionTestsTrait {
    override protected def shouldRun(testName: String): Boolean = true
    override def beforeAll(): Unit = ()
    override def afterAll(): Unit = ()
    private var route = "uncovered"
    private var hooks = 0

    override protected def evaluateHistoricalQuery(
        expression: => Expression,
        expected: Any,
        inputRow: InternalRow): Option[(Boolean, Expression, Seq[Attribute])] = {
      if (route == "uncovered") {
        onQueryNoEvaluation(inputRow)
        None
      } else {
        val expr = expression
        val isNative = route == "native"
        onQueryEvaluationRoute(
          expr,
          offloaded = isNative || route == "mixed",
          supportedTypes = route != "excluded",
          nativeProjectCount = if (route == "fallback") 0 else 1,
          sparkProjectCount = if (route == "fallback" || route == "mixed") 1 else 0,
          projectedExpression = if (isNative) Some(expr) else None,
          plannerOnly = false,
          reason = "routing statistics fixture",
          plan = "<fixture>"
        )
        Some((isNative, expr, Seq.empty))
      }
    }

    override protected def checkOffloadedExpression(
        expression: => Expression,
        expected: Any,
        inputRow: InternalRow): Unit = {
      hooks += 1
    }

    scenarios.foreach {
      case (name, routes) =>
        test(name) {
          assert(!TestStats.offloadGluten, "Each test starts without native evidence")
          var seen = Vector.empty[String]
          routes.foreach {
            current =>
              route = current
              val input =
                if (current == "uncovered") InternalRow(InternalRow(1))
                else InternalRow.empty
              checkEvaluation(Literal(1), 1, input)
              seen :+= current
              assert(TestStats.offloadGluten == seen.forall(_ == "native"))
          }
        }
    }

    def observe(): (Seq[(Boolean, Int)], Int) = {
      val failures = scala.collection.mutable.ArrayBuffer.empty[String]
      val reporter = new Reporter {
        override def apply(event: Event): Unit = event match {
          case failed: TestFailed => failures += failed.message
          case _ =>
        }
      }
      val results = scenarios.map {
        case (name, _) =>
          val before = TestStats.offloadGlutenTestNumber
          val status = runTest(name, Args(reporter))
          assert(status.succeeds(), failures.mkString("; "))
          (TestStats.offloadGluten, TestStats.offloadGlutenTestNumber - before)
      }
      (results, hooks)
    }
  }

  private def checkRouteStatistics(scenarios: Seq[Seq[String]], baseline: String): Unit = {
    val originalOffload = TestStats.offloadGluten
    val originalSuiteCount = TestStats.suiteTestNumber
    val originalOffloadCount = TestStats.offloadGlutenTestNumber
    val named = scenarios.zipWithIndex.map { case (routes, i) => s"route-$i" -> routes }
    // Nested probes must not replace the enclosing test case in TestStats private bookkeeping.
    val probeConf = SQLConf.get.clone()
    probeConf.setConfString(GlutenConfig.UT_STATISTIC.key, "false")
    try {
      SQLConf.withExistingConf(probeConf) {
        withBaselineMode(baseline) {
          val (observed, hooks) = new RouteStatsProbe(named).observe()
          val expected = scenarios.map {
            routes =>
              val nativeOnly = routes.nonEmpty && routes.forall(_ == "native")
              (nativeOnly, if (nativeOnly) 1 else 0)
          }
          assert(observed == expected)
          val expectedHooks =
            if (baseline == "false") scenarios.flatten.count(_ == "native") else 0
          assert(hooks == expectedHooks)
        }
      }
    } finally {
      TestStats.offloadGluten = originalOffload
      TestStats.suiteTestNumber = originalSuiteCount
      TestStats.offloadGlutenTestNumber = originalOffloadCount
    }
  }

  test("native-only query evidence updates TestStats even in baseline-only mode") {
    checkRouteStatistics(Seq(Seq("native", "native")), "true")
  }

  test("native-only replay updates TestStats and invokes the direct hook") {
    checkRouteStatistics(Seq(Seq("native", "native")), "false")
  }

  test("default baseline mode records native-only evidence without direct replay") {
    checkRouteStatistics(Seq(Seq("native")), null)
  }

  test("fallback and excluded query routes never count as native-only") {
    checkRouteStatistics(Seq(Seq("fallback"), Seq("excluded"), Seq("mixed")), "false")
  }

  test("fallback and native route ordering cannot restore native-only status") {
    checkRouteStatistics(Seq(Seq("native", "fallback"), Seq("fallback", "native")), "false")
  }

  test("policy and uncovered routes keep a mixed test non-native in either order") {
    checkRouteStatistics(
      Seq(
        Seq("native", "excluded"),
        Seq("excluded", "native"),
        Seq("native", "mixed"),
        Seq("mixed", "native"),
        Seq("native", "uncovered"),
        Seq("uncovered", "native")),
      "false"
    )
  }

  test("empty and uncovered checks retain the default non-native classification") {
    checkRouteStatistics(Seq(Seq.empty, Seq("uncovered")), null)
  }

  test("native classification and disqualifying routes reset between tests on one suite") {
    checkRouteStatistics(
      Seq(Seq("fallback"), Seq("native"), Seq.empty, Seq("excluded"), Seq("native")),
      "false")
  }

  test("successful actual direct native evaluation marks the test as offloaded") {
    executeDirect = true
    withBaselineMode("false") {
      withSQLConf(GlutenConfig.GLUTEN_ENABLED.key -> "true") {
        checkEvaluation(StringTrimRight(Literal("value ")), "value")
      }
      assert(directCalls == 1)
      assert(TestStats.offloadGluten)
    }
  }

  private class ComparatorProbe(acceptMarker: Boolean) extends GlutenExpressionTestsTrait {
    override protected def shouldRun(testName: String): Boolean = true
    override def beforeAll(): Unit = ()
    override def afterAll(): Unit = ()

    override protected def checkResult(
        result: Any,
        expected: Any,
        dataType: DataType,
        nullable: Boolean): Boolean = {
      if (acceptMarker && dataType == IntegerType && result == 7 && expected == 70) true
      else super.checkResult(result, expected, dataType, nullable)
    }

    def compare(result: Any, expected: Any, dataType: DataType, nullable: Boolean = true): Boolean =
      checkResult(result, expected, dataType, nullable)

    def assertResult(expression: Expression, actual: Any, expected: Any): Unit =
      assertQueryResult(expression, expected, EmptyRow, Array(Row(actual)))
  }

  test("query assertions dispatch through an overridden result comparator") {
    val custom = new ComparatorProbe(acceptMarker = true)
    assert(custom.compare(7, 70, IntegerType))
    custom.assertResult(Literal(7), 7, 70)
  }

  test("struct comparisons dispatch nested fields through the overridden comparator") {
    val custom = new ComparatorProbe(acceptMarker = true)
    val schema = new StructType().add("value", IntegerType, nullable = false)
    assert(custom.compare(InternalRow(7), InternalRow(70), schema))
    assert(
      !new ComparatorProbe(acceptMarker = false)
        .compare(InternalRow(7), InternalRow(70), schema))
  }

  test("array comparisons dispatch elements through the overridden comparator") {
    val custom = new ComparatorProbe(acceptMarker = true)
    val actual = new GenericArrayData(Array[Any](7, null))
    val expected = new GenericArrayData(Array[Any](70, null))
    assert(custom.compare(actual, expected, ArrayType(IntegerType)))
    assert(!custom.compare(actual, new GenericArrayData(Array[Any](70)), ArrayType(IntegerType)))
  }

  test("map comparisons dispatch sorted keys and nested values through the overridden comparator") {
    val custom = new ComparatorProbe(acceptMarker = true)
    val actual = new ArrayBasedMapData(
      new GenericArrayData(Array[Any](2, 1)),
      new GenericArrayData(Array[Any](null, InternalRow(7))))
    val expected = new ArrayBasedMapData(
      new GenericArrayData(Array[Any](1, 2)),
      new GenericArrayData(Array[Any](InternalRow(70), null)))
    val valueType = new StructType().add("value", IntegerType, nullable = false)
    assert(custom.compare(actual, expected, MapType(IntegerType, valueType)))
    val actualKey = new ArrayBasedMapData(
      new GenericArrayData(Array[Any](7)),
      new GenericArrayData(Array[Any](UTF8String.fromString("value"))))
    val expectedKey = new ArrayBasedMapData(
      new GenericArrayData(Array[Any](70)),
      new GenericArrayData(Array[Any](UTF8String.fromString("value"))))
    assert(custom.compare(actualKey, expectedKey, MapType(IntegerType, StringType)))
  }

  test("default comparator preserves null byte-array and numeric rules") {
    val original = new ComparatorProbe(acceptMarker = false)
    assert(original.compare(null, null, StringType))
    assert(!original.compare(null, "value", StringType))
    intercept[TestFailedException](original.compare(null, null, StringType, nullable = false))
    assert(original.compare(Array[Byte](1, 2), Array[Byte](1, 2), BinaryType))
    assert(!original.compare(Array[Byte](1, 2), Array[Byte](2, 1), BinaryType))
    assert(original.compare(1.0d, 1.000001d, DoubleType))
    assert(!original.compare(1.0d, 1.001d, DoubleType))
    assert(original.compare(Double.NaN, Double.NaN, DoubleType))
    assert(original.compare(Double.PositiveInfinity, Double.PositiveInfinity, DoubleType))
    assert(!original.compare(Double.PositiveInfinity, Double.NegativeInfinity, DoubleType))
    assert(original.compare(-0.0d, -0.0d, DoubleType))
    assert(!original.compare(0.0d, -0.0d, DoubleType))
    assert(original.compare(Float.NaN, Float.NaN, FloatType))
    assert(original.compare(0.0f, -0.0f, FloatType))
    assert(!original.compare(1.0f, 1.000001f, FloatType))
  }

  test("default comparator preserves nested nulls and unordered-map equality") {
    val original = new ComparatorProbe(acceptMarker = false)
    val actual = new ArrayBasedMapData(
      new GenericArrayData(Array[Any](2, 1)),
      new GenericArrayData(Array[Any](null, 1.0d)))
    val expected = new ArrayBasedMapData(
      new GenericArrayData(Array[Any](1, 2)),
      new GenericArrayData(Array[Any](1.000001d, null)))
    assert(original.compare(actual, expected, MapType(IntegerType, DoubleType)))
    val schema = new StructType().add("values", ArrayType(DoubleType))
    assert(
      original.compare(
        InternalRow(new GenericArrayData(Array[Any](1.0d, null))),
        InternalRow(new GenericArrayData(Array[Any](1.000001d, null))),
        schema))
  }

}
