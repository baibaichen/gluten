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
import org.apache.gluten.exception.GlutenNotSupportException
import org.apache.gluten.test.TestStats

import org.apache.spark.sql.{DataFrame, GlutenExpressionTestsTrait, Row}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.catalyst.util.{ArrayBasedMapData, GenericArrayData}
import org.apache.spark.sql.types._
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.unsafe.types.UTF8String

import org.scalatest.exceptions.TestFailedException

class GlutenExpressionRoutingSuite extends GlutenExpressionTestsTrait {
  import GlutenExpressionRoutingSuite._

  override protected def shouldRun(testName: String): Boolean = true

  private var nativeCalls = 0
  private var sparkCalls = 0
  private var nativeFailure: Option[RuntimeException] = None
  private var validationFailure: Option[RuntimeException] = None

  override protected def beforeEach(): Unit = {
    super.beforeEach()
    nativeCalls = 0
    sparkCalls = 0
    nativeFailure = None
    validationFailure = None
  }

  override protected def expressionDataFrame(
      expression: Expression,
      inputRow: InternalRow): DataFrame = {
    fail("Scalar dispatch must not build or collect a DataFrame")
  }

  override protected def nativeExpressionFallbackReason(
      expression: Expression,
      attributes: Seq[Attribute]): Option[String] = {
    validationFailure.foreach(throw _)
    super.nativeExpressionFallbackReason(expression, attributes)
  }

  override protected def checkEvaluationWithNative(
      expression: => Expression,
      expected: Seq[Any],
      input: ColumnarBatch,
      attributes: Seq[Attribute]): Unit = {
    nativeCalls += 1
    nativeFailure.foreach(throw _)
    super.checkEvaluationWithNative(expression, expected, input, attributes)
  }

  override protected def checkEvaluationWithoutCodegen(
      expression: Expression,
      expected: Any,
      inputRow: InternalRow = EmptyRow): Unit = {
    sparkCalls += 1
    super.checkEvaluationWithoutCodegen(expression, expected, inputRow)
  }

  test("supported RTRIM selects native only and constructs its expression once") {
    var constructed = 0
    checkEvaluation(
      {
        constructed += 1
        StringTrimRight(Literal("value "))
      },
      "value")
    assert(constructed == 1)
    assert(nativeCalls == 1)
    assert(sparkCalls == 0)
    assert(TestStats.offloadGluten)
  }

  test("unmapped expressions select Spark only") {
    checkEvaluation(SparkOnly(7), 7)
    assert(nativeCalls == 0)
    assert(sparkCalls > 0)
    assert(!TestStats.offloadGluten)
  }

  test("disabled Gluten and existing expression blacklist select Spark only") {
    withSQLConf(GlutenConfig.GLUTEN_ENABLED.key -> "false") {
      checkEvaluation(StringTrimRight(Literal("value ")), "value")
    }
    withSQLConf(GlutenConfig.EXPRESSION_BLACK_LIST.key -> "rtrim") {
      checkEvaluation(StringTrimRight(Literal("value ")), "value")
    }
    assert(nativeCalls == 0)
    assert(sparkCalls > 0)
  }

  test("unsupported native signatures use the existing native validator before fallback") {
    val values = Literal.create(Seq(1L, 2L), ArrayType(LongType))
    val expression = GreaterThan(values, values)
    val reason = nativeExpressionFallbackReason(expression, Seq.empty)
    assert(reason.contains("Native expression validation returned false"))
    checkEvaluation(expression, false)
    assert(nativeCalls == 0)
    assert(sparkCalls > 0)
  }

  test("native execution failures including unsupported exceptions never retry in Spark") {
    Seq(
      new IllegalStateException("native execution failure"),
      new GlutenNotSupportException("unsupported during execution")).foreach {
      failure =>
        nativeFailure = Some(failure)
        val error = intercept[RuntimeException] {
          checkEvaluation(StringTrimRight(Literal("value ")), "value")
        }
        assert(error eq failure)
        assert(sparkCalls == 0)
    }
    assert(nativeCalls == 2)
  }

  test("unexpected validation and invalid binding failures remain errors") {
    val failure = new IllegalStateException("validation JNI failure")
    validationFailure = Some(failure)
    assert(intercept[IllegalStateException](checkEvaluation(Literal(1), 1)) eq failure)
    validationFailure = None
    intercept[IllegalArgumentException] {
      checkEvaluation(BoundReference(1, IntegerType, true), null, InternalRow(null))
    }
    assert(nativeCalls == 0)
    assert(sparkCalls == 0)
  }

  test("support rejection precedes native input allocation and allocation failures propagate") {
    val expression = StringTrimRight(BoundReference(0, StringType, nullable = true))
    val input = InternalRow(UTF8String.fromString("value "))
    withSQLConf("spark.sql.inMemoryColumnarStorage.hugeVectorThreshold" -> "0") {
      withSQLConf(GlutenConfig.EXPRESSION_BLACK_LIST.key -> "rtrim") {
        checkEvaluation(expression, "value", input)
      }
      val fallbackChecks = sparkCalls
      assert(fallbackChecks > 0)
      intercept[UnsupportedOperationException] {
        checkEvaluation(expression, "value", input)
      }
      assert(sparkCalls == fallbackChecks)
      assert(nativeCalls == 0)
    }
  }

  test("native and fallback expected mismatches fail on the selected branch") {
    intercept[TestFailedException] {
      checkEvaluation(StringTrimRight(Literal("value ")), "wrong")
    }
    assert(nativeCalls == 1)
    assert(sparkCalls == 0)
    intercept[TestFailedException] {
      checkEvaluation(SparkOnly(7), 8)
    }
    assert(nativeCalls == 1)
    assert(sparkCalls > 0)
  }

  test("native and fallback retain the historical query comparison contract") {
    checkEvaluation(Literal(1.0d), 1.000001d)
    withSQLConf(GlutenConfig.GLUTEN_ENABLED.key -> "false") {
      checkEvaluation(Literal(1.0d), 1.000001d)
    }
    assert(nativeCalls == 1)
    assert(sparkCalls > 0)
  }

  test("literal functions stay foldable with one row and no input columns") {
    val expression = StringTrimRight(Literal("value "))
    withNativeInput(expression, InternalRow(new Object)) {
      (prepared, input, attributes) =>
        assert(prepared.isInstanceOf[StringTrimRight])
        assert(prepared.foldable)
        assert(prepared.children.head.isInstanceOf[Literal])
        assert(attributes.isEmpty)
        assert(input.numCols() == 0 && input.numRows() == 1)
    }
    checkEvaluation(expression, "value", InternalRow(new Object))
    assert(nativeCalls == 1)
    assert(sparkCalls == 0)
  }

  test("bound nested input is evaluated rather than skipped by the old DataFrame gate") {
    val schema = new StructType().add("values", ArrayType(IntegerType))
    val value = InternalRow(new GenericArrayData(Array[Any](1, null)))
    checkEvaluation(BoundReference(1, schema, true), value, InternalRow(new Object, value))
    assert(nativeCalls == 1)
    assert(sparkCalls == 0)
  }

  test("mixed checks cannot restore native-only statistics") {
    checkEvaluation(SparkOnly(7), 7)
    checkEvaluation(Literal(7), 7)
    assert(!TestStats.offloadGluten)
    checkEvaluation(SparkOnly(7), 7)
    assert(!TestStats.offloadGluten)
  }

  test("native ArrayExists preserves legacy three-valued-logic normalization") {
    val variable = NamedLambdaVariable("element", IntegerType, nullable = false)
    val predicate = LambdaFunction(Literal.create(null, BooleanType), Seq(variable))
    val expression = ArrayExists(
      Literal.create(Seq(1, 2, 3), ArrayType(IntegerType)),
      predicate,
      followThreeValuedLogic = false)
    checkEvaluation(expression, false)
    assert(nativeCalls == 1)
    assert(sparkCalls == 0)
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

    def verify(expression: Expression, expected: Any, input: InternalRow = EmptyRow): Unit =
      checkEvaluation(expression, expected, input)
  }

  test("query and native assertions dispatch through an overridden result comparator") {
    val custom = new ComparatorProbe(acceptMarker = true)
    assert(custom.compare(7, 70, IntegerType))
    custom.assertResult(Literal(7), 7, 70)
    custom.verify(Literal(7), 70)
    withSQLConf(GlutenConfig.GLUTEN_ENABLED.key -> "false") {
      custom.verify(Literal(7), 70)
    }
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
    custom.verify(BoundReference(0, ArrayType(IntegerType), true), expected, InternalRow(actual))
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
    val mapType = MapType(IntegerType, valueType)
    assert(custom.compare(actual, expected, mapType))
    custom.verify(BoundReference(0, mapType, true), expected, InternalRow(actual))
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
    assert(original.compare(Decimal("1.23"), Decimal("1.230"), DecimalType(10, 3)))
  }

  test("default comparator preserves nested nulls unordered maps and duplicate-key behavior") {
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
    val duplicates = new ArrayBasedMapData(
      new GenericArrayData(Array[Any](1, 1, 2)),
      new GenericArrayData(Array[Any](9.0d, 1.0d, null)))
    assert(original.compare(duplicates, expected, MapType(IntegerType, DoubleType)))
    original.verify(
      BoundReference(0, schema, true),
      InternalRow(null),
      InternalRow(InternalRow(null)))
  }

  test("borrowed maps preserve last-value duplicate complex keys") {
    val arrayType = ArrayType(IntegerType)
    val array = new GenericArrayData(Array[Any](1, null))
    val structType = new StructType().add("values", arrayType)
    Seq((arrayType, array), (structType, InternalRow(array))).foreach {
      case (keyType, key) =>
        val mapType = MapType(keyType, IntegerType)
        val actual = new ArrayBasedMapData(
          new GenericArrayData(Array.fill[Any](8)(key)),
          new GenericArrayData((0 until 8).map(Int.box).toArray))
        val expected = new ArrayBasedMapData(
          new GenericArrayData(Array[Any](key)),
          new GenericArrayData(Array[Any](7)))
        val original = new ComparatorProbe(acceptMarker = false)
        assert(original.compare(actual, expected, mapType))
        original.verify(BoundReference(0, mapType, true), expected, InternalRow(actual))
    }
  }
}

object GlutenExpressionRoutingSuite {
  case class SparkOnly(value: Int) extends LeafExpression with CodegenFallback {
    override def nullable: Boolean = false
    override def dataType: DataType = IntegerType
    override def eval(input: InternalRow): Any = value
  }
}
