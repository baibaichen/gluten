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

import org.apache.gluten.backendsapi.BackendsApiManager
import org.apache.gluten.columnarbatch.ColumnarBatches
import org.apache.gluten.velox.vector.VeloxInputBatch

import org.apache.spark.{SparkContext, SparkEnv}
import org.apache.spark.sql.{NativeExpressionTestsTrait, SparkSession}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.util.GenericArrayData
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

class GlutenNativeExpressionAdapterSuite extends NativeExpressionTestsTrait {
  override protected def shouldRun(testName: String): Boolean = true

  private def assertNoSparkContext(): Unit = {
    assert(SparkContext.getActive.isEmpty)
    assert(SparkEnv.get == null)
    assert(SparkSession.getActiveSession.isEmpty)
    assert(SparkSession.getDefaultSession.isEmpty)
  }

  test("native row adapter preserves UTF8String bytes and nulls") {
    assertNoSparkContext()
    val bytes = Array[Byte](0x61, 0, 0xc3.toByte, 0x28)
    val value = UTF8String.fromBytes(bytes)
    val ref = BoundReference(0, StringType, nullable = true)
    checkEvaluation(ref, value, InternalRow(value))
    checkEvaluation(ref, null, InternalRow(null))
    assertNoSparkContext()
  }

  test("native row adapter derives sparse and repeated bindings from expression types") {
    val ref = BoundReference(2, StringType, nullable = true)
    checkEvaluation(
      Concat(Seq(ref, ref)),
      "value value ",
      InternalRow(new Object, null, UTF8String.fromString("value ")))
    checkEvaluation(ref, null, InternalRow(new Object, new Object, null))
  }

  test("native literal evaluation does not infer unused input fields") {
    checkEvaluation(StringTrimRight(Literal("value ")), "value")
    checkEvaluation(StringTrimRight(Literal("value ")), "value", InternalRow(new Object))
    checkEvaluation(Literal.create(null, StringType), null, InternalRow(new Object))
  }

  test("native row adapter preserves nested arrays maps and structs") {
    val schema = StructType(
      Seq(
        StructField("text", StringType),
        StructField("array", ArrayType(StringType)),
        StructField("map", MapType(StringType, IntegerType))))
    val nested = InternalRow(
      UTF8String.fromBytes(Array[Byte](0x61, 0, 0xc3.toByte, 0x28)),
      new GenericArrayData(Array[Any](UTF8String.fromString("first"), null)),
      create_map("key" -> 7, "null" -> null)
    )
    val ref = BoundReference(0, schema, nullable = true)
    assert(!nested.getMap(2).keyArray().isNullAt(1))
    withNativeInput(ref, InternalRow(nested)) {
      (_, batch, _) =>
        val view = VeloxInputBatch.wrap(
          ColumnarBatches.getNativeHandle(BackendsApiManager.getBackendName(backendClass), batch),
          StructType(Seq(StructField("input", schema))),
          1)
        try {
          val actual = view.getRow(0).getStruct(0, schema.length)
          assert(
            java.util.Arrays
              .equals(actual.getUTF8String(0).getBytes, nested.getUTF8String(0).getBytes),
            "nested text bytes")
          val array = actual.getArray(1)
          assert(array.numElements() == 2, "nested array size after child growth")
          assert(array.getUTF8String(0) == UTF8String.fromString("first"), "nested array value")
          assert(array.isNullAt(1), "nested array null bitmap")
          val map = actual.getMap(2)
          assert(map.numElements() == 2, "nested map size after child growth")
          assert(map.keyArray().getUTF8String(0) == UTF8String.fromString("key"))
          assert(!map.keyArray().isNullAt(1), "the string key 'null' is not a null key")
          assert(map.keyArray().getUTF8String(1) == UTF8String.fromString("null"))
          assert(map.valueArray().getInt(0) == 7, "nested map value")
          assert(map.valueArray().isNullAt(1), "nested map null bitmap")
        } finally {
          view.close()
        }
    }
    checkEvaluation(ref, nested, InternalRow(nested))
    checkEvaluation(ref, null, InternalRow(null))
  }

  test("native row adapter rejects invalid and conflicting bindings before reading input") {
    intercept[IllegalArgumentException] {
      checkEvaluationWithNativeRow(BoundReference(1, StringType, true), null, InternalRow(null))
    }
    intercept[IllegalArgumentException] {
      checkEvaluationWithNativeRow(
        CreateNamedStruct(
          Seq(
            Literal("string"),
            BoundReference(0, StringType, true),
            Literal("integer"),
            BoundReference(0, IntegerType, true))),
        null,
        InternalRow(null)
      )
    }
    assertNoSparkContext()
  }

  test("native row adapter preserves multiple sparse input columns") {
    val first = BoundReference(0, IntegerType, nullable = false)
    val second = BoundReference(2, IntegerType, nullable = false)
    val expr = CreateNamedStruct(Seq(Literal("first"), first, Literal("second"), second))
    val input = InternalRow(1, new Object, 3)
    withNativeInput(expr, input) {
      (_, batch, attributes) =>
        val schema = StructType(attributes.map(a => StructField(a.name, a.dataType, a.nullable)))
        val view = VeloxInputBatch.wrap(
          ColumnarBatches.getNativeHandle(BackendsApiManager.getBackendName(backendClass), batch),
          schema,
          1)
        try {
          assert(view.getRow(0).getInt(0) == 1, "first native input column")
          assert(view.getRow(0).getInt(1) == 3, "second native input column")
        } finally {
          view.close()
        }
    }
    checkEvaluation(expr, InternalRow(1, 3), input)
  }

  test("native row adapter preserves mixed input column types") {
    val text = BoundReference(0, StringType, nullable = false)
    val count = BoundReference(2, IntegerType, nullable = false)
    checkEvaluation(
      StringRepeat(text, count),
      "hihi",
      InternalRow(UTF8String.fromString("hi"), new Object, 2))
  }

  test("nonfoldable literals remain native input columns") {
    val value = UTF8String.fromBytes(Array[Byte](0x61, 0, 0xc3.toByte, 0x28))
    val expr = NonFoldableLiteral(value, StringType)
    withNativeInput(expr, EmptyRow) {
      (bound, batch, attributes) =>
        assert(!bound.foldable)
        assert(bound.isInstanceOf[BoundReference])
        assert(batch.numCols() == 1)
        assert(attributes.head.dataType == StringType)
    }
    checkEvaluation(expr, value)
    checkEvaluation(NonFoldableLiteral(null, StringType), null)
  }

  test("logical interval and timestamp NTZ results retain their native carriers") {
    checkEvaluation(MakeYMInterval(Literal(1), Literal(1)), 13)
    checkEvaluation(
      Literal.create(java.time.LocalDateTime.of(1970, 1, 1, 0, 0, 1, 1000), TimestampNTZType),
      1000001L)
  }

  test("unsafe expected-row boxing preserves integer values exactly") {
    Seq(ByteType, ShortType, LongType, FloatType, DoubleType).foreach {
      dataType => checkEvaluation(Cast(Literal(1), dataType), 1)
    }
    val error = intercept[IllegalArgumentException] {
      checkEvaluationWithUnsafeProjection(Literal(16777216.0f), 16777217)
    }
    assert(error.getMessage.contains("not exactly representable"))
  }

  test("native preparation preserves mapped runtime replacements and their children") {
    val charset = Literal("UTF-8")
    val encoded = new Encode(Literal("value"), charset)
    val decoded = new Decode(Seq(encoded, charset))
    withNativeInput(decoded, EmptyRow) {
      (prepared, _, _) =>
        assert(prepared.isInstanceOf[StringDecode])
        assert(prepared.children.head.isInstanceOf[Encode])
    }
    checkEvaluation(encoded, "value".getBytes(java.nio.charset.StandardCharsets.UTF_8))
    checkEvaluation(decoded, "value")
    checkEvaluation(new TryAdd(Literal(1), Literal(2)), 3)
  }

  test("declared timezone is isolated from session configuration and other prepared expressions") {
    val utc = Cast(Literal(0L, TimestampType), StringType, Some("UTC"))
    val pacific = utc.copy(timeZoneId = Some("America/Los_Angeles"))
    withSQLConf(SQLConf.SESSION_LOCAL_TIMEZONE.key -> "America/Los_Angeles") {
      checkEvaluation(utc, "1970-01-01 00:00:00")
      withNativeInput(utc, EmptyRow) {
        (_, input, attributes) =>
          checkEvaluationWithNative(utc, Seq("1970-01-01 00:00:00"), input, attributes)
          checkEvaluationWithNative(pacific, Seq("1969-12-31 16:00:00"), input, attributes)
          checkEvaluationWithNative(utc, Seq("1970-01-01 00:00:00"), input, attributes)
      }
      assert(SQLConf.get.sessionLocalTimeZone == "America/Los_Angeles")
    }
  }

  test("mixed declared timezones do not select an expression-wide override") {
    val utc = Cast(Literal(0L, TimestampType), StringType, Some("UTC"))
    val pacific = utc.copy(timeZoneId = Some("America/Los_Angeles"))
    val mixed = CreateNamedStruct(Seq(Literal("utc"), utc, Literal("pacific"), pacific))
    assert(NativeExpressionPreparation.compatibleTimeZone(mixed).isEmpty)
    val equivalent = CreateNamedStruct(
      Seq(Literal("utc"), utc, Literal("alias"), utc.copy(timeZoneId = Some("Etc/UTC"))))
    assert(NativeExpressionPreparation.compatibleTimeZone(equivalent).contains("UTC"))
  }

  test("ArrayExists preserves legacy and three-valued predicate semantics") {
    val arrayType = ArrayType(IntegerType, containsNull = true)
    val input = BoundReference(0, arrayType, nullable = true)
    val element = NamedLambdaVariable("element", IntegerType, nullable = true)
    val even = EqualTo(Remainder(element, Literal(2)), Literal(0))
    val noMatch = InternalRow(new GenericArrayData(Array[Any](1, null, 3)))
    val withMatch = InternalRow(new GenericArrayData(Array[Any](1, null, 2)))
    val empty = InternalRow(new GenericArrayData(Array.empty[Any]))
    val nullArray = InternalRow(null)
    Seq(false, true).foreach {
      threeValued =>
        val nullResult: Any = if (threeValued) null else false
        val expr = ArrayExists(input, LambdaFunction(even, Seq(element)), threeValued)
        checkEvaluation(expr, nullResult, noMatch)
        checkEvaluation(expr, true, withMatch)
        checkEvaluation(expr, false, empty)
        checkEvaluation(expr, null, nullArray)

        val nullPredicate = ArrayExists(
          input,
          LambdaFunction(Literal.create(null, BooleanType), Seq(element)),
          threeValued)
        checkEvaluation(nullPredicate, nullResult, noMatch)
        checkEvaluation(nullPredicate, null, nullArray)

        val truePredicate =
          ArrayExists(input, LambdaFunction(Literal.TrueLiteral, Seq(element)), threeValued)
        checkEvaluation(truePredicate, true, noMatch)
        checkEvaluation(truePredicate, null, nullArray)
    }
  }
}
