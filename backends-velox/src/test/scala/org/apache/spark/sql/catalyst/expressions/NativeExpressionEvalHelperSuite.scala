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

import org.apache.gluten.backendsapi.{BackendsApiManager, SubstraitBackend}
import org.apache.gluten.backendsapi.velox.{VeloxBackend, VeloxListenerApi}
import org.apache.gluten.columnarbatch.{ColumnarBatches, ColumnarBatchJniWrapper, VeloxOutputBatchJniWrapper}
import org.apache.gluten.config.GlutenConfig
import org.apache.gluten.exception.{GlutenException, GlutenNotSupportException}
import org.apache.gluten.expression.ConverterUtils
import org.apache.gluten.runtime.Runtimes
import org.apache.gluten.test.MockVeloxBackend
import org.apache.gluten.velox.vector.{VeloxInputBatch, VeloxWritableColumnVector}

import org.apache.spark.{SparkContext, SparkEnv, SparkFunSuite}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.catalyst.util.ArrayData
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._
import org.apache.spark.sql.vectorized.{ColumnarArray, ColumnarBatch, ColumnVector}
import org.apache.spark.task.TaskResources
import org.apache.spark.unsafe.types.UTF8String

import org.scalatest.BeforeAndAfterAll
import org.scalatest.exceptions.TestFailedException

import java.nio.charset.StandardCharsets.UTF_8

class NativeExpressionEvalHelperSuite
  extends SparkFunSuite
  with NativeExpressionEvalHelper
  with BeforeAndAfterAll {

  implicit override protected val backendClass: Class[_ <: SubstraitBackend] =
    classOf[VeloxBackend]

  private val backend = new VeloxListenerApi
  private val attributes: Seq[Attribute] =
    Seq(AttributeReference("input", StringType, nullable = true)())
  private val bound = BoundReference(0, StringType, nullable = true)
  private var checkBorrowedArrays = false
  private var borrowedArraysCompared = 0

  override protected def checkResult(
      result: Any,
      expected: Any,
      dataType: DataType,
      nullable: Boolean): Boolean = {
    if (checkBorrowedArrays && result.isInstanceOf[ArrayData]) {
      assert(result.isInstanceOf[ColumnarArray], "Native arrays must be compared without copying")
      borrowedArraysCompared += 1
    }
    super.checkResult(result, expected, dataType, nullable)
  }

  override def beforeAll(): Unit = {
    super.beforeAll()
    assertNoSparkContext()
    MockVeloxBackend.initialize()
  }

  override def afterAll(): Unit = {
    try {
      backend.onExecutorShutdown()
      assertNoSparkContext()
    } finally {
      super.afterAll()
    }
  }

  private def assertNoSparkContext(): Unit = {
    assert(SparkContext.getActive.isEmpty)
    assert(SparkEnv.get == null)
    assert(SparkSession.getActiveSession.isEmpty)
    assert(SparkSession.getDefaultSession.isEmpty)
  }

  private def withInput(values: Seq[String])(f: ColumnarBatch => Unit): Unit = {
    TaskResources.runUnsafe {
      val column = new VeloxWritableColumnVector(values.size, StringType)
      val input =
        try {
          values.zipWithIndex.foreach {
            case (null, i) => column.putNull(i)
            case (value, i) => column.putByteArray(i, value.getBytes(UTF_8))
          }
          column.finishStringColumn()
          val runtime = Runtimes.contextInstance(
            BackendsApiManager.getBackendName(backendClass),
            "NativeExpressionInput")
          val names = ConverterUtils.collectAttributeNamesWithExprId(attributes)
          val handle = VeloxOutputBatchJniWrapper
            .create(runtime)
            .makeVeloxBatch(Array(column.ownerHandle()), names.toArray(new Array[String](0)))
          var batch: ColumnarBatch = null
          try {
            batch = ColumnarBatches.create(handle)
            batch
          } finally {
            if (batch == null) {
              ColumnarBatchJniWrapper.close(handle)
            }
          }
        } finally {
          column.close()
        }
      try {
        f(input)
        assertNoSparkContext()
      } finally {
        input.close()
      }
    }
  }

  private def sparkResults(expression: Expression, values: Seq[String]): Seq[Any] = {
    withSQLConf(SQLConf.CODEGEN_FACTORY_MODE.key -> "CODEGEN_ONLY") {
      val projection = MutableProjection.create(Seq(expression))
      projection.initialize(0)
      values.map {
        value =>
          val result = projection(InternalRow(UTF8String.fromString(value)))
          if (result.isNullAt(0)) {
            null
          } else {
            result.copy().get(0, expression.dataType)
          }
      }
    }
  }

  private def readStrings(batch: ColumnarBatch): Seq[String] = {
    val view = VeloxInputBatch.wrap(
      ColumnarBatches.getNativeHandle(BackendsApiManager.getBackendName(backendClass), batch),
      new StructType().add("result", StringType),
      batch.numRows())
    try {
      (0 until batch.numRows()).map {
        i =>
          val value = view.getRow(i).getUTF8String(0)
          if (value == null) null else value.toString
      }
    } finally {
      view.close()
    }
  }

  test("standalone native initialization can be reused without Spark services") {
    MockVeloxBackend.initialize()
    assertNoSparkContext()
  }

  test("literal expressions support zero-column input batches") {
    TaskResources.runUnsafe {
      Seq(0, 3).foreach {
        size =>
          val input = new ColumnarBatch(Array.empty[ColumnVector], size)
          try {
            checkEvaluationWithNative(Literal(7), Seq.fill(size)(7), input, Seq.empty)
          } finally {
            input.close()
          }
      }
    }
  }

  test("borrowed native arrays maps and structs compare nested NULLs before reading values") {
    val nullInt = Literal.create(null, IntegerType)
    val array = CreateArray(Seq(nullInt, nullInt))
    val nested = CreateNamedStruct(
      Seq(
        Literal("array"),
        array,
        Literal("map"),
        CreateMap(Seq(Literal("key"), array)),
        Literal("struct"),
        CreateNamedStruct(Seq(Literal("value"), nullInt)),
        Literal("missing"),
        Literal.create(null, ArrayType(IntegerType))
      ))
    val expected =
      create_row(Seq(null, null), create_map("key" -> Seq(null, null)), create_row(null), null)
    checkBorrowedArrays = true
    borrowedArraysCompared = 0
    try {
      TaskResources.runUnsafe {
        val input = new ColumnarBatch(Array.empty[ColumnVector], 1)
        try {
          checkEvaluationWithNative(nested, Seq(expected), input, Seq.empty)
          assert(borrowedArraysCompared > 0)
          intercept[TestFailedException] {
            checkEvaluationWithNative(array, Seq(Seq(0, null)), input, Seq.empty)
          }
          checkEvaluationWithNative(array, Seq(Seq(null, null)), input, Seq.empty)
        } finally {
          input.close()
        }
      }
    } finally {
      checkBorrowedArrays = false
    }
  }

  test("batch assembly rejects a live batch handle used as a column owner") {
    withInput(Seq("value ")) {
      input =>
        val backendName = BackendsApiManager.getBackendName(backendClass)
        val runtime = Runtimes.contextInstance(backendName, "NativeExpressionInvalidOwner")
        val wrapper = VeloxOutputBatchJniWrapper.create(runtime)
        intercept[GlutenException] {
          wrapper.makeVeloxBatch(
            Array(ColumnarBatches.getNativeHandle(backendName, input)),
            Array("input"))
        }
        checkEvaluationWithNative(StringTrimRight(bound), Seq("value"), input, attributes)
    }
  }

  test("rtrim evaluates a columnar batch without SparkContext or SparkSession") {
    // scalastyle:off nonascii
    val values = Seq(
      null,
      "",
      " ",
      "   ",
      "no-space",
      "  leading",
      "tail ",
      "tail   ",
      "\u4e16\u754c ",
      "tab\t",
      "line\n",
      "nbsp\u00a0",
      "a\u0000b ",
      "x" * 10,
      "x" * 12,
      "x" * 13 + " ",
      "x" * 64 + "  ",
      "x" * 256 + " "
    )
    // scalastyle:on nonascii
    val expression = StringTrimRight(bound)
    val expected = sparkResults(expression, values)
    withInput(values) {
      input => checkEvaluationWithNative(expression, expected, input, attributes)
    }
  }

  test("rtrim covers empty, 4096, 10240 and larger columnar batches") {
    withSQLConf(GlutenConfig.COLUMNAR_MAX_BATCH_SIZE.key -> "10240") {
      Seq(0, 1, 4096, 10240, 20481).foreach {
        size =>
          val values = (0 until size).map {
            i =>
              if (i % 17 == 0) null
              else if (i % 3 == 0) s"value-$i   "
              else s"value-$i"
          }
          val expression = StringTrimRight(bound)
          val expected = sparkResults(expression, values)
          withInput(values) {
            input => checkEvaluationWithNative(expression, expected, input, attributes)
          }
      }
    }
  }

  test("rtrim covers inline boundaries and sparse, clustered and dense trimming") {
    val size = 10240
    val patterns = Seq(
      Set.empty[Int],
      Set(0),
      Set(size / 2),
      Set(size - 1),
      (0 until size by 100).toSet,
      (0 until size / 100).toSet,
      (0 until size by 2).toSet,
      (0 until size / 2).toSet,
      (0 until size).toSet
    )
    Seq(10, 12, 13, 64, 256).foreach {
      length =>
        patterns.zipWithIndex.foreach {
          case (trimRows, pattern) =>
            val values = (0 until size).map {
              i => if (trimRows.contains(i)) "x" * (length - 2) + "  " else "x" * length
            }
            val expression = StringTrimRight(bound)
            val expected = sparkResults(expression, values)
            withClue(s"length=$length, pattern=$pattern: ") {
              withInput(values) {
                input => checkEvaluationWithNative(expression, expected, input, attributes)
              }
            }
        }
    }
  }

  test("native evaluation borrows input without changing its values") {
    val values = Seq("unchanged", "x" * 64, "trailing ", null)
    withInput(values) {
      input =>
        (0 until 3).foreach {
          _ =>
            checkEvaluationWithNative(bound, values, input, attributes)
            checkEvaluationWithNative(
              StringTrimRight(bound),
              Seq("unchanged", "x" * 64, "trailing", null),
              input,
              attributes)
        }
        checkEvaluationWithNative(bound, values, input, attributes)
    }
  }

  test("native evaluation supports attributes, nested expressions and custom trim") {
    // scalastyle:off nonascii
    val values = Seq("abcxy", "xy", "\u4e16\u754cxy", null)
    // scalastyle:on nonascii
    withInput(values) {
      input =>
        checkEvaluationWithNative(
          Length(StringTrimRight(attributes.head, Some(Literal("xy")))),
          Seq(3, 0, 2, null),
          input,
          attributes)
    }
  }

  test("a compiled expression is reusable and outputs survive evaluator close") {
    val values = Seq("x" * 64, "unchanged")
    TaskResources.runUnsafe {
      val prepared = prepareNativeExpression(Seq(StringTrimRight(bound)), attributes)
      try {
        Seq(1, 3).foreach {
          copies =>
            withInput(Seq.fill(copies)(values).flatten) {
              input =>
                val batch = prepared.evaluate(input)
                try {
                  assert(batch.numRows() == input.numRows())
                  assert(readStrings(batch) == Seq.fill(copies)(values).flatten)
                } finally {
                  batch.close()
                }
            }
        }
        withInput(values) {
          input =>
            val batch = prepared.evaluate(input)
            try {
              prepared.close()
              prepared.close()
              assert(batch.numRows() == values.size)
              assert(readStrings(batch) == values)
              intercept[IllegalArgumentException] {
                prepared.evaluate(input)
              }
            } finally {
              batch.close()
            }
        }
      } finally {
        prepared.close()
      }
    }
  }

  test("incorrect expectations fail and leave the input reusable") {
    withInput(Seq("value ")) {
      input =>
        intercept[TestFailedException] {
          checkEvaluationWithNative(StringTrimRight(bound), Seq("wrong"), input, attributes)
        }
        intercept[IllegalArgumentException] {
          checkEvaluationWithNative(StringTrimRight(bound), Seq.empty, input, attributes)
        }
        checkEvaluationWithNative(StringTrimRight(bound), Seq("value"), input, attributes)
    }
  }

  test("constant expressions preserve earlier outputs across different batch sizes") {
    TaskResources.runUnsafe {
      val prepared = prepareNativeExpression(Seq(StringTrimRight(Literal("constant "))), attributes)
      var first: ColumnarBatch = null
      var second: ColumnarBatch = null
      try {
        withInput(Seq.fill(3)("ignored"))(input => first = prepared.evaluate(input))
        withInput(Seq("ignored"))(input => second = prepared.evaluate(input))
        prepared.close()
        assert(readStrings(first) == Seq.fill(3)("constant"))
        assert(readStrings(second) == Seq("constant"))
      } finally {
        if (second != null) second.close()
        if (first != null) first.close()
        prepared.close()
      }
    }
  }

  test("an unchanged result retains long string buffers after input release") {
    val values = Seq("x" * 64, "y" * 256)
    TaskResources.runUnsafe {
      var result: ColumnarBatch = null
      try {
        withInput(values) {
          input => result = evaluateWithNative(StringTrimRight(bound), input, attributes)
        }
        assert(readStrings(result) == values)
      } finally {
        if (result != null) {
          result.close()
        }
      }
    }
  }

  test("native preparation requires exactly one output expression") {
    Seq(Seq.empty[Expression], Seq(Literal(1), Literal(2))).foreach {
      expressions =>
        val error = intercept[IllegalArgumentException] {
          prepareNativeExpression(expressions, attributes)
        }
        assert(error.getMessage.contains("exactly one output expression"))
    }
  }

  test("invalid input bindings and missing resource scope fail explicitly") {
    val empty = new ColumnarBatch(Array.empty[ColumnVector], 1)
    intercept[IllegalArgumentException] {
      evaluateWithNative(Literal(1), empty, Seq.empty)
    }
    withInput(Seq("value")) {
      input =>
        intercept[IllegalArgumentException] {
          evaluateWithNative(bound, input, Seq.empty)
        }
        intercept[IllegalArgumentException] {
          evaluateWithNative(BoundReference(1, StringType, nullable = true), input, attributes)
        }
        intercept[IllegalArgumentException] {
          evaluateWithNative(BoundReference(0, IntegerType, nullable = true), input, attributes)
        }
        intercept[GlutenException] {
          evaluateWithNative(
            BoundReference(0, IntegerType, nullable = true),
            input,
            Seq(AttributeReference("input", IntegerType, nullable = true)()))
        }
    }
  }

  // scalastyle:off nonascii
  test("JNI length consumption observes actual results and borrows live output batches") {
    val values = Seq("value", null, "", "   ", "tail  ", "中文 ")
    val expected = Seq("value", null, "", "", "tail", "中文")
    withInput(values) {
      input =>
        val prepared = prepareNativeExpression(Seq(StringTrimRight(bound)), attributes)
        val output = prepared.evaluate(input)
        try {
          assert(prepared.consumeStringLengths(output) == 14L)
          assert(prepared.consumeStringLengths(output) == 14L)
          assert(readStrings(output) == expected)
          prepared.close()
          assert(prepared.consumeStringLengths(output) == 14L)
          assert(readStrings(output) == expected)
          assert(readStrings(input) == values)
        } finally {
          output.close()
          prepared.close()
        }
    }
  }

  test("JNI length consumption covers constant NULL empty and zero-row results") {
    TaskResources.runUnsafe {
      Seq((Literal("中 "), 4L), (Literal(null, StringType), -1L), (Literal(""), 0L)).foreach {
        case (expression, length) =>
          val prepared = prepareNativeExpression(Seq(expression), attributes)
          try {
            Seq(0, 1, 5).foreach {
              size =>
                withInput(Seq.fill(size)("ignored")) {
                  input =>
                    val output = prepared.evaluate(input)
                    try {
                      assert(prepared.consumeStringLengths(output) == size * length)
                      assert(readStrings(output).size == size)
                    } finally {
                      output.close()
                    }
                }
            }
          } finally {
            prepared.close()
          }
      }
    }
  }

  test("JNI length consumption rejects non-string result without invalidating its owner") {
    withInput(Seq("value")) {
      input =>
        val prepared = prepareNativeExpression(Seq(Literal(7)), attributes)
        val output = prepared.evaluate(input)
        try {
          intercept[GlutenException] {
            prepared.consumeStringLengths(output)
          }
          assert(output.numRows() == 1 && output.numCols() == 1)
          checkEvaluationWithNative(StringTrimRight(bound), Seq("value"), input, attributes)
        } finally {
          output.close()
          prepared.close()
        }
    }
  }

  // scalastyle:on nonascii

  test("unsupported expressions never fall back to JVM evaluation") {
    case class JvmOnlyExpression() extends LeafExpression with CodegenFallback {
      override def nullable: Boolean = false
      override def dataType: DataType = StringType
      override def eval(input: InternalRow): Any = fail("Unexpected JVM fallback")
    }
    withInput(Seq("value")) {
      input =>
        intercept[GlutenNotSupportException] {
          evaluateWithNative(JvmOnlyExpression(), input, attributes)
        }
    }
  }

  test("huge-vector configuration rejects native writers at any nonnegative threshold") {
    val key = "spark.sql.inMemoryColumnarStorage.hugeVectorThreshold"
    Seq("0", "1", "4096").foreach {
      threshold =>
        withSQLConf(key -> threshold) {
          TaskResources.runUnsafe {
            Seq(IntegerType, StringType, ArrayType(StringType)).foreach {
              dataType =>
                val error = intercept[UnsupportedOperationException] {
                  val column = new VeloxWritableColumnVector(4, dataType)
                  try {
                    assert(column.ownerHandle() != 0L)
                  } finally {
                    column.close()
                  }
                }
                assert(error.getMessage.contains("hugeVector"))
            }
          }
        }
    }
  }

  test("disabled huge-vector configuration preserves native writer allocation and close") {
    withSQLConf(
      "spark.sql.inMemoryColumnarStorage.hugeVectorThreshold" -> "-1",
      "spark.sql.inMemoryColumnarStorage.hugeVectorReserveRatio" -> "3.0") {
      TaskResources.runUnsafe {
        val column = new VeloxWritableColumnVector(4, IntegerType)
        try {
          assert(column.ownerHandle() != 0L)
          column.putInt(0, 37)
          assert(column.getInt(0) == 37)
        } finally {
          column.close()
          column.close()
        }
      }
    }
  }

}
