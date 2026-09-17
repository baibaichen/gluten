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
import org.apache.gluten.backendsapi.velox.{VeloxBackend, VeloxListenerApi}
import org.apache.gluten.test.{MockVeloxBackend, TestStats}

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.types._

import org.scalatest.{Args, Status}

import java.util.concurrent.atomic.AtomicLong

/** Runs Spark's original expression checks, then a direct native scalar check. */
trait NativeExpressionTestsTrait extends GlutenTestsCommonTrait with NativeExpressionRowEvalHelper {

  implicit override protected val backendClass: Class[_ <: SubstraitBackend] =
    classOf[VeloxBackend]

  override protected def defaultOffloadGluten: Boolean = false

  private val nativeChecksSucceeded = new AtomicLong
  private val nativeChecksFailed = new AtomicLong

  private val nativeExpressionConf = Seq(
    "spark.gluten.experimental.enabled" -> "true",
    "spark.gluten.experimental.from_csv.enabled" -> "true",
    "spark.gluten.experimental.to_csv.enabled" -> "true",
    "spark.gluten.experimental.to_number.enabled" -> "true",
    "spark.gluten.experimental.to_char.enabled" -> "true",
    "spark.gluten.sql.timestampNtzAsTimestampUtc.enabled" -> "true",
    "spark.gluten.sql.enableRegexpInstrOffload" -> "true"
  )

  override def beforeAll(): Unit = {
    super.beforeAll()
    MockVeloxBackend.initialize()
  }

  override def afterAll(): Unit = {
    try {
      new VeloxListenerApi().onExecutorShutdown()
    } finally {
      super.afterAll()
    }
  }

  override protected def checkEvaluation(
      expression: => Expression,
      expected: Any,
      inputRow: InternalRow = EmptyRow): Unit = {
    checkEvaluationWithSpark(expression, expected, inputRow)
    var succeeded = false
    try {
      val nativeExpression = expression
      withSQLConf(nativeExpressionConf: _*) {
        checkEvaluationWithNativeRow(nativeExpression, expected, inputRow)
      }
      nativeChecksSucceeded.incrementAndGet()
      TestStats.offloadGluten = true
      succeeded = true
    } finally {
      if (!succeeded) {
        nativeChecksFailed.incrementAndGet()
      }
    }
  }

  override protected def checkEvaluationWithUnsafeProjection(
      expression: Expression,
      expected: Any,
      inputRow: InternalRow = EmptyRow): Unit = {
    // Spark builds its expected UnsafeRow with typed getters; rebox integer expectations
    // without changing their value. All other original checks still receive `expected`.
    val boxed: Any = (expected, expression.dataType) match {
      case (n: Int, ByteType) => n.toByte
      case (n: Int, ShortType) => n.toShort
      case (n: Int, LongType) => n.toLong
      case (n: Int, FloatType) => n.toFloat
      case (n: Int, DoubleType) => n.toDouble
      case _ => expected
    }
    expected match {
      case n: Int =>
        require(
          boxed.asInstanceOf[Number].doubleValue() == n.toDouble,
          s"Expected integer $n is not exactly representable as ${expression.dataType}")
      case _ =>
    }
    super.checkEvaluationWithUnsafeProjection(expression, boxed, inputRow)
  }

  override protected def checkResult(
      result: Any,
      expected: Any,
      dataType: DataType,
      nullable: Boolean): Boolean = {
    checkResultWithSpark(result, expected, dataType, nullable)
  }

  override def runTest(testName: String, args: Args): Status = {
    val succeeded = nativeChecksSucceeded.get()
    val failed = nativeChecksFailed.get()
    try {
      super.runTest(testName, args)
    } finally {
      val validated = nativeChecksSucceeded.get() - succeeded
      // scalastyle:off println
      println(
        s"[native-expression] $testName: " +
          s"validated=$validated, " +
          s"failed=${nativeChecksFailed.get() - failed}; " +
          (if (validated == 0) "not native coverage"
           else "validated native coverage"))
      // scalastyle:on println
    }
  }
}
