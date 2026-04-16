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
package org.apache.gluten.test

import org.apache.gluten.execution.VeloxWholeStageTransformerSuite

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

class VeloxExpressionEvaluatorSuite extends VeloxWholeStageTransformerSuite {

  override protected val resourcePath: String = "N/A"
  override protected val fileFormat: String = "N/A"

  private def strRow(s: String): InternalRow =
    InternalRow(UTF8String.fromString(s))

  private def intRow(i: Int): InternalRow = InternalRow(i)

  test("evaluate Cast string '123' to int") {
    val expr = Cast(
      BoundReference(0, StringType, nullable = true),
      IntegerType)
    val result = VeloxExpressionEvaluator.evaluate(expr, strRow("123"), spark)
    assert(result == 123)
  }

  test("evaluate Add(1, 2)") {
    val expr = Add(
      BoundReference(0, IntegerType, nullable = false),
      Literal(2))
    val result = VeloxExpressionEvaluator.evaluate(expr, intRow(1), spark)
    assert(result == 3)
  }

  test("evaluate Cast string 'abc' to int — ANSI should throw") {
    val expr = Cast(
      BoundReference(0, StringType, nullable = true),
      IntegerType,
      evalMode = EvalMode.ANSI)
    intercept[Exception] {
      VeloxExpressionEvaluator.evaluate(expr, strRow("abc"), spark)
    }
  }

  test("evaluate Cast string 'abc' to int — TRY should return null") {
    val expr = Cast(
      BoundReference(0, StringType, nullable = true),
      IntegerType,
      evalMode = EvalMode.TRY)
    val result = VeloxExpressionEvaluator.evaluate(expr, strRow("abc"), spark)
    assert(result == null)
  }
}
