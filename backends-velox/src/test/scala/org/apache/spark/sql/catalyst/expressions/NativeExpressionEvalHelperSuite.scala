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

import org.apache.gluten.execution.VeloxWholeStageTransformerSuite
import org.apache.gluten.utils.Arm

import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.vectorized.{ColumnarBatch, ColumnVector}
import org.apache.spark.task.TaskResources

class NativeExpressionEvalHelperSuite
  extends VeloxWholeStageTransformerSuite
  with NativeExpressionEvalHelper {
  override protected val resourcePath: String = "N/A"
  override protected val fileFormat: String = "N/A"

  test("invalid inputs and incorrect native results remain failures") {
    TaskResources.runUnsafe {
      intercept[IllegalArgumentException] {
        prepareNativeExpression(Seq(BoundReference(0, StringType, nullable = true)), Seq.empty)
      }
      Arm.withResource(new ColumnarBatch(Array.empty[ColumnVector], 1)) {
        input =>
          intercept[org.scalatest.exceptions.TestFailedException] {
            checkEvaluationWithNative(Literal("actual"), Seq("wrong"), input, Seq.empty)
          }
      }
    }
  }

  test("closed native preparation rejects evaluation and close is idempotent") {
    TaskResources.runUnsafe {
      Arm.withResource(new ColumnarBatch(Array.empty[ColumnVector], 1)) {
        input =>
          val prepared = prepareNativeExpression(Seq(Literal("value")), Seq.empty)
          prepared.close()
          prepared.close()
          val error = intercept[IllegalArgumentException](prepared.evaluate(input))
          assert(error.getMessage.contains("closed"))
      }
    }
  }

  test("repeated evaluation owns outputs and preserves borrowed null and empty inputs") {
    TaskResources.runUnsafe {
      Arm.withResource(new ColumnarBatch(Array.empty[ColumnVector], 2)) {
        constantInput =>
          val attribute = AttributeReference("value", StringType, nullable = true)()
          Arm.withResource(prepareNativeExpression(Seq(attribute), Seq(attribute))) {
            identity =>
              Seq("value", null, "", "next", null).foreach {
                expected =>
                  val output = Arm.withResource(evaluateWithNative(
                    Literal.create(expected, StringType),
                    constantInput,
                    Seq.empty)) {
                    input =>
                      val first = identity.evaluate(input)
                      first.close()
                      withReadableBatch(input) {
                        readable =>
                          val value = readable.getRow(0)
                          assert(if (expected == null) value.isNullAt(0)
                          else value.getUTF8String(0).toString == expected)
                      }
                      identity.evaluate(input)
                  }
                  Arm.withResource(output) {
                    result =>
                      withReadableBatch(result) {
                        readable =>
                          assert(readable.numRows() == 2)
                          (0 until 2).foreach {
                            i =>
                              val row = readable.getRow(i)
                              assert(if (expected == null) row.isNullAt(0)
                              else row.getUTF8String(0).toString == expected)
                          }
                      }
                  }
              }
          }
      }
    }
  }
}
