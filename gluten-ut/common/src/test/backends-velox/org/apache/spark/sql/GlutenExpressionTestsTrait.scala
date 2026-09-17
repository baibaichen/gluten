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

import org.apache.spark.sql.catalyst.{CatalystTypeConverters, InternalRow}
import org.apache.spark.sql.catalyst.expressions.{EmptyRow, Expression, NativeExpressionRowEvalHelper}
import org.apache.spark.sql.types.DataType

/** Scalar checks select native or Spark evaluation; explicit query checks keep their own path. */
trait GlutenExpressionTestsTrait extends GlutenTestsTrait with NativeExpressionRowEvalHelper {
  implicit override protected val backendClass: Class[_ <: SubstraitBackend] =
    classOf[VeloxBackend]

  override protected def defaultOffloadGluten: Boolean = false

  override protected def checkEvaluation(
      expression: => Expression,
      expected: Any,
      inputRow: InternalRow = EmptyRow): Unit = {
    val expr = prepareQueryExpression(expression)
    val offloaded = checkEvaluationWithNativeRowIfSupported(expr, expected, inputRow)
    recordExpressionEvaluation(offloaded)
    if (!offloaded) {
      checkEvaluationWithoutCodegen(
        replace(expr),
        CatalystTypeConverters.convertToCatalyst(expected),
        inputRow)
    }
  }

  override protected def checkResult(
      result: Any,
      expected: Any,
      dataType: DataType,
      nullable: Boolean): Boolean = {
    super[GlutenTestsTrait].checkResult(result, expected, dataType, nullable)
  }
}
