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

import org.apache.spark.sql.shim.GlutenExpressionOffloadTracker

class GlutenArithmeticExpressionSuite
  extends ArithmeticExpressionSuite
  with GlutenExpressionOffloadTracker {
  override protected def panoramaMeta(expression: Expression): String = expression match {
    case _: Add => "operator=Add"
    case _: Subtract => "operator=Subtract"
    case _: Multiply => "operator=Multiply"
    case _: Divide => "operator=Divide"
    case _: IntegralDivide => "operator=IntegralDivide"
    case _: Remainder => "operator=Remainder"
    case _: Pmod => "operator=Pmod"
    case _: Abs => "operator=Abs"
    case _: UnaryMinus => "operator=UnaryMinus"
    case _ => ""
  }
}
