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
package org.apache.gluten.expression

import org.apache.spark.sql.catalyst.expressions.{Attribute, Expression}

/**
 * Context object passed to backend expression transformer. Contains all the information needed to
 * transform an expression.
 *
 * @param expr
 *   The original Spark expression to transform
 * @param substraitExprName
 *   The substrait function name mapped from the expression
 * @param attributeSeq
 *   The sequence of attributes for binding references
 */
case class ExpressionTransformContext(
    expr: Expression,
    substraitExprName: String,
    attributeSeq: Seq[Attribute]) {

  /**
   * Helper method to transform a child expression using the same attribute sequence.
   *
   * @param childExpr
   *   The child expression to transform
   * @return
   *   The transformed expression
   */
  def transformChild(childExpr: Expression): ExpressionTransformer = {
    ExpressionConverter.replaceWithExpressionTransformer(childExpr, attributeSeq)
  }

  /**
   * Helper method to transform multiple child expressions.
   *
   * @param children
   *   The child expressions to transform
   * @return
   *   The transformed expressions
   */
  def transformChildren(children: Seq[Expression]): Seq[ExpressionTransformer] = {
    children.map(transformChild)
  }
}

/**
 * Type alias for backend expression transformer. Uses PartialFunction to leverage Scala's pattern
 * matching - if a pattern doesn't match, it means the backend doesn't handle that expression.
 *
 * This design addresses two orthogonal concerns:
 *   1. Spark version changes (e.g., Spark 4.1 renames TimeAdd to TimestampAddInterval) - handled by
 *      SparkShims.xxxExpressionMappings
 *   1. Backend-specific handling for certain expressions - handled by this PartialFunction
 *
 * The transformer is only invoked before falling back to GenericExpressionTransformer, after all
 * other specific transformations in ExpressionConverter.transformExpression have been tried.
 *
 * Benefits:
 *   - Idiomatic Scala: PartialFunction combines `isDefinedAt` and `apply` naturally
 *   - No boilerplate: no need to maintain a separate Set of supported expressions
 *   - Composable: PartialFunctions can be combined with `orElse`
 *   - Type-safe pattern matching with exhaustiveness checking
 */
object BackendExpressionTransformer {

  /** Type alias for the backend expression transformer partial function. */
  type TransformFunction = PartialFunction[ExpressionTransformContext, ExpressionTransformer]

  /** Empty transformer that handles no expressions. */
  val empty: TransformFunction = PartialFunction.empty
}
