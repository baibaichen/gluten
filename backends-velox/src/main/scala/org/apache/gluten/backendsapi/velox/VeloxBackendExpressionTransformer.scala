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
package org.apache.gluten.backendsapi.velox

import org.apache.gluten.expression._

/**
 * Velox backend-specific expression transformer.
 *
 * This PartialFunction provides a unified way to handle expression transformations specific to
 * Velox. It is invoked just before falling back to GenericExpressionTransformer, after all other
 * specific transformations in ExpressionConverter.transformExpression have been tried.
 *
 * Design rationale:
 *   - Two orthogonal concerns are addressed:
 *     1. Spark version changes (e.g., Spark 4.1 renames TimeAdd to TimestampAddInterval)
 *        - Handled by SparkShims.xxxExpressionMappings 2. Backend-specific handling for certain
 *          expressions (e.g., NaNvl, DateDiff)
 *        - Handled by this PartialFunction
 *
 * Benefits:
 *   - Idiomatic Scala: PartialFunction combines isDefinedAt and apply naturally
 *   - No boilerplate: no need to maintain a separate Set of supported expressions
 *   - Composable: PartialFunctions can be combined with `orElse`
 *
 * Migration strategy:
 *   1. New backend-specific expressions should be added here 2. Existing genXXXTransformer methods
 *      can be gradually migrated here 3. The legacy extraExpressionConverter and genXXXTransformer
 *      are kept for backward compatibility
 *
 * Example migration of genNaNvlTransformer:
 *
 * Before (in VeloxSparkPlanExecApi):
 * {{{
 *   override def genNaNvlTransformer(...): ExpressionTransformer = { ... }
 * }}}
 *
 * After (add case here):
 * {{{
 *   case ExpressionTransformContext(n: NaNvl, substraitExprName, _, _) =>
 *     // transformation logic
 * }}}
 */
object VeloxBackendExpressionTransformer {

  val transform: BackendExpressionTransformer.TransformFunction = {
    // Example: NaNvl migration (uncomment to migrate from genNaNvlTransformer)
    // case ExpressionTransformContext(n: NaNvl, substraitExprName, attributeSeq) =>
    //   val left = ExpressionConverter.replaceWithExpressionTransformer(n.left, attributeSeq)
    //   val right = ExpressionConverter.replaceWithExpressionTransformer(n.right, attributeSeq)
    //   val condExpr = IsNaN(n.left)
    //   val condFuncName = ExpressionMappings.expressionsMap(classOf[IsNaN])
    //   val newExpr = If(condExpr, n.right, n.left)
    //   IfTransformer(
    //     substraitExprName,
    //     GenericExpressionTransformer(condFuncName, Seq(left), condExpr),
    //     right,
    //     left,
    //     newExpr)

    // Example: DateDiff migration
    // case ExpressionTransformContext(d: DateDiff, substraitExprName, attributeSeq) =>
    //   GenericExpressionTransformer(
    //     substraitExprName,
    //     Seq(
    //       ExpressionConverter.replaceWithExpressionTransformer(d.endDate, attributeSeq),
    //       ExpressionConverter.replaceWithExpressionTransformer(d.startDate, attributeSeq)),
    //     d)

    // Empty for now - add cases as expressions are migrated
    PartialFunction.empty
  }
}
