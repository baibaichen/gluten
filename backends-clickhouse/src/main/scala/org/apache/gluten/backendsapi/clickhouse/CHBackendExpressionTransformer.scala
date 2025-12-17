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
package org.apache.gluten.backendsapi.clickhouse

import org.apache.gluten.expression._

/**
 * ClickHouse backend-specific expression transformer.
 *
 * This PartialFunction provides a unified way to handle expression transformations specific to
 * ClickHouse. It is invoked just before falling back to GenericExpressionTransformer.
 *
 * Note: The existing ExpressionExtensionTrait mechanism in CH backend works through
 * extraExpressionConverter. New expressions specific to CH can be added here.
 *
 * Migration strategy:
 *   1. New backend-specific expressions should be added here
 *   2. Existing genXXXTransformer methods can be gradually migrated here
 *   3. ExpressionExtensionTrait is kept for backward compatibility with user extensions
 */
object CHBackendExpressionTransformer {

  val transform: BackendExpressionTransformer.TransformFunction = {
    // Example: DateDiff migration for CH (uncomment to migrate from genDateDiffTransformer)
    // case ExpressionTransformContext(d: DateDiff, substraitExprName, attributeSeq) =>
    //   GenericExpressionTransformer(
    //     substraitExprName,
    //     Seq(
    //       LiteralTransformer("day"),
    //       ExpressionConverter.replaceWithExpressionTransformer(d.startDate, attributeSeq),
    //       ExpressionConverter.replaceWithExpressionTransformer(d.endDate, attributeSeq)),
    //     d)

    // Empty for now - add cases as expressions are migrated
    PartialFunction.empty
  }
}

