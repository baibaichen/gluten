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
package org.apache.gluten.vectorized;

import org.apache.gluten.runtime.Runtime;
import org.apache.gluten.runtime.RuntimeAware;

/**
 * Evaluates one compiled Substrait scalar expression on native columnar batches.
 *
 * <p>Compiled evaluators are reusable but not thread-safe. The runtime must outlive the evaluator,
 * its output batches. Inputs may belong to another runtime in the same TaskResources scope, whose
 * memory managers must remain alive. Closing an evaluator does not close any batch.
 */
public final class VeloxExpressionEvaluatorJniWrapper implements RuntimeAware {
  private final Runtime runtime;

  private VeloxExpressionEvaluatorJniWrapper(Runtime runtime) {
    this.runtime = runtime;
  }

  public static VeloxExpressionEvaluatorJniWrapper create(Runtime runtime) {
    return new VeloxExpressionEvaluatorJniWrapper(runtime);
  }

  @Override
  public long rtHandle() {
    return runtime.getHandle();
  }

  /** Compiles an ExtendedExpression containing exactly one scalar expression. */
  public native long compile(byte[] extendedExpression);

  /**
   * Returns a caller-owned, single-column native batch with the input row count. The evaluator must
   * belong to this runtime; the input may belong to another runtime in the same task scope. No JVM
   * fallback or query pipeline is used.
   */
  public native long evaluate(long evaluatorHandle, long inputBatchHandle);

  /** Borrows a live result batch and sums byte lengths, with -1 for each null result. */
  public native long consumeStringLengths(long resultBatchHandle);

  /** Releases the compiled evaluator, without invalidating previously returned batches. */
  public native void close(long evaluatorHandle);
}
