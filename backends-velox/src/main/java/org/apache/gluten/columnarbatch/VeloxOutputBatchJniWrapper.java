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
package org.apache.gluten.columnarbatch;

import org.apache.gluten.runtime.Runtime;
import org.apache.gluten.runtime.RuntimeAware;

/**
 * JNI wrapper for the write-side Velox batch assembly path.
 *
 * <p>Assembles N owned output column vectors (created by {@code
 * VeloxColumnHandleJniWrapper#allocateNestedOutput}) into a native Velox RowVector, wraps it in a
 * VeloxColumnarBatch, persists it in the runtime's ObjectStore via {@code ctx->saveObject}, and
 * returns the resulting batch handle. The handle is compatible with {@code ColumnarBatches.create}.
 *
 * <p>This wrapper is {@link RuntimeAware} so that the JNI method receives the runtime handle (via
 * {@link #rtHandle()}), allowing the native side to call {@code getRuntime(env, wrapper)} and store
 * the assembled batch in the correct runtime ObjectStore.
 *
 * <p>Usage:
 *
 * <pre>{@code
 * VeloxOutputBatchJniWrapper wrapper = VeloxOutputBatchJniWrapper.create(runtime);
 * long batchHandle = wrapper.makeVeloxBatch(ownerHandles, columnNames);
 * ColumnarBatch batch = ColumnarBatches.create(batchHandle);
 * }</pre>
 */
public class VeloxOutputBatchJniWrapper implements RuntimeAware {

  private final Runtime runtime;

  private VeloxOutputBatchJniWrapper(Runtime runtime) {
    this.runtime = runtime;
  }

  /**
   * Create a new wrapper bound to the given runtime.
   *
   * @param runtime The active Gluten runtime; must remain alive for the lifetime of this wrapper.
   * @return A new {@code VeloxOutputBatchJniWrapper} instance.
   */
  public static VeloxOutputBatchJniWrapper create(Runtime runtime) {
    return new VeloxOutputBatchJniWrapper(runtime);
  }

  /** {@inheritDoc} */
  @Override
  public long rtHandle() {
    return runtime.getHandle();
  }

  /**
   * Assemble output column vectors into a runtime-owned Velox ColumnarBatch.
   *
   * <p>Each entry in {@code ownerHandles} is an ObjectStore handle returned by {@code
   * VeloxColumnHandleJniWrapper#allocateNestedOutput}. The native implementation retrieves each
   * VectorPtr, assembles them into a RowVector keyed by {@code names}, wraps the result in a
   * VeloxColumnarBatch, and saves it in the runtime's ObjectStore via {@code ctx->saveObject}.
   *
   * <p>Ownership: this method does NOT release the ownerHandles. The caller is responsible for
   * releasing each column vector via {@code VeloxColumnHandleJniWrapper#releaseOutput} after the
   * batch handle is no longer needed (the RowVector holds its own shared_ptr references to the
   * child vectors, so releasing the ownerHandles does not free the data prematurely).
   *
   * @param ownerHandles ObjectStore handles for the assembled column vectors, in column order.
   * @param names Column names corresponding to each handle, in order.
   * @return An ObjectStore handle for the assembled VeloxColumnarBatch, suitable for use with
   *     {@code ColumnarBatches.create(handle)}.
   * @throws RuntimeException If handle count and name count differ, any handle is not found, or
   *     column sizes are mismatched.
   */
  public native long makeVeloxBatch(long[] ownerHandles, String[] names);
}
