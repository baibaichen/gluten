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
package org.apache.gluten.velox.vector;

/**
 * JNI wrapper for the read-side zero-copy VeloxColumnHandle descriptor.
 *
 * <p>Provides the JNI boundary for exporting a Velox vector as a fixed-layout {@code
 * VeloxColumnHandle} descriptor that Java can read field-by-field via {@code sun.misc.Unsafe}
 * without copying vector data.
 *
 * <p><b>Read-side only.</b> This class covers the export (Velox -> descriptor) direction.
 * Write-side allocation and ObjectStore integration are handled separately in Task 11.
 *
 * <p><b>Ownership contract:</b>
 *
 * <ul>
 *   <li>{@link #exportDescriptor} allocates a descriptor tree and returns its root address. The
 *       caller is responsible for calling {@link #freeDescriptor} when done.
 *   <li>{@link #freeDescriptor} releases only the descriptor memory (the {@code VeloxColumnHandle}
 *       nodes and their pointer arrays). It does NOT touch the underlying Velox vector data; vector
 *       lifetime is managed by the owning {@code VectorPtr} on the native side.
 *   <li>{@link #allocateNestedOutput} allocates a writable descriptor tree and a Velox vector,
 *       returning the descriptor address and owner handle. The caller must release the vector via
 *       {@link #releaseOutput} and free the descriptor via {@link #freeDescriptor}.
 * </ul>
 */
public class VeloxColumnHandleJniWrapper {

  /**
   * Export a Velox vector as a read-side view descriptor.
   *
   * <p>The native implementation reinterpret-casts {@code veloxVectorPtr} to a {@code
   * facebook::velox::VectorPtr*} and delegates to {@code gluten::exportVector}.
   *
   * @param veloxVectorPtr Raw address of a {@code facebook::velox::VectorPtr} on the native heap.
   *     The referenced vector must remain alive for the full lifetime of the returned descriptor.
   * @return Address of the root {@code VeloxColumnHandle} descriptor. Caller must release it with
   *     {@link #freeDescriptor}. Failures propagate as exceptions rather than a sentinel value.
   */
  public static native long exportDescriptor(long veloxVectorPtr);

  /**
   * Free the descriptor tree produced by {@link #exportDescriptor}.
   *
   * <p>Recursively releases the {@code VeloxColumnHandle} nodes and their pointer arrays. Does NOT
   * release any underlying Velox buffer data.
   *
   * @param rootDescAddr Address returned by {@link #exportDescriptor} (0 is a safe no-op).
   */
  public static native void freeDescriptor(long rootDescAddr);

  /**
   * Release the writable Velox vector associated with an owner handle.
   *
   * <p>Drops the {@code ObjectStore} reference to the {@code VectorPtr}, freeing the Velox vector
   * and its backing memory. The caller must stop accessing the native buffers after this call.
   *
   * @param ownerHandle The second element of the array returned by {@link #allocateNestedOutput}.
   */
  public static native void releaseOutput(long ownerHandle);

  /**
   * Exports each column of a native Velox batch as a read-side descriptor address.
   *
   * @param batchHandle native ColumnarBatch object handle (from ColumnarBatches.getNativeHandle)
   * @return per-column descriptor addresses, in column order; each must be imported via {@link
   *     VeloxColumnVector#importFromNative} and released via that vector's close().
   */
  public static native long[] exportBatchColumnDescriptors(long batchHandle);

  /**
   * Allocate a new string-data chunk for a VARCHAR output column.
   *
   * <p>The native implementation calls {@code gluten::allocateStringChunk}, which allocates a Velox
   * Buffer attached to the {@code FlatVector<StringView>} identified by {@code ownerHandle}. Java
   * writes long (&gt;12-byte) string data directly into the chunk via {@code Unsafe} and calls this
   * method again when the chunk is full. All chunks are finalized by {@link #finalizeStringColumn}
   * once writing is complete.
   *
   * @param ownerHandle ObjectStore handle returned by {@link #allocateNestedOutput} for a VARCHAR
   *     column.
   * @param minBytes Minimum usable capacity (in bytes) requested for the new chunk.
   * @return {@code long[2]} where {@code [0]} is the raw address ({@code int64_t} cast of the
   *     mutable {@code char*} base) and {@code [1]} is the usable capacity in bytes.
   */
  public static native long[] allocateStringChunk(long ownerHandle, long minBytes);

  /**
   * Finalize a VARCHAR output column by trimming each string-data chunk to the bytes written.
   *
   * <p>Must be called once after all string values have been written (via {@link
   * #allocateStringChunk} + {@code Unsafe} writes) and before the column is assembled into a batch
   * via {@code makeVeloxBatch}. Velox's serializer uses {@code Buffer::size()} (not capacity) to
   * determine byte counts; without this call trailing unwritten bytes would be included in
   * serialization (e.g. shuffle).
   *
   * @param ownerHandle ObjectStore handle returned by {@link #allocateNestedOutput} for a VARCHAR
   *     column.
   * @param chunkUsedBytes One entry per chunk allocated via {@link #allocateStringChunk}, in
   *     registration order. Each value is the number of bytes written into that chunk.
   */
  public static native void finalizeStringColumn(long ownerHandle, long[] chunkUsedBytes);

  /**
   * childPath-aware variant of {@link #allocateStringChunk} for NESTED VARCHAR children.
   *
   * <p>Navigates from the ROOT vector (identified by {@code rootOwnerHandle}, obtained from {@link
   * #allocateNestedOutput}) down {@code childPath} (ARRAY->elements, MAP->keys/values,
   * ROW->childAt) to the target {@code FlatVector<StringView>} child, then allocates a new
   * string-data chunk attached to that child. An empty {@code childPath} addresses the root itself.
   *
   * @param rootOwnerHandle ObjectStore handle of the ROOT vector.
   * @param childPath steps from root to the target VARCHAR child vector.
   * @param minBytes minimum usable capacity (in bytes) requested for the new chunk.
   * @return {@code long[2]} where {@code [0]} is the raw base address and {@code [1]} is the usable
   *     capacity in bytes (see {@link #allocateStringChunk}).
   */
  public static native long[] allocateStringChunkAt(
      long rootOwnerHandle, int[] childPath, long minBytes);

  /**
   * childPath-aware variant of {@link #finalizeStringColumn} for NESTED VARCHAR children.
   *
   * <p>Navigates from the ROOT vector to the target {@code FlatVector<StringView>} child (as in
   * {@link #allocateStringChunkAt}) and trims each registered string-data chunk of that child to
   * the bytes actually written.
   *
   * @param rootOwnerHandle ObjectStore handle of the ROOT vector.
   * @param childPath steps from root to the target VARCHAR child vector.
   * @param chunkUsedBytes one entry per chunk allocated via {@link #allocateStringChunkAt}, in
   *     registration order.
   */
  public static native void finalizeStringColumnAt(
      long rootOwnerHandle, int[] childPath, long[] chunkUsedBytes);

  /**
   * Grow a nested child vector to at least {@code newCapacity} elements and return its refreshed
   * mutable buffer addresses so Java can re-fetch write pointers after potential reallocation.
   *
   * <p>Navigates from the root vector (identified by {@code ownerHandle}) along {@code childPath}
   * to the target child, calls {@code BaseVector::resize(newCapacity)}, and returns the child's
   * fresh buffer addresses. Velox resize is geometric and may reallocate backing buffers,
   * invalidating previously-held raw pointers; callers must discard old addresses and use these
   * refreshed ones.
   *
   * <p>childPath navigation:
   *
   * <ul>
   *   <li>At an ARRAY vector: index must be 0 -> {@code elements()}
   *   <li>At a MAP vector: index 0 -> {@code mapKeys()}, index 1 -> {@code mapValues()}
   *   <li>At a ROW vector: index i -> {@code childAt(i)}
   * </ul>
   *
   * <p>Returned {@code long[]} layout (mirrors {@code VeloxColumnHandle} descriptor buffer layout):
   *
   * <ul>
   *   <li>FLAT leaf: {@code [nullsAddr, valuesAddr]} -- 2 longs
   *   <li>ARRAY/MAP: {@code [nullsAddr, offsetsAddr, sizesAddr]} -- 3 longs
   *   <li>ROW: {@code [nullsAddr]} -- 1 long
   * </ul>
   *
   * <p>All returned addresses are non-null and point into pool-backed Velox buffers. The nulls
   * bitmap is initialized to all-valid (0xFF bytes) for the full new capacity after growth.
   *
   * @param ownerHandle ObjectStore handle returned by {@link #allocateNestedOutput}.
   * @param childPath Steps from root to the target child vector.
   * @param newCapacity Minimum new capacity for the target child.
   * @return Refreshed mutable buffer addresses for the child (layout described above).
   */
  public static native long[] growChild(long ownerHandle, int[] childPath, int newCapacity);

  /**
   * Allocates a writable Velox output vector for any supported flat or nested type.
   *
   * <p>All nulls bitmaps are initialized to all-valid. ARRAY/MAP offsets and sizes are
   * zero-initialized. Child capacity initially matches {@code rowCapacity}; use {@link #growChild}
   * for additional element slots.
   *
   * <p>The type is encoded as a DFS pre-order {@code int[]} array:
   *
   * <ul>
   *   <li>0=INTEGER, 1=BIGINT, 2=SMALLINT, 3=TINYINT, 4=REAL, 5=DOUBLE, 6=VARCHAR
   *   <li>7=ARRAY followed by element type encoding
   *   <li>8=MAP followed by key type encoding then value type encoding
   *   <li>9=ROW followed by nFields then nFields type encodings
   *   <li>10=DATE, 11=VARBINARY, 12=BOOLEAN, 13=TIMESTAMP
   *   <li>14=DECIMAL followed by precision then scale
   * </ul>
   *
   * @param typeEncoding DFS pre-order type code array.
   * @param rowCapacity Number of rows (and initial element slots for children).
   * @return {@code long[2]} where {@code [0]} is the root descriptor address and {@code [1]} is the
   *     owner handle. Free the descriptor via {@link #freeDescriptor} and release the vector via
   *     {@link #releaseOutput} when done.
   */
  public static native long[] allocateNestedOutput(int[] typeEncoding, int rowCapacity);
}
