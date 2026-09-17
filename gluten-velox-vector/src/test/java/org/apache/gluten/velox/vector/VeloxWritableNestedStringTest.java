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

import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.unsafe.Platform;
import org.apache.spark.unsafe.types.UTF8String;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * JVM-only unit tests for the NESTED VARCHAR child write path (Task 5).
 *
 * <p>A nested VARCHAR leaf (e.g. the element of {@code ARRAY<String>}) is constructed with {@code
 * ownerHandle == 0} but a real {@code rootOwnerHandle} and a non-empty {@code childPath}. Before
 * Task 5, {@link VeloxWritableColumnVector#putByteArray} of a long (&gt;12-byte) string threw
 * {@link IllegalStateException} from {@code ensureChunk} because the chunk allocation only
 * understood the root-style {@code ownerHandle}. Task 5 makes chunk allocation/finalization
 * childPath-aware.
 *
 * <p>These tests exercise the Java-side routing without loading the native library by overriding
 * the {@link VeloxWritableColumnVector#allocateStringChunkNative}/{@link
 * VeloxWritableColumnVector#finalizeStringColumnNative} seams to supply a Java-allocated chunk. The
 * end-to-end native path (allocateNestedOutput → allocateStringChunkAt → finalizeStringColumnAt) is
 * covered by {@code VeloxNativeJvmUDFNestedStringSuite}.
 */
public class VeloxWritableNestedStringTest {

  /** A simulated, non-zero root ObjectStore handle (never dereferenced in JVM-only mode). */
  private static final long FAKE_ROOT_OWNER_HANDLE = 0x5A5AL;

  /**
   * A nested VARCHAR child backed by a Java-allocated string chunk. Overrides the native chunk
   * seams so the childPath-aware routing can be exercised without the native library.
   */
  private static final class JavaChunkVarcharChild extends VeloxWritableColumnVector {
    private final long chunkAddr;
    private final long chunkCap;
    int allocCalls = 0;
    long[] capturedFinalizeUsed = null;

    JavaChunkVarcharChild(
        long descAddr, int capacity, int[] childPath, long chunkAddr, long chunkCap) {
      super(descAddr, 0L, capacity, DataTypes.StringType, FAKE_ROOT_OWNER_HANDLE, childPath);
      this.chunkAddr = chunkAddr;
      this.chunkCap = chunkCap;
    }

    @Override
    protected long[] allocateStringChunkNative(int need) {
      allocCalls++;
      return new long[] {chunkAddr, chunkCap};
    }

    @Override
    protected void finalizeStringColumnNative(long[] used) {
      capturedFinalizeUsed = used;
    }
  }

  /**
   * Allocates a hand-crafted FLAT StringView descriptor (nulls + 16-byte/slot values buffer).
   *
   * @param capacity number of element slots
   * @return {@code long[4] = {desc, buffers, nulls, values}} — all must be freed by the caller
   */
  private static long[] allocStringViewDescriptor(int capacity) {
    int nullsBytes = Math.max(8, ((capacity + 7) / 8 + 7) & ~7);
    long nulls = Platform.allocateMemory(nullsBytes);
    for (int i = 0; i < nullsBytes; i++) {
      Platform.putByte(null, nulls + i, (byte) 0xFF); // all valid
    }
    long values = Platform.allocateMemory(Math.max(8, (long) capacity * 16));
    for (long i = 0; i < Math.max(8, (long) capacity * 16); i++) {
      Platform.putByte(null, values + i, (byte) 0);
    }
    long buffers = Platform.allocateMemory(16);
    Platform.putLong(null, buffers, nulls);
    Platform.putLong(null, buffers + 8, values);
    long desc = Platform.allocateMemory(48);
    Platform.putLong(null, desc, capacity); // length
    Platform.putLong(null, desc + 8, 0L); // nullCount
    Platform.putLong(null, desc + 16, 2L); // nBuffers
    Platform.putLong(null, desc + 24, 0L); // nChildren
    Platform.putLong(null, desc + 32, buffers); // buffers ptr
    Platform.putLong(null, desc + 40, 0L); // children ptr (none)
    return new long[] {desc, buffers, nulls, values};
  }

  private static void freeStringViewDescriptor(long[] ptrs) {
    Platform.freeMemory(ptrs[0]);
    Platform.freeMemory(ptrs[1]);
    Platform.freeMemory(ptrs[2]);
    Platform.freeMemory(ptrs[3]);
  }

  /**
   * A nested VARCHAR child (ownerHandle=0, rootOwnerHandle!=0, childPath=[0]) must NOT throw on an
   * out-of-line (&gt;12-byte) string: {@code ensureChunk} routes to the childPath-aware chunk
   * allocator. Both an inline (&le;12-byte) and a long string round-trip correctly, and {@code
   * finishStringColumn} routes to the childPath-aware finalizer with the used-byte count.
   */
  @Test
  public void nestedVarcharChildWritesInlineAndLongStrings() {
    final int capacity = 4;
    long[] ptrs = allocStringViewDescriptor(capacity);
    // Generous Java chunk so the write stays within a single chunk (no growth).
    final long chunkCap = 4096L;
    long chunk = Platform.allocateMemory(chunkCap);
    try {
      int[] childPath = new int[] {0}; // element of an ARRAY<String>
      JavaChunkVarcharChild child =
          new JavaChunkVarcharChild(ptrs[0], capacity, childPath, chunk, chunkCap);

      String inline = "hi"; // 2 bytes, inline
      String longStr = "this-string-is-definitely-longer-than-twelve-bytes"; // out-of-line
      byte[] inlineBytes = UTF8String.fromString(inline).getBytes();
      byte[] longBytes = UTF8String.fromString(longStr).getBytes();

      child.putByteArray(0, inlineBytes, 0, inlineBytes.length);
      // The following would throw IllegalStateException before Task 5 (nested child had no
      // way to allocate a string chunk); after Task 5 it routes to allocateStringChunkNative.
      child.putByteArray(1, longBytes, 0, longBytes.length);

      assertEquals(1, child.allocCalls, "long string must allocate exactly one childPath chunk");

      // Read back via the write-side StringView reader (inverse of putByteArray).
      assertEquals(inline, child.getUTF8String(0).toString(), "inline string round-trip");
      assertEquals(longStr, child.getUTF8String(1).toString(), "long string round-trip");

      child.finishStringColumn();
      assertNotNull(
          child.capturedFinalizeUsed, "finishStringColumn must route to childPath finalize");
      assertEquals(1, child.capturedFinalizeUsed.length, "exactly one chunk finalized");
      assertEquals(
          longBytes.length,
          child.capturedFinalizeUsed[0],
          "finalized used-byte count must equal the long string length");

      child.close(); // no-op: ownsNativeResources=false
    } finally {
      Platform.freeMemory(chunk);
      freeStringViewDescriptor(ptrs);
    }
  }

  /**
   * A nested VARCHAR child that writes only inline (&le;12-byte) strings allocates no chunk, yet
   * {@code finishStringColumn} still routes to the childPath-aware finalizer with an empty
   * used-byte array (matching zero registered chunks on the native side).
   */
  @Test
  public void nestedVarcharChildInlineOnlyAllocatesNoChunk() {
    final int capacity = 2;
    long[] ptrs = allocStringViewDescriptor(capacity);
    try {
      JavaChunkVarcharChild child =
          new JavaChunkVarcharChild(ptrs[0], capacity, new int[] {0}, 0L, 0L);
      byte[] a = UTF8String.fromString("abc").getBytes();
      byte[] b = UTF8String.fromString("xyz12").getBytes();
      child.putByteArray(0, a, 0, a.length);
      child.putByteArray(1, b, 0, b.length);

      assertEquals(0, child.allocCalls, "inline-only writes must not allocate a chunk");
      assertEquals("abc", child.getUTF8String(0).toString());
      assertEquals("xyz12", child.getUTF8String(1).toString());

      child.finishStringColumn();
      assertNotNull(child.capturedFinalizeUsed, "finalize must still be routed for a nested child");
      assertEquals(0, child.capturedFinalizeUsed.length, "no chunks -> empty used array");

      child.close();
    } finally {
      freeStringViewDescriptor(ptrs);
    }
  }
}
