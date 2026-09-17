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

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * JVM-only unit test for the NESTED VARCHAR child GROWTH path (Task 6, the use-after-free guard).
 *
 * <p>When an {@code ARRAY<String>} output writes more elements than the VARCHAR element child's
 * initial capacity, the child is grown via {@link VeloxWritableColumnVector#reserveInternal} ->
 * {@code growChild}. Velox's {@code FlatVector<StringView>::resize} resizes (and may REALLOCATE)
 * the 16-byte StringView SLOT buffer. {@link VeloxWritableColumnVector#refreshFrom} must update the
 * cached {@code valuesAddr} to the fresh slot base, while leaving the pinned, append-only
 * string-data chunk cursor ({@code curChunkAddr}/{@code curChunkOffset}) untouched.
 *
 * <p>If {@code refreshFrom} failed to refresh {@code valuesAddr}, subsequent writes/reads at grown
 * indices would land in the freed/too-small pre-grow slot buffer -> native heap UAF / corruption.
 *
 * <p>This test drives the REAL {@code reserveInternal} + {@code refreshFrom} production path,
 * overriding only the {@link VeloxWritableColumnVector#growChildNative} seam to faithfully simulate
 * the native {@code growChild} result: it allocates a NEW StringView slot buffer at a DIFFERENT
 * address, copies the existing slots (preserving inline data AND the absolute {@code char*}
 * pointers into the pinned chunk), value-initializes the new slots, and returns the FLAT {@code
 * [nulls, values]} block. The end-to-end native path is covered by {@code
 * VeloxNativeJvmUDFNestedStringSuite}.
 */
public class VeloxWritableNestedStringGrowTest {

  /** A simulated, non-zero root ObjectStore handle (never dereferenced in JVM-only mode). */
  private static final long FAKE_ROOT_OWNER_HANDLE = 0x5A5AL;

  /**
   * A nested VARCHAR child whose {@code growChildNative} simulates the native {@code growChild}
   * StringView slot-buffer reallocation, so the JVM-only test can exercise the production {@code
   * reserveInternal} + {@code refreshFrom} refresh logic without the native library.
   */
  private static final class GrowSimVarcharChild extends VeloxWritableColumnVector {
    private final long chunkAddr;
    private final long chunkCap;

    /** The currently-live native StringView slot buffer (kept in sync with {@code valuesAddr}). */
    private long liveValuesAddr;

    /** All slot/nulls buffers this test allocated, to be freed on teardown. */
    private final List<Long> allocated = new ArrayList<>();

    int growCalls = 0;

    GrowSimVarcharChild(
        long descAddr,
        int capacity,
        int[] childPath,
        long origValuesAddr,
        long chunkAddr,
        long chunkCap) {
      super(descAddr, 0L, capacity, DataTypes.StringType, FAKE_ROOT_OWNER_HANDLE, childPath);
      this.liveValuesAddr = origValuesAddr;
      this.chunkAddr = chunkAddr;
      this.chunkCap = chunkCap;
      // Inject the pinned string-data chunk so out-of-line writes append into a stable buffer.
      injectChunkForTest(chunkAddr, chunkCap);
    }

    @Override
    protected long[] allocateStringChunkNative(int need) {
      // A single pinned chunk suffices for this test; re-hand the same buffer.
      return new long[] {chunkAddr, chunkCap};
    }

    @Override
    protected void finalizeStringColumnNative(long[] used) {
      // no-op in JVM-only mode.
    }

    @Override
    protected long[] growChildNative(int newCapacity) {
      growCalls++;
      final int oldCap = capacity; // inherited protected field: pre-grow capacity.
      final long oldValues = liveValuesAddr;
      // New slot buffer at a DIFFERENT address (models a reallocation).
      final long newValues = Platform.allocateMemory((long) newCapacity * 16);
      allocated.add(newValues);
      // Preserve existing slots verbatim (inline bytes + absolute char* pointers into the
      // pinned chunk survive because the chunk never moves).
      Platform.copyMemory(null, oldValues, null, newValues, (long) oldCap * 16);
      // Value-initialize the new slots [oldCap, newCapacity) to empty (size 0).
      Platform.setMemory(
          newValues + (long) oldCap * 16, (byte) 0, (long) (newCapacity - oldCap) * 16);
      // Poison the OLD slot buffer to model native realloc reusing/freeing it: reset the old slots
      // to empty StringViews (size 0). Any code that keeps reading through a STALE valuesAddr (i.e.
      // refreshFrom failed to refresh it) now reads back "" instead of the preserved string -> this
      // turns the use-after-free into a DETERMINISTIC, crash-free assertion failure.
      Platform.setMemory(oldValues, (byte) 0x00, (long) oldCap * 16);
      // Fresh, all-valid nulls buffer (models growChild's fillBits over new slots).
      final long nullsWords = (newCapacity + 63L) / 64L;
      final long newNulls = Platform.allocateMemory(nullsWords * 8L);
      allocated.add(newNulls);
      Platform.setMemory(newNulls, (byte) 0xFF, nullsWords * 8L);
      // Keep the local mirror in sync with what refreshFrom will store into valuesAddr.
      this.liveValuesAddr = newValues;
      return new long[] {newNulls, newValues};
    }

    void freeAllocated() {
      for (long p : allocated) {
        Platform.freeMemory(p);
      }
      allocated.clear();
    }

    /** Exposes the inherited protected {@code capacity} for assertions in the enclosing test. */
    int currentCapacity() {
      return capacity;
    }
  }

  /**
   * Allocates a hand-crafted FLAT StringView descriptor (nulls + 16-byte/slot values buffer).
   *
   * @return {@code long[4] = {desc, buffers, nulls, values}} -- all must be freed by the caller.
   */
  private static long[] allocStringViewDescriptor(int capacity) {
    int nullsBytes = Math.max(8, ((capacity + 7) / 8 + 7) & ~7);
    long nulls = Platform.allocateMemory(nullsBytes);
    Platform.setMemory(nulls, (byte) 0xFF, nullsBytes); // all valid
    long valuesBytes = Math.max(8, (long) capacity * 16);
    long values = Platform.allocateMemory(valuesBytes);
    Platform.setMemory(values, (byte) 0, valuesBytes);
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
    for (long p : ptrs) {
      Platform.freeMemory(p);
    }
  }

  /**
   * An {@code ARRAY<String>} VARCHAR element child, initially sized for 2 elements, that grows past
   * its capacity while writing LONG (&gt;12-byte) strings. After the grow: (1) the previously
   * written strings still read back correctly through the refreshed slot pointer (views preserved,
   * pinned chunk cursor intact), and (2) new strings written at grown indices read back correctly.
   */
  @Test
  public void nestedVarcharChildGrowsPastCapacityWithoutStaleSlot() {
    final int initialCapacity = 2;
    long[] ptrs = allocStringViewDescriptor(initialCapacity);
    final long origValues = ptrs[3];
    final long chunkCap = 8192L;
    long chunk = Platform.allocateMemory(chunkCap);
    GrowSimVarcharChild child = null;
    try {
      int[] childPath = new int[] {0}; // element of ARRAY<String>
      child =
          new GrowSimVarcharChild(ptrs[0], initialCapacity, childPath, origValues, chunk, chunkCap);

      // Two LONG strings within the initial capacity (out-of-line -> pinned chunk).
      String[] before = {
        "elem-zero-string-well-over-twelve-bytes", "elem-one-string-also-well-over-twelve-bytes"
      };
      for (int i = 0; i < before.length; i++) {
        byte[] b = UTF8String.fromString(before[i]).getBytes();
        child.putByteArray(i, b, 0, b.length);
      }
      assertEquals(before[0], child.getUTF8String(0).toString(), "pre-grow read-back");
      assertEquals(before[1], child.getUTF8String(1).toString(), "pre-grow read-back");

      // Force a grow past the initial capacity (element count 6 > capacity 2). Spark's reserve
      // doubles: requiredCapacity 6 -> reserveInternal(12) -> growChildNative(12).
      final int required = 6;
      child.reserve(required);
      assertEquals(1, child.growCalls, "reserve past capacity must trigger exactly one grow");
      assertTrue(
          child.currentCapacity() >= required, "capacity must have grown to at least " + required);

      // (1) Previously-written StringViews still read back through the refreshed slot pointer.
      assertEquals(before[0], child.getUTF8String(0).toString(), "existing view lost after grow");
      assertEquals(before[1], child.getUTF8String(1).toString(), "existing view lost after grow");

      // (2) New LONG strings at grown indices read back correctly (fresh slot base + chunk cursor
      //     continuity: they append into the SAME pinned chunk after the first two).
      String[] grown = {
        "grown-two-string-longer-than-twelve",
        "grown-three-string-longer-than-twelve",
        "grown-four-string-longer-than-twelve",
        "grown-five-string-longer-than-twelve"
      };
      for (int i = 0; i < grown.length; i++) {
        int idx = initialCapacity + i;
        byte[] b = UTF8String.fromString(grown[i]).getBytes();
        child.putByteArray(idx, b, 0, b.length);
      }
      for (int i = 0; i < grown.length; i++) {
        int idx = initialCapacity + i;
        assertEquals(grown[i], child.getUTF8String(idx).toString(), "grown-index read-back");
      }
      // And the originals are STILL intact after the additional writes.
      assertEquals(
          before[0], child.getUTF8String(0).toString(), "original clobbered by grown writes");
      assertEquals(
          before[1], child.getUTF8String(1).toString(), "original clobbered by grown writes");

      child.close(); // no-op: ownsNativeResources=false
    } finally {
      if (child != null) {
        child.freeAllocated();
      }
      Platform.freeMemory(chunk);
      freeStringViewDescriptor(ptrs);
    }
  }
}
