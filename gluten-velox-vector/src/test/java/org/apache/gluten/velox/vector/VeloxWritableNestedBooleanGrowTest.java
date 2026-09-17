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
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * JVM-only unit test for the NESTED BOOLEAN child GROWTH path (Wave-2 Task 4).
 *
 * <p>When an {@code ARRAY<Boolean>} output writes more elements than the BOOLEAN element child's
 * initial capacity, the child is grown via {@link VeloxWritableColumnVector#reserveInternal} ->
 * {@code growChild}. BOOLEAN values are bit-packed (1 bit/row, LSB-first). Velox's {@code
 * FlatVector<bool>::resize} resizes (and may REALLOCATE) the bit-packed values buffer and does NOT
 * value-initialize the grown region, so the native {@code growChild} clears the new bits to FALSE
 * and returns the FRESH bit-buffer base. {@link VeloxWritableColumnVector#refreshFrom} must update
 * the cached {@code valuesAddr} to that fresh base.
 *
 * <p>If {@code refreshFrom} failed to refresh {@code valuesAddr}, subsequent bit reads/writes at
 * grown indices would land in the freed/too-small pre-grow buffer -> native heap corruption / stale
 * bits. This test drives the REAL {@code reserveInternal} + {@code refreshFrom} production path,
 * overriding only the {@link VeloxWritableColumnVector#growChildNative} seam to faithfully simulate
 * the native {@code growChild} result: it allocates a NEW bit buffer at a DIFFERENT address,
 * preserves the existing bits, clears the new bits to false, and POISONS the OLD buffer to all-ones
 * so a stale {@code valuesAddr} yields deterministic garbage (a written {@code false} read back as
 * {@code true}) instead of a crash. The end-to-end native path is covered by {@code
 * VeloxNativeJvmUDFNestedBooleanSuite}.
 */
public class VeloxWritableNestedBooleanGrowTest {

  /** A simulated, non-zero root ObjectStore handle (never dereferenced in JVM-only mode). */
  private static final long FAKE_ROOT_OWNER_HANDLE = 0x5A5AL;

  private static int bytesFor(int capacityBits) {
    return Math.max(8, ((capacityBits + 7) / 8 + 7) & ~7);
  }

  /**
   * A nested BOOLEAN child whose {@code growChildNative} simulates the native {@code growChild}
   * bit-buffer reallocation (preserve existing bits, clear new bits false, poison old buffer), so
   * the JVM-only test can exercise the production {@code reserveInternal} + {@code refreshFrom}
   * refresh logic without the native library.
   */
  private static final class GrowSimBooleanChild extends VeloxWritableColumnVector {

    /**
     * The currently-live native bit-packed values buffer (kept in sync with {@code valuesAddr}).
     */
    private long liveValuesAddr;

    /** All values/nulls buffers this test allocated, to be freed on teardown. */
    private final List<Long> allocated = new ArrayList<>();

    int growCalls = 0;

    GrowSimBooleanChild(long descAddr, int capacity, int[] childPath, long origValuesAddr) {
      super(descAddr, 0L, capacity, DataTypes.BooleanType, FAKE_ROOT_OWNER_HANDLE, childPath);
      this.liveValuesAddr = origValuesAddr;
    }

    @Override
    protected long[] growChildNative(int newCapacity) {
      growCalls++;
      final int oldCap = capacity; // inherited protected field: pre-grow capacity.
      final long oldValues = liveValuesAddr;
      final int newBytes = bytesFor(newCapacity);
      final long newValues = Platform.allocateMemory(newBytes);
      allocated.add(newValues);
      // Start every new bit at FALSE (models growChild's explicit new-bit clear to false).
      Platform.setMemory(newValues, (byte) 0x00, newBytes);
      // Preserve the existing bits [0, oldCap) exactly (bit-granular copy, LSB-first).
      for (int r = 0; r < oldCap; r++) {
        final int ob = Platform.getByte(null, oldValues + (r >> 3)) & 0xFF;
        if ((ob & (1 << (r & 7))) != 0) {
          final long na = newValues + (r >> 3);
          final int nb = Platform.getByte(null, na) & 0xFF;
          Platform.putByte(null, na, (byte) (nb | (1 << (r & 7))));
        }
      }
      // Poison the OLD buffer to all-ones to model native realloc reusing/freeing it: any code that
      // keeps reading through a STALE valuesAddr (i.e. refreshFrom failed to refresh it) now reads
      // back `true` for bits that were written false -> a DETERMINISTIC, crash-free failure.
      Platform.setMemory(oldValues, (byte) 0xFF, bytesFor(oldCap));
      // Fresh, all-valid nulls buffer (models growChild's fillBits over new rows).
      final int nullsBytes = bytesFor(newCapacity);
      final long newNulls = Platform.allocateMemory(nullsBytes);
      allocated.add(newNulls);
      Platform.setMemory(newNulls, (byte) 0xFF, nullsBytes);
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
   * Allocates a hand-crafted FLAT BOOLEAN descriptor (nulls + bit-packed values buffer).
   *
   * @return {@code long[4] = {desc, buffers, nulls, values}} -- all must be freed by the caller.
   */
  private static long[] allocBooleanDescriptor(int capacity) {
    int nullsBytes = bytesFor(capacity);
    long nulls = Platform.allocateMemory(nullsBytes);
    Platform.setMemory(nulls, (byte) 0xFF, nullsBytes); // all valid
    int valuesBytes = bytesFor(capacity);
    long values = Platform.allocateMemory(valuesBytes);
    Platform.setMemory(values, (byte) 0, valuesBytes); // all bits false initially
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

  private static void freeBooleanDescriptor(long[] ptrs) {
    for (long p : ptrs) {
      Platform.freeMemory(p);
    }
  }

  /**
   * An {@code ARRAY<Boolean>} BOOLEAN element child, initially sized for 2 elements, that grows
   * past its capacity while writing bits spanning byte boundaries. After the grow: (1) the
   * previously written bits still read back correctly through the refreshed bit-buffer pointer, (2)
   * new bits default to false, and (3) new bits written at grown indices read back correctly.
   */
  @Test
  public void nestedBooleanChildGrowsPastCapacityWithoutStaleBits() {
    final int initialCapacity = 2;
    long[] ptrs = allocBooleanDescriptor(initialCapacity);
    final long origValues = ptrs[3];
    GrowSimBooleanChild child = null;
    try {
      int[] childPath = new int[] {0}; // element of ARRAY<Boolean>
      child = new GrowSimBooleanChild(ptrs[0], initialCapacity, childPath, origValues);

      // Two bits within the initial capacity: bit 0 = true, bit 1 = false.
      child.putBoolean(0, true);
      child.putBoolean(1, false);
      assertTrue(child.getBoolean(0), "pre-grow read-back");
      assertFalse(child.getBoolean(1), "pre-grow read-back");

      // Force a grow past the initial capacity (element count 20 > capacity 2). Spark's reserve
      // doubles: requiredCapacity 20 -> reserveInternal(40) -> growChildNative(40).
      final int required = 20;
      child.reserve(required);
      assertEquals(1, child.growCalls, "reserve past capacity must trigger exactly one grow");
      assertTrue(
          child.currentCapacity() >= required, "capacity must have grown to at least " + required);

      // (1) Previously-written bits still read back through the refreshed bit-buffer pointer.
      assertTrue(child.getBoolean(0), "existing true bit lost after grow");
      assertFalse(child.getBoolean(1), "existing false bit corrupted after grow");

      // (2) New (unwritten) bits default to FALSE (not stale/garbage from the poisoned old buffer).
      for (int r = initialCapacity; r < required; r++) {
        assertFalse(child.getBoolean(r), "grown-index bit " + r + " not defaulted to false");
      }

      // (3) New bits written at grown indices spanning byte boundaries read back correctly.
      child.putBoolean(8, true); // byte 1, bit 0
      child.putBoolean(15, true); // byte 1, bit 7
      child.putBoolean(16, false); // byte 2, bit 0
      child.putBoolean(19, true); // byte 2, bit 3
      assertTrue(child.getBoolean(8), "grown-index read-back");
      assertTrue(child.getBoolean(15), "grown-index read-back");
      assertFalse(child.getBoolean(16), "grown-index read-back");
      assertTrue(child.getBoolean(19), "grown-index read-back");
      // Neighbors stay false; originals stay intact.
      assertFalse(child.getBoolean(9), "grown-index neighbor bit corrupted");
      assertFalse(child.getBoolean(14), "grown-index neighbor bit corrupted");
      assertTrue(child.getBoolean(0), "original clobbered by grown writes");
      assertFalse(child.getBoolean(1), "original clobbered by grown writes");

      child.close(); // no-op: ownsNativeResources=false
    } finally {
      if (child != null) {
        child.freeAllocated();
      }
      freeBooleanDescriptor(ptrs);
    }
  }
}
