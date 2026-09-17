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
 * JVM-only unit tests for the NESTED DATE + VARBINARY child write path (Wave-1 last gate flip).
 *
 * <p>These are the last two types whose nested output was gated off under a Wave-1 YAGNI guard.
 * Both are pre-wired end-to-end:
 *
 * <ul>
 *   <li><b>DATE</b> ({@link org.apache.spark.sql.types.DateType}) – 4-byte int32 fixed-width leaf
 *       ({@code epoch-day}). Written via {@link VeloxWritableColumnVector#putInt} and read back via
 *       {@link VeloxWritableColumnVector#getInt}. Growth is the same as any other 4-byte leaf (same
 *       path as INTEGER).
 *   <li><b>VARBINARY</b> ({@link org.apache.spark.sql.types.BinaryType}) – 16-byte StringView leaf;
 *       {@code isVarchar=true}. Written via {@link VeloxWritableColumnVector#putByteArray} (inline
 *       ≤12 B or out-of-line via a childPath-aware string chunk) and read back via {@link
 *       VeloxWritableColumnVector#getBinary}. Growth reuses the exact same childPath chunk + slot
 *       refresh machinery built for nested STRING.
 * </ul>
 *
 * <p>Tests exercise the Java-side write/read leaf machinery and the child GROWTH refresh path
 * ({@link VeloxWritableColumnVector#reserveInternal} → {@link
 * VeloxWritableColumnVector#growChildNative} → {@code refreshFrom}) WITHOUT loading the native
 * library. The end-to-end native path (allocateNestedOutput → putX → growChild) is covered by
 * {@code ColumnarPartialProjectZeroCopyE2ESuite} nested DATE/VARBINARY tests.
 */
public class VeloxWritableNestedDateBinaryTest {

  /** A simulated, non-zero root ObjectStore handle (never dereferenced in JVM-only mode). */
  private static final long FAKE_ROOT_OWNER_HANDLE = 0x5A5AL;

  // =========================================================================
  // Helpers – fixed-width flat descriptor (DATE is a 4-byte int32 leaf)
  // =========================================================================

  /**
   * Allocates a hand-crafted FLAT fixed-width descriptor (nulls + {@code slotBytes}-per-slot values
   * buffer), all rows valid.
   *
   * @param capacity number of element slots
   * @param slotBytes bytes per value slot (4 for DATE / INTEGER)
   * @return {@code long[4] = {desc, buffers, nulls, values}} — all must be freed by the caller
   */
  private static long[] allocFlatDescriptor(int capacity, int slotBytes) {
    int nullsBytes = Math.max(8, ((capacity + 7) / 8 + 7) & ~7);
    long nulls = Platform.allocateMemory(nullsBytes);
    Platform.setMemory(nulls, (byte) 0xFF, nullsBytes); // all valid
    long valuesBytes = Math.max(8, (long) capacity * slotBytes);
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

  private static void freeFlatDescriptor(long[] ptrs) {
    for (long p : ptrs) {
      Platform.freeMemory(p);
    }
  }

  // =========================================================================
  // Helpers – 16-byte StringView flat descriptor (VARBINARY / BinaryType)
  // =========================================================================

  /**
   * Allocates a hand-crafted FLAT StringView descriptor (nulls + 16-byte/slot values buffer), all
   * rows valid.
   *
   * @param capacity number of element slots
   * @return {@code long[4] = {desc, buffers, nulls, values}} — all must be freed by the caller
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

  /** Builds a nested (childPath=[0]) leaf child of the given type over the descriptor. */
  private static VeloxWritableColumnVector nestedDateChild(long descAddr, int capacity) {
    return new VeloxWritableColumnVector(
        descAddr, 0L, capacity, DataTypes.DateType, FAKE_ROOT_OWNER_HANDLE, new int[] {0});
  }

  // =========================================================================
  // GrowSimFixedChild – simulates native growChild for fixed-width DATE leaf
  // =========================================================================

  /**
   * A nested fixed-width leaf child whose {@code growChildNative} simulates the native {@code
   * growChild} 4-byte-per-slot value-buffer reallocation. Preserves existing slots verbatim and
   * poisons the old buffer so a stale {@code valuesAddr} (i.e. a refreshFrom that failed to
   * re-point) turns into a deterministic assertion failure.
   */
  private static final class GrowSimFixedChild extends VeloxWritableColumnVector {
    private final int slotBytes;
    private long liveValuesAddr;
    private final List<Long> allocated = new ArrayList<>();
    int growCalls = 0;

    GrowSimFixedChild(long descAddr, int capacity, int slotBytes, long origValuesAddr) {
      super(descAddr, 0L, capacity, DataTypes.DateType, FAKE_ROOT_OWNER_HANDLE, new int[] {0});
      this.slotBytes = slotBytes;
      this.liveValuesAddr = origValuesAddr;
    }

    @Override
    protected long[] growChildNative(int newCapacity) {
      growCalls++;
      final int oldCap = capacity; // inherited protected field: pre-grow capacity.
      final long oldValues = liveValuesAddr;
      // New value buffer at a DIFFERENT address (models a reallocation).
      final long newValues = Platform.allocateMemory((long) newCapacity * slotBytes);
      allocated.add(newValues);
      // Preserve existing slots verbatim (Velox FlatVector<T>::resize copies them).
      Platform.copyMemory(null, oldValues, null, newValues, (long) oldCap * slotBytes);
      // Value-initialize the new slots [oldCap, newCapacity).
      Platform.setMemory(
          newValues + (long) oldCap * slotBytes,
          (byte) 0,
          (long) (newCapacity - oldCap) * slotBytes);
      // Poison the OLD value buffer so a stale valuesAddr causes deterministic failure.
      Platform.setMemory(oldValues, (byte) 0x7F, (long) oldCap * slotBytes);
      // Fresh, all-valid nulls buffer.
      final long nullsWords = (newCapacity + 63L) / 64L;
      final long newNulls = Platform.allocateMemory(nullsWords * 8L);
      allocated.add(newNulls);
      Platform.setMemory(newNulls, (byte) 0xFF, nullsWords * 8L);
      this.liveValuesAddr = newValues;
      return new long[] {newNulls, newValues};
    }

    void freeAllocated() {
      for (long p : allocated) {
        Platform.freeMemory(p);
      }
      allocated.clear();
    }

    int currentCapacity() {
      return capacity;
    }
  }

  // =========================================================================
  // GrowSimVarcharChild – simulates native growChild for VARBINARY StringView
  // =========================================================================

  /**
   * A nested VARBINARY (BinaryType, StringView, 16B/slot) child whose {@code growChildNative}
   * simulates the native {@code growChild} StringView slot-buffer reallocation. Preserves existing
   * slots verbatim (inline bytes + absolute char* pointers into the pinned chunk survive) and
   * poisons the old slot buffer so a stale {@code valuesAddr} causes a deterministic failure.
   */
  private static final class GrowSimBinaryChild extends VeloxWritableColumnVector {
    private final long chunkAddr;
    private final long chunkCap;
    private long liveValuesAddr;
    private final List<Long> allocated = new ArrayList<>();
    int growCalls = 0;

    GrowSimBinaryChild(
        long descAddr, int capacity, long origValuesAddr, long chunkAddr, long chunkCap) {
      super(descAddr, 0L, capacity, DataTypes.BinaryType, FAKE_ROOT_OWNER_HANDLE, new int[] {0});
      this.liveValuesAddr = origValuesAddr;
      this.chunkAddr = chunkAddr;
      this.chunkCap = chunkCap;
      // Inject the pinned binary-data chunk so out-of-line writes append into a stable buffer.
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
      final int oldCap = capacity;
      final long oldValues = liveValuesAddr;
      // New slot buffer at a DIFFERENT address (models a reallocation).
      final long newValues = Platform.allocateMemory((long) newCapacity * 16);
      allocated.add(newValues);
      // Preserve existing slots verbatim (inline bytes + absolute char* pointers into the
      // pinned chunk survive because the chunk never moves).
      Platform.copyMemory(null, oldValues, null, newValues, (long) oldCap * 16);
      // Value-initialize the new slots [oldCap, newCapacity) to empty StringViews (size=0).
      Platform.setMemory(
          newValues + (long) oldCap * 16, (byte) 0, (long) (newCapacity - oldCap) * 16);
      // Poison the OLD slot buffer: zero out all slots so a stale valuesAddr returns empty
      // byte arrays instead of the preserved data -> deterministic assertion failure.
      Platform.setMemory(oldValues, (byte) 0x00, (long) oldCap * 16);
      // Fresh, all-valid nulls buffer.
      final long nullsWords = (newCapacity + 63L) / 64L;
      final long newNulls = Platform.allocateMemory(nullsWords * 8L);
      allocated.add(newNulls);
      Platform.setMemory(newNulls, (byte) 0xFF, nullsWords * 8L);
      this.liveValuesAddr = newValues;
      return new long[] {newNulls, newValues};
    }

    void freeAllocated() {
      for (long p : allocated) {
        Platform.freeMemory(p);
      }
      allocated.clear();
    }

    int currentCapacity() {
      return capacity;
    }
  }

  // =========================================================================
  // DATE: within-capacity write + read-back
  // =========================================================================

  /**
   * A nested DATE (int32 epoch-day, 4B) child writes several day values — pre-epoch (negative),
   * epoch (0), and post-epoch — via {@link VeloxWritableColumnVector#putInt} and reads them back
   * via {@link VeloxWritableColumnVector#getInt} (DATE is an int32 flat leaf).
   */
  @Test
  public void nestedDateChildWritesAndReadsBack() {
    final int capacity = 6;
    long[] ptrs = allocFlatDescriptor(capacity, 4);
    try {
      VeloxWritableColumnVector child = nestedDateChild(ptrs[0], capacity);
      // epoch-day values: epoch, pre-epoch (negative), typical post-epoch dates
      int[] days = {
        0, // epoch (1970-01-01)
        -1, // one day before epoch (1969-12-31)
        -365, // 1969-01-01 (one year before epoch)
        18628, // 2021-01-07 (post-epoch)
        19000, // 2022-01-14
        -10000 // deep pre-epoch (~1942-07-13)
      };
      for (int i = 0; i < days.length; i++) {
        child.putInt(i, days[i]);
      }
      for (int i = 0; i < days.length; i++) {
        assertEquals(days[i], child.getInt(i), "DATE epoch-day round-trip at " + i);
      }
      child.close();
    } finally {
      freeFlatDescriptor(ptrs);
    }
  }

  // =========================================================================
  // DATE: child growth (4-byte fixed-width)
  // =========================================================================

  /**
   * A nested DATE element child, initially sized for 2 elements, grows past its capacity while
   * writing more day values. After the grow: (1) the previously written days still read back
   * correctly through the REFRESHED 4-byte-stride value pointer, and (2) new days at grown indices
   * read back correctly. Guards the fixed-width nested-growth use-after-free for int32 DATE.
   */
  @Test
  public void nestedDateChildGrowsPastCapacityWithoutStaleSlot() {
    final int initialCapacity = 2;
    long[] ptrs = allocFlatDescriptor(initialCapacity, 4);
    final long origValues = ptrs[3];
    GrowSimFixedChild child = null;
    try {
      child = new GrowSimFixedChild(ptrs[0], initialCapacity, 4, origValues);
      int[] before = {18628, -365}; // one post-epoch, one pre-epoch
      for (int i = 0; i < before.length; i++) {
        child.putInt(i, before[i]);
      }
      assertEquals(before[0], child.getInt(0), "pre-grow read-back at 0");
      assertEquals(before[1], child.getInt(1), "pre-grow read-back at 1");

      // Force a grow past the initial capacity: reserve(6) -> reserveInternal(12) -> grow.
      final int required = 6;
      child.reserve(required);
      assertEquals(1, child.growCalls, "reserve past capacity must trigger exactly one grow");
      assertTrue(
          child.currentCapacity() >= required, "capacity must have grown to at least " + required);

      // (1) Previously-written day values survive the refresh.
      assertEquals(before[0], child.getInt(0), "existing DATE lost after grow at 0");
      assertEquals(before[1], child.getInt(1), "existing DATE lost after grow at 1");

      // (2) New day values at grown indices read back correctly through the fresh pointer.
      int[] grown = {0, 19000, -10000, -1};
      for (int i = 0; i < grown.length; i++) {
        child.putInt(initialCapacity + i, grown[i]);
      }
      for (int i = 0; i < grown.length; i++) {
        assertEquals(
            grown[i], child.getInt(initialCapacity + i), "grown-index DATE read-back at " + i);
      }
      // Originals still intact after the additional writes.
      assertEquals(before[0], child.getInt(0), "original DATE clobbered by grown writes at 0");
      assertEquals(before[1], child.getInt(1), "original DATE clobbered by grown writes at 1");

      child.close();
    } finally {
      if (child != null) {
        child.freeAllocated();
      }
      freeFlatDescriptor(ptrs);
    }
  }

  // =========================================================================
  // VARBINARY: within-capacity write + read-back (inline <=12B AND long >12B)
  // =========================================================================

  /**
   * A nested VARBINARY (BinaryType, StringView) child writes both INLINE (≤12-byte) and LONG
   * (&gt;12-byte) byte arrays via {@link VeloxWritableColumnVector#putByteArray} and reads them
   * back via {@link VeloxWritableColumnVector#getBinary}. The long byte array exercises the
   * childPath-aware {@code ensureChunk} → {@code allocateStringChunkNative} path.
   */
  @Test
  public void nestedBinaryChildWritesInlineAndLongBinaries() {
    final int capacity = 5;
    long[] ptrs = allocStringViewDescriptor(capacity);
    final long chunkCap = 4096L;
    long chunk = Platform.allocateMemory(chunkCap);
    try {
      // A simulated child that re-hands the pinned chunk (no native library).
      VeloxWritableColumnVector child =
          new VeloxWritableColumnVector(
              ptrs[0], 0L, capacity, DataTypes.BinaryType, FAKE_ROOT_OWNER_HANDLE, new int[] {0}) {
            int allocCalls = 0;

            @Override
            protected long[] allocateStringChunkNative(int need) {
              allocCalls++;
              return new long[] {chunk, chunkCap};
            }

            @Override
            protected void finalizeStringColumnNative(long[] used) {
              // no-op in JVM-only mode
            }
          };

      byte[] inline1 = {1, 2, 3}; // 3 bytes, inline
      byte[] inline2 = {10, 20}; // 2 bytes, inline
      byte[] longBin = new byte[20]; // 20 bytes, out-of-line
      byte[] longBin2 = new byte[30]; // 30 bytes, out-of-line
      for (int i = 0; i < longBin.length; i++) {
        longBin[i] = (byte) (i + 1);
      }
      for (int i = 0; i < longBin2.length; i++) {
        longBin2[i] = (byte) (i + 100);
      }
      byte[] inline3 = {7, 8, 9, 10, 11}; // 5 bytes, inline

      child.putByteArray(0, inline1, 0, inline1.length);
      child.putByteArray(1, longBin, 0, longBin.length);
      child.putByteArray(2, inline2, 0, inline2.length);
      child.putByteArray(3, longBin2, 0, longBin2.length);
      child.putByteArray(4, inline3, 0, inline3.length);

      // Inline reads.
      assertArrayEquals(inline1, child.getBinary(0), "inline1 round-trip");
      assertArrayEquals(inline2, child.getBinary(2), "inline2 round-trip");
      assertArrayEquals(inline3, child.getBinary(4), "inline3 round-trip");
      // Long reads: out-of-line bytes retrieved via chunk pointer.
      assertArrayEquals(longBin, child.getBinary(1), "longBin round-trip");
      assertArrayEquals(longBin2, child.getBinary(3), "longBin2 round-trip");

      child.close();
    } finally {
      Platform.freeMemory(chunk);
      freeStringViewDescriptor(ptrs);
    }
  }

  // =========================================================================
  // VARBINARY: child growth with long binaries spanning growth
  // =========================================================================

  /**
   * A nested VARBINARY element child, initially sized for 2 elements, grows past its capacity while
   * writing more byte arrays. After the grow: (1) the previously written byte arrays still read
   * back correctly (inline bytes in slot + absolute char* pointers into the pinned chunk survive
   * via refreshed slot pointer), and (2) new byte arrays at grown indices read back correctly. The
   * pinned chunk cursor is NOT part of the slot buffer, so chunk continuity is maintained across
   * the grow. Guards the StringView nested-growth use-after-free for VARBINARY.
   */
  @Test
  public void nestedBinaryChildGrowsPastCapacityWithoutStaleSlot() {
    final int initialCapacity = 2;
    long[] ptrs = allocStringViewDescriptor(initialCapacity);
    final long origValues = ptrs[3];
    final long chunkCap = 8192L;
    long chunk = Platform.allocateMemory(chunkCap);
    GrowSimBinaryChild child = null;
    try {
      child = new GrowSimBinaryChild(ptrs[0], initialCapacity, origValues, chunk, chunkCap);

      // Two LONG binaries within the initial capacity (out-of-line -> pinned chunk).
      byte[] before0 = new byte[25]; // >12B, out-of-line
      byte[] before1 = new byte[18]; // >12B, out-of-line
      for (int i = 0; i < before0.length; i++) {
        before0[i] = (byte) (i + 42);
      }
      for (int i = 0; i < before1.length; i++) {
        before1[i] = (byte) (i + 99);
      }
      child.putByteArray(0, before0, 0, before0.length);
      child.putByteArray(1, before1, 0, before1.length);

      assertArrayEquals(before0, child.getBinary(0), "pre-grow read-back at 0");
      assertArrayEquals(before1, child.getBinary(1), "pre-grow read-back at 1");

      // Force a grow past the initial capacity.
      final int required = 6;
      child.reserve(required);
      assertEquals(1, child.growCalls, "reserve past capacity must trigger exactly one grow");
      assertTrue(
          child.currentCapacity() >= required, "capacity must have grown to at least " + required);

      // (1) Previously-written binaries survive the refresh (slot pointer refreshed, chunk intact).
      assertArrayEquals(before0, child.getBinary(0), "existing binary lost after grow at 0");
      assertArrayEquals(before1, child.getBinary(1), "existing binary lost after grow at 1");

      // (2) New byte arrays at grown indices (mix of inline and long) read back correctly.
      byte[] grown0 = new byte[30]; // >12B, long binary spanning growth
      byte[] grown1 = {1, 2, 3}; // <=12B, inline
      byte[] grown2 = new byte[20]; // >12B
      byte[] grown3 = {9, 8, 7, 6}; // <=12B, inline
      for (int i = 0; i < grown0.length; i++) {
        grown0[i] = (byte) (i + 7);
      }
      for (int i = 0; i < grown2.length; i++) {
        grown2[i] = (byte) (i + 200);
      }
      child.putByteArray(initialCapacity, grown0, 0, grown0.length);
      child.putByteArray(initialCapacity + 1, grown1, 0, grown1.length);
      child.putByteArray(initialCapacity + 2, grown2, 0, grown2.length);
      child.putByteArray(initialCapacity + 3, grown3, 0, grown3.length);

      assertArrayEquals(grown0, child.getBinary(initialCapacity), "grown-index binary (long) at 0");
      assertArrayEquals(
          grown1, child.getBinary(initialCapacity + 1), "grown-index binary (inline) at 1");
      assertArrayEquals(
          grown2, child.getBinary(initialCapacity + 2), "grown-index binary (long) at 2");
      assertArrayEquals(
          grown3, child.getBinary(initialCapacity + 3), "grown-index binary (inline) at 3");

      // Originals are still intact after the additional writes.
      assertArrayEquals(
          before0, child.getBinary(0), "original binary clobbered by grown writes at 0");
      assertArrayEquals(
          before1, child.getBinary(1), "original binary clobbered by grown writes at 1");

      child.close();
    } finally {
      if (child != null) {
        child.freeAllocated();
      }
      Platform.freeMemory(chunk);
      freeStringViewDescriptor(ptrs);
    }
  }
}
