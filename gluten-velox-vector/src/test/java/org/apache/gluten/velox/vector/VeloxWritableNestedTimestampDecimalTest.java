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

import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Decimal;
import org.apache.spark.sql.types.DecimalType;
import org.apache.spark.unsafe.Platform;
import org.junit.jupiter.api.Test;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * JVM-only unit tests for the NESTED TIMESTAMP + DECIMAL child write path (Wave-3 Task 5).
 *
 * <p>Both TIMESTAMP (16 bytes/slot: {@code {int64 sec, uint64 nanos}}) and long DECIMAL (int128, 16
 * bytes/slot) are FIXED-WIDTH leaves; short DECIMAL (int64, 8 bytes/slot) is an ordinary 8-byte
 * leaf. A nested leaf (e.g. the element of {@code ARRAY<TIMESTAMP>} / {@code ARRAY<DECIMAL>}) is
 * constructed with {@code ownerHandle == 0} but a real {@code rootOwnerHandle} and a non-empty
 * {@code childPath}, matching the production child wiring built by {@code buildArrayChild}.
 *
 * <p>These tests exercise the Java-side write/read leaf machinery ({@link
 * VeloxWritableColumnVector#putTimestampMicros}/{@link VeloxWritableColumnVector#putLong}/{@link
 * VeloxWritableColumnVector#putDecimal128} and the {@link VeloxWritableColumnVector#getLong}/{@link
 * VeloxWritableColumnVector#getDecimal} read-back) plus the child GROWTH refresh path ({@link
 * VeloxWritableColumnVector#reserveInternal} -> {@link VeloxWritableColumnVector#growChildNative}
 * -> {@code refreshFrom}) for 16-byte-stride leaves, WITHOUT loading the native library. The
 * end-to-end native path (allocateNestedOutput -> putX -> growChild) is covered by {@code
 * ColumnarPartialProjectZeroCopyE2ESuite} nested TIMESTAMP/DECIMAL tests.
 */
public class VeloxWritableNestedTimestampDecimalTest {

  /** A simulated, non-zero root ObjectStore handle (never dereferenced in JVM-only mode). */
  private static final long FAKE_ROOT_OWNER_HANDLE = 0x5A5AL;

  /**
   * Allocates a hand-crafted FLAT fixed-width descriptor (nulls + {@code slotBytes}-per-slot values
   * buffer), all rows valid.
   *
   * @param capacity number of element slots
   * @param slotBytes bytes per value slot (8 for short DECIMAL, 16 for TIMESTAMP / long DECIMAL)
   * @return {@code long[4] = {desc, buffers, nulls, values}} -- all must be freed by the caller
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

  /** Builds a nested (childPath=[0]) leaf child of the given type over the descriptor. */
  private static VeloxWritableColumnVector nestedChild(long descAddr, int capacity, DataType type) {
    return new VeloxWritableColumnVector(
        descAddr, 0L, capacity, type, FAKE_ROOT_OWNER_HANDLE, new int[] {0});
  }

  private static void writeShortDec(VeloxWritableColumnVector col, int rowId, Decimal dec) {
    col.putLong(rowId, dec.toUnscaledLong());
  }

  private static void writeLongDec(VeloxWritableColumnVector col, int rowId, Decimal dec) {
    BigInteger bi = dec.toJavaBigDecimal().unscaledValue();
    long[] loHi = DecimalAccessor.bigIntegerToInt128(bi);
    col.putDecimal128(rowId, loHi[0], loHi[1]);
  }

  // -------------------------------------------------------------------------
  // Within-capacity write + read-back
  // -------------------------------------------------------------------------

  /**
   * A nested TIMESTAMP (16B) child writes several micros values -- pre-epoch, epoch, post-epoch
   * with sub-second precision -- via {@link VeloxWritableColumnVector#putTimestampMicros} and reads
   * them back via {@link VeloxWritableColumnVector#getLong} (the isTimestamp branch reconstructs
   * micros from the {@code {sec, nanos}} struct).
   */
  @Test
  public void nestedTimestampChildWritesAndReadsBack() {
    final int capacity = 6;
    long[] ptrs = allocFlatDescriptor(capacity, 16);
    try {
      VeloxWritableColumnVector child = nestedChild(ptrs[0], capacity, DataTypes.TimestampType);
      long[] micros = {
        1641945600L * 1_000_000L + 500000L, // post-epoch, sub-second
        -1_000_000L, // pre-epoch (-1 second)
        0L, // epoch
        1686828645L * 1_000_000L + 123456L, // 2023-06-15T12:30:45.123456Z
        -315619200L * 1_000_000L, // large negative (1960-01-01)
        999_999L // < 1 second, positive sub-second only
      };
      for (int i = 0; i < micros.length; i++) {
        child.putTimestampMicros(i, micros[i]);
      }
      for (int i = 0; i < micros.length; i++) {
        assertEquals(micros[i], child.getLong(i), "timestamp micros round-trip at " + i);
      }
      child.close();
    } finally {
      freeFlatDescriptor(ptrs);
    }
  }

  /**
   * A nested SHORT DECIMAL(10,2) (8B, int64 unscaled) child writes positive, negative, zero and
   * near-precision-max values and reads them back via {@link VeloxWritableColumnVector#getDecimal}.
   */
  @Test
  public void nestedShortDecimalChildWritesAndReadsBack() {
    final int capacity = 5;
    final DecimalType dt = new DecimalType(10, 2);
    long[] ptrs = allocFlatDescriptor(capacity, 8);
    try {
      VeloxWritableColumnVector child = nestedChild(ptrs[0], capacity, dt);
      String[] literals = {"123.45", "-67.89", "0.00", "9999999.99", "-100.01"};
      Decimal[] decs = new Decimal[literals.length];
      for (int i = 0; i < literals.length; i++) {
        decs[i] = Decimal.apply(new java.math.BigDecimal(literals[i]), 10, 2);
        writeShortDec(child, i, decs[i]);
      }
      for (int i = 0; i < literals.length; i++) {
        assertEquals(
            decs[i].toJavaBigDecimal().stripTrailingZeros(),
            child.getDecimal(i).toJavaBigDecimal().stripTrailingZeros(),
            "short decimal round-trip at " + i);
      }
      child.close();
    } finally {
      freeFlatDescriptor(ptrs);
    }
  }

  /**
   * A nested LONG DECIMAL(38,10) (16B, int128 unscaled, LE two's complement) child writes
   * near-max-magnitude positive and negative values, zero and unit values, and reads them back via
   * {@link VeloxWritableColumnVector#getDecimal} (int128 -> BigInteger reconstruction).
   */
  @Test
  public void nestedLongDecimalChildWritesAndReadsBack() {
    final int capacity = 5;
    final DecimalType dt = new DecimalType(38, 10);
    long[] ptrs = allocFlatDescriptor(capacity, 16);
    try {
      VeloxWritableColumnVector child = nestedChild(ptrs[0], capacity, dt);
      String[] literals = {
        "12345678901234567890.1234567890",
        "-98765432109876543210.9876543210",
        "0.0000000000",
        "1.0000000000",
        "-1.0000000000"
      };
      Decimal[] decs = new Decimal[literals.length];
      for (int i = 0; i < literals.length; i++) {
        decs[i] = Decimal.apply(new java.math.BigDecimal(literals[i]), 38, 10);
        writeLongDec(child, i, decs[i]);
      }
      for (int i = 0; i < literals.length; i++) {
        assertEquals(
            decs[i].toJavaBigDecimal().stripTrailingZeros(),
            child.getDecimal(i).toJavaBigDecimal().stripTrailingZeros(),
            "long decimal round-trip at " + i);
      }
      child.close();
    } finally {
      freeFlatDescriptor(ptrs);
    }
  }

  // -------------------------------------------------------------------------
  // Growth: 16-byte-stride leaves (TIMESTAMP + long DECIMAL)
  // -------------------------------------------------------------------------

  /**
   * A nested fixed-width leaf child whose {@code growChildNative} simulates the native {@code
   * growChild} value-buffer reallocation, so the JVM-only test can exercise the production {@code
   * reserveInternal} + {@code refreshFrom} refresh logic for a {@code slotBytes}-stride leaf
   * without the native library. It preserves the existing slots verbatim (Velox resize copies
   * them), value-initializes the new slots, and POISONS the old buffer so a stale {@code
   * valuesAddr} (i.e. a refreshFrom that failed to re-point) turns into a deterministic assertion
   * failure rather than a silent pass.
   */
  private static final class GrowSimFixedChild extends VeloxWritableColumnVector {
    private final int slotBytes;
    private long liveValuesAddr;
    private final List<Long> allocated = new ArrayList<>();
    int growCalls = 0;

    GrowSimFixedChild(
        long descAddr, int capacity, DataType type, int slotBytes, long origValuesAddr) {
      super(descAddr, 0L, capacity, type, FAKE_ROOT_OWNER_HANDLE, new int[] {0});
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
      // Value-initialize the new slots [oldCap, newCapacity) -- don't-care for reads (only written
      // rows are read), but zeroed here to mirror a clean grown region.
      Platform.setMemory(
          newValues + (long) oldCap * slotBytes,
          (byte) 0,
          (long) (newCapacity - oldCap) * slotBytes);
      // Poison the OLD value buffer to model native realloc reuse/free: any code reading through a
      // STALE valuesAddr (refreshFrom failed) now reads garbage -> deterministic assertion failure.
      Platform.setMemory(oldValues, (byte) 0x7F, (long) oldCap * slotBytes);
      // Fresh, all-valid nulls buffer (models growChild's fillBits over new slots).
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

  /**
   * A nested TIMESTAMP element child, initially sized for 2 elements, grows past its capacity while
   * writing more timestamps. After the grow: (1) the previously written timestamps still read back
   * correctly through the REFRESHED 16-byte-stride value pointer, and (2) new timestamps at grown
   * indices read back correctly. Guards the 16B-stride nested-growth use-after-free.
   */
  @Test
  public void nestedTimestampChildGrowsPastCapacityWithoutStaleSlot() {
    final int initialCapacity = 2;
    long[] ptrs = allocFlatDescriptor(initialCapacity, 16);
    final long origValues = ptrs[3];
    GrowSimFixedChild child = null;
    try {
      child =
          new GrowSimFixedChild(ptrs[0], initialCapacity, DataTypes.TimestampType, 16, origValues);
      long[] before = {1641945600L * 1_000_000L + 250000L, -1_000_000L};
      for (int i = 0; i < before.length; i++) {
        child.putTimestampMicros(i, before[i]);
      }
      assertEquals(before[0], child.getLong(0), "pre-grow read-back at 0");
      assertEquals(before[1], child.getLong(1), "pre-grow read-back at 1");

      // Force a grow past the initial capacity: reserve(6) -> reserveInternal(12) -> grow.
      final int required = 6;
      child.reserve(required);
      assertEquals(1, child.growCalls, "reserve past capacity must trigger exactly one grow");
      assertTrue(
          child.currentCapacity() >= required, "capacity must have grown to at least " + required);

      // (1) Previously-written values survive the refresh.
      assertEquals(before[0], child.getLong(0), "existing value lost after grow at 0");
      assertEquals(before[1], child.getLong(1), "existing value lost after grow at 1");

      // (2) New values at grown indices read back correctly through the fresh pointer.
      long[] grown = {0L, 1686828645L * 1_000_000L + 123456L, -315619200L * 1_000_000L, 999_999L};
      for (int i = 0; i < grown.length; i++) {
        child.putTimestampMicros(initialCapacity + i, grown[i]);
      }
      for (int i = 0; i < grown.length; i++) {
        assertEquals(grown[i], child.getLong(initialCapacity + i), "grown-index read-back at " + i);
      }
      // Originals still intact after the additional writes.
      assertEquals(before[0], child.getLong(0), "original clobbered by grown writes at 0");
      assertEquals(before[1], child.getLong(1), "original clobbered by grown writes at 1");

      child.close();
    } finally {
      if (child != null) {
        child.freeAllocated();
      }
      freeFlatDescriptor(ptrs);
    }
  }

  /**
   * A nested LONG DECIMAL(38,10) element child (16B int128), initially sized for 2 elements, grows
   * past its capacity while writing more decimals near the precision-38 magnitude limit. After the
   * grow the previously written decimals survive (refreshed 16B pointer) and new decimals at grown
   * indices read back correctly.
   */
  @Test
  public void nestedLongDecimalChildGrowsPastCapacityWithoutStaleSlot() {
    final int initialCapacity = 2;
    final DecimalType dt = new DecimalType(38, 10);
    long[] ptrs = allocFlatDescriptor(initialCapacity, 16);
    final long origValues = ptrs[3];
    GrowSimFixedChild child = null;
    try {
      child = new GrowSimFixedChild(ptrs[0], initialCapacity, dt, 16, origValues);
      String[] beforeLit = {"12345678901234567890.1234567890", "-98765432109876543210.9876543210"};
      Decimal[] before = new Decimal[beforeLit.length];
      for (int i = 0; i < beforeLit.length; i++) {
        before[i] = Decimal.apply(new java.math.BigDecimal(beforeLit[i]), 38, 10);
        writeLongDec(child, i, before[i]);
      }
      assertEquals(
          before[0].toJavaBigDecimal().stripTrailingZeros(),
          child.getDecimal(0).toJavaBigDecimal().stripTrailingZeros(),
          "pre-grow read-back at 0");

      final int required = 6;
      child.reserve(required);
      assertEquals(1, child.growCalls, "reserve past capacity must trigger exactly one grow");
      assertTrue(child.currentCapacity() >= required, "capacity must have grown");

      // Existing decimals survive the refresh.
      for (int i = 0; i < before.length; i++) {
        assertEquals(
            before[i].toJavaBigDecimal().stripTrailingZeros(),
            child.getDecimal(i).toJavaBigDecimal().stripTrailingZeros(),
            "existing decimal lost after grow at " + i);
      }

      // New decimals at grown indices read back correctly.
      String[] grownLit = {"0.0000000000", "1.0000000000", "-1.0000000000", "42.4200000000"};
      Decimal[] grown = new Decimal[grownLit.length];
      for (int i = 0; i < grownLit.length; i++) {
        grown[i] = Decimal.apply(new java.math.BigDecimal(grownLit[i]), 38, 10);
        writeLongDec(child, initialCapacity + i, grown[i]);
      }
      for (int i = 0; i < grownLit.length; i++) {
        assertEquals(
            grown[i].toJavaBigDecimal().stripTrailingZeros(),
            child.getDecimal(initialCapacity + i).toJavaBigDecimal().stripTrailingZeros(),
            "grown-index decimal read-back at " + i);
      }
      child.close();
    } finally {
      if (child != null) {
        child.freeAllocated();
      }
      freeFlatDescriptor(ptrs);
    }
  }
}
