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

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.unsafe.Platform;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;

/**
 * TDD tests for DateType read via {@link VeloxColumnarReadRow#get(int,
 * org.apache.spark.sql.types.DataType)}.
 *
 * <p>RED: before the DateType branch is added to {@link VeloxColumnarReadRow#get}, calling {@code
 * row.get(0, DataTypes.DateType)} throws {@link UnsupportedOperationException}. GREEN: after the
 * fix the int32 epoch-day value is returned, and {@link VeloxColumnarReadRow#copy()} on a
 * DATE-column row also returns the correct value (copy() delegates to get() for fixed-width types,
 * so the same branch fixes both paths).
 *
 * <p>No native library is loaded. Descriptors are built in Java-allocated off-heap memory via
 * {@link Platform} (i.e. {@code sun.misc.Unsafe}). Memory is freed manually after each test to
 * avoid invoking native-free on Java-allocated addresses.
 */
public class VeloxColumnarReadRowDateTest {

  // ---------------------------------------------------------------------------
  // Helper: build a flat int32 DATE descriptor with given values (no nulls)
  // ---------------------------------------------------------------------------

  /**
   * Allocates a VeloxColumnHandle descriptor for a flat DATE (int32) column.
   *
   * @param dates epoch-day values to write
   * @return {@code long[4] = {desc, buffers, nullsPlaceholder(0), values}} — all freed by caller
   */
  private static long[] allocDateDescriptor(int[] dates) {
    long values = Platform.allocateMemory((long) dates.length * 4);
    for (int i = 0; i < dates.length; i++) {
      Platform.putInt(null, values + (long) i * 4, dates[i]);
    }

    long buffers = Platform.allocateMemory(16);
    Platform.putLong(null, buffers, 0L); // nulls = nullptr (no nulls)
    Platform.putLong(null, buffers + 8, values);

    long desc = Platform.allocateMemory(48);
    Platform.putLong(null, desc, dates.length); // length
    Platform.putLong(null, desc + 8, 0L); // nullCount = 0
    Platform.putLong(null, desc + 16, 2L); // nBuffers
    Platform.putLong(null, desc + 24, 0L); // nChildren
    Platform.putLong(null, desc + 32, buffers); // buffers ptr
    Platform.putLong(null, desc + 40, 0L); // children ptr

    return new long[] {desc, buffers, 0L /* unused */, values};
  }

  private static void freeDescriptor(long[] ptrs) {
    Platform.freeMemory(ptrs[0]); // desc
    Platform.freeMemory(ptrs[1]); // buffers
    // ptrs[2] is 0L (no nulls buffer allocated)
    Platform.freeMemory(ptrs[3]); // values
  }

  // ---------------------------------------------------------------------------
  // Tests
  // ---------------------------------------------------------------------------

  /**
   * Verifies that {@link VeloxColumnarReadRow#get} with {@code DataTypes.DateType} returns the
   * int32 epoch-day value rather than throwing {@link UnsupportedOperationException}.
   *
   * <p>RED (before fix): get(0, DateType) throws UnsupportedOperationException. GREEN (after fix):
   * returns 19000 (epoch days ~2022-01-13).
   */
  @Test
  public void getDateReturnsInt32EpochDays() {
    int[] dates = {0, 19000, -1};
    long[] ptrs = allocDateDescriptor(dates);
    try {
      VeloxColumnVector cv = VeloxColumnVector.importFromNative(ptrs[0], DataTypes.DateType);
      VeloxColumnarReadRow row = new VeloxColumnarReadRow(new VeloxColumnVector[] {cv});

      row.rowId = 0;
      assertEquals(0, row.get(0, DataTypes.DateType), "epoch (day 0) must round-trip");

      row.rowId = 1;
      assertEquals(19000, row.get(0, DataTypes.DateType), "~2022-01-13 must round-trip");

      row.rowId = 2;
      assertEquals(-1, row.get(0, DataTypes.DateType), "pre-epoch (-1) must round-trip");

      // Also verify getInt works (used by codegen path — should already pass)
      row.rowId = 1;
      assertEquals(19000, row.getInt(0), "getInt must still work for DATE");

      assertFalse(cv.hasNull(), "no null values expected");
    } finally {
      // Do NOT call cv.close() — that would native-free our Java-allocated desc.
      Platform.freeMemory(ptrs[3]); // values
      Platform.freeMemory(ptrs[1]); // buffers
      // desc was consumed by importFromNative; free it too
      // (ptrs[0] was already handed to importFromNative but Java-allocated, so safe to free)
      Platform.freeMemory(ptrs[0]);
    }
  }

  /**
   * Verifies that {@link VeloxColumnarReadRow#copy()} on a DATE-column row returns a {@link
   * org.apache.spark.sql.catalyst.expressions.GenericInternalRow} with the correct int32 epoch-day
   * value. The copy() method delegates fixed-width types to get(i, dt), so the same DateType branch
   * in get() fixes the copy path.
   *
   * <p>RED (before fix): copy() → get(0, DateType) throws UnsupportedOperationException. GREEN
   * (after fix): copy().getInt(0) returns 19000.
   */
  @Test
  public void copyDateColumnPreservesEpochDays() {
    int[] dates = {19000, 0};
    long[] ptrs = allocDateDescriptor(dates);
    try {
      VeloxColumnVector cv = VeloxColumnVector.importFromNative(ptrs[0], DataTypes.DateType);
      VeloxColumnarReadRow row = new VeloxColumnarReadRow(new VeloxColumnVector[] {cv});

      row.rowId = 0;
      InternalRow copied = row.copy();
      assertEquals(
          19000, copied.getInt(0), "copy() of DATE row must preserve int32 epoch-day value");

      row.rowId = 1;
      InternalRow copied2 = row.copy();
      assertEquals(0, copied2.getInt(0), "copy() of epoch DATE row must return 0");
    } finally {
      Platform.freeMemory(ptrs[3]);
      Platform.freeMemory(ptrs[1]);
      Platform.freeMemory(ptrs[0]);
    }
  }

  /**
   * Verifies that {@link VeloxColumnarReadRow#get} returns {@code null} for a null DATE value (C1
   * null-safety constraint).
   */
  @Test
  public void getDateNullReturnsNull() {
    // Build a 1-row DATE column with nullCount=1 (null bitmap byte = 0x00)
    long values = Platform.allocateMemory(4);
    Platform.putInt(null, values, 99999); // any value — should be masked by null

    long nullsBitmap = Platform.allocateMemory(8);
    Platform.putLong(null, nullsBitmap, 0L); // all bits 0 → all null

    long buffers = Platform.allocateMemory(16);
    Platform.putLong(null, buffers, nullsBitmap);
    Platform.putLong(null, buffers + 8, values);

    long desc = Platform.allocateMemory(48);
    Platform.putLong(null, desc, 1L); // length = 1
    Platform.putLong(null, desc + 8, 1L); // nullCount = 1
    Platform.putLong(null, desc + 16, 2L); // nBuffers
    Platform.putLong(null, desc + 24, 0L); // nChildren
    Platform.putLong(null, desc + 32, buffers);
    Platform.putLong(null, desc + 40, 0L);

    try {
      VeloxColumnVector cv = VeloxColumnVector.importFromNative(desc, DataTypes.DateType);
      VeloxColumnarReadRow row = new VeloxColumnarReadRow(new VeloxColumnVector[] {cv});

      row.rowId = 0;
      assertNull(row.get(0, DataTypes.DateType), "null DATE slot must return null from get()");
    } finally {
      Platform.freeMemory(desc);
      Platform.freeMemory(buffers);
      Platform.freeMemory(nullsBitmap);
      Platform.freeMemory(values);
    }
  }
}
