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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * TDD tests for DATE write (int32, days-since-epoch) via {@link VeloxColumnarRow#update}.
 *
 * <p>RED: before the DateType branch is added to {@link VeloxColumnarRow#update}, calling {@code
 * row.update(0, 19000)} with a DateType column throws {@link UnsupportedOperationException}.
 *
 * <p>GREEN: after the DateType branch is added, the int32 value is written correctly and read back
 * via {@link VeloxWritableColumnVector#getInt}.
 *
 * <p>No native library is loaded. Descriptors are built in Java-allocated off-heap memory via
 * {@link Platform} (i.e. {@code sun.misc.Unsafe}). Memory is freed manually after each test.
 */
public class VeloxColumnarRowDateWriteTest {

  /**
   * Allocates a 48-byte VeloxColumnHandle descriptor backed by a 2-pointer buffers array, a nulls
   * bitmap (all-valid), and an int32 values buffer.
   *
   * @param capacity number of rows
   * @return {@code long[4] = {desc, buffers, nulls, values}} -- all must be freed by the caller
   */
  private static long[] allocInt32Descriptor(int capacity) {
    int nullsBytes = Math.max(8, ((capacity + 7) / 8 + 7) & ~7);
    long nulls = Platform.allocateMemory(nullsBytes);
    for (int i = 0; i < nullsBytes; i++) {
      Platform.putByte(null, nulls + i, (byte) 0xFF);
    }

    long values = Platform.allocateMemory(Math.max(8, capacity * Integer.BYTES));

    long buffers = Platform.allocateMemory(16);
    Platform.putLong(null, buffers, nulls);
    Platform.putLong(null, buffers + 8, values);

    long desc = Platform.allocateMemory(48);
    Platform.putLong(null, desc, capacity);
    Platform.putLong(null, desc + 8, 0L);
    Platform.putLong(null, desc + 16, 2L);
    Platform.putLong(null, desc + 24, 0L);
    Platform.putLong(null, desc + 32, buffers);
    Platform.putLong(null, desc + 40, 0L);

    return new long[] {desc, buffers, nulls, values};
  }

  private static void freeDescriptor(long[] ptrs) {
    Platform.freeMemory(ptrs[0]);
    Platform.freeMemory(ptrs[1]);
    Platform.freeMemory(ptrs[2]);
    Platform.freeMemory(ptrs[3]);
  }

  /**
   * Verifies that {@link VeloxColumnarRow#update} writes a DateType int32 value (days since epoch)
   * correctly into the backing {@link VeloxWritableColumnVector}.
   *
   * <p>RED state: {@code row.update(0, 19000)} throws {@link UnsupportedOperationException} because
   * DateType is not handled.
   *
   * <p>GREEN state: the int32 value 19000 is written and {@link VeloxWritableColumnVector#getInt}
   * reads it back correctly.
   */
  @Test
  public void updateDateWritesInt32DaysSinceEpoch() {
    int capacity = 4;
    long[] ptrs = allocInt32Descriptor(capacity);
    try {
      VeloxWritableColumnVector cv =
          new VeloxWritableColumnVector(ptrs[0], 0L, capacity, DataTypes.DateType);
      VeloxColumnarRow row = new VeloxColumnarRow(new VeloxWritableColumnVector[] {cv});

      row.rowId = 0;
      row.update(0, 19000); // 19000 days since epoch ~ 2022-01-13

      assertEquals(19000, cv.getInt(0), "DATE value must round-trip as int32 days since epoch");
      assertFalse(cv.isNullAt(0), "DATE slot must not be null after update");
    } finally {
      freeDescriptor(ptrs);
    }
  }

  /**
   * Verifies that multiple DATE rows can be written and read back correctly, including pre-epoch
   * values (negative days).
   */
  @Test
  public void updateDateMultipleRows() {
    int capacity = 4;
    int[] days = {0, 19000, -1, 18628}; // epoch, ~2022-01-13, 1969-12-31, ~2021-01-01
    long[] ptrs = allocInt32Descriptor(capacity);
    try {
      VeloxWritableColumnVector cv =
          new VeloxWritableColumnVector(ptrs[0], 0L, capacity, DataTypes.DateType);
      VeloxColumnarRow row = new VeloxColumnarRow(new VeloxWritableColumnVector[] {cv});

      for (int i = 0; i < days.length; i++) {
        row.rowId = i;
        row.update(0, days[i]);
      }

      for (int i = 0; i < days.length; i++) {
        assertEquals(days[i], cv.getInt(i), "DATE value at row " + i + " must match after update");
      }
    } finally {
      freeDescriptor(ptrs);
    }
  }

  /** Verifies that {@link VeloxColumnarRow#get} with DateType returns the int32 days value. */
  @Test
  public void getDateReturnsInt32DaysValue() {
    int capacity = 2;
    long[] ptrs = allocInt32Descriptor(capacity);
    try {
      VeloxWritableColumnVector cv =
          new VeloxWritableColumnVector(ptrs[0], 0L, capacity, DataTypes.DateType);
      VeloxColumnarRow row = new VeloxColumnarRow(new VeloxWritableColumnVector[] {cv});

      row.rowId = 0;
      row.update(0, 19000);
      row.rowId = 1;
      row.update(0, 0);

      row.rowId = 0;
      assertEquals(19000, row.get(0, DataTypes.DateType), "get(DateType) must return int32 days");
      row.rowId = 1;
      assertEquals(0, row.get(0, DataTypes.DateType), "get(DateType) epoch must return 0");
    } finally {
      freeDescriptor(ptrs);
    }
  }

  /** Verifies that setting null on a DateType column clears the validity bit. */
  @Test
  public void setNullDateColumn() {
    int capacity = 2;
    long[] ptrs = allocInt32Descriptor(capacity);
    try {
      VeloxWritableColumnVector cv =
          new VeloxWritableColumnVector(ptrs[0], 0L, capacity, DataTypes.DateType);
      VeloxColumnarRow row = new VeloxColumnarRow(new VeloxWritableColumnVector[] {cv});

      row.rowId = 0;
      row.update(0, 19000);
      row.rowId = 1;
      row.update(0, null); // setNullAt

      assertFalse(cv.isNullAt(0), "row 0 must be valid");
      assertTrue(cv.isNullAt(1), "row 1 must be null");
    } finally {
      freeDescriptor(ptrs);
    }
  }
}
