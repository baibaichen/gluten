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
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * TDD tests for BOOLEAN write (bit-packed, LSB-first) via {@link VeloxColumnarRow#update}.
 *
 * <p>RED: before the BooleanType branch is added to {@link VeloxColumnarRow#update} (and {@code
 * putBoolean}/{@code getBoolean} are implemented), writing/reading a BooleanType column throws
 * {@link UnsupportedOperationException}.
 *
 * <p>GREEN: after the BooleanType support is added, boolean bits round-trip correctly across byte
 * boundaries via {@link VeloxWritableColumnVector#getBoolean}.
 *
 * <p>No native library is loaded. Descriptors are built in Java-allocated off-heap memory via
 * {@link Platform} (i.e. {@code sun.misc.Unsafe}). Memory is freed manually after each test.
 */
public class VeloxColumnarRowBooleanWriteTest {

  /**
   * Allocates a 48-byte VeloxColumnHandle descriptor backed by a 2-pointer buffers array, a nulls
   * bitmap (all-valid), and a bit-packed values buffer (1 bit/row).
   *
   * @param capacity number of rows
   * @return {@code long[4] = {desc, buffers, nulls, values}} -- all must be freed by the caller
   */
  private static long[] allocBoolDescriptor(int capacity) {
    int nullsBytes = Math.max(8, ((capacity + 7) / 8 + 7) & ~7);
    long nulls = Platform.allocateMemory(nullsBytes);
    for (int i = 0; i < nullsBytes; i++) {
      Platform.putByte(null, nulls + i, (byte) 0xFF);
    }

    // Bit-packed values: ceil(capacity / 8) bytes, zero-initialised.
    int valuesBytes = Math.max(8, ((capacity + 7) / 8 + 7) & ~7);
    long values = Platform.allocateMemory(valuesBytes);
    for (int i = 0; i < valuesBytes; i++) {
      Platform.putByte(null, values + i, (byte) 0x00);
    }

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
   * Verifies that {@link VeloxColumnarRow#update} writes BooleanType bits (LSB-first, bit-packed)
   * correctly into the backing {@link VeloxWritableColumnVector} across a byte boundary.
   *
   * <p>Rows 0, 7, 8, 9 are {@code true}; rows 1, 2 are {@code false}. Rows 7/8 straddle the first
   * byte boundary, exercising the {@code rowId >> 3} / {@code rowId & 7} bit addressing.
   */
  @Test
  public void updateBooleanWritesBitPackedAcrossByteBoundary() {
    int capacity = 16;
    long[] ptrs = allocBoolDescriptor(capacity);
    try {
      VeloxWritableColumnVector cv =
          new VeloxWritableColumnVector(ptrs[0], 0L, capacity, DataTypes.BooleanType);
      VeloxColumnarRow row = new VeloxColumnarRow(new VeloxWritableColumnVector[] {cv});

      int[] trueRows = {0, 7, 8, 9};
      int[] falseRows = {1, 2};
      for (int r : trueRows) {
        row.rowId = r;
        row.update(0, Boolean.TRUE);
      }
      for (int r : falseRows) {
        row.rowId = r;
        row.update(0, Boolean.FALSE);
      }

      for (int r : trueRows) {
        assertTrue(cv.getBoolean(r), "BOOLEAN row " + r + " must read back true");
        assertFalse(cv.isNullAt(r), "BOOLEAN row " + r + " must not be null");
      }
      for (int r : falseRows) {
        assertFalse(cv.getBoolean(r), "BOOLEAN row " + r + " must read back false");
      }
    } finally {
      freeDescriptor(ptrs);
    }
  }

  /** Verifies {@link VeloxColumnarRow#get} with BooleanType returns the bit value. */
  @Test
  public void getBooleanReturnsBitValue() {
    int capacity = 8;
    long[] ptrs = allocBoolDescriptor(capacity);
    try {
      VeloxWritableColumnVector cv =
          new VeloxWritableColumnVector(ptrs[0], 0L, capacity, DataTypes.BooleanType);
      VeloxColumnarRow row = new VeloxColumnarRow(new VeloxWritableColumnVector[] {cv});

      row.rowId = 0;
      row.update(0, Boolean.TRUE);
      row.rowId = 1;
      row.update(0, Boolean.FALSE);

      row.rowId = 0;
      assertEquals(true, row.get(0, DataTypes.BooleanType), "get(BooleanType) row 0 must be true");
      row.rowId = 1;
      assertEquals(
          false, row.get(0, DataTypes.BooleanType), "get(BooleanType) row 1 must be false");
    } finally {
      freeDescriptor(ptrs);
    }
  }

  /** Verifies that setting null on a BooleanType column clears the validity bit. */
  @Test
  public void setNullBooleanColumn() {
    int capacity = 8;
    long[] ptrs = allocBoolDescriptor(capacity);
    try {
      VeloxWritableColumnVector cv =
          new VeloxWritableColumnVector(ptrs[0], 0L, capacity, DataTypes.BooleanType);
      VeloxColumnarRow row = new VeloxColumnarRow(new VeloxWritableColumnVector[] {cv});

      row.rowId = 0;
      row.update(0, Boolean.TRUE);
      row.rowId = 1;
      row.update(0, null); // setNullAt

      assertFalse(cv.isNullAt(0), "row 0 must be valid");
      assertTrue(cv.isNullAt(1), "row 1 must be null");
    } finally {
      freeDescriptor(ptrs);
    }
  }

  private void assertBooleanCopy(Boolean value) {
    long[] ptrs = allocBoolDescriptor(8);
    InternalRow copied;
    try (VeloxWritableColumnVector cv =
        new VeloxWritableColumnVector(ptrs[0], 0L, 8, DataTypes.BooleanType)) {
      VeloxColumnarRow row = new VeloxColumnarRow(new VeloxWritableColumnVector[] {cv});
      row.rowId = 0;
      row.update(0, value);
      copied = row.copy();
      assertNotSame(row, copied);
      assertEquals(value, copied.get(0, DataTypes.BooleanType));
      row.rowId = 1;
      row.update(0, Boolean.TRUE);
      row.rowId = 0;
      row.update(0, value == null || !value);
      assertEquals(value, copied.get(0, DataTypes.BooleanType));
    } finally {
      freeDescriptor(ptrs);
    }
    assertEquals(value, copied.get(0, DataTypes.BooleanType));
  }

  @Test
  public void copyBooleanTrueProducesAnIndependentRow() {
    assertBooleanCopy(Boolean.TRUE);
  }

  @Test
  public void copyBooleanFalseProducesAnIndependentRow() {
    assertBooleanCopy(Boolean.FALSE);
  }

  @Test
  public void copyBooleanNullPreservesNullIndependently() {
    assertBooleanCopy(null);
  }
}
