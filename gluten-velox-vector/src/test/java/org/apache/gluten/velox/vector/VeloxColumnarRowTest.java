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

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for {@link VeloxColumnarRow} (write-view, fixed-width) using the test-only constructor
 * pattern from {@link VeloxWritableColumnVectorTest}.
 *
 * <p>All native memory is allocated via {@link Platform#allocateMemory} and freed in {@code
 * finally} blocks so no native library is required.
 */
public class VeloxColumnarRowTest {

  // ---- Helper (mirrors VeloxWritableColumnVectorTest.allocDescriptor) ----

  /**
   * Allocates a hand-crafted 48-byte VeloxColumnHandle descriptor backed by a 2-pointer buffers
   * array, a nulls bitmap, and a values buffer.
   *
   * @param capacity number of rows
   * @param valueBytes total byte size of the values buffer (capacity × element stride)
   * @return {@code long[4] = {desc, buffers, nulls, values}} — all must be freed by the caller
   */
  private static long[] allocDescriptor(int capacity, int valueBytes) {
    int nullsBytes = Math.max(8, ((capacity + 7) / 8 + 7) & ~7);
    long nulls = Platform.allocateMemory(nullsBytes);
    for (int i = 0; i < nullsBytes; i++) {
      Platform.putByte(null, nulls + i, (byte) 0xFF); // 1 = valid (Velox convention)
    }

    long values = Platform.allocateMemory(Math.max(8, valueBytes));

    long buffers = Platform.allocateMemory(16);
    Platform.putLong(null, buffers, nulls); // buffers[0] = nulls
    Platform.putLong(null, buffers + 8, values); // buffers[1] = values

    long desc = Platform.allocateMemory(48);
    Platform.putLong(null, desc, capacity); // length
    Platform.putLong(null, desc + 8, 0L); // nullCount
    Platform.putLong(null, desc + 16, 2L); // nBuffers
    Platform.putLong(null, desc + 24, 0L); // nChildren
    Platform.putLong(null, desc + 32, buffers); // buffers ptr
    Platform.putLong(null, desc + 40, 0L); // children ptr

    return new long[] {desc, buffers, nulls, values};
  }

  private static void freeDescriptor(long[] ptrs) {
    Platform.freeMemory(ptrs[0]); // desc
    Platform.freeMemory(ptrs[1]); // buffers
    Platform.freeMemory(ptrs[2]); // nulls
    Platform.freeMemory(ptrs[3]); // values
  }

  // ---- Tests ----

  /**
   * Verifies that {@link VeloxColumnarRow#setInt} and {@link VeloxColumnarRow#setLong} write values
   * into the backing {@link VeloxWritableColumnVector}s and that those values can be read back via
   * the columns' typed getters.
   */
  @Test
  public void writeThenReadFixedWidthRow() {
    int capacity = 3;
    long[] ptrsInt = allocDescriptor(capacity, capacity * Integer.BYTES);
    long[] ptrsLong = allocDescriptor(capacity, capacity * Long.BYTES);
    try {
      VeloxWritableColumnVector ci =
          new VeloxWritableColumnVector(ptrsInt[0], 0L, capacity, DataTypes.IntegerType);
      VeloxWritableColumnVector cl =
          new VeloxWritableColumnVector(ptrsLong[0], 0L, capacity, DataTypes.LongType);

      VeloxColumnarRow row = new VeloxColumnarRow(new VeloxWritableColumnVector[] {ci, cl});
      row.rowId = 1;
      row.setInt(0, 42);
      row.setLong(1, 4200L);

      assertEquals(42, ci.getInt(1), "INT value must match after setInt");
      assertEquals(4200L, cl.getLong(1), "LONG value must match after setLong");

      ci.close();
      cl.close();
    } finally {
      freeDescriptor(ptrsInt);
      freeDescriptor(ptrsLong);
    }
  }

  /**
   * Verifies that {@link VeloxColumnarRow#setNullAt} marks the backing column cell as null, and
   * that non-null neighbours are unaffected.
   */
  @Test
  public void setNullAtMarksNullCorrectly() {
    int capacity = 3;
    long[] ptrsInt = allocDescriptor(capacity, capacity * Integer.BYTES);
    try {
      VeloxWritableColumnVector ci =
          new VeloxWritableColumnVector(ptrsInt[0], 0L, capacity, DataTypes.IntegerType);
      VeloxColumnarRow row = new VeloxColumnarRow(new VeloxWritableColumnVector[] {ci});

      row.rowId = 0;
      row.setInt(0, 7);
      assertFalse(ci.isNullAt(0), "row 0 must be non-null after setInt");

      row.rowId = 1;
      row.setNullAt(0);
      assertTrue(ci.isNullAt(1), "row 1 must be null after setNullAt");
      assertFalse(ci.isNullAt(0), "row 0 must still be non-null");

      ci.close();
    } finally {
      freeDescriptor(ptrsInt);
    }
  }

  /**
   * Verifies that {@link VeloxColumnarRow#update} dispatches correctly for fixed-width types, and
   * that passing {@code null} calls {@link VeloxColumnarRow#setNullAt}.
   */
  @Test
  public void updateDispatchesFixedWidthAndNull() {
    int capacity = 4;
    long[] ptrsInt = allocDescriptor(capacity, capacity * Integer.BYTES);
    long[] ptrsLong = allocDescriptor(capacity, capacity * Long.BYTES);
    try {
      VeloxWritableColumnVector ci =
          new VeloxWritableColumnVector(ptrsInt[0], 0L, capacity, DataTypes.IntegerType);
      VeloxWritableColumnVector cl =
          new VeloxWritableColumnVector(ptrsLong[0], 0L, capacity, DataTypes.LongType);

      VeloxColumnarRow row = new VeloxColumnarRow(new VeloxWritableColumnVector[] {ci, cl});

      row.rowId = 2;
      row.update(0, 99);
      row.update(1, 9900L);
      assertEquals(99, ci.getInt(2), "update(INT) must write correctly");
      assertEquals(9900L, cl.getLong(2), "update(LONG) must write correctly");

      row.rowId = 3;
      row.update(0, null);
      assertTrue(ci.isNullAt(3), "update(null) must mark null");

      ci.close();
      cl.close();
    } finally {
      freeDescriptor(ptrsInt);
      freeDescriptor(ptrsLong);
    }
  }

  /** Verifies that {@link VeloxColumnarRow#numFields} returns the column count. */
  @Test
  public void numFieldsReturnsColumnCount() {
    int capacity = 2;
    long[] ptrsInt = allocDescriptor(capacity, capacity * Integer.BYTES);
    long[] ptrsLong = allocDescriptor(capacity, capacity * Long.BYTES);
    try {
      VeloxWritableColumnVector ci =
          new VeloxWritableColumnVector(ptrsInt[0], 0L, capacity, DataTypes.IntegerType);
      VeloxWritableColumnVector cl =
          new VeloxWritableColumnVector(ptrsLong[0], 0L, capacity, DataTypes.LongType);

      VeloxColumnarRow row = new VeloxColumnarRow(new VeloxWritableColumnVector[] {ci, cl});
      assertEquals(2, row.numFields(), "numFields must equal column count");

      ci.close();
      cl.close();
    } finally {
      freeDescriptor(ptrsInt);
      freeDescriptor(ptrsLong);
    }
  }

  /**
   * Verifies that {@link VeloxColumnarRow#finishWriteRow} is a no-op and does not throw for
   * fixed-width columns.
   */
  @Test
  public void finishWriteRowIsNoOp() {
    int capacity = 2;
    long[] ptrsInt = allocDescriptor(capacity, capacity * Integer.BYTES);
    try {
      VeloxWritableColumnVector ci =
          new VeloxWritableColumnVector(ptrsInt[0], 0L, capacity, DataTypes.IntegerType);
      VeloxColumnarRow row = new VeloxColumnarRow(new VeloxWritableColumnVector[] {ci});
      row.rowId = 0;
      row.setInt(0, 5);
      assertDoesNotThrow(row::finishWriteRow, "finishWriteRow must not throw");
      ci.close();
    } finally {
      freeDescriptor(ptrsInt);
    }
  }

  /**
   * Allocates a StringView (VARBINARY/VARCHAR) descriptor with an all-valid nulls bitmap and a
   * zeroed StringView values buffer (16 bytes per slot).
   *
   * @param capacity number of rows
   * @return {@code long[4] = {desc, buffers, nulls, values}} — all must be freed by the caller
   */
  private static long[] allocStringViewDescriptor(int capacity) {
    int nullsBytes = Math.max(8, ((capacity + 7) / 8 + 7) & ~7);
    long nulls = Platform.allocateMemory(nullsBytes);
    for (int i = 0; i < nullsBytes; i++) {
      Platform.putByte(null, nulls + i, (byte) 0xFF);
    }

    long values = Platform.allocateMemory(Math.max(8, (long) capacity * 16));
    for (int i = 0; i < capacity * 16; i++) {
      Platform.putByte(null, values + i, (byte) 0);
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

  /**
   * Verifies that {@link VeloxColumnarRow#copy()} correctly handles a {@code DateType} column.
   *
   * <p>DateType is stored as an epoch-day int32 (same physical layout as IntegerType). The copied
   * row must return the same integer value, and the copy must be independent of the source —
   * overwriting the source column does not affect the copied value.
   */
  @Test
  public void copyProducesCorrectValueForDateType() {
    int capacity = 2;
    long[] ptrsDate = allocDescriptor(capacity, capacity * Integer.BYTES);
    try {
      VeloxWritableColumnVector cd =
          new VeloxWritableColumnVector(ptrsDate[0], 0L, capacity, DataTypes.DateType);

      VeloxColumnarRow row = new VeloxColumnarRow(new VeloxWritableColumnVector[] {cd});

      // Write epoch-day 19000 (approx. 2022-01-01)
      int epochDay = 19000;
      row.rowId = 0;
      row.setInt(0, epochDay);

      // Copy the row
      InternalRow copied = row.copy();

      // The copy must return the correct epoch-day value.
      assertEquals(epochDay, copied.getInt(0), "copy() must return correct DateType epoch-day");

      // Independence check: overwrite the source column and confirm the copy is unaffected.
      row.setInt(0, 0);
      assertEquals(
          epochDay,
          copied.getInt(0),
          "copy() must be independent of the source column after mutation");

      cd.close();
    } finally {
      freeDescriptor(ptrsDate);
    }
  }

  /**
   * Verifies that {@link VeloxColumnarRow#copy()} correctly handles a {@code BinaryType} column.
   *
   * <p>{@link VeloxWritableColumnVector#getBinary} always returns a fresh heap {@code byte[]}, so
   * the copy is guaranteed to be independent of native memory. The test also confirms that
   * overwriting the source column does not change the copied byte array.
   */
  @Test
  public void copyProducesCorrectValueForBinaryType() {
    int capacity = 2;
    long[] ptrsBin = allocStringViewDescriptor(capacity);
    try {
      VeloxWritableColumnVector cb =
          new VeloxWritableColumnVector(ptrsBin[0], 0L, capacity, DataTypes.BinaryType);

      VeloxColumnarRow row = new VeloxColumnarRow(new VeloxWritableColumnVector[] {cb});

      byte[] original = {10, 20, 30, 40, 50};
      row.rowId = 0;
      row.update(0, original);

      // Copy the row
      InternalRow copied = row.copy();

      // The copy must contain the same bytes.
      assertArrayEquals(
          original,
          (byte[]) copied.get(0, DataTypes.BinaryType),
          "copy() must return correct BinaryType bytes");

      // Independence check: write different bytes to the same slot and confirm the copy is
      // unchanged (getBinary already returns a heap copy, so aliasing cannot occur).
      row.update(0, new byte[] {99});
      assertArrayEquals(
          original,
          (byte[]) copied.get(0, DataTypes.BinaryType),
          "copy() BinaryType value must be independent of source column after mutation");

      cb.close();
    } finally {
      freeDescriptor(ptrsBin);
    }
  }

  /**
   * Verifies that {@link VeloxColumnarRow#copy()} correctly handles null values in {@code DateType}
   * and {@code BinaryType} columns — null cells must produce {@code null} in the copy.
   */
  @Test
  public void copyHandlesNullDateAndBinaryColumns() {
    int capacity = 2;
    long[] ptrsDate = allocDescriptor(capacity, capacity * Integer.BYTES);
    long[] ptrsBin = allocStringViewDescriptor(capacity);
    try {
      VeloxWritableColumnVector cd =
          new VeloxWritableColumnVector(ptrsDate[0], 0L, capacity, DataTypes.DateType);
      VeloxWritableColumnVector cb =
          new VeloxWritableColumnVector(ptrsBin[0], 0L, capacity, DataTypes.BinaryType);

      VeloxColumnarRow row = new VeloxColumnarRow(new VeloxWritableColumnVector[] {cd, cb});

      row.rowId = 0;
      row.setNullAt(0);
      row.setNullAt(1);

      InternalRow copied = row.copy();

      assertTrue(copied.isNullAt(0), "copy() must preserve null for DateType column");
      assertTrue(copied.isNullAt(1), "copy() must preserve null for BinaryType column");

      cd.close();
      cb.close();
    } finally {
      freeDescriptor(ptrsDate);
      freeDescriptor(ptrsBin);
    }
  }

  @Test
  public void spark41WriterRowKeepsUnsupportedSpecializedGetters() {
    VeloxColumnarRow row = new VeloxColumnarRow(new VeloxWritableColumnVector[0]);
    assertThrows(UnsupportedOperationException.class, () -> row.getVariant(0));
    assertThrows(UnsupportedOperationException.class, () -> row.getGeography(0));
    assertThrows(UnsupportedOperationException.class, () -> row.getGeometry(0));
  }
}
