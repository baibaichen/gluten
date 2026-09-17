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

import org.apache.spark.sql.execution.vectorized.WritableColumnVector;
import org.apache.spark.sql.types.Decimal;
import org.apache.spark.sql.types.DecimalType;
import org.apache.spark.sql.vectorized.ColumnVector;
import org.apache.spark.sql.vectorized.ColumnarArray;
import org.apache.spark.unsafe.Platform;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.math.BigDecimal;
import java.math.BigInteger;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * TDD tests for DECIMAL write via {@link VeloxColumnarRow#update} and {@link
 * VeloxColumnarRow#setDecimal} (the Spark {@code MutableProjection} typed-setter path).
 *
 * <p>Two physical backings are exercised:
 *
 * <ul>
 *   <li><b>Short decimal</b> ({@code precision <= 18}, here {@code DecimalType(10,2)}): backed by
 *       {@code BIGINT}; each row is an int64 unscaled value (8 bytes) written via {@link
 *       VeloxWritableColumnVector#putLong}.
 *   <li><b>Long decimal</b> ({@code precision > 18}, here {@code DecimalType(38,10)}): backed by
 *       {@code HUGEINT}; each row is an int128 unscaled value (16 bytes, little-endian two's
 *       complement) written via {@link VeloxWritableColumnVector#putDecimal128}.
 * </ul>
 *
 * <p>The long-decimal round-trip asserts that the write (via {@code bigIntegerToInt128}) is the
 * <b>exact inverse</b> of the Task-3 read ({@link DecimalAccessor#int128ToBigInteger(long, long)}):
 * the raw {@code (lo, hi)} pair read back from native memory, when fed through {@code
 * int128ToBigInteger}, must reconstruct the original unscaled {@link BigInteger} for negative, zero
 * and near-10^37 magnitude values.
 *
 * <p>RED: before {@code DecimalType} is handled, {@link VeloxColumnarRow#update} throws {@link
 * UnsupportedOperationException}, {@link VeloxColumnarRow#setDecimal} throws (default {@code
 * InternalRow} behaviour) and {@link VeloxColumnarRow#getDecimal} throws.
 *
 * <p>No native library is loaded. Descriptors are built in Java-allocated off-heap memory via
 * {@link Platform}. Memory is freed manually after each test.
 */
public class VeloxColumnarRowDecimalWriteTest {

  /**
   * Allocates a VeloxColumnHandle descriptor backed by a 2-pointer buffers array, an all-valid
   * nulls bitmap and a values buffer of {@code bytesPerRow} bytes/row.
   *
   * @param capacity number of rows
   * @param bytesPerRow 8 for short decimal (int64), 16 for long decimal (int128)
   * @return {@code long[4] = {desc, buffers, nulls, values}} -- all must be freed by the caller
   */
  private static long[] allocDecimalDescriptor(int capacity, int bytesPerRow) {
    int nullsBytes = Math.max(8, ((capacity + 7) / 8 + 7) & ~7);
    long nulls = Platform.allocateMemory(nullsBytes);
    for (int i = 0; i < nullsBytes; i++) {
      Platform.putByte(null, nulls + i, (byte) 0xFF);
    }

    long values = Platform.allocateMemory(Math.max(bytesPerRow, (long) capacity * bytesPerRow));

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

  private static Decimal dec(String s, int precision, int scale) {
    return Decimal.apply(new BigDecimal(s), precision, scale);
  }

  private static Decimal[] standardDecimalValues(int precision) {
    BigInteger unscaled = BigInteger.TEN.pow(precision - 1).subtract(BigInteger.ONE);
    return new Decimal[] {
      Decimal.apply(new BigDecimal(unscaled, 2), precision, 2),
      Decimal.apply(new BigDecimal(unscaled.negate(), 2), precision, 2)
    };
  }

  @ParameterizedTest
  @ValueSource(ints = {9, 18, 19, 38})
  public void standardDecimalReadsNativeSlotsAndColumnarArray(int precision) {
    int stride = precision <= 18 ? 8 : 16;
    long[] ptrs = allocDecimalDescriptor(3, stride);
    Decimal[] expected = standardDecimalValues(precision);
    try (VeloxWritableColumnVector vector =
        new VeloxWritableColumnVector(ptrs[0], 0L, 3, new DecimalType(precision, 2))) {
      for (int row = 0; row < expected.length; row++) {
        BigInteger unscaled = expected[row].toJavaBigDecimal().unscaledValue();
        Platform.putLong(null, ptrs[3] + (long) row * stride, unscaled.longValue());
        if (stride == 16) {
          Platform.putLong(
              null, ptrs[3] + (long) row * stride + 8, unscaled.shiftRight(64).longValue());
        }
      }
      vector.putNull(2);
      ColumnVector column = vector;
      ColumnarArray array = new ColumnarArray(column, 0, 3);
      assertAll(
          () -> assertEquals(expected[0], column.getDecimal(0, precision, 2), "native row 0"),
          () -> assertEquals(expected[1], column.getDecimal(1, precision, 2), "native row 1"),
          () -> assertNull(column.getDecimal(2, precision, 2)),
          () -> assertEquals(expected[0], array.getDecimal(0, precision, 2), "array row 0"),
          () -> assertEquals(expected[1], array.getDecimal(1, precision, 2), "array row 1"),
          () -> assertTrue(array.isNullAt(2)),
          () -> assertNull(array.getDecimal(2, precision, 2)));
    } finally {
      freeDescriptor(ptrs);
    }
  }

  @ParameterizedTest
  @ValueSource(ints = {9, 18, 19, 38})
  public void standardDecimalWritesNativeSlots(int precision) {
    int stride = precision <= 18 ? 8 : 16;
    long[] ptrs = allocDecimalDescriptor(2, stride);
    Decimal[] expected = standardDecimalValues(precision);
    try (WritableColumnVector column =
        new VeloxWritableColumnVector(ptrs[0], 0L, 2, new DecimalType(precision, 2))) {
      for (int offset = 0; offset < 2 * stride; offset += 8) {
        Platform.putLong(null, ptrs[3] + offset, 0L);
      }
      for (int row = 0; row < expected.length; row++) {
        column.putDecimal(row, expected[row], precision);
      }
      for (int row = 0; row < expected.length; row++) {
        BigInteger unscaled = expected[row].toJavaBigDecimal().unscaledValue();
        long slot = ptrs[3] + (long) row * stride;
        assertEquals(
            unscaled.longValue(), Platform.getLong(null, slot), "raw low bits, row " + row);
        if (stride == 16) {
          assertEquals(
              unscaled.shiftRight(64).longValue(),
              Platform.getLong(null, slot + 8),
              "raw high bits, row " + row);
        }
      }
    } finally {
      freeDescriptor(ptrs);
    }
  }

  @ParameterizedTest
  @ValueSource(ints = {9, 18, 19, 38})
  public void standardDecimalWritePreservesNullBitAndRejectsNull(int precision) {
    long[] ptrs = allocDecimalDescriptor(2, precision <= 18 ? 8 : 16);
    try (WritableColumnVector column =
        new VeloxWritableColumnVector(ptrs[0], 0L, 2, new DecimalType(precision, 2))) {
      column.putNull(1);
      assertThrows(NullPointerException.class, () -> column.putDecimal(0, null, precision));
      column.putDecimal(1, dec("-1.23", precision, 2), precision);
      assertTrue(column.isNullAt(1), "putDecimal must not clear an existing null bit");
      assertEquals(1, column.numNulls());
      assertNull(column.getDecimal(1, precision, 2));
    } finally {
      freeDescriptor(ptrs);
    }
  }

  // -------------------------------------------------------------------------
  // SHORT decimal (DecimalType(10,2)) -- int64 unscaled, 8 bytes/row.
  // -------------------------------------------------------------------------

  @Test
  public void updateShortDecimalWritesInt64Unscaled() {
    int capacity = 5;
    DecimalType type = new DecimalType(10, 2);
    long[] ptrs = allocDecimalDescriptor(capacity, 8);
    try {
      VeloxWritableColumnVector cv = new VeloxWritableColumnVector(ptrs[0], 0L, capacity, type);
      VeloxColumnarRow row = new VeloxColumnarRow(new VeloxWritableColumnVector[] {cv});

      Decimal[] values = {
        dec("123.45", 10, 2),
        dec("-67.89", 10, 2),
        dec("0.00", 10, 2),
        dec("99999999.99", 10, 2),
        dec("-100.01", 10, 2)
      };

      for (int i = 0; i < values.length; i++) {
        row.rowId = i;
        row.update(0, values[i]);
      }

      for (int i = 0; i < values.length; i++) {
        // Raw int64 unscaled at 8B stride.
        long rawUnscaled = Platform.getLong(null, ptrs[3] + (long) i * 8);
        assertEquals(
            values[i].toUnscaledLong(),
            rawUnscaled,
            "short decimal raw int64 unscaled must match at row " + i);
        // Read-back via the row view.
        row.rowId = i;
        assertEquals(values[i], row.getDecimal(0, 10, 2), "short decimal round-trip at row " + i);
        assertFalse(cv.isNullAt(i), "row " + i + " must not be null");
      }
    } finally {
      freeDescriptor(ptrs);
    }
  }

  @Test
  public void setDecimalShortDispatchesToInt64Write() {
    int capacity = 3;
    DecimalType type = new DecimalType(10, 2);
    long[] ptrs = allocDecimalDescriptor(capacity, 8);
    try {
      VeloxWritableColumnVector cv = new VeloxWritableColumnVector(ptrs[0], 0L, capacity, type);
      VeloxColumnarRow row = new VeloxColumnarRow(new VeloxWritableColumnVector[] {cv});

      Decimal[] values = {dec("42.42", 10, 2), dec("-1.00", 10, 2), dec("0.00", 10, 2)};
      for (int i = 0; i < values.length; i++) {
        row.rowId = i;
        // MutableProjection path: typed setter, not update().
        row.setDecimal(0, values[i], 10);
      }

      for (int i = 0; i < values.length; i++) {
        long rawUnscaled = Platform.getLong(null, ptrs[3] + (long) i * 8);
        assertEquals(
            values[i].toUnscaledLong(),
            rawUnscaled,
            "setDecimal short must write int64 unscaled at row " + i);
      }
    } finally {
      freeDescriptor(ptrs);
    }
  }

  // -------------------------------------------------------------------------
  // LONG decimal (DecimalType(38,10)) -- int128 unscaled, 16 bytes/row.
  // The write (bigIntegerToInt128) must be the exact inverse of the Task-3
  // read (DecimalAccessor.int128ToBigInteger).
  // -------------------------------------------------------------------------

  private static final String[] LONG_DEC_STRINGS = {
    "12345678901234567890.1234567890", // positive mid magnitude
    "-98765432109876543210.9876543210", // negative mid magnitude
    "0.0000000000", // zero
    "1.0000000000", // +1 unscaled 10^10
    "-1.0000000000", // -1
    "1234567890123456789012345678.9012345678", // near-10^37 (38 digits)
    "-1234567890123456789012345678.9012345678" // negative near-10^37
  };

  @Test
  public void updateLongDecimalWritesInt128InverseOfRead() {
    int capacity = LONG_DEC_STRINGS.length;
    DecimalType type = new DecimalType(38, 10);
    long[] ptrs = allocDecimalDescriptor(capacity, 16);
    try {
      VeloxWritableColumnVector cv = new VeloxWritableColumnVector(ptrs[0], 0L, capacity, type);
      VeloxColumnarRow row = new VeloxColumnarRow(new VeloxWritableColumnVector[] {cv});

      Decimal[] values = new Decimal[capacity];
      for (int i = 0; i < capacity; i++) {
        values[i] = dec(LONG_DEC_STRINGS[i], 38, 10);
        row.rowId = i;
        row.update(0, values[i]);
      }

      for (int i = 0; i < capacity; i++) {
        BigInteger expectedUnscaled = values[i].toJavaBigDecimal().unscaledValue();
        long base = ptrs[3] + (long) i * 16;
        long lo = Platform.getLong(null, base);
        long hi = Platform.getLong(null, base + 8);
        // Compose the write output through the Task-3 read helper: must be identity.
        BigInteger reconstructed = DecimalAccessor.int128ToBigInteger(lo, hi);
        assertEquals(
            expectedUnscaled,
            reconstructed,
            "int128 write must be exact inverse of int128ToBigInteger at row " + i);
        // Read-back via the row view.
        row.rowId = i;
        assertEquals(values[i], row.getDecimal(0, 38, 10), "long decimal round-trip at row " + i);
      }
    } finally {
      freeDescriptor(ptrs);
    }
  }

  @Test
  public void setDecimalLongDispatchesToInt128Write() {
    int capacity = LONG_DEC_STRINGS.length;
    DecimalType type = new DecimalType(38, 10);
    long[] ptrs = allocDecimalDescriptor(capacity, 16);
    try {
      VeloxWritableColumnVector cv = new VeloxWritableColumnVector(ptrs[0], 0L, capacity, type);
      VeloxColumnarRow row = new VeloxColumnarRow(new VeloxWritableColumnVector[] {cv});

      Decimal[] values = new Decimal[capacity];
      for (int i = 0; i < capacity; i++) {
        values[i] = dec(LONG_DEC_STRINGS[i], 38, 10);
        row.rowId = i;
        // MutableProjection path: typed setter, not update().
        row.setDecimal(0, values[i], 38);
      }

      for (int i = 0; i < capacity; i++) {
        BigInteger expectedUnscaled = values[i].toJavaBigDecimal().unscaledValue();
        long base = ptrs[3] + (long) i * 16;
        long lo = Platform.getLong(null, base);
        long hi = Platform.getLong(null, base + 8);
        assertEquals(
            expectedUnscaled,
            DecimalAccessor.int128ToBigInteger(lo, hi),
            "setDecimal long must write int128 inverse of read at row " + i);
      }
    } finally {
      freeDescriptor(ptrs);
    }
  }

  /**
   * Direct round-trip identity check on the helper pair: {@code bigIntegerToInt128} composed with
   * {@code int128ToBigInteger} must be the identity for negative, zero and near-10^37 magnitudes.
   */
  @Test
  public void bigIntegerToInt128RoundTripIsIdentity() {
    BigInteger[] samples = {
      BigInteger.ZERO,
      BigInteger.ONE,
      BigInteger.valueOf(-1),
      BigInteger.TEN.pow(37), // near max magnitude
      BigInteger.TEN.pow(37).negate(),
      new BigInteger("12345678901234567890123456789012345678"),
      new BigInteger("-12345678901234567890123456789012345678")
    };
    for (BigInteger bi : samples) {
      long[] loHi = DecimalAccessor.bigIntegerToInt128(bi);
      BigInteger back = DecimalAccessor.int128ToBigInteger(loHi[0], loHi[1]);
      assertEquals(bi, back, "bigIntegerToInt128 -> int128ToBigInteger must be identity for " + bi);
    }
  }

  @Test
  public void updateNullDecimalSetsNull() {
    int capacity = 2;
    DecimalType type = new DecimalType(38, 10);
    long[] ptrs = allocDecimalDescriptor(capacity, 16);
    try {
      VeloxWritableColumnVector cv = new VeloxWritableColumnVector(ptrs[0], 0L, capacity, type);
      VeloxColumnarRow row = new VeloxColumnarRow(new VeloxWritableColumnVector[] {cv});

      row.rowId = 0;
      row.update(0, dec("1.0000000000", 38, 10));
      row.rowId = 1;
      row.update(0, null);

      assertFalse(cv.isNullAt(0), "row 0 must not be null");
      assertTrue(cv.isNullAt(1), "row 1 must be null after update(null)");
    } finally {
      freeDescriptor(ptrs);
    }
  }
}
