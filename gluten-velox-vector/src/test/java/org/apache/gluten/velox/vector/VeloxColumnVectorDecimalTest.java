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
import org.apache.spark.sql.types.Decimal;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.unsafe.Platform;
import org.junit.jupiter.api.Test;

import java.math.BigInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * TDD tests for DECIMAL zero-copy read via {@link VeloxColumnVector}.
 *
 * <p>Velox DECIMAL is a logical type over a physical backing:
 *
 * <ul>
 *   <li><b>Short decimal</b> ({@code precision <= 18}): backed by {@code BIGINT} (int64 unscaled, 8
 *       bytes/row).
 *   <li><b>Long decimal</b> ({@code precision > 18}): backed by {@code HUGEINT} (int128 unscaled,
 *       16 bytes/row, little-endian two's complement).
 * </ul>
 *
 * <p>No native library is loaded. Descriptors are built in Java-allocated off-heap memory via
 * {@link Platform} (i.e. {@code sun.misc.Unsafe}). Memory is freed manually after each test rather
 * than via {@link VeloxColumnVector#close()} to avoid invoking the native {@link
 * VeloxColumnHandleJniWrapper#freeDescriptor} on Java-allocated addresses.
 *
 * <p>The int128 path is the correctness-critical case: negatives must round-trip exactly through
 * the little-endian two's-complement encoding, so this test asserts negative, zero and near-{@code
 * 10^37} values for the long-decimal column.
 */
public class VeloxColumnVectorDecimalTest {

  /**
   * Verifies a flat SHORT decimal column ({@code p=10, s=2}, int64 unscaled, 8B/row) reads back
   * exactly via {@link VeloxColumnVector#getDecimal}, including a negative, a zero and a null row.
   *
   * <p>Rows (unscaled int64):
   *
   * <ul>
   *   <li>Row 0: 12345 -> 123.45
   *   <li>Row 1: -6789 -> -67.89
   *   <li>Row 2: 0 -> 0.00
   *   <li>Row 3: null
   * </ul>
   */
  @Test
  public void shortDecimalColumnReadsViaUnsafe() {
    int precision = 10;
    int scale = 2;
    long[] unscaled = {12345L, -6789L, 0L, 0L /* null */};
    int numRows = unscaled.length;

    // values buffer: 8 bytes per row.
    long values = Platform.allocateMemory((long) numRows * 8);
    for (int i = 0; i < numRows; i++) {
      Platform.putLong(null, values + (long) i * 8, unscaled[i]);
    }

    // nulls bitmap: 4 rows -> 1 byte; all valid except row 3.
    long nulls = Platform.allocateMemory(1);
    Platform.putByte(null, nulls, (byte) (0x0F & ~(1 << 3))); // = 0x07

    long buffers = Platform.allocateMemory(2 * 8);
    Platform.putLong(null, buffers, nulls);
    Platform.putLong(null, buffers + 8, values);

    long h = allocDesc(numRows, 1L, 2L, 0L, buffers, 0L);

    VeloxColumnVector cv =
        VeloxColumnVector.importFromNative(h, DataTypes.createDecimalType(precision, scale));

    assertEquals(
        Decimal.createUnsafe(12345L, precision, scale),
        cv.getDecimal(0, precision, scale),
        "Row 0 short decimal mismatch");
    assertEquals(
        Decimal.createUnsafe(-6789L, precision, scale),
        cv.getDecimal(1, precision, scale),
        "Row 1 negative short decimal mismatch");
    assertEquals(
        Decimal.createUnsafe(0L, precision, scale),
        cv.getDecimal(2, precision, scale),
        "Row 2 zero short decimal mismatch");
    assertTrue(cv.isNullAt(3), "Row 3 should be null");
    assertTrue(cv.hasNull(), "Column should have nulls");

    Platform.freeMemory(h);
    Platform.freeMemory(buffers);
    Platform.freeMemory(nulls);
    Platform.freeMemory(values);
  }

  /**
   * Verifies a flat LONG decimal column ({@code p=38, s=10}, int128 unscaled, 16B/row little-endian
   * two's complement) reads back exactly via {@link VeloxColumnVector#getDecimal}, including a
   * large value near {@code 10^37}, its negation, zero and a null row.
   */
  @Test
  public void longDecimalColumnReadsViaUnsafe() {
    int precision = 38;
    int scale = 10;
    // 37 nines ~ 10^37
    BigInteger nearMax = new BigInteger("9999999999999999999999999999999999999");
    BigInteger[] unscaled = {
      nearMax, nearMax.negate(), BigInteger.ZERO, BigInteger.ZERO /* null */
    };
    int numRows = unscaled.length;

    // values buffer: 16 bytes per row, little-endian two's complement.
    long values = Platform.allocateMemory((long) numRows * 16);
    for (int i = 0; i < numRows; i++) {
      put16LE(values + (long) i * 16, unscaled[i]);
    }

    long nulls = Platform.allocateMemory(1);
    Platform.putByte(null, nulls, (byte) (0x0F & ~(1 << 3))); // = 0x07

    long buffers = Platform.allocateMemory(2 * 8);
    Platform.putLong(null, buffers, nulls);
    Platform.putLong(null, buffers + 8, values);

    long h = allocDesc(numRows, 1L, 2L, 0L, buffers, 0L);

    VeloxColumnVector cv =
        VeloxColumnVector.importFromNative(h, DataTypes.createDecimalType(precision, scale));

    assertEquals(
        expectedLong(nearMax, precision, scale),
        cv.getDecimal(0, precision, scale),
        "Row 0 near-1e37 long decimal mismatch");
    assertEquals(
        expectedLong(nearMax.negate(), precision, scale),
        cv.getDecimal(1, precision, scale),
        "Row 1 negative long decimal mismatch (sign/byte-order)");
    assertEquals(
        expectedLong(BigInteger.ZERO, precision, scale),
        cv.getDecimal(2, precision, scale),
        "Row 2 zero long decimal mismatch");
    assertTrue(cv.isNullAt(3), "Row 3 should be null");
    assertTrue(cv.hasNull(), "Column should have nulls");

    Platform.freeMemory(h);
    Platform.freeMemory(buffers);
    Platform.freeMemory(nulls);
    Platform.freeMemory(values);
  }

  /** Verifies {@link VeloxInputBatch#canImport} admits a flat DecimalType leaf. */
  @Test
  public void canImportFlatDecimalSchema() {
    StructType schema = new StructType().add("d", DataTypes.createDecimalType(10, 2));
    assertTrue(VeloxInputBatch.canImport(schema), "canImport should admit flat DecimalType leaf");
  }

  /** Verifies {@link VeloxInputBatch#canImport} recursively admits ARRAY&lt;DecimalType&gt;. */
  @Test
  public void canImportNestedArrayDecimalSchema() {
    StructType schema =
        new StructType().add("a", DataTypes.createArrayType(DataTypes.createDecimalType(38, 10)));
    assertTrue(
        VeloxInputBatch.canImport(schema), "canImport should admit ARRAY<DecimalType> nested leaf");
    assertFalse(schema.isEmpty(), "sanity");
  }

  // ---------------------------------------------------------------------------
  // Helpers
  // ---------------------------------------------------------------------------

  private static Decimal expectedLong(BigInteger unscaled, int precision, int scale) {
    return Decimal.apply(new java.math.BigDecimal(unscaled, scale), precision, scale);
  }

  /** Writes {@code v} as a 16-byte little-endian two's-complement value at {@code addr}. */
  private static void put16LE(long addr, BigInteger v) {
    byte[] be = v.toByteArray(); // minimal big-endian two's complement
    byte[] full = new byte[16];
    byte fill = (byte) (v.signum() < 0 ? 0xFF : 0x00);
    java.util.Arrays.fill(full, fill);
    int len = Math.min(be.length, 16);
    // right-align the minimal big-endian bytes into the 16-byte big-endian buffer
    for (int i = 0; i < len; i++) {
      full[16 - len + i] = be[be.length - len + i];
    }
    // write little-endian: reverse the big-endian buffer
    for (int i = 0; i < 16; i++) {
      Platform.putByte(null, addr + i, full[15 - i]);
    }
  }

  private static long allocDesc(
      long length, long nullCount, long nBuffers, long nChildren, long buffers, long children) {
    long h = Platform.allocateMemory(48);
    Platform.putLong(null, h + 0, length);
    Platform.putLong(null, h + 8, nullCount);
    Platform.putLong(null, h + 16, nBuffers);
    Platform.putLong(null, h + 24, nChildren);
    Platform.putLong(null, h + 32, buffers);
    Platform.putLong(null, h + 40, children);
    return h;
  }
}
