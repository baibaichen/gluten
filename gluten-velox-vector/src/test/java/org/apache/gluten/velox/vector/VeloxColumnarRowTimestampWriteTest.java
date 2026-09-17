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
 * TDD tests for TIMESTAMP write (Velox 16-byte {sec, nanos} struct, micros-since-epoch) via {@link
 * VeloxColumnarRow#update} and {@link VeloxWritableColumnVector#putTimestampMicros}.
 *
 * <p>RED: before {@code TimestampType} is handled in {@link VeloxColumnarRow#update}, calling
 * {@code row.update(0, micros)} with a TimestampType column throws {@link
 * UnsupportedOperationException}. Also, {@link VeloxWritableColumnVector#getLong} would use 8-byte
 * stride instead of 16-byte, producing wrong values.
 *
 * <p>GREEN: after implementation, micros round-trip correctly through the 16-byte Velox Timestamp
 * struct. Pre-epoch (negative micros) values are handled via floor-division / floor-modulo.
 *
 * <p>No native library is loaded. Descriptors are built in Java-allocated off-heap memory via
 * {@link Platform} (i.e. {@code sun.misc.Unsafe}). Memory is freed manually after each test.
 */
public class VeloxColumnarRowTimestampWriteTest {

  /**
   * Allocates a VeloxColumnHandle descriptor backed by a 2-pointer buffers array, a nulls bitmap
   * (all-valid), and a 16-byte-per-row values buffer (Velox Timestamp layout).
   *
   * @param capacity number of rows
   * @return {@code long[4] = {desc, buffers, nulls, values}} -- all must be freed by the caller
   */
  private static long[] allocTimestampDescriptor(int capacity) {
    int nullsBytes = Math.max(8, ((capacity + 7) / 8 + 7) & ~7);
    long nulls = Platform.allocateMemory(nullsBytes);
    for (int i = 0; i < nullsBytes; i++) {
      Platform.putByte(null, nulls + i, (byte) 0xFF);
    }

    // 16 bytes per row for Velox Timestamp {int64 sec; uint64 nanos}
    long values = Platform.allocateMemory(Math.max(16L, (long) capacity * 16));

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
   * Reads the raw {sec, nanos} pair from the Velox Timestamp buffer at the given row.
   *
   * @param valuesAddr base address of the values buffer
   * @param rowId row index
   * @return {@code long[2] = {sec, nanos}}
   */
  private static long[] readRawTimestamp(long valuesAddr, int rowId) {
    long base = valuesAddr + (long) rowId * 16;
    long sec = Platform.getLong(null, base);
    long nanos = Platform.getLong(null, base + 8);
    return new long[] {sec, nanos};
  }

  /**
   * Verifies that {@link VeloxColumnarRow#update} writes a normal post-epoch TimestampType micros
   * value as the correct 16-byte {sec, nanos} Velox Timestamp struct and that {@link
   * VeloxWritableColumnVector#getLong} reconstructs the original micros value.
   *
   * <p>RED state: {@code row.update(0, micros)} throws {@link UnsupportedOperationException} since
   * TimestampType is not handled and/or {@link VeloxWritableColumnVector#getLong} returns wrong
   * value from 8-byte stride.
   *
   * <p>GREEN state: the Velox Timestamp layout is written correctly and micros round-trip.
   */
  @Test
  public void updateTimestampPostEpochWritesCorrect16ByteStruct() {
    int capacity = 4;
    long[] ptrs = allocTimestampDescriptor(capacity);
    try {
      VeloxWritableColumnVector cv =
          new VeloxWritableColumnVector(ptrs[0], 0L, capacity, DataTypes.TimestampType);
      VeloxColumnarRow row = new VeloxColumnarRow(new VeloxWritableColumnVector[] {cv});

      // 2022-01-13T00:00:00Z in micros = 1641945600 * 1_000_000
      long micros = 1641945600L * 1_000_000L;
      row.rowId = 0;
      row.update(0, micros);

      // Verify raw 16-byte struct
      long expectedSec = Math.floorDiv(micros, 1_000_000L);
      long expectedNanos = Math.floorMod(micros, 1_000_000L) * 1_000L;
      long[] raw = readRawTimestamp(ptrs[3], 0);
      assertEquals(expectedSec, raw[0], "sec field must equal floorDiv(micros, 1_000_000)");
      assertEquals(
          expectedNanos, raw[1], "nanos field must equal floorMod(micros, 1_000_000)*1000");

      // Verify round-trip via getLong
      assertEquals(
          micros, cv.getLong(0), "getLong must reconstruct original micros via 16B stride");
      assertFalse(cv.isNullAt(0), "row 0 must not be null after update");
    } finally {
      freeDescriptor(ptrs);
    }
  }

  /**
   * Verifies that pre-epoch (negative micros) timestamps are handled correctly by floor-division /
   * floor-modulo decomposition.
   *
   * <p>For example, {@code micros = -1} (1 microsecond before epoch) must decompose as {@code sec =
   * -1, nanos = 999_999_000} (not {@code sec = 0, nanos = -1000}).
   */
  @Test
  public void updateTimestampPreEpochUsesFloorDivMod() {
    int capacity = 4;
    long[] ptrs = allocTimestampDescriptor(capacity);
    try {
      VeloxWritableColumnVector cv =
          new VeloxWritableColumnVector(ptrs[0], 0L, capacity, DataTypes.TimestampType);
      VeloxColumnarRow row = new VeloxColumnarRow(new VeloxWritableColumnVector[] {cv});

      // -1 microsecond (1 us before epoch): sec=-1, nanos=999_999_000
      long micros0 = -1L;
      row.rowId = 0;
      row.update(0, micros0);
      long[] raw0 = readRawTimestamp(ptrs[3], 0);
      assertEquals(-1L, raw0[0], "sec for micros=-1 must be -1 (floor division)");
      assertEquals(999_999_000L, raw0[1], "nanos for micros=-1 must be 999_999_000 (floor modulo)");
      assertEquals(micros0, cv.getLong(0), "getLong must round-trip micros=-1");

      // -1_000_001 micros (~1 second and 1 microsecond before epoch)
      long micros1 = -1_000_001L;
      row.rowId = 1;
      row.update(0, micros1);
      long expectedSec1 = Math.floorDiv(micros1, 1_000_000L); // -2
      long expectedNanos1 = Math.floorMod(micros1, 1_000_000L) * 1_000L; // 999_000 * 1000
      long[] raw1 = readRawTimestamp(ptrs[3], 1);
      assertEquals(expectedSec1, raw1[0], "sec for pre-epoch must use floor division");
      assertEquals(expectedNanos1, raw1[1], "nanos for pre-epoch must use floor modulo");
      assertEquals(micros1, cv.getLong(1), "getLong must round-trip pre-epoch micros");

      // Epoch itself: sec=0, nanos=0
      row.rowId = 2;
      row.update(0, 0L);
      long[] raw2 = readRawTimestamp(ptrs[3], 2);
      assertEquals(0L, raw2[0], "sec for epoch must be 0");
      assertEquals(0L, raw2[1], "nanos for epoch must be 0");
      assertEquals(0L, cv.getLong(2), "getLong must round-trip epoch=0");
    } finally {
      freeDescriptor(ptrs);
    }
  }

  /**
   * Verifies that multiple TIMESTAMP rows (including sub-microsecond-precision post-epoch,
   * pre-epoch, and null) can be written and read back correctly.
   */
  @Test
  public void updateTimestampMultipleRowsIncludingNull() {
    int capacity = 4;
    long[] ptrs = allocTimestampDescriptor(capacity);
    try {
      VeloxWritableColumnVector cv =
          new VeloxWritableColumnVector(ptrs[0], 0L, capacity, DataTypes.TimestampType);
      VeloxColumnarRow row = new VeloxColumnarRow(new VeloxWritableColumnVector[] {cv});

      long[] microsValues = {
        1641945600L * 1_000_000L + 500L, // post-epoch with sub-second micros
        -1L, // 1 us before epoch
        0L, // epoch
      };

      for (int i = 0; i < microsValues.length; i++) {
        row.rowId = i;
        row.update(0, microsValues[i]);
      }
      // Write null at row 3
      row.rowId = 3;
      row.update(0, null);

      for (int i = 0; i < microsValues.length; i++) {
        assertFalse(cv.isNullAt(i), "row " + i + " must not be null");
        assertEquals(
            microsValues[i], cv.getLong(i), "TIMESTAMP micros must round-trip at row " + i);
      }
      assertTrue(cv.isNullAt(3), "row 3 must be null after update(null)");
    } finally {
      freeDescriptor(ptrs);
    }
  }

  /**
   * Verifies that {@link VeloxColumnarRow#get} with {@code TimestampType} returns the micros value.
   */
  @Test
  public void getTimestampReturnsMicrosValue() {
    int capacity = 2;
    long[] ptrs = allocTimestampDescriptor(capacity);
    try {
      VeloxWritableColumnVector cv =
          new VeloxWritableColumnVector(ptrs[0], 0L, capacity, DataTypes.TimestampType);
      VeloxColumnarRow row = new VeloxColumnarRow(new VeloxWritableColumnVector[] {cv});

      long micros = 1641945600L * 1_000_000L;
      row.rowId = 0;
      row.update(0, micros);
      row.rowId = 1;
      row.update(0, -1L);

      row.rowId = 0;
      assertEquals(
          micros,
          row.get(0, DataTypes.TimestampType),
          "get(TimestampType) must return micros for post-epoch");
      row.rowId = 1;
      assertEquals(
          -1L,
          row.get(0, DataTypes.TimestampType),
          "get(TimestampType) must return -1 for pre-epoch micros=-1");
    } finally {
      freeDescriptor(ptrs);
    }
  }

  /**
   * Verifies that {@link VeloxColumnarRow#setLong} for a {@link
   * org.apache.spark.sql.types.TimestampType} column correctly dispatches to {@link
   * VeloxWritableColumnVector#putTimestampMicros} (16B stride), not to the generic 8B {@code
   * putLong}.
   *
   * <p>Spark's {@code MutableProjection} calls the typed {@code setLong} setter for all {@code
   * long}-valued column types, including {@code TimestampType}. This test verifies the critical
   * dispatch so that micros are correctly decomposed into {sec, nanos}.
   */
  @Test
  public void setLongForTimestampTypeUses16BStride() {
    int capacity = 2;
    long[] ptrs = allocTimestampDescriptor(capacity);
    try {
      VeloxWritableColumnVector cv =
          new VeloxWritableColumnVector(ptrs[0], 0L, capacity, DataTypes.TimestampType);
      VeloxColumnarRow row = new VeloxColumnarRow(new VeloxWritableColumnVector[] {cv});

      long micros0 = 1641945600L * 1_000_000L + 500_000L;
      long micros1 = -1L;

      row.rowId = 0;
      row.setLong(0, micros0);
      row.rowId = 1;
      row.setLong(0, micros1);

      // Verify raw 16B struct for row 0
      long[] raw0 = readRawTimestamp(ptrs[3], 0);
      assertEquals(Math.floorDiv(micros0, 1_000_000L), raw0[0], "row 0 sec must use floorDiv");
      assertEquals(
          Math.floorMod(micros0, 1_000_000L) * 1_000L,
          raw0[1],
          "row 0 nanos must use floorMod*1000");

      // Verify raw 16B struct for row 1 (pre-epoch)
      long[] raw1 = readRawTimestamp(ptrs[3], 1);
      assertEquals(-1L, raw1[0], "row 1 sec must be -1 (floor division of -1 micros)");
      assertEquals(
          999_999_000L, raw1[1], "row 1 nanos must be 999_999_000 (floor modulo of -1 micros)");

      // Verify round-trip via getLong
      assertEquals(micros0, cv.getLong(0), "getLong must round-trip row 0 via 16B stride");
      assertEquals(micros1, cv.getLong(1), "getLong must round-trip row 1 via 16B stride");
    } finally {
      freeDescriptor(ptrs);
    }
  }
}
