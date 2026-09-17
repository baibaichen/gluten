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
import org.apache.spark.sql.types.StructType;
import org.apache.spark.unsafe.Platform;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * TDD tests for TIMESTAMP (16-byte {int64 seconds, uint64 nanos} struct) zero-copy read via {@link
 * VeloxColumnVector}.
 *
 * <p>Velox stores TIMESTAMP columns as packed 16-byte structs: {@code int64 seconds} at offset 0
 * and {@code uint64 nanos} at offset 8. Spark's {@code TimestampType} is a signed {@code int64}
 * count of microseconds since the Unix epoch. The conversion is:
 *
 * <pre>
 *   micros = seconds * 1_000_000L + nanos / 1_000L
 * </pre>
 *
 * <p>No native library is loaded. Descriptors are built in Java-allocated off-heap memory via
 * {@link Platform} (i.e. {@code sun.misc.Unsafe}). Memory is freed manually after each test rather
 * than via {@link VeloxColumnVector#close()} to avoid invoking the native {@link
 * VeloxColumnHandleJniWrapper#freeDescriptor} on Java-allocated addresses.
 *
 * <p>Edge cases covered:
 *
 * <ul>
 *   <li>Normal post-epoch timestamp (row 0)
 *   <li>Pre-epoch timestamp with negative seconds (row 1): conversion must produce a negative
 *       micros value
 *   <li>Sub-microsecond nanos (row 2): nanos=1500 truncates to 1 microsecond (not rounded)
 *   <li>Null row (row 3): marked null in the validity bitmap; {@code isNullAt} must return {@code
 *       true}
 * </ul>
 */
public class VeloxColumnVectorTimestampTest {

  /**
   * Verifies that a flat TIMESTAMP column (16B/row) is read correctly as microseconds-since-epoch
   * via {@link VeloxColumnVector#getLong}, including pre-epoch and sub-micro rows plus a null.
   *
   * <p>Rows:
   *
   * <ul>
   *   <li>Row 0: seconds=1_000_000, nanos=500_000 → micros = 1_000_000*1e6 + 500 =
   *       1_000_000_000_500
   *   <li>Row 1: seconds=-1, nanos=999_000_000 → micros = -1*1e6 + 999_000 = -1000
   *   <li>Row 2: seconds=0, nanos=1_500 → micros = 0 + 1 (truncated, not 2)
   *   <li>Row 3: null (validity bit cleared)
   * </ul>
   */
  @Test
  public void timestampColumnReadsMicrosViaUnsafe() {
    // Row definitions: {seconds, nanos}
    long[][] rows = {
      {1_000_000L, 500_000L}, // row 0: normal post-epoch
      {-1L, 999_000_000L}, // row 1: pre-epoch, negative seconds
      {0L, 1_500L}, // row 2: sub-micro nanos (1500 -> truncates to 1 us)
      {0L, 0L} // row 3: null (value irrelevant)
    };
    int numRows = rows.length;

    // --- values buffer: 16 bytes per row ---
    long values = Platform.allocateMemory((long) numRows * 16);
    for (int i = 0; i < numRows; i++) {
      long base = values + (long) i * 16;
      Platform.putLong(null, base, rows[i][0]); // seconds
      Platform.putLong(null, base + 8, rows[i][1]); // nanos
    }

    // --- nulls bitmap: all valid except row 3 ---
    // 4 rows → 1 byte (bits 0..3); set all bits then clear bit 3.
    long nulls = Platform.allocateMemory(1);
    Platform.putByte(null, nulls, (byte) 0x0F); // bits 0,1,2,3 all set = valid
    // Clear bit 3: row 3 is null (Velox validity: 1=valid, 0=null).
    Platform.putByte(null, nulls, (byte) (0x0F & ~(1 << 3))); // = 0x07

    // --- buffers array: [nulls ptr, values ptr] ---
    long buffers = Platform.allocateMemory(2 * 8);
    Platform.putLong(null, buffers, nulls); // buffers[0] = nulls
    Platform.putLong(null, buffers + 8, values); // buffers[1] = values

    // --- descriptor ---
    long h = Platform.allocateMemory(48);
    Platform.putLong(null, h + 0, (long) numRows); // length
    Platform.putLong(null, h + 8, 1L); // nullCount = 1
    Platform.putLong(null, h + 16, 2L); // nBuffers
    Platform.putLong(null, h + 24, 0L); // nChildren
    Platform.putLong(null, h + 32, buffers); // buffers ptr
    Platform.putLong(null, h + 40, 0L); // children ptr

    // importFromNative with TimestampType — must NOT throw UnsupportedOperationException
    VeloxColumnVector cv = VeloxColumnVector.importFromNative(h, DataTypes.TimestampType);

    // Row 0: normal timestamp.
    long expectedRow0 = 1_000_000L * 1_000_000L + 500_000L / 1_000L;
    assertEquals(expectedRow0, cv.getLong(0), "Row 0: post-epoch micros mismatch");
    assertFalse(cv.isNullAt(0), "Row 0 should not be null");

    // Row 1: pre-epoch. micros = -1*1_000_000 + 999_000_000/1_000 = -1_000_000 + 999_000 = -1000
    long expectedRow1 = -1L * 1_000_000L + 999_000_000L / 1_000L;
    assertEquals(-1000L, expectedRow1, "Expected pre-epoch micros = -1000");
    assertEquals(expectedRow1, cv.getLong(1), "Row 1: pre-epoch micros mismatch");
    assertFalse(cv.isNullAt(1), "Row 1 should not be null");

    // Row 2: sub-micro truncation. nanos=1500 -> 1500/1000 = 1 (truncated).
    long expectedRow2 = 0L * 1_000_000L + 1_500L / 1_000L; // = 1
    assertEquals(1L, expectedRow2, "Expected sub-micro truncated micros = 1");
    assertEquals(expectedRow2, cv.getLong(2), "Row 2: sub-micro truncation mismatch");
    assertFalse(cv.isNullAt(2), "Row 2 should not be null");

    // Row 3: null.
    assertTrue(cv.isNullAt(3), "Row 3 should be null");

    // Null metadata.
    assertTrue(cv.hasNull(), "Column should have nulls");

    // Free manually — do NOT call cv.close() as that would try to native-free h.
    Platform.freeMemory(h);
    Platform.freeMemory(buffers);
    Platform.freeMemory(nulls);
    Platform.freeMemory(values);
  }

  /**
   * Verifies that {@link VeloxInputBatch#canImport} returns {@code true} for a schema containing a
   * single flat TimestampType leaf.
   */
  @Test
  public void canImportFlatTimestampSchema() {
    StructType schema = new StructType().add("ts", DataTypes.TimestampType);
    assertTrue(VeloxInputBatch.canImport(schema), "canImport should admit flat TimestampType leaf");
  }

  /**
   * Verifies that {@link VeloxInputBatch#canImport} returns {@code true} for a schema containing an
   * {@code ARRAY<TimestampType>} — i.e., nested TimestampType is recursively admitted.
   */
  @Test
  public void canImportNestedArrayTimestampSchema() {
    StructType schema =
        new StructType().add("a", DataTypes.createArrayType(DataTypes.TimestampType));
    assertTrue(
        VeloxInputBatch.canImport(schema),
        "canImport should admit ARRAY<TimestampType> nested leaf");
  }
}
