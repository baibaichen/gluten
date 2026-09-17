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

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * TDD tests for BOOLEAN (bit-packed) zero-copy read via {@link VeloxColumnVector}.
 *
 * <p>Velox stores boolean columns bit-packed (1 bit/row, LSB-first within each byte), identical in
 * layout to the validity bitmap. Bit {@code r} lives at byte {@code r >> 3}, bit position {@code r
 * & 7}.
 *
 * <p>The test exercises byte-boundary rows: rows 0, 7, 8, 9, 15 are {@code true}; all others are
 * {@code false}. Row 3 is marked null in the validity bitmap. No native library is loaded —
 * descriptors are built via {@link Platform} (i.e. {@code sun.misc.Unsafe}) and freed manually.
 */
public class VeloxColumnVectorBooleanTest {

  /**
   * Bit-set helper: set bit {@code r} in a byte array (LSB-first, matching Velox / Arrow layout).
   *
   * @param mem base address of byte buffer
   * @param r row index whose bit to set
   */
  private static void setBit(long mem, int r) {
    long byteAddr = mem + (r >> 3);
    byte cur = Platform.getByte(null, byteAddr);
    Platform.putByte(null, byteAddr, (byte) (cur | (1 << (r & 7))));
  }

  /**
   * Verifies that a flat BOOLEAN column (bit-packed, 16 rows) is read correctly via {@link
   * VeloxColumnVector#getBoolean}, including byte-boundary rows and a null entry.
   *
   * <p>True rows: 0, 7, 8, 9, 15. Null row: 3 (nullCount = 1; all other rows are non-null so the
   * validity bitmap has all bits set except bit 3).
   */
  @Test
  public void booleanColumnReadsBitPackedValuesViaUnsafe() {
    int numRows = 16;
    // 16 rows → 2 bytes for bit-packed values buffer
    int byteCount = (numRows + 7) >> 3; // = 2

    // --- values buffer: set bits for true rows ---
    long values = Platform.allocateMemory(byteCount);
    Platform.setMemory(null, values, byteCount, (byte) 0);
    int[] trueRows = {0, 7, 8, 9, 15};
    for (int r : trueRows) {
      setBit(values, r);
    }

    // --- nulls bitmap: all valid except row 3 ---
    long nulls = Platform.allocateMemory(byteCount);
    Platform.setMemory(null, nulls, byteCount, (byte) 0xFF); // all valid initially
    // clear bit 3 → row 3 is null (Velox validity bitmap: 1 = valid, 0 = null)
    byte b0 = Platform.getByte(null, nulls);
    Platform.putByte(null, nulls, (byte) (b0 & ~(1 << 3)));

    // --- buffers array: [nulls ptr, values ptr] ---
    long buffers = Platform.allocateMemory(2 * 8);
    Platform.putLong(null, buffers, nulls); // buffers[0] = nulls
    Platform.putLong(null, buffers + 8, values); // buffers[1] = values

    // --- descriptor: {length, nullCount, nBuffers, nChildren, buffersPtr, childrenPtr} ---
    long h = Platform.allocateMemory(48);
    Platform.putLong(null, h + 0, (long) numRows); // length
    Platform.putLong(null, h + 8, 1L); // nullCount = 1
    Platform.putLong(null, h + 16, 2L); // nBuffers
    Platform.putLong(null, h + 24, 0L); // nChildren
    Platform.putLong(null, h + 32, buffers); // buffers ptr
    Platform.putLong(null, h + 40, 0L); // children ptr

    VeloxColumnVector cv = VeloxColumnVector.importFromNative(h, DataTypes.BooleanType);

    // --- assert bit values for all 16 rows ---
    boolean[] expected = new boolean[numRows]; // default false
    for (int r : trueRows) {
      expected[r] = true;
    }
    for (int r = 0; r < numRows; r++) {
      if (r == 3) {
        // null row — skip value assertion, just check isNullAt
        continue;
      }
      if (expected[r]) {
        assertTrue(cv.getBoolean(r), "Expected true at row " + r);
      } else {
        assertFalse(cv.getBoolean(r), "Expected false at row " + r);
      }
    }

    // --- assert nulls ---
    assertTrue(cv.hasNull(), "Column should have nulls");
    assertTrue(cv.isNullAt(3), "Row 3 should be null");
    assertFalse(cv.isNullAt(0), "Row 0 should not be null");
    assertFalse(cv.isNullAt(15), "Row 15 should not be null");

    // Free manually — do NOT call cv.close() as that would try to native-free h.
    Platform.freeMemory(h);
    Platform.freeMemory(buffers);
    Platform.freeMemory(nulls);
    Platform.freeMemory(values);
  }

  /**
   * Verifies that {@link VeloxInputBatch#canImport} returns {@code true} for a schema containing a
   * flat BooleanType leaf.
   */
  @Test
  public void canImportFlatBooleanSchema() {
    StructType schema = new StructType().add("b", DataTypes.BooleanType);
    assertTrue(VeloxInputBatch.canImport(schema), "canImport should admit BooleanType leaf");
  }

  /**
   * Verifies that {@link VeloxInputBatch#canImport} returns {@code true} for a schema containing an
   * ARRAY&lt;BOOLEAN&gt; — i.e., nested BooleanType is recursively admitted.
   */
  @Test
  public void canImportNestedArrayBooleanSchema() {
    StructType schema = new StructType().add("a", DataTypes.createArrayType(DataTypes.BooleanType));
    assertTrue(VeloxInputBatch.canImport(schema), "canImport should admit ARRAY<BooleanType> leaf");
  }
}
