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

import org.apache.spark.sql.catalyst.util.ArrayData;
import org.apache.spark.sql.catalyst.util.GenericArrayData;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.unsafe.Platform;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * TDD tests for nested BOOLEAN write (ARRAY&lt;Boolean&gt;) via {@link VeloxColumnarRow#update}.
 *
 * <p>RED state: {@link VeloxColumnarRow#writeValue} has no BooleanType branch → writes to nested
 * BOOLEAN child throw {@link UnsupportedOperationException}.
 *
 * <p>GREEN state: the BooleanType branch is added → nested boolean values are written bit-packed
 * (LSB-first) into the child's values buffer and read back correctly via {@link
 * VeloxWritableColumnVector#getBoolean}.
 *
 * <p>No native library is loaded. All descriptors are hand-crafted in Java-allocated off-heap
 * memory via {@link Platform} (i.e. {@code sun.misc.Unsafe}). Memory is freed manually after each
 * test.
 *
 * <p>Bit addressing: row {@code r} is at byte {@code r >> 3}, bit {@code r & 7} (LSB-first).
 */
public class VeloxWritableNestedBooleanTest {

  /**
   * Allocates a hand-crafted VeloxColumnHandle descriptor for a flat BOOLEAN (bit-packed) vector.
   *
   * <p>Layout: {@code nBuffers=2}, {@code buffers[0]=nulls}, {@code buffers[1]=values}. The nulls
   * bitmap is all-valid (0xFF). The values buffer is zero-initialised.
   *
   * @param capacity number of boolean element slots
   * @return {@code long[4] = {desc, buffers, nulls, values}} — all must be freed by the caller
   */
  private static long[] allocBoolDesc(int capacity) {
    int nullsBytes = Math.max(8, ((capacity + 7) / 8 + 7) & ~7);
    long nulls = Platform.allocateMemory(nullsBytes);
    for (int i = 0; i < nullsBytes; i++) {
      Platform.putByte(null, nulls + i, (byte) 0xFF);
    }
    int valBytes = Math.max(8, ((capacity + 7) / 8 + 7) & ~7);
    long vals = Platform.allocateMemory(valBytes);
    for (int i = 0; i < valBytes; i++) {
      Platform.putByte(null, vals + i, (byte) 0x00);
    }
    long buffers = Platform.allocateMemory(16);
    Platform.putLong(null, buffers, nulls);
    Platform.putLong(null, buffers + 8, vals);
    long desc = Platform.allocateMemory(48);
    Platform.putLong(null, desc, capacity); // length
    Platform.putLong(null, desc + 8, 0L); // nullCount
    Platform.putLong(null, desc + 16, 2L); // nBuffers
    Platform.putLong(null, desc + 24, 0L); // nChildren
    Platform.putLong(null, desc + 32, buffers); // buffers ptr
    Platform.putLong(null, desc + 40, 0L); // children ptr
    return new long[] {desc, buffers, nulls, vals};
  }

  private static void freeBoolDesc(long[] ptrs) {
    Platform.freeMemory(ptrs[0]);
    Platform.freeMemory(ptrs[1]);
    Platform.freeMemory(ptrs[2]);
    Platform.freeMemory(ptrs[3]);
  }

  /**
   * Allocates a hand-crafted ARRAY descriptor with a single BOOLEAN child.
   *
   * <p>The ARRAY descriptor has {@code nBuffers=3} (nulls, offsets, sizes) and {@code nChildren=1}
   * pointing to the child BOOLEAN descriptor. The child capacity is {@code childCapacity}.
   *
   * @param arrayCapacity number of array rows (top-level)
   * @param childCapacity number of element slots in the child boolean vector
   * @return {@code long[7] = {arrayDesc, arrayBuffers, arrayNulls, offsets, sizes, childPtr, +4
   *     child ptrs...}} — allocated in a flat array for easy cleanup; the child desc ptrs are at
   *     indices 6..9.
   */
  private static long[] allocArrayBoolDesc(int arrayCapacity, int childCapacity) {
    // Child BOOLEAN descriptor (4 pointers: desc, buffers, nulls, vals)
    long[] child = allocBoolDesc(childCapacity);

    // Array offsets & sizes buffers (int32 arrays)
    int intBytes = Math.max(8, arrayCapacity * 4);
    long offsets = Platform.allocateMemory(intBytes);
    long sizes = Platform.allocateMemory(intBytes);
    for (int i = 0; i < intBytes; i++) {
      Platform.putByte(null, offsets + i, (byte) 0);
      Platform.putByte(null, sizes + i, (byte) 0);
    }
    // Array nulls bitmap (all-valid)
    int nullsBytes = Math.max(8, ((arrayCapacity + 7) / 8 + 7) & ~7);
    long arrayNulls = Platform.allocateMemory(nullsBytes);
    for (int i = 0; i < nullsBytes; i++) {
      Platform.putByte(null, arrayNulls + i, (byte) 0xFF);
    }
    // Array buffers pointer array: [nulls, offsets, sizes]
    long arrayBuffers = Platform.allocateMemory(24);
    Platform.putLong(null, arrayBuffers, arrayNulls);
    Platform.putLong(null, arrayBuffers + 8, offsets);
    Platform.putLong(null, arrayBuffers + 16, sizes);

    // Children pointer array (1 child: the BOOLEAN desc)
    long childrenPtr = Platform.allocateMemory(8);
    Platform.putLong(null, childrenPtr, child[0]);

    // Array descriptor
    long arrayDesc = Platform.allocateMemory(48);
    Platform.putLong(null, arrayDesc, arrayCapacity); // length
    Platform.putLong(null, arrayDesc + 8, 0L); // nullCount
    Platform.putLong(null, arrayDesc + 16, 3L); // nBuffers
    Platform.putLong(null, arrayDesc + 24, 1L); // nChildren
    Platform.putLong(null, arrayDesc + 32, arrayBuffers); // buffers ptr
    Platform.putLong(null, arrayDesc + 40, childrenPtr); // children ptr

    // Return: [arrayDesc, arrayBuffers, arrayNulls, offsets, sizes, childrenPtr,
    //          child[0]=childDesc, child[1]=childBuffers, child[2]=childNulls, child[3]=childVals]
    return new long[] {
      arrayDesc,
      arrayBuffers,
      arrayNulls,
      offsets,
      sizes,
      childrenPtr,
      child[0],
      child[1],
      child[2],
      child[3]
    };
  }

  private static void freeArrayBoolDesc(long[] ptrs) {
    // ptrs[0..5] = array-level allocations; ptrs[6..9] = child allocations
    for (long ptr : ptrs) {
      Platform.freeMemory(ptr);
    }
  }

  /**
   * Verifies that nested BOOLEAN elements spanning a byte boundary (10 elements, mix of true/false)
   * are written correctly via {@link VeloxColumnarRow#update} and read back bit-by-bit.
   *
   * <p>RED: throws {@link UnsupportedOperationException} from {@code writeValue} before the
   * BooleanType branch is added. GREEN: all 10 elements round-trip.
   */
  @Test
  public void arrayBooleanWriteAcrossByteBoundary() {
    // 10 boolean elements: indices 0-9, values [T,F,T,T,F,F,F,T, T,F]
    // Byte 0 covers bits 0-7; byte 1 covers bit 8-9 (i.e. index 8=T, 9=F)
    final boolean[] expected = {true, false, true, true, false, false, false, true, true, false};
    final int numElems = expected.length;

    // Generous capacity: 32 rows for the array, 64 element slots for the child
    final int arrayCapacity = 32;
    final int childCapacity = 64;
    long[] ptrs = allocArrayBoolDesc(arrayCapacity, childCapacity);
    try {
      // Build the ARRAY<BooleanType> VeloxWritableColumnVector from the hand-crafted descriptor.
      VeloxWritableColumnVector arrayCv =
          new VeloxWritableColumnVector(
              ptrs[0], 0L, arrayCapacity, DataTypes.createArrayType(DataTypes.BooleanType));

      // Build the VeloxColumnarRow with one column (the ARRAY<Bool>).
      VeloxColumnarRow row = new VeloxColumnarRow(new VeloxWritableColumnVector[] {arrayCv});

      // Write row 0: an array of 10 booleans.
      // This calls writeArray -> writeValue(child, BooleanType, ...) for each element.
      // RED: writeValue throws UnsupportedOperationException (no BooleanType branch).
      // GREEN: putBoolean writes each bit correctly.
      Object[] boxed = new Object[numElems];
      for (int i = 0; i < numElems; i++) {
        boxed[i] = expected[i];
      }
      ArrayData arrData = new GenericArrayData(boxed);
      row.rowId = 0;
      row.update(0, arrData);
      row.finishWriteRow();

      // Verify: read the child column directly.
      VeloxWritableColumnVector child = arrayCv.getChildColumn();
      assertNotNull(child, "ARRAY child column must be non-null");
      for (int i = 0; i < numElems; i++) {
        assertEquals(
            expected[i],
            child.getBoolean(i),
            "Element "
                + i
                + " must round-trip correctly (bit "
                + (i & 7)
                + " of byte "
                + (i >> 3)
                + ")");
        assertFalse(child.isNullAt(i), "Element " + i + " must not be null");
      }
      // Verify the offsets/sizes for row 0 were written by putArray.
      assertEquals(0, arrayCv.getArrayOffset(0), "Row 0 offset must be 0");
      assertEquals(numElems, arrayCv.getArrayLength(0), "Row 0 size must equal numElems");
    } finally {
      freeArrayBoolDesc(ptrs);
    }
  }

  /**
   * Verifies that a null element in the nested BOOLEAN array is handled correctly (putNull).
   *
   * <p>Array has 3 elements: [true, null, false]. The child's nullsAddr bit for slot 1 must be
   * cleared; slots 0 and 2 must be valid.
   */
  @Test
  public void arrayBooleanWriteNullElement() {
    final int arrayCapacity = 4;
    final int childCapacity = 16;
    long[] ptrs = allocArrayBoolDesc(arrayCapacity, childCapacity);
    try {
      VeloxWritableColumnVector arrayCv =
          new VeloxWritableColumnVector(
              ptrs[0], 0L, arrayCapacity, DataTypes.createArrayType(DataTypes.BooleanType));
      VeloxColumnarRow row = new VeloxColumnarRow(new VeloxWritableColumnVector[] {arrayCv});

      // [true, null, false]
      Object[] boxed = {Boolean.TRUE, null, Boolean.FALSE};
      ArrayData arrData = new GenericArrayData(boxed);
      row.rowId = 0;
      row.update(0, arrData);
      row.finishWriteRow();

      VeloxWritableColumnVector child = arrayCv.getChildColumn();
      assertTrue(child.getBoolean(0), "slot 0 must be true");
      assertFalse(child.isNullAt(0), "slot 0 must not be null");
      assertTrue(child.isNullAt(1), "slot 1 must be null");
      assertFalse(child.getBoolean(2), "slot 2 must be false");
      assertFalse(child.isNullAt(2), "slot 2 must not be null");
    } finally {
      freeArrayBoolDesc(ptrs);
    }
  }
}
