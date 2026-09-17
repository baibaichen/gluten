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

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * TDD tests for VARBINARY write (raw bytes, StringView layout) via {@link VeloxColumnarRow#update}.
 *
 * <p>RED: before the BinaryType branch is added to {@link VeloxColumnarRow#update}, calling {@code
 * row.update(0, bytes)} with a BinaryType column throws {@link UnsupportedOperationException}.
 *
 * <p>GREEN: after the BinaryType branch is added, raw bytes are written correctly using the same
 * Velox StringView layout (mechanism D) as VARCHAR, and read back correctly.
 *
 * <p>No native library is loaded. Descriptors are built in Java-allocated off-heap memory via
 * {@link Platform} (i.e. {@code sun.misc.Unsafe}). Memory is freed manually after each test.
 *
 * <p>StringView layout (16 bytes per slot):
 *
 * <pre>
 * [0..4)  int32  size       — byte length of the value
 * [4..8)  4 bytes prefix    — first 4 bytes (for inline: data; for out-of-line: prefix)
 * [8..16) 8 bytes:
 *           if size &le; 12: remaining inline bytes (prefix + these = full value)
 *           if size &gt; 12: 8-byte native pointer to out-of-line data buffer
 * </pre>
 */
public class VeloxColumnarRowBinaryWriteTest {

  /**
   * Allocates a StringView (VARBINARY) descriptor backed by a 2-pointer buffers array, an all-valid
   * nulls bitmap, and a zeroed StringView values buffer.
   *
   * @param capacity number of rows
   * @return {@code long[4] = {desc, buffers, nulls, values}} -- all must be freed by the caller
   */
  private static long[] allocStringViewDescriptor(int capacity) {
    int nullsBytes = Math.max(8, ((capacity + 7) / 8 + 7) & ~7);
    long nulls = Platform.allocateMemory(nullsBytes);
    for (int i = 0; i < nullsBytes; i++) {
      Platform.putByte(null, nulls + i, (byte) 0xFF);
    }

    long values = Platform.allocateMemory(Math.max(8, (long) capacity * 16));
    // zero the StringView buffer
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

  private static void freeDescriptor(long[] ptrs) {
    Platform.freeMemory(ptrs[0]);
    Platform.freeMemory(ptrs[1]);
    Platform.freeMemory(ptrs[2]);
    Platform.freeMemory(ptrs[3]);
  }

  /**
   * Reads back bytes at {@code rowId} from a StringView buffer at {@code valuesBase}. Handles both
   * inline (size &le; 12) and out-of-line (size &gt; 12) layouts.
   */
  private static byte[] readStringViewBytes(long valuesBase, int rowId) {
    long sv = valuesBase + (long) rowId * 16;
    int size = Platform.getInt(null, sv);
    byte[] result = new byte[size];
    if (size <= 12) {
      if (size > 0) {
        Platform.copyMemory(null, sv + 4, result, Platform.BYTE_ARRAY_OFFSET, size);
      }
    } else {
      long ptr = Platform.getLong(null, sv + 8);
      Platform.copyMemory(null, ptr, result, Platform.BYTE_ARRAY_OFFSET, size);
    }
    return result;
  }

  /**
   * Verifies that {@link VeloxColumnarRow#update} writes an inline binary value (size &le; 12) into
   * a BinaryType column and reads it back correctly.
   *
   * <p>RED state: {@code row.update(0, bytes)} throws {@link UnsupportedOperationException}.
   *
   * <p>GREEN state: the bytes are written in the StringView inline layout and read back correctly.
   */
  @Test
  public void updateBinaryWritesInlineBytes() {
    int capacity = 3;
    long[] ptrs = allocStringViewDescriptor(capacity);
    try {
      VeloxWritableColumnVector cv =
          new VeloxWritableColumnVector(ptrs[0], 0L, capacity, DataTypes.BinaryType);
      VeloxColumnarRow row = new VeloxColumnarRow(new VeloxWritableColumnVector[] {cv});

      byte[] inlineBytes = {1, 2, 3, 4, 5}; // 5 bytes, fits inline (<=12)
      row.rowId = 0;
      row.update(0, inlineBytes);
      row.finishWriteRow();

      assertArrayEquals(
          inlineBytes, cv.getBinary(0), "inline binary bytes must round-trip via StringView");
      assertFalse(cv.isNullAt(0), "slot must not be null after update");
    } finally {
      freeDescriptor(ptrs);
    }
  }

  /**
   * Verifies that an out-of-line binary value (size &gt; 12) is written correctly using the
   * injected string chunk and read back via {@link VeloxWritableColumnVector#getBinary}.
   *
   * <p>A pre-allocated native chunk is injected via {@link
   * VeloxWritableColumnVector#injectChunkForTest} to simulate the production JNI chunk path without
   * loading the native library.
   *
   * <p>RED state: {@code row.update(0, bytes)} throws because BinaryType is not handled.
   *
   * <p>GREEN state: bytes are written out-of-line (pointer stored in StringView slot) and read back
   * correctly.
   */
  @Test
  public void updateBinaryWritesOutOfLineBytes() {
    int capacity = 2;
    long[] ptrs = allocStringViewDescriptor(capacity);
    int longSize = 20; // >12 bytes, forces out-of-line path
    long chunk = Platform.allocateMemory(longSize + 16); // extra headroom
    try {
      VeloxWritableColumnVector cv =
          new VeloxWritableColumnVector(ptrs[0], 0L, capacity, DataTypes.BinaryType);
      cv.injectChunkForTest(chunk, longSize + 16);
      VeloxColumnarRow row = new VeloxColumnarRow(new VeloxWritableColumnVector[] {cv});

      byte[] longBytes = new byte[longSize];
      for (int i = 0; i < longSize; i++) {
        longBytes[i] = (byte) (10 + i);
      }
      row.rowId = 0;
      row.update(0, longBytes);
      row.finishWriteRow();

      assertArrayEquals(
          longBytes, cv.getBinary(0), "out-of-line binary bytes must round-trip via StringView");
      assertFalse(cv.isNullAt(0), "slot must not be null after update");
    } finally {
      Platform.freeMemory(chunk);
      freeDescriptor(ptrs);
    }
  }

  /**
   * Verifies that multiple rows with both inline and out-of-line byte arrays can be written and
   * read back via {@link VeloxColumnarRow#getBinary}.
   */
  @Test
  public void updateBinaryMultipleRows() {
    int capacity = 3;
    long[] ptrs = allocStringViewDescriptor(capacity);
    long chunk = Platform.allocateMemory(64);
    try {
      VeloxWritableColumnVector cv =
          new VeloxWritableColumnVector(ptrs[0], 0L, capacity, DataTypes.BinaryType);
      cv.injectChunkForTest(chunk, 64);
      VeloxColumnarRow row = new VeloxColumnarRow(new VeloxWritableColumnVector[] {cv});

      byte[] b0 = {42}; // 1 byte, inline
      byte[] b1 = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12}; // exactly 12 bytes, inline
      byte[] b2 = new byte[15]; // 15 bytes, out-of-line
      for (int i = 0; i < 15; i++) b2[i] = (byte) (i + 100);

      row.rowId = 0;
      row.update(0, b0);
      row.rowId = 1;
      row.update(0, b1);
      row.rowId = 2;
      row.update(0, b2);
      row.finishWriteRow();

      row.rowId = 0;
      assertArrayEquals(b0, row.getBinary(0), "row 0 must round-trip 1-byte inline value");
      row.rowId = 1;
      assertArrayEquals(b1, row.getBinary(0), "row 1 must round-trip 12-byte inline value");
      row.rowId = 2;
      assertArrayEquals(b2, row.getBinary(0), "row 2 must round-trip 15-byte out-of-line value");
    } finally {
      Platform.freeMemory(chunk);
      freeDescriptor(ptrs);
    }
  }

  /**
   * Verifies that setting null on a BinaryType column clears the validity bit and preserves the
   * StringView size field at 0.
   */
  @Test
  public void setNullBinaryColumn() {
    int capacity = 2;
    long[] ptrs = allocStringViewDescriptor(capacity);
    try {
      VeloxWritableColumnVector cv =
          new VeloxWritableColumnVector(ptrs[0], 0L, capacity, DataTypes.BinaryType);
      VeloxColumnarRow row = new VeloxColumnarRow(new VeloxWritableColumnVector[] {cv});

      row.rowId = 0;
      row.update(0, new byte[] {7, 8, 9});
      row.rowId = 1;
      row.update(0, null); // should call setNullAt
      row.finishWriteRow();

      assertFalse(cv.isNullAt(0), "row 0 must be valid after non-null write");
      assertTrue(cv.isNullAt(1), "row 1 must be null after null write");
      assertArrayEquals(new byte[] {7, 8, 9}, cv.getBinary(0), "row 0 value must round-trip");
    } finally {
      freeDescriptor(ptrs);
    }
  }
}
