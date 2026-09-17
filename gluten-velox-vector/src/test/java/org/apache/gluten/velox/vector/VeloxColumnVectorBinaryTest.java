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

import org.apache.spark.sql.types.BinaryType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.unsafe.Platform;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * TDD tests for VARBINARY (BinaryType) zero-copy read via {@link VeloxColumnVector}.
 *
 * <p>Velox VARBINARY columns are physically stored using the same 16-byte StringView layout as
 * VARCHAR. These tests verify that {@link VeloxColumnVector} routes {@link BinaryType} to the
 * existing {@link FlatAccessor} (which decodes StringView bytes into a {@code byte[]}), and that
 * {@link VeloxInputBatch#canImport} admits schemas containing BinaryType leaves.
 *
 * <p>No native library is loaded. Descriptors are built in Java-allocated off-heap memory via
 * {@link Platform} (i.e. {@code sun.misc.Unsafe}). Memory is freed manually after each test rather
 * than via {@link VeloxColumnVector#close()} to avoid invoking the native {@link
 * VeloxColumnHandleJniWrapper#freeDescriptor} on Java-allocated addresses.
 *
 * <p>StringView layout (16 bytes per slot):
 *
 * <pre>
 * [0..4)  int32  size       — byte length of the binary value
 * [4..8)  4 bytes prefix    — first 4 bytes (used for comparisons)
 * [8..16) 8 bytes inline/ptr:
 *           if size &le; 12: remaining inline bytes (prefix + these = full value)
 *           if size &gt; 12: 8-byte native pointer to the out-of-line data buffer
 * </pre>
 */
public class VeloxColumnVectorBinaryTest {

  /**
   * Verifies that {@link VeloxColumnVector#getBinary} correctly reads both an inline binary value
   * ({@code size <= 12}) and an out-of-line binary value ({@code size > 12}) from Velox StringView
   * slots.
   *
   * <p>Layout:
   *
   * <ul>
   *   <li>sv[0]: inline 3 bytes {1, 2, 3} — size=3, data at sv[0]+4
   *   <li>sv[1]: out-of-line 20 bytes — size=20, pointer at sv[1]+8 to a separately allocated
   *       native buffer containing bytes {10, 11, 12, ..., 29}
   * </ul>
   */
  @Test
  public void binaryColumnReadsInlineAndOutOfLineStringViewBytes() {
    // Inline value: 3 bytes {1, 2, 3}
    byte[] inlineBytes = {1, 2, 3};

    // Out-of-line value: 20 bytes {10, 11, ..., 29}
    int longSize = 20;
    byte[] longBytes = new byte[longSize];
    for (int i = 0; i < longSize; i++) {
      longBytes[i] = (byte) (10 + i);
    }

    // Allocate native buffer for the out-of-line value.
    long longBuf = Platform.allocateMemory(longSize);
    for (int i = 0; i < longSize; i++) {
      Platform.putByte(null, longBuf + i, longBytes[i]);
    }

    // Allocate values buffer: 2 StringViews × 16 bytes = 32 bytes.
    long values = Platform.allocateMemory(32);

    // sv[0]: inline {1, 2, 3} — size=3, data written at offset 4 (prefix field).
    Platform.putInt(null, values + 0, inlineBytes.length); // size
    Platform.putByte(null, values + 4, inlineBytes[0]);
    Platform.putByte(null, values + 5, inlineBytes[1]);
    Platform.putByte(null, values + 6, inlineBytes[2]);

    // sv[1]: out-of-line — size=20, pointer at sv[1]+8.
    Platform.putInt(null, values + 16 + 0, longSize); // size
    Platform.putLong(null, values + 16 + 8, longBuf); // pointer

    // Buffers array: [nulls=0, valuesAddr].
    long buffers = Platform.allocateMemory(2 * 8);
    Platform.putLong(null, buffers, 0L); // nulls = nullptr
    Platform.putLong(null, buffers + 8, values);

    // 48-byte descriptor: length=2, nullCount=0, nBuffers=2, nChildren=0.
    long h = Platform.allocateMemory(48);
    Platform.putLong(null, h + 0, 2L); // length
    Platform.putLong(null, h + 8, 0L); // nullCount
    Platform.putLong(null, h + 16, 2L); // nBuffers
    Platform.putLong(null, h + 24, 0L); // nChildren
    Platform.putLong(null, h + 32, buffers); // buffers ptr
    Platform.putLong(null, h + 40, 0L); // children ptr

    // importFromNative with BinaryType — must NOT throw UnsupportedOperationException
    VeloxColumnVector cv = VeloxColumnVector.importFromNative(h, DataTypes.BinaryType);

    // Free manually — do NOT call cv.close() as that would try to native-free h.
    // Use try-finally so native buffers are always released even if an assertion throws.
    try {
      assertArrayEquals(inlineBytes, cv.getBinary(0));
      assertArrayEquals(longBytes, cv.getBinary(1));
    } finally {
      Platform.freeMemory(h);
      Platform.freeMemory(buffers);
      Platform.freeMemory(values);
      Platform.freeMemory(longBuf);
    }
  }

  /**
   * Verifies that {@link VeloxInputBatch#canImport} returns {@code true} for a schema containing a
   * single BinaryType leaf.
   */
  @Test
  public void canImportBinarySchema() {
    StructType schema = new StructType().add("b", DataTypes.BinaryType);
    assertTrue(VeloxInputBatch.canImport(schema));
  }
}
