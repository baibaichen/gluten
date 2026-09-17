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
import org.apache.spark.sql.types.ArrayType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.MapType;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.vectorized.ColumnarArray;
import org.apache.spark.sql.vectorized.ColumnarMap;
import org.apache.spark.unsafe.Platform;
import org.apache.spark.unsafe.types.CalendarInterval;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Unit tests for {@link VeloxColumnVector} and {@link FlatAccessor} using hand-crafted native
 * memory layouts via {@link Platform} (i.e. {@code sun.misc.Unsafe}).
 *
 * <p>These tests do not load any native library. The descriptor structs are built entirely in
 * Java-allocated off-heap memory, so all assertions are pure JVM computations.
 *
 * <p>Memory is freed manually after each test rather than via {@link VeloxColumnVector#close()} to
 * avoid invoking the native {@link VeloxColumnHandleJniWrapper#freeDescriptor} on Java-allocated
 * addresses (which would segfault). Tests that pass a Java-allocated address to {@code
 * importFromNative} should therefore NOT call {@code cv.close()}.
 */
public class VeloxColumnVectorTest {

  /**
   * Verifies that a flat integer column with no nulls can be read correctly via Unsafe.
   *
   * <p>Layout:
   *
   * <ul>
   *   <li>5 ints: 10, 20, 30, 40, 50
   *   <li>No validity bitmap (nulls pointer = 0)
   *   <li>Expected: {@code cv.getInt(2) == 30}, {@code cv.hasNull() == false}
   * </ul>
   */
  @Test
  public void flatIntReadsViaUnsafe() {
    long values = Platform.allocateMemory(5 * 4);
    for (int i = 0; i < 5; i++) {
      Platform.putInt(null, values + (long) i * 4, (i + 1) * 10);
    }

    long buffers = Platform.allocateMemory(2 * 8);
    Platform.putLong(null, buffers, 0L); // nulls = nullptr
    Platform.putLong(null, buffers + 8, values); // values

    long h = Platform.allocateMemory(48);
    Platform.putLong(null, h + 0, 5L); // length
    Platform.putLong(null, h + 8, 0L); // nullCount = 0
    Platform.putLong(null, h + 16, 2L); // nBuffers
    Platform.putLong(null, h + 24, 0L); // nChildren
    Platform.putLong(null, h + 32, buffers); // buffers ptr
    Platform.putLong(null, h + 40, 0L); // children ptr

    // Pass h as descAddr; close() would call freeDescriptor(h), which would segfault on
    // Java-allocated memory. We free manually below instead.
    VeloxColumnVector cv = VeloxColumnVector.importFromNative(h, DataTypes.IntegerType);

    assertEquals(30, cv.getInt(2));
    assertFalse(cv.hasNull());

    // Free manually — do NOT call cv.close() as that would try to native-free h.
    Platform.freeMemory(h);
    Platform.freeMemory(buffers);
    Platform.freeMemory(values);
  }

  /**
   * Verifies that {@link VeloxColumnVector#isNullAt} correctly interprets the Velox validity bitmap
   * where {@code 1 = valid} and {@code 0 = null}.
   *
   * <p>Layout:
   *
   * <ul>
   *   <li>8 rows; bitmap byte {@code 0x55} = {@code 0b01010101}: bits 0,2,4,6 set (valid at even
   *       indices), bits 1,3,5,7 clear (null at odd indices).
   *   <li>nullCount = 4
   *   <li>Values: 0, 100, 200, 300, 400, 500, 600, 700 (values at null rows are ignored)
   * </ul>
   */
  @Test
  public void isNullAtWithBitmap() {
    // 8 elements; alternating valid/null: valid at even indices (0,2,4,6), null at odd (1,3,5,7).
    // Velox bitmap: 1=valid, 0=null. Byte for rows 0-7: bits 0,2,4,6 set = 0b01010101 = 0x55.
    long nulls = Platform.allocateMemory(1);
    Platform.putByte(null, nulls, (byte) 0x55);

    long values = Platform.allocateMemory(8 * 4);
    for (int i = 0; i < 8; i++) {
      Platform.putInt(null, values + (long) i * 4, i * 100);
    }

    long buffers = Platform.allocateMemory(2 * 8);
    Platform.putLong(null, buffers, nulls);
    Platform.putLong(null, buffers + 8, values);

    long h = Platform.allocateMemory(48);
    Platform.putLong(null, h + 0, 8L); // length
    Platform.putLong(null, h + 8, 4L); // nullCount = 4
    Platform.putLong(null, h + 16, 2L); // nBuffers
    Platform.putLong(null, h + 24, 0L); // nChildren
    Platform.putLong(null, h + 32, buffers); // buffers ptr
    Platform.putLong(null, h + 40, 0L); // children ptr

    VeloxColumnVector cv = VeloxColumnVector.importFromNative(h, DataTypes.IntegerType);

    assertFalse(cv.isNullAt(0)); // bit 0 set → valid
    assertTrue(cv.isNullAt(1)); // bit 1 clear → null
    assertFalse(cv.isNullAt(2)); // bit 2 set → valid
    assertTrue(cv.isNullAt(3)); // bit 3 clear → null
    assertTrue(cv.hasNull());
    assertEquals(400, cv.getInt(4)); // index 4, value = 400, valid (bit 4 set)

    // Free manually — do NOT call cv.close().
    Platform.freeMemory(h);
    Platform.freeMemory(buffers);
    Platform.freeMemory(nulls);
    Platform.freeMemory(values);
  }

  /**
   * Verifies that {@link FlatAccessor#getUTF8String} correctly handles both inline (short) and
   * out-of-line (long) Velox {@code StringView} entries using zero-copy {@code fromAddress}.
   *
   * <p>Layout (two 16-byte StringViews in a hand-crafted values buffer):
   *
   * <ul>
   *   <li>sv[0]: size=2, inline "hi" — bytes 'h','i' written starting at offset 4.
   *   <li>sv[1]: size=21, out-of-line — pointer at offset 8 within the second StringView points to
   *       a separately allocated buffer containing "this is a long string".
   * </ul>
   */
  @Test
  public void stringViewInlineAndOutOfLine() {
    // Allocate values buffer: 2 StringViews × 16 bytes = 32 bytes.
    long values = Platform.allocateMemory(32);

    // sv[0]: inline "hi" — size=2, data at offset 4.
    Platform.putInt(null, values, 2); // size
    Platform.putByte(null, values + 4, (byte) 'h');
    Platform.putByte(null, values + 5, (byte) 'i');

    // sv[1]: out-of-line "this is a long string" (21 bytes).
    byte[] longStr = "this is a long string".getBytes(java.nio.charset.StandardCharsets.UTF_8);
    long longBuf = Platform.allocateMemory(longStr.length);
    for (int i = 0; i < longStr.length; i++) {
      Platform.putByte(null, longBuf + i, longStr[i]);
    }
    Platform.putInt(null, values + 16, longStr.length); // size at sv[1]+0
    Platform.putLong(null, values + 16 + 8, longBuf); // pointer at sv[1]+8

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

    VeloxColumnVector cv = VeloxColumnVector.importFromNative(h, DataTypes.StringType);

    assertEquals("hi", cv.getUTF8String(0).toString());
    assertEquals("this is a long string", cv.getUTF8String(1).toString());

    // Free manually — do NOT call cv.close() on Java-allocated memory.
    Platform.freeMemory(h);
    Platform.freeMemory(buffers);
    Platform.freeMemory(values);
    Platform.freeMemory(longBuf);
  }

  // ---------------------------------------------------------------------------
  // Helper: allocate a 48B descriptor node in native memory
  // ---------------------------------------------------------------------------

  /**
   * Allocates a 48-byte {@code VeloxColumnHandle} descriptor in native memory, along with the
   * backing arrays for {@code buffers} and {@code children}. All allocated addresses are appended
   * to {@code allocs} so the caller can free them in a {@code finally} block.
   *
   * @param rowCount number of rows ({@code length} field)
   * @param nullCount pre-computed null count
   * @param bufferAddrs addresses to store in the buffers array (index 0 = nulls bitmap)
   * @param childDescAddrs addresses of child descriptor nodes
   * @param allocs mutable list to which every newly allocated address is appended (for cleanup)
   * @return the native address of the newly allocated descriptor
   */
  private static long allocDesc(
      long rowCount, long nullCount, long[] bufferAddrs, long[] childDescAddrs, List<Long> allocs) {
    // Allocate and populate the buffers pointer array.
    long buffersPtr = 0L;
    if (bufferAddrs.length > 0) {
      buffersPtr = Platform.allocateMemory((long) bufferAddrs.length * 8);
      allocs.add(buffersPtr);
      for (int i = 0; i < bufferAddrs.length; i++) {
        Platform.putLong(null, buffersPtr + (long) i * 8, bufferAddrs[i]);
      }
    }

    // Allocate and populate the children pointer array.
    long childrenPtr = 0L;
    if (childDescAddrs.length > 0) {
      childrenPtr = Platform.allocateMemory((long) childDescAddrs.length * 8);
      allocs.add(childrenPtr);
      for (int i = 0; i < childDescAddrs.length; i++) {
        Platform.putLong(null, childrenPtr + (long) i * 8, childDescAddrs[i]);
      }
    }

    // Allocate the 48-byte descriptor and fill all six fields.
    long desc = Platform.allocateMemory(48);
    allocs.add(desc);
    Platform.putLong(null, desc + 0, rowCount); // length
    Platform.putLong(null, desc + 8, nullCount); // nullCount
    Platform.putLong(null, desc + 16, (long) bufferAddrs.length); // nBuffers
    Platform.putLong(null, desc + 24, (long) childDescAddrs.length); // nChildren
    Platform.putLong(null, desc + 32, buffersPtr); // buffers ptr
    Platform.putLong(null, desc + 40, childrenPtr); // children ptr
    return desc;
  }

  /**
   * Frees all addresses previously collected by {@link #allocDesc} calls.
   *
   * @param allocs list of native addresses to free
   */
  private static void freeAll(List<Long> allocs) {
    for (long addr : allocs) {
      Platform.freeMemory(addr);
    }
  }

  @Test
  public void calendarIntervalReadsPackedFieldsAndCopiesRow() {
    CalendarInterval[] expected = {
      new CalendarInterval(0, 0, 0),
      new CalendarInterval(14, 40, 5400000001L),
      new CalendarInterval(-1, 5, -1000001L),
      new CalendarInterval(Integer.MIN_VALUE, Integer.MAX_VALUE, Long.MIN_VALUE),
      new CalendarInterval(Integer.MAX_VALUE, Integer.MIN_VALUE, Long.MAX_VALUE)
    };
    List<Long> allocs = new ArrayList<>();
    try {
      long values = Platform.allocateMemory(6 * 16);
      long nulls = Platform.allocateMemory(1);
      allocs.add(values);
      allocs.add(nulls);
      Platform.putByte(null, nulls, (byte) 0x1f);
      for (int i = 0; i < expected.length; i++) {
        Platform.putInt(null, values + i * 16L, expected[i].months);
        Platform.putInt(null, values + i * 16L + 4, expected[i].days);
        Platform.putLong(null, values + i * 16L + 8, expected[i].microseconds);
      }
      long desc = allocDesc(6, 1, new long[] {nulls, values}, new long[0], allocs);
      try (VeloxColumnVector cv =
          VeloxColumnVector.importFromNativeView(desc, DataTypes.CalendarIntervalType)) {
        assertEquals(DataTypes.CalendarIntervalType, cv.dataType());
        assertTrue(cv.hasNull());
        assertEquals(1, cv.numNulls());
        VeloxColumnarReadRow row = new VeloxColumnarReadRow(new VeloxColumnVector[] {cv});
        for (int i = 0; i < expected.length; i++) {
          assertEquals(expected[i].months, cv.getChild(0).getInt(i));
          assertEquals(expected[i].days, cv.getChild(1).getInt(i));
          assertEquals(expected[i].microseconds, cv.getChild(2).getLong(i));
          assertEquals(expected[i], cv.getInterval(i));
          row.rowId = i;
          assertEquals(expected[i], row.getInterval(0));
          assertEquals(expected[i], row.get(0, DataTypes.CalendarIntervalType));
          assertEquals(expected[i], row.copy().getInterval(0));
        }
        row.rowId = 5;
        assertTrue(cv.isNullAt(5));
        assertTrue(cv.getChild(0).isNullAt(5));
        assertTrue(cv.getChild(1).isNullAt(5));
        assertTrue(cv.getChild(2).isNullAt(5));
        assertNull(cv.getInterval(5));
        assertNull(row.getInterval(0));
        assertNull(row.get(0, DataTypes.CalendarIntervalType));
        assertTrue(row.copy().isNullAt(0));
        assertThrows(UnsupportedOperationException.class, () -> cv.getLong(0));
        assertThrows(UnsupportedOperationException.class, () -> cv.getDecimal(0, 38, 0));
        row.rowId = 0;
        InternalRow copied = row.copy();
        Platform.putInt(null, values, 99);
        assertEquals(99, cv.getInterval(0).months);
        assertEquals(expected[0], copied.getInterval(0));
      }
    } finally {
      freeAll(allocs);
    }
  }

  @Test
  public void allNullAndEmptyCalendarIntervalsNeedNoValuesBuffer() {
    List<Long> allocs = new ArrayList<>();
    try {
      long nulls = Platform.allocateMemory(1);
      allocs.add(nulls);
      Platform.putByte(null, nulls, (byte) 0);
      long desc = allocDesc(2, 2, new long[] {nulls, 0}, new long[0], allocs);
      try (VeloxColumnVector cv =
          VeloxColumnVector.importFromNativeView(desc, DataTypes.CalendarIntervalType)) {
        assertEquals(2, cv.numNulls());
        assertTrue(cv.isNullAt(0));
        assertTrue(cv.isNullAt(1));
        assertNull(cv.getInterval(0));
        assertNull(cv.getInterval(1));
      }
      long empty = allocDesc(0, 0, new long[] {0, 0}, new long[0], allocs);
      try (VeloxColumnVector cv =
          VeloxColumnVector.importFromNativeView(empty, DataTypes.CalendarIntervalType)) {
        assertFalse(cv.hasNull());
        assertEquals(0, cv.numNulls());
      }
    } finally {
      freeAll(allocs);
    }
  }

  @Test
  public void nestedCalendarIntervalViewsPreserveLogicalValuesAndOwnership() {
    List<Long> allocs = new ArrayList<>();
    try {
      CalendarInterval expected = new CalendarInterval(-14, 40, -86400000001L);
      long values = Platform.allocateMemory(32);
      long nulls = Platform.allocateMemory(1);
      long offsets = Platform.allocateMemory(8);
      long sizes = Platform.allocateMemory(8);
      long keys = Platform.allocateMemory(8);
      allocs.add(values);
      allocs.add(nulls);
      allocs.add(offsets);
      allocs.add(sizes);
      allocs.add(keys);
      Platform.putInt(null, values, expected.months);
      Platform.putInt(null, values + 4, expected.days);
      Platform.putLong(null, values + 8, expected.microseconds);
      Platform.putByte(null, nulls, (byte) 1);
      Platform.putInt(null, offsets, 0);
      Platform.putInt(null, offsets + 4, 2);
      Platform.putInt(null, sizes, 2);
      Platform.putInt(null, sizes + 4, 0);
      Platform.putInt(null, keys, 10);
      Platform.putInt(null, keys + 4, 11);
      long leaf = allocDesc(2, 1, new long[] {nulls, values}, new long[0], allocs);
      long array = allocDesc(2, 0, new long[] {0, offsets, sizes}, new long[] {leaf}, allocs);
      long keyDesc = allocDesc(2, 0, new long[] {0, keys}, new long[0], allocs);
      long map =
          allocDesc(2, 0, new long[] {0, offsets, sizes}, new long[] {keyDesc, leaf}, allocs);
      long root = allocDesc(2, 0, new long[] {0}, new long[] {leaf, array, map}, allocs);
      StructType type =
          new StructType()
              .add("interval", DataTypes.CalendarIntervalType)
              .add("array", DataTypes.createArrayType(DataTypes.CalendarIntervalType))
              .add(
                  "map",
                  DataTypes.createMapType(DataTypes.IntegerType, DataTypes.CalendarIntervalType));
      try (VeloxColumnVector cv = VeloxColumnVector.importFromNativeView(root, type)) {
        assertEquals(expected, cv.getStruct(0).getInterval(0));
        assertTrue(cv.getStruct(1).isNullAt(0));
        ColumnarArray elements = cv.getChild(1).getArray(0);
        assertEquals(expected, elements.getInterval(0));
        assertNull(elements.getInterval(1));
        assertEquals(expected, elements.copy().getInterval(0));
        assertEquals(0, cv.getChild(1).getArray(1).numElements());
        ColumnarMap entries = cv.getChild(2).getMap(0);
        assertEquals(10, entries.keyArray().getInt(0));
        assertEquals(expected, entries.valueArray().getInterval(0));
        assertNull(entries.valueArray().getInterval(1));
        cv.getChild(0).close();
        assertEquals(expected, cv.getChild(0).getInterval(0));
      }
    } finally {
      freeAll(allocs);
    }
  }

  @Test
  public void invalidCalendarIntervalMetadataRejectsAndFreesOnlyOwnedRoot() {
    List<Long> allocs = new ArrayList<>();
    try {
      long[] invalid = {
        allocDesc(-1, -1, new long[] {0, 0}, new long[0], allocs),
        allocDesc(1, -2, new long[] {0, 0}, new long[0], allocs),
        allocDesc(1, 2, new long[] {0, 0}, new long[0], allocs),
        allocDesc(1, 0, new long[0], new long[0], allocs),
        allocDesc(1, 0, new long[] {0}, new long[0], allocs),
        allocDesc(1, 0, new long[] {0, 0}, new long[] {0}, allocs),
        allocDesc(1, 1, new long[] {0, 0}, new long[0], allocs),
        allocDesc(1, 0, new long[] {0, 0}, new long[0], allocs)
      };
      for (long desc : invalid) {
        assertThrows(
            IllegalArgumentException.class,
            () -> VeloxColumnVector.importFromNativeView(desc, DataTypes.CalendarIntervalType));
      }
      long root = allocDesc(1, 0, new long[] {0}, new long[] {invalid[7]}, allocs);
      List<Long> freed = new ArrayList<>();
      StructType type = new StructType().add("interval", DataTypes.CalendarIntervalType);
      assertThrows(
          IllegalArgumentException.class,
          () -> VeloxColumnVector.importFromNative(root, type, freed::add));
      assertEquals(List.of(root), freed);
    } finally {
      freeAll(allocs);
    }
  }

  @Test
  public void nullTypeReadsOnlyValidityAndRejectsDataGetters() {
    List<Long> allocs = new ArrayList<>();
    try {
      long nulls = Platform.allocateMemory(2);
      allocs.add(nulls);
      Platform.putByte(null, nulls, (byte) 0);
      Platform.putByte(null, nulls + 1, (byte) 0xfe); // Padding bits are not rows.
      for (long nullCount : new long[] {9, -1}) {
        long desc = allocDesc(9, nullCount, new long[] {nulls, 0}, new long[0], allocs);
        try (VeloxColumnVector cv =
            VeloxColumnVector.importFromNativeView(desc, DataTypes.NullType)) {
          assertTrue(cv.hasNull());
          assertEquals(9, cv.numNulls());
          VeloxColumnarReadRow row = new VeloxColumnarReadRow(new VeloxColumnVector[] {cv});
          for (int i = 0; i < 9; i++) {
            assertTrue(cv.isNullAt(i));
            row.rowId = i;
            assertNull(row.get(0, DataTypes.NullType));
            assertTrue(row.copy().isNullAt(0));
          }
          assertThrows(UnsupportedOperationException.class, () -> cv.getBoolean(0));
          assertThrows(UnsupportedOperationException.class, () -> cv.getByte(0));
          assertThrows(UnsupportedOperationException.class, () -> cv.getShort(0));
          assertThrows(UnsupportedOperationException.class, () -> cv.getInt(0));
          assertThrows(UnsupportedOperationException.class, () -> cv.getLong(0));
          assertThrows(UnsupportedOperationException.class, () -> cv.getFloat(0));
          assertThrows(UnsupportedOperationException.class, () -> cv.getDouble(0));
          assertThrows(UnsupportedOperationException.class, () -> cv.getUTF8String(0));
          assertThrows(UnsupportedOperationException.class, () -> cv.getBinary(0));
          assertThrows(UnsupportedOperationException.class, () -> cv.getDecimal(0, 10, 2));
          assertThrows(UnsupportedOperationException.class, () -> cv.getArray(0));
          assertThrows(UnsupportedOperationException.class, () -> cv.getMap(0));
        }
      }
      long empty = allocDesc(0, 0, new long[] {0, 0}, new long[0], allocs);
      try (VeloxColumnVector cv =
          VeloxColumnVector.importFromNativeView(empty, DataTypes.NullType)) {
        assertFalse(cv.hasNull());
        assertEquals(0, cv.numNulls());
      }
    } finally {
      freeAll(allocs);
    }
  }

  @Test
  public void nestedNullLeavesPreserveArraysMapsAndStructs() {
    List<Long> allocs = new ArrayList<>();
    try {
      long nulls = Platform.allocateMemory(1);
      allocs.add(nulls);
      Platform.putByte(null, nulls, (byte) 0);
      long leaf = allocDesc(3, 3, new long[] {nulls, 0}, new long[0], allocs);
      long offsets = Platform.allocateMemory(12);
      long sizes = Platform.allocateMemory(12);
      long keys = Platform.allocateMemory(12);
      allocs.add(offsets);
      allocs.add(sizes);
      allocs.add(keys);
      for (int i = 0; i < 3; i++) {
        Platform.putInt(null, offsets + i * 4L, i == 0 ? 0 : 2);
        Platform.putInt(null, sizes + i * 4L, i == 0 ? 2 : i == 1 ? 0 : 1);
        Platform.putInt(null, keys + i * 4L, 10 + i);
      }
      long array = allocDesc(3, 0, new long[] {0, offsets, sizes}, new long[] {leaf}, allocs);
      long keyDesc = allocDesc(3, 0, new long[] {0, keys}, new long[0], allocs);
      long map =
          allocDesc(3, 0, new long[] {0, offsets, sizes}, new long[] {keyDesc, leaf}, allocs);
      long struct = allocDesc(3, 0, new long[] {0}, new long[] {leaf, array, map}, allocs);
      StructType type =
          new StructType()
              .add("null", DataTypes.NullType)
              .add("array", DataTypes.createArrayType(DataTypes.NullType))
              .add("map", DataTypes.createMapType(DataTypes.IntegerType, DataTypes.NullType));
      try (VeloxColumnVector cv = VeloxColumnVector.importFromNativeView(struct, type)) {
        assertFalse(cv.isNullAt(0));
        assertTrue(cv.getStruct(0).isNullAt(0));
        ColumnarArray values = cv.getChild(1).getArray(0);
        assertEquals(2, values.numElements());
        assertTrue(values.isNullAt(0));
        assertTrue(values.isNullAt(1));
        assertTrue(values.copy().isNullAt(1));
        assertEquals(0, cv.getChild(1).getArray(1).numElements());
        assertEquals(1, cv.getChild(1).getArray(2).numElements());
        ColumnarMap entries = cv.getChild(2).getMap(0);
        assertEquals(2, entries.numElements());
        assertEquals(10, entries.keyArray().getInt(0));
        assertTrue(entries.valueArray().isNullAt(0));
        assertTrue(entries.valueArray().isNullAt(1));
        assertEquals(0, cv.getChild(2).getMap(1).numElements());
      }

      long nested = allocDesc(3, 0, new long[] {0, offsets, sizes}, new long[] {array}, allocs);
      try (VeloxColumnVector cv =
          VeloxColumnVector.importFromNativeView(
              nested, DataTypes.createArrayType(DataTypes.createArrayType(DataTypes.NullType)))) {
        assertTrue(cv.getArray(0).getArray(0).isNullAt(0));
        assertEquals(0, cv.getArray(0).getArray(1).numElements());
        assertEquals(0, cv.getArray(1).numElements());
      }

      long emptyLeaf = allocDesc(0, 0, new long[] {0, 0}, new long[0], allocs);
      Platform.putInt(null, sizes, 0);
      long emptyArray =
          allocDesc(1, 0, new long[] {0, offsets, sizes}, new long[] {emptyLeaf}, allocs);
      try (VeloxColumnVector cv =
          VeloxColumnVector.importFromNativeView(
              emptyArray, DataTypes.createArrayType(DataTypes.NullType))) {
        assertFalse(cv.isNullAt(0));
        assertEquals(0, cv.getArray(0).numElements());
        assertEquals(0, cv.getChild(0).numNulls());
      }
    } finally {
      freeAll(allocs);
    }
  }

  @Test
  public void nullTypeRejectsInvalidMetadataAndNonNullRows() {
    List<Long> allocs = new ArrayList<>();
    try {
      long nulls = Platform.allocateMemory(1);
      allocs.add(nulls);
      Platform.putByte(null, nulls, (byte) 0);
      long[] invalid = {
        allocDesc(-1, -1, new long[] {nulls, 0}, new long[0], allocs),
        allocDesc(1, 0, new long[] {nulls, 0}, new long[0], allocs),
        allocDesc(1, -2, new long[] {nulls, 0}, new long[0], allocs),
        allocDesc(1, 1, new long[0], new long[0], allocs),
        allocDesc(1, 1, new long[] {nulls}, new long[0], allocs),
        allocDesc(1, 1, new long[] {nulls, 0}, new long[] {0}, allocs),
        allocDesc(1, 1, new long[] {0, 0}, new long[0], allocs),
        allocDesc(1, 1, new long[] {nulls, 1}, new long[0], allocs)
      };
      for (long desc : invalid) {
        assertThrows(
            IllegalArgumentException.class,
            () -> VeloxColumnVector.importFromNativeView(desc, DataTypes.NullType));
      }
      long desc = allocDesc(1, 1, new long[] {nulls, 0}, new long[0], allocs);
      Platform.putLong(null, desc + 32, 0L);
      assertThrows(
          IllegalArgumentException.class,
          () -> VeloxColumnVector.importFromNativeView(desc, DataTypes.NullType));
      long nonNull = allocDesc(1, 1, new long[] {nulls, 0}, new long[0], allocs);
      Platform.putByte(null, nulls, (byte) 1);
      assertThrows(
          IllegalArgumentException.class,
          () -> VeloxColumnVector.importFromNativeView(nonNull, DataTypes.NullType));
    } finally {
      freeAll(allocs);
    }
  }

  @Test
  public void zeroDescriptorDoesNotAcquireOwnershipOrInvokeNativeCleanup() {
    List<Long> freed = new ArrayList<>();
    IllegalArgumentException error =
        assertThrows(
            IllegalArgumentException.class,
            () -> VeloxColumnVector.importFromNative(0L, DataTypes.NullType, freed::add));
    assertEquals("Descriptor address must not be zero", error.getMessage());
    assertTrue(freed.isEmpty());
    assertThrows(
        IllegalArgumentException.class,
        () -> VeloxColumnVector.importFromNative(0L, DataTypes.NullType));
    assertThrows(
        IllegalArgumentException.class,
        () -> VeloxColumnVector.importFromNativeView(0L, DataTypes.NullType));
  }

  @Test
  public void zeroChildDescriptorRejectsBeforeUnsafeAndFreesOnlyOwnedRoot() {
    List<Long> allocs = new ArrayList<>();
    try {
      long root = allocDesc(1, 0, new long[] {0}, new long[] {0}, allocs);
      StructType type = new StructType().add("null", DataTypes.NullType);
      List<Long> freed = new ArrayList<>();
      assertThrows(
          IllegalArgumentException.class,
          () -> VeloxColumnVector.importFromNative(root, type, freed::add));
      assertEquals(List.of(root), freed);
      assertThrows(
          IllegalArgumentException.class, () -> VeloxColumnVector.importFromNativeView(root, type));
    } finally {
      freeAll(allocs);
    }
  }

  @Test
  public void invalidNullImportFreesOnlyTheOwningRoot() {
    List<Long> allocs = new ArrayList<>();
    try {
      long invalid = allocDesc(1, 1, new long[] {0, 0}, new long[0], allocs);
      List<Long> freed = new ArrayList<>();
      assertThrows(
          IllegalArgumentException.class,
          () -> VeloxColumnVector.importFromNative(invalid, DataTypes.NullType, freed::add));
      assertEquals(List.of(invalid), freed);

      long valid = allocDesc(1, 0, new long[] {0, 0}, new long[0], allocs);
      long root = allocDesc(1, 0, new long[] {0}, new long[] {valid, invalid}, allocs);
      StructType type =
          new StructType().add("int", DataTypes.IntegerType).add("null", DataTypes.NullType);
      freed.clear();
      assertThrows(
          IllegalArgumentException.class,
          () -> VeloxColumnVector.importFromNative(root, type, freed::add));
      assertEquals(List.of(root), freed);
      VeloxInputBatch.freeUnimportedDescriptors(new long[] {root, valid}, 1, freed::add);
      assertEquals(List.of(root, valid), freed);
    } finally {
      freeAll(allocs);
    }
  }

  // ---------------------------------------------------------------------------
  // Task 9 nested-accessor tests
  // ---------------------------------------------------------------------------

  /**
   * ARRAY&lt;INT&gt;: row0=[10,20], row1=[30,40,50].
   *
   * <p>Verifies that {@link VeloxColumnVector#getArray} returns the correct start, length, and
   * element values for both rows.
   */
  @Test
  public void arrayOfIntRead() {
    List<Long> allocs = new ArrayList<>();
    try {
      // Leaf flat INT column: values [10, 20, 30, 40, 50].
      long leafValues = Platform.allocateMemory(5 * 4);
      allocs.add(leafValues);
      for (int i = 0; i < 5; i++) {
        Platform.putInt(null, leafValues + (long) i * 4, (i + 1) * 10);
      }
      long leafDesc =
          allocDesc(
              5L, 0L, new long[] {0L, leafValues}, new long[0], allocs); // nBuffers=2, nChildren=0

      // Outer ARRAY column: 2 rows.
      // offsets: row0 starts at 0, row1 starts at 2.
      // sizes:   row0 has 2 elements, row1 has 3 elements.
      long outerOffsets = Platform.allocateMemory(2 * 4);
      allocs.add(outerOffsets);
      Platform.putInt(null, outerOffsets, 0);
      Platform.putInt(null, outerOffsets + 4, 2);

      long outerSizes = Platform.allocateMemory(2 * 4);
      allocs.add(outerSizes);
      Platform.putInt(null, outerSizes, 2);
      Platform.putInt(null, outerSizes + 4, 3);

      long outerDesc =
          allocDesc(
              2L, 0L, new long[] {0L, outerOffsets, outerSizes}, new long[] {leafDesc}, allocs);

      VeloxColumnVector cv =
          VeloxColumnVector.importFromNative(
              outerDesc, new ArrayType(DataTypes.IntegerType, false));

      ColumnarArray row0 = cv.getArray(0);
      assertEquals(2, row0.numElements());
      assertEquals(10, row0.getInt(0));
      assertEquals(20, row0.getInt(1));

      ColumnarArray row1 = cv.getArray(1);
      assertEquals(3, row1.numElements());
      assertEquals(30, row1.getInt(0));
      assertEquals(40, row1.getInt(1));
      assertEquals(50, row1.getInt(2));
    } finally {
      freeAll(allocs);
    }
  }

  /**
   * ROW&lt;INT,BIGINT&gt;: 2 rows; reads field values via {@link VeloxColumnVector#getChild}.
   *
   * <p>Spark's {@code ColumnarRow} internally calls {@code getChild(i)} to access struct fields, so
   * this test exercises the same code path.
   */
  @Test
  public void rowStructRead() {
    List<Long> allocs = new ArrayList<>();
    try {
      // Child 0: INT column [1, 2].
      long child0Values = Platform.allocateMemory(2 * 4);
      allocs.add(child0Values);
      Platform.putInt(null, child0Values, 1);
      Platform.putInt(null, child0Values + 4, 2);
      long child0Desc = allocDesc(2L, 0L, new long[] {0L, child0Values}, new long[0], allocs);

      // Child 1: BIGINT column [100, 200].
      long child1Values = Platform.allocateMemory(2 * 8);
      allocs.add(child1Values);
      Platform.putLong(null, child1Values, 100L);
      Platform.putLong(null, child1Values + 8, 200L);
      long child1Desc = allocDesc(2L, 0L, new long[] {0L, child1Values}, new long[0], allocs);

      // Struct outer: nBuffers=1 (nulls only), nChildren=2.
      long outerDesc =
          allocDesc(2L, 0L, new long[] {0L}, new long[] {child0Desc, child1Desc}, allocs);

      StructType structType =
          DataTypes.createStructType(
              new StructField[] {
                DataTypes.createStructField("a", DataTypes.IntegerType, false),
                DataTypes.createStructField("b", DataTypes.LongType, false)
              });
      VeloxColumnVector cv = VeloxColumnVector.importFromNative(outerDesc, structType);

      assertEquals(1, cv.getChild(0).getInt(0));
      assertEquals(2, cv.getChild(0).getInt(1));
      assertEquals(100L, cv.getChild(1).getLong(0));
      assertEquals(200L, cv.getChild(1).getLong(1));
      assertFalse(cv.isNullAt(0));
    } finally {
      freeAll(allocs);
    }
  }

  /**
   * MAP&lt;INT,INT&gt;: row0 = {1-&gt;10, 2-&gt;20}.
   *
   * <p>Verifies that {@link VeloxColumnVector#getMap} returns the correct offset/length and that
   * {@code keyArray()} / {@code valueArray()} expose the individual entries.
   */
  @Test
  public void mapIntIntRead() {
    List<Long> allocs = new ArrayList<>();
    try {
      // Key child INT [1, 2].
      long keyValues = Platform.allocateMemory(2 * 4);
      allocs.add(keyValues);
      Platform.putInt(null, keyValues, 1);
      Platform.putInt(null, keyValues + 4, 2);
      long keyDesc = allocDesc(2L, 0L, new long[] {0L, keyValues}, new long[0], allocs);

      // Value child INT [10, 20].
      long valValues = Platform.allocateMemory(2 * 4);
      allocs.add(valValues);
      Platform.putInt(null, valValues, 10);
      Platform.putInt(null, valValues + 4, 20);
      long valDesc = allocDesc(2L, 0L, new long[] {0L, valValues}, new long[0], allocs);

      // Outer MAP: 1 row; offset=0, size=2.
      long outerOffsets = Platform.allocateMemory(4);
      allocs.add(outerOffsets);
      Platform.putInt(null, outerOffsets, 0);

      long outerSizes = Platform.allocateMemory(4);
      allocs.add(outerSizes);
      Platform.putInt(null, outerSizes, 2);

      long outerDesc =
          allocDesc(
              1L,
              0L,
              new long[] {0L, outerOffsets, outerSizes},
              new long[] {keyDesc, valDesc},
              allocs);

      MapType mapType = DataTypes.createMapType(DataTypes.IntegerType, DataTypes.IntegerType);
      VeloxColumnVector cv = VeloxColumnVector.importFromNative(outerDesc, mapType);

      ColumnarMap map0 = cv.getMap(0);
      ColumnarArray keys = map0.keyArray();
      ColumnarArray vals = map0.valueArray();

      assertEquals(2, keys.numElements());
      assertEquals(1, keys.getInt(0));
      assertEquals(2, keys.getInt(1));

      assertEquals(2, vals.numElements());
      assertEquals(10, vals.getInt(0));
      assertEquals(20, vals.getInt(1));
    } finally {
      freeAll(allocs);
    }
  }

  /**
   * ARRAY&lt;ARRAY&lt;INT&gt;&gt;: row0 = [[10, 20], [30]].
   *
   * <p>Exercises the full recursive depth: the outer {@link ArrayAccessor} calls {@code
   * inner.getArray(i)}, which in turn calls the leaf {@link FlatAccessor}. This validates that
   * {@link VeloxColumnVector#importFromNative} recurses correctly through two levels of {@code
   * ArrayType} nesting.
   */
  @Test
  public void nestedArrayArrayIntRead() {
    List<Long> allocs = new ArrayList<>();
    try {
      // Leaf flat INT column: [10, 20, 30].
      long leafValues = Platform.allocateMemory(3 * 4);
      allocs.add(leafValues);
      Platform.putInt(null, leafValues, 10);
      Platform.putInt(null, leafValues + 4, 20);
      Platform.putInt(null, leafValues + 8, 30);
      long leafDesc = allocDesc(3L, 0L, new long[] {0L, leafValues}, new long[0], allocs);

      // Inner ARRAY<INT>: 2 inner arrays: [10,20] at offset 0 len 2; [30] at offset 2 len 1.
      long innerOffsets = Platform.allocateMemory(2 * 4);
      allocs.add(innerOffsets);
      Platform.putInt(null, innerOffsets, 0);
      Platform.putInt(null, innerOffsets + 4, 2);

      long innerSizes = Platform.allocateMemory(2 * 4);
      allocs.add(innerSizes);
      Platform.putInt(null, innerSizes, 2);
      Platform.putInt(null, innerSizes + 4, 1);

      long innerDesc =
          allocDesc(
              2L, 0L, new long[] {0L, innerOffsets, innerSizes}, new long[] {leafDesc}, allocs);

      // Outer ARRAY<ARRAY<INT>>: 1 row; outer row0 spans inner rows [0,1] → offset=0, size=2.
      long outerOffsets = Platform.allocateMemory(4);
      allocs.add(outerOffsets);
      Platform.putInt(null, outerOffsets, 0);

      long outerSizes = Platform.allocateMemory(4);
      allocs.add(outerSizes);
      Platform.putInt(null, outerSizes, 2);

      long outerDesc =
          allocDesc(
              1L, 0L, new long[] {0L, outerOffsets, outerSizes}, new long[] {innerDesc}, allocs);

      ArrayType innerArrayType = new ArrayType(DataTypes.IntegerType, false);
      ArrayType outerArrayType = new ArrayType(innerArrayType, false);
      VeloxColumnVector cv = VeloxColumnVector.importFromNative(outerDesc, outerArrayType);

      // outer row 0 → two inner arrays
      ColumnarArray outerRow0 = cv.getArray(0);
      assertEquals(2, outerRow0.numElements());

      // inner array 0 → [10, 20]
      ColumnarArray inner0 = outerRow0.getArray(0);
      assertEquals(2, inner0.numElements());
      assertEquals(10, inner0.getInt(0));
      assertEquals(20, inner0.getInt(1));

      // inner array 1 → [30]
      ColumnarArray inner1 = outerRow0.getArray(1);
      assertEquals(1, inner1.numElements());
      assertEquals(30, inner1.getInt(0));
    } finally {
      freeAll(allocs);
    }
  }

  /**
   * Verifies that {@link VeloxColumnarReadRow#copy()} produces an independent {@code
   * GenericInternalRow} with equal values for fixed-width (INT, LONG) columns and correctly
   * preserves {@code null} for null fields.
   *
   * <p>Two columns: INT [1, null] and LONG [100L, 200L]. Row 0 has values (1, 100L); row 1 has null
   * in col0 and 200L in col1.
   */
  @Test
  public void copyProducesIndependentRow() {
    List<Long> allocs = new ArrayList<>();
    try {
      // --- Column 0: INT [1, null] ---
      // Validity bitmap: row0=valid(1), row1=null(0) → byte 0b00000001 = 0x01.
      long nulls0 = Platform.allocateMemory(1);
      allocs.add(nulls0);
      Platform.putByte(null, nulls0, (byte) 0x01);

      long values0 = Platform.allocateMemory(2 * 4);
      allocs.add(values0);
      Platform.putInt(null, values0, 1);
      Platform.putInt(null, values0 + 4, 0); // null row — value ignored

      long col0Desc = allocDesc(2L, 1L, new long[] {nulls0, values0}, new long[0], allocs);

      // --- Column 1: LONG [100, 200] (no nulls) ---
      long values1 = Platform.allocateMemory(2 * 8);
      allocs.add(values1);
      Platform.putLong(null, values1, 100L);
      Platform.putLong(null, values1 + 8, 200L);

      long col1Desc = allocDesc(2L, 0L, new long[] {0L, values1}, new long[0], allocs);

      // Build two VeloxColumnVectors (no native close — Java-allocated descriptors).
      VeloxColumnVector cv0 = VeloxColumnVector.importFromNative(col0Desc, DataTypes.IntegerType);
      VeloxColumnVector cv1 = VeloxColumnVector.importFromNative(col1Desc, DataTypes.LongType);

      VeloxColumnarReadRow readRow = new VeloxColumnarReadRow(new VeloxColumnVector[] {cv0, cv1});

      // --- row 0: INT=1, LONG=100 ---
      readRow.rowId = 0;
      InternalRow copy0 = readRow.copy();
      assertNotSame(readRow, copy0);
      assertFalse(copy0.isNullAt(0));
      assertEquals(1, copy0.getInt(0));
      assertFalse(copy0.isNullAt(1));
      assertEquals(100L, copy0.getLong(1));

      // --- row 1: INT=null, LONG=200 ---
      readRow.rowId = 1;
      InternalRow copy1 = readRow.copy();
      assertNotSame(readRow, copy1);
      assertTrue(copy1.isNullAt(0));
      assertNull(copy1.get(0, DataTypes.IntegerType));
      assertFalse(copy1.isNullAt(1));
      assertEquals(200L, copy1.getLong(1));

      // Copy independence: mutating rowId should not affect already-copied rows.
      readRow.rowId = 0;
      assertEquals(200L, copy1.getLong(1)); // copy1 still holds row-1 value
    } finally {
      freeAll(allocs);
    }
  }

  private void assertReportedNullCount(long reported, byte validity) {
    List<Long> allocs = new ArrayList<>();
    try {
      long nulls = Platform.allocateMemory(8);
      long values = Platform.allocateMemory(16);
      allocs.add(nulls);
      allocs.add(values);
      Platform.putLong(null, nulls, -1L);
      Platform.putByte(null, nulls, validity);
      for (int i = 0; i < 4; i++) {
        Platform.putInt(null, values + i * 4L, 10 + i);
      }
      long desc = allocDesc(4, reported, new long[] {nulls, values}, new long[0], allocs);
      try (VeloxColumnVector cv =
          VeloxColumnVector.importFromNativeView(desc, DataTypes.IntegerType)) {
        assertEquals((int) reported, cv.numNulls());
        assertEquals((validity & 1) == 0, cv.isNullAt(0));
        assertEquals((validity & 2) == 0, cv.isNullAt(1));
        assertEquals((int) reported, cv.numNulls());
      }
    } finally {
      freeAll(allocs);
    }
  }

  @Test
  public void unknownNullCountRemainsUnknownAfterReadingValidity() {
    assertReportedNullCount(-1, (byte) 0x05);
  }

  @Test
  public void unknownNullCountRemainsUnknownWithAllValidRows() {
    assertReportedNullCount(-1, (byte) 0x0f);
  }

  @Test
  public void knownZeroAndPositiveNullCountsPassThrough() {
    assertReportedNullCount(0, (byte) 0x0f);
    assertReportedNullCount(2, (byte) 0x05);
  }

  @Test
  public void spark41ReadRowKeepsUnsupportedSpecializedGetters() {
    VeloxColumnarReadRow row = new VeloxColumnarReadRow(new VeloxColumnVector[0]);
    assertThrows(UnsupportedOperationException.class, () -> row.getVariant(0));
    assertThrows(UnsupportedOperationException.class, () -> row.getGeography(0));
    assertThrows(UnsupportedOperationException.class, () -> row.getGeometry(0));
  }
}
