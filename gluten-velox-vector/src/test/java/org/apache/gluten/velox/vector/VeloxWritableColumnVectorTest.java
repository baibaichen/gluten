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
import org.apache.spark.sql.internal.SQLConf;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.vectorized.ColumnarArray;
import org.apache.spark.unsafe.Platform;
import org.junit.jupiter.api.Test;

import javax.management.ObjectName;

import java.lang.management.ManagementFactory;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for {@link VeloxWritableColumnVector} using the test-only constructor.
 *
 * <p>All tests use manually managed off-heap memory so that no native library is required. The
 * descriptor, buffers pointer array, nulls bitmap, and values buffer are all allocated via {@link
 * Platform#allocateMemory} and freed in {@code finally} blocks.
 *
 * <h3>Descriptor layout</h3>
 *
 * <pre>
 * offset  field
 *  0      length      (int64)
 *  8      nullCount   (int64)
 * 16      nBuffers    (int64)
 * 24      nChildren   (int64)
 * 32      buffers     (pointer → array of nBuffers 8-byte addresses)
 * 40      children    (pointer → 0 for flat)
 * </pre>
 *
 * <h3>Null encoding</h3>
 *
 * Velox validity bitmap: {@code 1 = valid, 0 = null}, LSB-first within each byte. All rows are
 * pre-initialised to valid (0xFF) to match the contract of {@link
 * VeloxColumnHandleJniWrapper#allocateNestedOutput}.
 */
public class VeloxWritableColumnVectorTest {

  // ---- Helper ----

  /**
   * Allocates a hand-crafted 48-byte VeloxColumnHandle descriptor backed by a 2-pointer buffers
   * array, a nulls bitmap, and a values buffer.
   *
   * @param capacity number of rows
   * @param valueBytes total byte size of the values buffer (capacity × element stride)
   * @return {@code long[4] = {desc, buffers, nulls, values}} — all must be freed by the caller
   */
  private static long[] allocDescriptor(int capacity, int valueBytes) {
    // nulls: 1 byte per 8 rows; round up to 8 bytes for alignment
    int nullsBytes = Math.max(8, ((capacity + 7) / 8 + 7) & ~7);
    long nulls = Platform.allocateMemory(nullsBytes);
    // Initialize all rows to valid (Velox convention: 1 = valid).
    for (int i = 0; i < nullsBytes; i++) {
      Platform.putByte(null, nulls + i, (byte) 0xFF);
    }

    long values = Platform.allocateMemory(Math.max(8, valueBytes));

    // buffers pointer array: 2 entries × 8 bytes.
    long buffers = Platform.allocateMemory(16);
    Platform.putLong(null, buffers, nulls); // buffers[0] = nulls
    Platform.putLong(null, buffers + 8, values); // buffers[1] = values

    // 48-byte descriptor.
    long desc = Platform.allocateMemory(48);
    Platform.putLong(null, desc, capacity); // length
    Platform.putLong(null, desc + 8, 0L); // nullCount
    Platform.putLong(null, desc + 16, 2L); // nBuffers
    Platform.putLong(null, desc + 24, 0L); // nChildren
    Platform.putLong(null, desc + 32, buffers); // buffers ptr
    Platform.putLong(null, desc + 40, 0L); // children ptr (none)

    return new long[] {desc, buffers, nulls, values};
  }

  private static void freeDescriptor(long[] ptrs) {
    Platform.freeMemory(ptrs[0]); // desc
    Platform.freeMemory(ptrs[1]); // buffers
    Platform.freeMemory(ptrs[2]); // nulls
    Platform.freeMemory(ptrs[3]); // values
  }

  // ---- Tests ----

  @Test
  public void closeReclaimsPlaceholderNativeMemory() throws Exception {
    List<String> command = new ArrayList<>();
    command.add(Path.of(System.getProperty("java.home"), "bin", "java").toString());
    command.add("-XX:NativeMemoryTracking=summary");
    command.add("-Djava.io.tmpdir=" + System.getProperty("java.io.tmpdir"));
    ManagementFactory.getRuntimeMXBean().getInputArguments().stream()
        .filter(arg -> arg.startsWith("--add-opens=") || arg.startsWith("--add-exports="))
        .forEach(command::add);
    command.add("-cp");
    command.add(
        System.getProperty("surefire.test.class.path", System.getProperty("java.class.path")));
    command.add(VeloxWritableColumnVectorTest.class.getName());
    ProcessBuilder builder = new ProcessBuilder(command).redirectErrorStream(true);
    builder.environment().remove("JAVA_TOOL_OPTIONS");
    builder.environment().remove("_JAVA_OPTIONS");
    builder.environment().remove("JDK_JAVA_OPTIONS");
    Process child = builder.start();
    try {
      assertTrue(child.waitFor(30, TimeUnit.SECONDS), "NMT probe timed out");
      String output = new String(child.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
      assertEquals(0, child.exitValue(), output);
    } finally {
      if (child.isAlive()) {
        child.destroyForcibly();
        assertTrue(child.waitFor(5, TimeUnit.SECONDS), "Owned NMT probe did not terminate");
      }
    }
  }

  private static long otherMallocBytes() throws Exception {
    String summary =
        (String)
            ManagementFactory.getPlatformMBeanServer()
                .invoke(
                    new ObjectName("com.sun.management:type=DiagnosticCommand"),
                    "vmNativeMemory",
                    new Object[] {new String[] {"summary", "scale=KB"}},
                    new String[] {String[].class.getName()});
    Matcher other =
        Pattern.compile("(?m)^-\\s+Other\\s+\\([^\\r\\n]*\\)\\s*\\(malloc=(\\d+)KB")
            .matcher(summary);
    assertTrue(other.find(), "NMT Other malloc metric missing:\n" + summary);
    return Long.parseLong(other.group(1)) * 1024;
  }

  /** Runs only in the owned NMT-enabled child; leaked allocations die with that JVM. */
  public static void main(String[] args) throws Exception {
    int capacity = 4096;
    long[] ptrs = allocDescriptor(capacity, capacity * 16);
    try (VeloxWritableColumnVector allocator =
        new VeloxWritableColumnVector(ptrs[0], 0L, capacity, DataTypes.IntegerType)) {
      DataType[] types = {
        DataTypes.StringType,
        DataTypes.BinaryType,
        DataTypes.createArrayType(DataTypes.IntegerType),
        DataTypes.createMapType(DataTypes.IntegerType, DataTypes.StringType),
        DataTypes.createStructType(
            new StructField[] {
              DataTypes.createStructField("text", DataTypes.StringType, true),
              DataTypes.createStructField(
                  "ints", DataTypes.createArrayType(DataTypes.IntegerType), true)
            })
      };
      long largestGrowth = 0;
      for (DataType type : types) {
        Runnable constructAndClose =
            () -> {
              try (WritableColumnVector vector =
                  type == DataTypes.StringType || type == DataTypes.BinaryType
                      ? new VeloxWritableColumnVector(ptrs[0], 0L, capacity, type)
                      : allocator.reserveNewColumn(capacity, type)) {
                vector.reset();
                vector.setIsConstant();
              }
            };
        for (int i = 0; i < 8; i++) {
          constructAndClose.run();
        }
        otherMallocBytes();
        otherMallocBytes();
        long before = otherMallocBytes();
        for (int i = 0; i < 16; i++) {
          constructAndClose.run();
        }
        long after = otherMallocBytes();
        long growth = after - before;
        System.out.printf(
            "NMT Other malloc: %s before=%d after=%d unreclaimed=%d bytes%n",
            type, before, after, growth);
        largestGrowth = Math.max(largestGrowth, growth);
      }
      assertTrue(
          largestGrowth <= 4096,
          "Construct/close left " + largestGrowth + " native bytes unreclaimed (4KB tolerance)");
    } finally {
      freeDescriptor(ptrs);
    }
  }

  /**
   * Verifies that integer values written via {@link VeloxWritableColumnVector#putInt} are
   * immediately readable via {@link VeloxWritableColumnVector#getInt}.
   */
  @Test
  public void putAndGetIntRoundTrip() {
    int capacity = 4;
    long[] ptrs = allocDescriptor(capacity, capacity * 4);
    try {
      VeloxWritableColumnVector w =
          new VeloxWritableColumnVector(ptrs[0], 0L, capacity, DataTypes.IntegerType);

      w.putInt(0, 100);
      w.putInt(1, 200);
      w.putInt(2, 300);
      w.putInt(3, 400);

      assertEquals(100, w.getInt(0));
      assertEquals(200, w.getInt(1));
      assertEquals(300, w.getInt(2));
      assertEquals(400, w.getInt(3));

      // close() with ownerHandle=0 is a safe no-op for native resources.
      w.close();
    } finally {
      freeDescriptor(ptrs);
    }
  }

  /**
   * Verifies null-marking via the Velox validity bitmap. {@link VeloxWritableColumnVector#putNull}
   * must flip {@link VeloxWritableColumnVector#isNullAt} without corrupting neighbouring rows.
   */
  @Test
  public void nullHandlingUpdatesBitmapCorrectly() {
    int capacity = 4;
    long[] ptrs = allocDescriptor(capacity, capacity * 4);
    try {
      VeloxWritableColumnVector w =
          new VeloxWritableColumnVector(ptrs[0], 0L, capacity, DataTypes.IntegerType);

      w.putInt(0, 100);
      w.putInt(1, 200);
      w.putNull(2); // mark row 2 as null
      w.putInt(3, 400);

      assertFalse(w.isNullAt(0), "row 0 must be valid");
      assertFalse(w.isNullAt(1), "row 1 must be valid");
      assertTrue(w.isNullAt(2), "row 2 must be null");
      assertFalse(w.isNullAt(3), "row 3 must be valid");

      assertEquals(100, w.getInt(0));
      assertEquals(200, w.getInt(1));
      assertEquals(400, w.getInt(3));

      w.close();
    } finally {
      freeDescriptor(ptrs);
    }
  }

  /**
   * Verifies that {@link VeloxWritableColumnVector#putNotNull} re-validates a previously null row
   * and that the inherited {@code nullCount} tracks transitions correctly.
   */
  @Test
  public void putNotNullRevalidatesRow() {
    int capacity = 3;
    long[] ptrs = allocDescriptor(capacity, capacity * 4);
    try {
      VeloxWritableColumnVector w =
          new VeloxWritableColumnVector(ptrs[0], 0L, capacity, DataTypes.IntegerType);

      // Mark all rows null, then restore row 1.
      w.putNull(0);
      w.putNull(1);
      w.putNull(2);
      assertEquals(3, w.numNulls());

      w.putNotNull(1);
      w.putInt(1, 42);

      assertTrue(w.isNullAt(0));
      assertFalse(w.isNullAt(1));
      assertTrue(w.isNullAt(2));
      assertEquals(42, w.getInt(1));
      assertEquals(2, w.numNulls());
      assertTrue(w.hasNull());

      w.close();
    } finally {
      freeDescriptor(ptrs);
    }
  }

  /**
   * Verifies {@code numNulls()} and {@code hasNull()} consistency with the validity bitmap. Initial
   * state: all valid (0xFF bytes) → {@code nullCount = 0}, {@code hasNull() = false}.
   */
  @Test
  public void initialStateAllValid() {
    int capacity = 8;
    long[] ptrs = allocDescriptor(capacity, capacity * 8);
    try {
      VeloxWritableColumnVector w =
          new VeloxWritableColumnVector(ptrs[0], 0L, capacity, DataTypes.LongType);

      assertEquals(0, w.numNulls(), "fresh vector must have zero nulls");
      assertFalse(w.hasNull(), "fresh vector must report hasNull=false");

      for (int i = 0; i < capacity; i++) {
        assertFalse(w.isNullAt(i), "row " + i + " must be valid");
      }

      w.close();
    } finally {
      freeDescriptor(ptrs);
    }
  }

  /**
   * Verifies that {@link VeloxWritableColumnVector#putLong} and {@link
   * VeloxWritableColumnVector#getLong} round-trip correctly.
   */
  @Test
  public void putAndGetLongRoundTrip() {
    int capacity = 3;
    long[] ptrs = allocDescriptor(capacity, capacity * 8);
    try {
      VeloxWritableColumnVector w =
          new VeloxWritableColumnVector(ptrs[0], 0L, capacity, DataTypes.LongType);

      w.putLong(0, Long.MIN_VALUE);
      w.putLong(1, 0L);
      w.putLong(2, Long.MAX_VALUE);

      assertEquals(Long.MIN_VALUE, w.getLong(0));
      assertEquals(0L, w.getLong(1));
      assertEquals(Long.MAX_VALUE, w.getLong(2));

      w.close();
    } finally {
      freeDescriptor(ptrs);
    }
  }

  /**
   * Verifies that {@link VeloxWritableColumnVector#putDouble} and {@link
   * VeloxWritableColumnVector#getDouble} round-trip correctly.
   */
  @Test
  public void putAndGetDoubleRoundTrip() {
    int capacity = 2;
    long[] ptrs = allocDescriptor(capacity, capacity * 8);
    try {
      VeloxWritableColumnVector w =
          new VeloxWritableColumnVector(ptrs[0], 0L, capacity, DataTypes.DoubleType);

      w.putDouble(0, Math.PI);
      w.putDouble(1, -Math.E);

      assertEquals(Math.PI, w.getDouble(0), 0.0);
      assertEquals(-Math.E, w.getDouble(1), 0.0);

      w.close();
    } finally {
      freeDescriptor(ptrs);
    }
  }

  /**
   * Verifies that {@link VeloxWritableColumnVector#putShort} and {@link
   * VeloxWritableColumnVector#getShort} round-trip correctly.
   */
  @Test
  public void putAndGetShortRoundTrip() {
    int capacity = 4;
    long[] ptrs = allocDescriptor(capacity, capacity * 2);
    try {
      VeloxWritableColumnVector w =
          new VeloxWritableColumnVector(ptrs[0], 0L, capacity, DataTypes.ShortType);

      w.putShort(0, (short) -1);
      w.putShort(1, (short) 0);
      w.putShort(2, (short) 32767);
      w.putShort(3, (short) -32768);

      assertEquals((short) -1, w.getShort(0));
      assertEquals((short) 0, w.getShort(1));
      assertEquals((short) 32767, w.getShort(2));
      assertEquals((short) -32768, w.getShort(3));

      w.close();
    } finally {
      freeDescriptor(ptrs);
    }
  }

  /**
   * Verifies that {@link VeloxWritableColumnVector#putByte} and {@link
   * VeloxWritableColumnVector#getByte} round-trip correctly.
   */
  @Test
  public void putAndGetByteRoundTrip() {
    int capacity = 3;
    long[] ptrs = allocDescriptor(capacity, capacity);
    try {
      VeloxWritableColumnVector w =
          new VeloxWritableColumnVector(ptrs[0], 0L, capacity, DataTypes.ByteType);

      w.putByte(0, (byte) 0x7F);
      w.putByte(1, (byte) 0x00);
      w.putByte(2, (byte) -1);

      assertEquals((byte) 0x7F, w.getByte(0));
      assertEquals((byte) 0x00, w.getByte(1));
      assertEquals((byte) -1, w.getByte(2));

      w.close();
    } finally {
      freeDescriptor(ptrs);
    }
  }

  /**
   * Verifies that {@link VeloxWritableColumnVector#putFloat} and {@link
   * VeloxWritableColumnVector#getFloat} round-trip correctly.
   */
  @Test
  public void putAndGetFloatRoundTrip() {
    int capacity = 2;
    long[] ptrs = allocDescriptor(capacity, capacity * 4);
    try {
      VeloxWritableColumnVector w =
          new VeloxWritableColumnVector(ptrs[0], 0L, capacity, DataTypes.FloatType);

      w.putFloat(0, 1.5f);
      w.putFloat(1, Float.NaN);

      assertEquals(1.5f, w.getFloat(0), 0.0f);
      assertTrue(Float.isNaN(w.getFloat(1)));

      w.close();
    } finally {
      freeDescriptor(ptrs);
    }
  }

  /** Verifies that close() is idempotent — a second call must not throw or crash. */
  @Test
  public void closeIsIdempotent() {
    int capacity = 2;
    long[] ptrs = allocDescriptor(capacity, capacity * 4);
    try {
      VeloxWritableColumnVector w =
          new VeloxWritableColumnVector(ptrs[0], 0L, capacity, DataTypes.IntegerType);
      w.putInt(0, 1);
      w.close();
      // Second close must be a no-op.
      assertDoesNotThrow(w::close);
    } finally {
      freeDescriptor(ptrs);
    }
  }

  /**
   * Verifies that an unsupported type throws {@link UnsupportedOperationException} rather than
   * producing a silent incorrect result.
   */
  @Test
  public void unsupportedTypeThrows() {
    assertThrows(
        UnsupportedOperationException.class,
        () -> new VeloxWritableColumnVector(4, DataTypes.NullType));
  }

  // ---- Hand-crafted ARRAY<INT> descriptor helpers ----

  /**
   * Allocates a two-level hand-crafted ARRAY<INT> descriptor backed entirely by off-heap memory.
   *
   * <p>Root layout: nBuffers=3 (nulls at [0], offsets at [1], sizes at [2]), nChildren=1. Child
   * layout: nBuffers=2 (nulls at [0], values at [1]), nChildren=0.
   *
   * <p>All nulls bytes are initialized to {@code 0xFF} (Velox: 1=valid). All offsets, sizes, and
   * values are zero-initialized.
   *
   * @param rowCapacity number of array rows
   * @param elementCapacity number of element slots in the flat INT child
   * @return {@code long[10]} — all allocations, in order: [0]=rootDesc, [1]=rootBuffers,
   *     [2]=rootNulls, [3]=offsets, [4]=sizes, [5]=rootChildren, [6]=childDesc, [7]=childBuffers,
   *     [8]=childNulls, [9]=childValues
   */
  private static long[] allocArrayIntDescriptor(int rowCapacity, int elementCapacity) {
    // Root nulls bitmap (all valid).
    int rootNullsBytes = Math.max(8, ((rowCapacity + 7) / 8 + 7) & ~7);
    long rootNulls = Platform.allocateMemory(rootNullsBytes);
    for (int i = 0; i < rootNullsBytes; i++) {
      Platform.putByte(null, rootNulls + i, (byte) 0xFF);
    }

    // Offsets and sizes (int32 per row, zero-initialized).
    int rowBytes = Math.max(8, rowCapacity * 4);
    long offsets = Platform.allocateMemory(rowBytes);
    long sizes = Platform.allocateMemory(rowBytes);
    for (int i = 0; i < rowCapacity; i++) {
      Platform.putInt(null, offsets + (long) i * 4, 0);
      Platform.putInt(null, sizes + (long) i * 4, 0);
    }

    // Root buffers pointer array: 3 entries × 8 bytes.
    long rootBuffers = Platform.allocateMemory(24);
    Platform.putLong(null, rootBuffers, rootNulls); // buffers[0] = nulls
    Platform.putLong(null, rootBuffers + 8, offsets); // buffers[1] = offsets
    Platform.putLong(null, rootBuffers + 16, sizes); // buffers[2] = sizes

    // Child nulls bitmap (all valid).
    int childNullsBytes = Math.max(8, ((elementCapacity + 7) / 8 + 7) & ~7);
    long childNulls = Platform.allocateMemory(childNullsBytes);
    for (int i = 0; i < childNullsBytes; i++) {
      Platform.putByte(null, childNulls + i, (byte) 0xFF);
    }

    // Child values buffer (int32 per element, zero-initialized).
    int elemBytes = Math.max(8, elementCapacity * 4);
    long childValues = Platform.allocateMemory(elemBytes);
    for (int i = 0; i < elementCapacity; i++) {
      Platform.putInt(null, childValues + (long) i * 4, 0);
    }

    // Child buffers pointer array: 2 entries × 8 bytes.
    long childBuffers = Platform.allocateMemory(16);
    Platform.putLong(null, childBuffers, childNulls); // buffers[0] = nulls
    Platform.putLong(null, childBuffers + 8, childValues); // buffers[1] = values

    // Child descriptor (48 bytes).
    long childDesc = Platform.allocateMemory(48);
    Platform.putLong(null, childDesc, elementCapacity); // length
    Platform.putLong(null, childDesc + 8, 0L); // nullCount
    Platform.putLong(null, childDesc + 16, 2L); // nBuffers
    Platform.putLong(null, childDesc + 24, 0L); // nChildren
    Platform.putLong(null, childDesc + 32, childBuffers); // buffers ptr
    Platform.putLong(null, childDesc + 40, 0L); // children ptr (none)

    // Root children pointer array: 1 entry × 8 bytes.
    long rootChildren = Platform.allocateMemory(8);
    Platform.putLong(null, rootChildren, childDesc); // children[0] = childDesc

    // Root descriptor (48 bytes).
    long rootDesc = Platform.allocateMemory(48);
    Platform.putLong(null, rootDesc, rowCapacity); // length
    Platform.putLong(null, rootDesc + 8, 0L); // nullCount
    Platform.putLong(null, rootDesc + 16, 3L); // nBuffers
    Platform.putLong(null, rootDesc + 24, 1L); // nChildren
    Platform.putLong(null, rootDesc + 32, rootBuffers); // buffers ptr
    Platform.putLong(null, rootDesc + 40, rootChildren); // children ptr

    return new long[] {
      rootDesc, rootBuffers, rootNulls, offsets, sizes,
      rootChildren, childDesc, childBuffers, childNulls, childValues
    };
  }

  /** Frees all allocations from {@link #allocArrayIntDescriptor}. */
  private static void freeArrayIntDescriptor(long[] ptrs) {
    Platform.freeMemory(ptrs[9]); // childValues
    Platform.freeMemory(ptrs[8]); // childNulls
    Platform.freeMemory(ptrs[7]); // childBuffers
    Platform.freeMemory(ptrs[6]); // childDesc
    Platform.freeMemory(ptrs[5]); // rootChildren
    Platform.freeMemory(ptrs[4]); // sizes
    Platform.freeMemory(ptrs[3]); // offsets
    Platform.freeMemory(ptrs[2]); // rootNulls
    Platform.freeMemory(ptrs[1]); // rootBuffers
    Platform.freeMemory(ptrs[0]); // rootDesc
  }

  /**
   * TDD test (1): Write ARRAY&lt;INT&gt; rows via the write path, then read back via the read path
   * ({@link VeloxColumnVector#importFromNativeView}) and verify element values.
   *
   * <p>Writes: row0=[10,20], row1=[30,40,50]. Reads back with the read-side {@link
   * VeloxColumnVector} and asserts via {@link ColumnarArray#getInt}.
   */
  @Test
  public void putArrayNestedRoundTrip() {
    // 2 rows, 5 total element slots: row0=[10,20], row1=[30,40,50]
    final int rowCapacity = 2;
    final int elemCapacity = 5;
    long[] ptrs = allocArrayIntDescriptor(rowCapacity, elemCapacity);
    try {
      // --- WRITE phase ---
      VeloxWritableColumnVector w =
          new VeloxWritableColumnVector(
              ptrs[0], 0L, rowCapacity, DataTypes.createArrayType(DataTypes.IntegerType));

      // row0 = [10, 20] at element offsets 0..1
      w.putArray(0, 0, 2);
      VeloxWritableColumnVector child = w.getChildColumn();
      assertNotNull(child, "getChildColumn() must be non-null for ARRAY type");
      child.putInt(0, 10);
      child.putInt(1, 20);

      // row1 = [30, 40, 50] at element offsets 2..4
      w.putArray(1, 2, 3);
      child.putInt(2, 30);
      child.putInt(3, 40);
      child.putInt(4, 50);

      // Sanity-check writable-side getters before read-path verification.
      assertEquals(0, w.getArrayOffset(0), "row0 offset");
      assertEquals(2, w.getArrayLength(0), "row0 length");
      assertEquals(2, w.getArrayOffset(1), "row1 offset");
      assertEquals(3, w.getArrayLength(1), "row1 length");
      assertEquals(10, child.getInt(0));
      assertEquals(20, child.getInt(1));
      assertEquals(30, child.getInt(2));
      assertEquals(40, child.getInt(3));
      assertEquals(50, child.getInt(4));

      // --- READ-BACK phase via the read-side VeloxColumnVector ---
      // importFromNativeView does NOT take ownership of the descriptor (rootDescAddr=0),
      // so close() is a no-op and we free via freeArrayIntDescriptor in the finally block.
      VeloxColumnVector readVec =
          VeloxColumnVector.importFromNativeView(
              ptrs[0], DataTypes.createArrayType(DataTypes.IntegerType));

      ColumnarArray arr0 = readVec.getArray(0);
      assertEquals(2, arr0.numElements(), "row0 should have 2 elements");
      assertEquals(10, arr0.getInt(0), "row0[0]");
      assertEquals(20, arr0.getInt(1), "row0[1]");

      ColumnarArray arr1 = readVec.getArray(1);
      assertEquals(3, arr1.numElements(), "row1 should have 3 elements");
      assertEquals(30, arr1.getInt(0), "row1[0]");
      assertEquals(40, arr1.getInt(1), "row1[1]");
      assertEquals(50, arr1.getInt(2), "row1[2]");

      // close() is a no-op for both (ownerHandle=0, rootDescAddr=0).
      w.close();
      readVec.close();
    } finally {
      freeArrayIntDescriptor(ptrs);
    }
  }

  /**
   * Verifies that {@link VeloxWritableColumnVector#putArray} throws {@link
   * UnsupportedOperationException} when called on a flat scalar (non-ARRAY) vector.
   */
  @Test
  public void putArrayOnFlatTypeThrows() {
    int capacity = 4;
    long[] ptrs = allocDescriptor(capacity, capacity * 4);
    try {
      VeloxWritableColumnVector w =
          new VeloxWritableColumnVector(ptrs[0], 0L, capacity, DataTypes.IntegerType);
      assertThrows(UnsupportedOperationException.class, () -> w.putArray(0, 0, 1));
      w.close();
    } finally {
      freeDescriptor(ptrs);
    }
  }

  /**
   * Verifies that {@link VeloxWritableColumnVector#putByteArray} throws {@link
   * UnsupportedOperationException} for non-VARCHAR column types.
   */
  @Test
  public void putByteArrayThrowsForNonVarcharType() {
    int capacity = 2;
    long[] ptrs = allocDescriptor(capacity, capacity * 4);
    try {
      VeloxWritableColumnVector w =
          new VeloxWritableColumnVector(ptrs[0], 0L, capacity, DataTypes.IntegerType);
      assertThrows(
          UnsupportedOperationException.class, () -> w.putByteArray(0, new byte[] {1, 2}, 0, 2));
      w.close();
    } finally {
      freeDescriptor(ptrs);
    }
  }

  /**
   * Verifies that the {@link VeloxWritableColumnVector#ownerHandle()} getter returns the field
   * value (0 in test-only mode).
   */
  @Test
  public void ownerHandleGetterReturnsField() {
    // Minimal descriptor setup: just need the buffers pointer to be valid.
    long desc = Platform.allocateMemory(48);
    long buffers = Platform.allocateMemory(16);
    long nulls = Platform.allocateMemory(8);
    Platform.putByte(null, nulls, (byte) 0xFF); // all valid
    long values = Platform.allocateMemory(16);

    // Initialize descriptor fields.
    Platform.putLong(null, desc, 4L); // length
    Platform.putLong(null, desc + 8, 0L); // nullCount
    Platform.putLong(null, desc + 16, 2L); // nBuffers
    Platform.putLong(null, desc + 24, 0L); // nChildren
    Platform.putLong(null, desc + 32, buffers); // buffers ptr
    Platform.putLong(null, desc + 40, 0L); // children ptr

    // Point buffers[0] to nulls, buffers[1] to values.
    Platform.putLong(null, buffers, nulls);
    Platform.putLong(null, buffers + 8, values);

    VeloxWritableColumnVector v = new VeloxWritableColumnVector(desc, 0L, 4, DataTypes.IntegerType);
    assertEquals(0L, v.ownerHandle());
    v.close();

    Platform.freeMemory(values);
    Platform.freeMemory(nulls);
    Platform.freeMemory(buffers);
    Platform.freeMemory(desc);
  }

  /**
   * Allocates a hand-crafted descriptor for a VARCHAR (StringView) column backed entirely by
   * off-heap memory.
   *
   * <p>The values buffer holds {@code capacity × 16} bytes (one 16-byte StringView slot per row).
   * All StringView slots are zero-initialized. Nulls bitmap is initialized to all-valid (0xFF).
   *
   * @param capacity number of rows.
   * @return {@code long[4] = {desc, buffers, nulls, values}} — all must be freed by the caller.
   */
  private static long[] allocStringViewDescriptor(int capacity) {
    return allocDescriptor(capacity, capacity * 16);
  }

  // ---- VARCHAR / StringView write tests ----

  /**
   * TDD test (Task 3): write an inline string (≤12 bytes), an out-of-line string (&gt;12 bytes),
   * and a null row; then read back via {@link VeloxWritableColumnVector#getUTF8String} and {@link
   * VeloxWritableColumnVector#isNullAt}.
   *
   * <p>This test uses the package-private test-only constructor and {@link
   * VeloxWritableColumnVector#injectChunkForTest} to avoid loading the native library. The chunk
   * injection simulates the memory region that the native {@code allocateStringChunk} would
   * provide.
   *
   * <p>Layout verified against {@link FlatAccessor#getUTF8String}: the write in {@code
   * putByteArray} is the exact inverse of the FlatAccessor read.
   */
  @Test
  public void writeInlineAndLongStringView() {
    final int capacity = 3;
    long[] ptrs = allocStringViewDescriptor(capacity);
    // Allocate a fake string-data chunk (large enough for the 16-byte long string).
    long chunk = Platform.allocateMemory(256);
    try {
      VeloxWritableColumnVector v =
          new VeloxWritableColumnVector(ptrs[0], 0L, capacity, DataTypes.StringType);
      // Inject the pre-allocated chunk so ensureChunk() does not call JNI.
      v.injectChunkForTest(chunk, 256L);

      // Row 0: inline string "hi" (2 bytes, ≤12 → inline path).
      byte[] hi = "hi".getBytes(java.nio.charset.StandardCharsets.UTF_8);
      v.putByteArray(0, hi, 0, hi.length);

      // Row 1: long string "0123456789ABCDEF" (16 bytes, >12 → out-of-line path).
      byte[] longs = "0123456789ABCDEF".getBytes(java.nio.charset.StandardCharsets.UTF_8);
      v.putByteArray(1, longs, 0, longs.length);

      // Row 2: null.
      v.putNull(2);

      // finishStringColumn is a no-op in test-only mode (ownerHandle=0).
      v.finishStringColumn();

      // ---- Assertions ----
      assertEquals("hi", v.getUTF8String(0).toString(), "row 0: inline string round-trip");
      assertEquals(
          "0123456789ABCDEF", v.getUTF8String(1).toString(), "row 1: long string round-trip");
      assertTrue(v.isNullAt(2), "row 2 must be null");
      assertFalse(v.isNullAt(0), "row 0 must not be null");
      assertFalse(v.isNullAt(1), "row 1 must not be null");

      v.close();
    } finally {
      Platform.freeMemory(chunk);
      freeDescriptor(ptrs);
    }
  }

  /**
   * Verifies the inline-only StringView path for an empty string (size=0) and a 12-byte string (the
   * maximum inline size). Both must read back correctly via {@link
   * VeloxWritableColumnVector#getUTF8String}.
   */
  @Test
  public void writeStringViewInlineBoundary() {
    final int capacity = 3;
    long[] ptrs = allocStringViewDescriptor(capacity);
    try {
      VeloxWritableColumnVector v =
          new VeloxWritableColumnVector(ptrs[0], 0L, capacity, DataTypes.StringType);

      // Row 0: empty string (size=0, inline path, all 12 data bytes stay zero).
      v.putByteArray(0, new byte[0], 0, 0);

      // Row 1: exactly 12 bytes ("123456789012") — maximum inline size.
      byte[] twelve = "123456789012".getBytes(java.nio.charset.StandardCharsets.UTF_8);
      assertEquals(12, twelve.length, "must be exactly 12 bytes");
      v.putByteArray(1, twelve, 0, twelve.length);

      // Row 2: null via putNull.
      v.putNull(2);

      assertEquals("", v.getUTF8String(0).toString(), "row 0: empty string");
      assertEquals("123456789012", v.getUTF8String(1).toString(), "row 1: 12-byte inline string");
      assertTrue(v.isNullAt(2), "row 2 must be null");

      v.close();
    } finally {
      freeDescriptor(ptrs);
    }
  }

  /**
   * Verifies that {@link VeloxWritableColumnVector#getUTF8String} throws {@link
   * UnsupportedOperationException} for non-VARCHAR vectors.
   */
  @Test
  public void getUTF8StringThrowsForNonVarcharType() {
    int capacity = 2;
    long[] ptrs = allocDescriptor(capacity, capacity * 4);
    try {
      VeloxWritableColumnVector w =
          new VeloxWritableColumnVector(ptrs[0], 0L, capacity, DataTypes.IntegerType);
      assertThrows(UnsupportedOperationException.class, () -> w.getUTF8String(0));
      w.close();
    } finally {
      freeDescriptor(ptrs);
    }
  }

  /**
   * Verifies that writing an out-of-line string when the test-mode injected chunk is too small
   * throws {@link IllegalStateException} rather than silently corrupting native memory.
   *
   * <p>The injected chunk is only 4 bytes; the out-of-line string is 16 bytes (&gt;12, so it uses
   * the chunk path). {@code ensureChunk(16)} detects that 4 bytes are insufficient and, since
   * {@code ownerHandle == 0}, throws instead of silently writing past the end of the chunk.
   */
  @Test
  public void testModeChunkExhaustionThrowsIllegalStateException() {
    final int capacity = 2;
    long[] ptrs = allocStringViewDescriptor(capacity);
    // Deliberately inject a 4-byte chunk — too small for any out-of-line string (>12 bytes).
    long tinyChunk = Platform.allocateMemory(4);
    try {
      VeloxWritableColumnVector v =
          new VeloxWritableColumnVector(ptrs[0], 0L, capacity, DataTypes.StringType);
      v.injectChunkForTest(tinyChunk, 4L);

      byte[] longStr = "0123456789ABCDEF".getBytes(java.nio.charset.StandardCharsets.UTF_8);
      // Must throw: chunk is only 4 bytes, string is 16 bytes.
      assertThrows(
          IllegalStateException.class,
          () -> v.putByteArray(0, longStr, 0, longStr.length),
          "ensureChunk must throw when test-mode chunk is exhausted");

      v.close();
    } finally {
      Platform.freeMemory(tinyChunk);
      freeDescriptor(ptrs);
    }
  }

  /**
   * I2 regression: constructing VeloxWritableColumnVector for ARRAY&lt;VARCHAR&gt; must succeed.
   *
   * <p>Before the Task 3 fix, {@link VeloxWritableColumnVector#reserveNewColumn} threw {@link
   * UnsupportedOperationException} for {@code StringType}, causing the super-constructor to fail
   * when building the {@code childColumns[]} array for {@code ArrayType(StringType)}.
   *
   * <p>Uses a hand-crafted ARRAY&lt;VARCHAR&gt; descriptor (root: nBuffers=3, child: nBuffers=2
   * with 16 bytes/slot StringView).
   */
  @Test
  public void constructArrayOfVarcharSucceeds() {
    final int rowCapacity = 4;
    final int elemCapacity = 8;
    long[] ptrs = allocArrayIntDescriptor(rowCapacity, elemCapacity);
    // The allocArrayIntDescriptor gives us an ARRAY<INT> descriptor tree.
    // We reinterpret the root as ARRAY<VARCHAR> — the physical layout is compatible:
    // the child values buffer is 8 × 4 = 32 bytes, which is enough for 2 StringViews
    // (16 bytes each). Since we only test construction, no writes are needed.
    try {
      assertDoesNotThrow(
          () -> {
            VeloxWritableColumnVector w =
                new VeloxWritableColumnVector(
                    ptrs[0], 0L, rowCapacity, DataTypes.createArrayType(DataTypes.StringType));
            w.close();
          },
          "Construction of ARRAY<VARCHAR> must not throw (I2 regression)");
    } finally {
      freeArrayIntDescriptor(ptrs);
    }
  }

  /**
   * Allocates a hand-crafted ROW(INT, STRING) descriptor.
   *
   * <p>Root layout: nBuffers=1 (nulls), nChildren=2. Child 0 (INT): nBuffers=2 (nulls, int32
   * values), nChildren=0. Child 1 (STRING/VARCHAR): nBuffers=2 (nulls, StringView values 16
   * bytes/slot), nChildren=0.
   *
   * @param rowCapacity number of struct rows
   * @return {@code long[12]} all allocations in order: [0]=rootDesc, [1]=rootBuffers,
   *     [2]=rootNulls, [3]=rootChildren, [4]=child0Desc, [5]=child0Buffers, [6]=child0Nulls,
   *     [7]=child0Values, [8]=child1Desc, [9]=child1Buffers, [10]=child1Nulls, [11]=child1Values
   */
  private static long[] allocRowIntStringDescriptor(int rowCapacity) {
    // Root nulls bitmap (all valid).
    int rootNullsBytes = Math.max(8, ((rowCapacity + 7) / 8 + 7) & ~7);
    long rootNulls = Platform.allocateMemory(rootNullsBytes);
    for (int i = 0; i < rootNullsBytes; i++) {
      Platform.putByte(null, rootNulls + i, (byte) 0xFF);
    }

    // Root buffers pointer array: 1 entry (nulls only for ROW).
    long rootBuffers = Platform.allocateMemory(8);
    Platform.putLong(null, rootBuffers, rootNulls);

    // Child 0: INT field — nBuffers=2 (nulls + int32 values).
    int c0NullsBytes = Math.max(8, ((rowCapacity + 7) / 8 + 7) & ~7);
    long c0Nulls = Platform.allocateMemory(c0NullsBytes);
    for (int i = 0; i < c0NullsBytes; i++) Platform.putByte(null, c0Nulls + i, (byte) 0xFF);
    long c0Values = Platform.allocateMemory(Math.max(8, rowCapacity * 4));
    long c0Buffers = Platform.allocateMemory(16);
    Platform.putLong(null, c0Buffers, c0Nulls);
    Platform.putLong(null, c0Buffers + 8, c0Values);
    long c0Desc = Platform.allocateMemory(48);
    Platform.putLong(null, c0Desc, rowCapacity);
    Platform.putLong(null, c0Desc + 8, 0L);
    Platform.putLong(null, c0Desc + 16, 2L);
    Platform.putLong(null, c0Desc + 24, 0L);
    Platform.putLong(null, c0Desc + 32, c0Buffers);
    Platform.putLong(null, c0Desc + 40, 0L);

    // Child 1: STRING/VARCHAR field — nBuffers=2 (nulls + StringView values, 16 bytes/slot).
    int c1NullsBytes = Math.max(8, ((rowCapacity + 7) / 8 + 7) & ~7);
    long c1Nulls = Platform.allocateMemory(c1NullsBytes);
    for (int i = 0; i < c1NullsBytes; i++) Platform.putByte(null, c1Nulls + i, (byte) 0xFF);
    long c1Values = Platform.allocateMemory(Math.max(8, rowCapacity * 16)); // 16 bytes/StringView
    long c1Buffers = Platform.allocateMemory(16);
    Platform.putLong(null, c1Buffers, c1Nulls);
    Platform.putLong(null, c1Buffers + 8, c1Values);
    long c1Desc = Platform.allocateMemory(48);
    Platform.putLong(null, c1Desc, rowCapacity);
    Platform.putLong(null, c1Desc + 8, 0L);
    Platform.putLong(null, c1Desc + 16, 2L);
    Platform.putLong(null, c1Desc + 24, 0L);
    Platform.putLong(null, c1Desc + 32, c1Buffers);
    Platform.putLong(null, c1Desc + 40, 0L);

    // Root children pointer array: 2 entries (child0, child1).
    long rootChildren = Platform.allocateMemory(16);
    Platform.putLong(null, rootChildren, c0Desc);
    Platform.putLong(null, rootChildren + 8, c1Desc);

    // Root descriptor.
    long rootDesc = Platform.allocateMemory(48);
    Platform.putLong(null, rootDesc, rowCapacity);
    Platform.putLong(null, rootDesc + 8, 0L);
    Platform.putLong(null, rootDesc + 16, 1L); // nBuffers=1 (nulls only for ROW)
    Platform.putLong(null, rootDesc + 24, 2L); // nChildren=2
    Platform.putLong(null, rootDesc + 32, rootBuffers);
    Platform.putLong(null, rootDesc + 40, rootChildren);

    return new long[] {
      rootDesc, rootBuffers, rootNulls, rootChildren,
      c0Desc, c0Buffers, c0Nulls, c0Values,
      c1Desc, c1Buffers, c1Nulls, c1Values
    };
  }

  /** Frees all allocations from {@link #allocRowIntStringDescriptor}. */
  private static void freeRowIntStringDescriptor(long[] ptrs) {
    Platform.freeMemory(ptrs[11]); // c1Values
    Platform.freeMemory(ptrs[10]); // c1Nulls
    Platform.freeMemory(ptrs[9]); // c1Buffers
    Platform.freeMemory(ptrs[8]); // c1Desc
    Platform.freeMemory(ptrs[7]); // c0Values
    Platform.freeMemory(ptrs[6]); // c0Nulls
    Platform.freeMemory(ptrs[5]); // c0Buffers
    Platform.freeMemory(ptrs[4]); // c0Desc
    Platform.freeMemory(ptrs[3]); // rootChildren
    Platform.freeMemory(ptrs[2]); // rootNulls
    Platform.freeMemory(ptrs[1]); // rootBuffers
    Platform.freeMemory(ptrs[0]); // rootDesc
  }

  /**
   * I2 regression: constructing VeloxWritableColumnVector for a StructType (ROW) with INT and
   * VARCHAR fields must succeed.
   *
   * <p>Before the Task 3 fix, {@link VeloxWritableColumnVector#reserveNewColumn} threw for {@code
   * StringType}, causing the super-constructor to fail when populating {@code childColumns[]} for
   * {@code StructType}.
   *
   * <p>Also verifies that the native-backed INT child can be written and read back via Spark's
   * {@code getChild(0)} after construction.
   */
  @Test
  public void constructRowOfIntAndStringSucceeds() {
    final int rowCapacity = 3;
    long[] ptrs = allocRowIntStringDescriptor(rowCapacity);
    try {
      VeloxWritableColumnVector w =
          new VeloxWritableColumnVector(
              ptrs[0],
              0L,
              rowCapacity,
              DataTypes.createStructType(
                  new org.apache.spark.sql.types.StructField[] {
                    DataTypes.createStructField("id", DataTypes.IntegerType, false),
                    DataTypes.createStructField("name", DataTypes.StringType, true)
                  }));

      assertNotNull(w, "ROW<INT,STRING> construction must succeed");

      // The INT field child (field 0) must be a native-backed VeloxWritableColumnVector
      // that supports putInt/getInt.
      VeloxWritableColumnVector intChild = (VeloxWritableColumnVector) w.getChild(0);
      assertNotNull(intChild, "INT child (ordinal 0) must not be null");
      intChild.putInt(0, 42);
      intChild.putInt(1, 100);
      intChild.putInt(2, -7);
      assertEquals(42, intChild.getInt(0));
      assertEquals(100, intChild.getInt(1));
      assertEquals(-7, intChild.getInt(2));

      w.close();
    } finally {
      freeRowIntStringDescriptor(ptrs);
    }
  }

  /**
   * Verifies that {@link VeloxWritableColumnVector#putArray} correctly writes offset and size
   * values into the ARRAY descriptor's offsets and sizes buffers, and that {@link
   * VeloxWritableColumnVector#getChildColumn()} returns the native-backed element child for direct
   * writes.
   *
   * <p>This is a supplemental test for the offset/size write path and child descent. The full
   * end-to-end round-trip (write then read-back via VeloxColumnVector) is covered by {@link
   * #putArrayNestedRoundTrip}.
   */
  @Test
  public void putArrayOffsetSizeWriteAndChildDescent() {
    final int rowCapacity = 3;
    final int elemCapacity = 7;
    long[] ptrs = allocArrayIntDescriptor(rowCapacity, elemCapacity);
    try {
      VeloxWritableColumnVector w =
          new VeloxWritableColumnVector(
              ptrs[0], 0L, rowCapacity, DataTypes.createArrayType(DataTypes.IntegerType));

      // Write three arrays: row0=[5,6], row1=[7,8,9], row2=[10]
      w.putArray(0, 0, 2);
      w.putArray(1, 2, 3);
      w.putArray(2, 5, 1);

      // Verify offsets and lengths
      assertEquals(0, w.getArrayOffset(0), "row0 offset");
      assertEquals(2, w.getArrayLength(0), "row0 length");
      assertEquals(2, w.getArrayOffset(1), "row1 offset");
      assertEquals(3, w.getArrayLength(1), "row1 length");
      assertEquals(5, w.getArrayOffset(2), "row2 offset");
      assertEquals(1, w.getArrayLength(2), "row2 length");

      // Child descent: getChildColumn() must return the native-backed element child.
      VeloxWritableColumnVector child = w.getChildColumn();
      assertNotNull(child, "getChildColumn() must return non-null for ARRAY type");
      child.putInt(0, 5);
      child.putInt(1, 6);
      child.putInt(2, 7);
      child.putInt(3, 8);
      child.putInt(4, 9);
      child.putInt(5, 10);

      assertEquals(5, child.getInt(0));
      assertEquals(6, child.getInt(1));
      assertEquals(9, child.getInt(4));
      assertEquals(10, child.getInt(5));

      w.close();
    } finally {
      freeArrayIntDescriptor(ptrs);
    }
  }

  /**
   * Verifies that {@link VeloxWritableColumnVector#reserveInternal} is a no-op when {@code
   * newCapacity <= current capacity} (no native call needed).
   */
  @Test
  public void reserveInternalSameCapacityIsNoOp() {
    int capacity = 4;
    long[] ptrs = allocDescriptor(capacity, capacity * 4);
    try {
      VeloxWritableColumnVector w =
          new VeloxWritableColumnVector(ptrs[0], 0L, capacity, DataTypes.IntegerType);
      w.putInt(0, 99);
      // Calling reserve with same capacity must not throw or corrupt data.
      assertDoesNotThrow(() -> w.reserve(capacity));
      assertEquals(99, w.getInt(0), "value must survive no-op reserve");
      w.close();
    } finally {
      freeDescriptor(ptrs);
    }
  }

  /**
   * Verifies that {@link VeloxWritableColumnVector#reserveInternal} throws with a descriptive error
   * when growth is attempted in test-only mode (no native rootOwnerHandle).
   *
   * <p>Root-level and test-only vectors cannot call {@code growChild} (ownerHandle=0, empty
   * childPath). Growth via JNI is tested in the native-backed scalatest suite (Task 4).
   */
  @Test
  public void reserveInternalGrowthThrowsInTestOnlyMode() {
    int capacity = 4;
    long[] ptrs = allocDescriptor(capacity, capacity * 4);
    try {
      VeloxWritableColumnVector w =
          new VeloxWritableColumnVector(ptrs[0], 0L, capacity, DataTypes.IntegerType);
      // Requesting a larger capacity in test-only mode must throw since growChild cannot
      // be called without a native owner handle.
      assertThrows(UnsupportedOperationException.class, () -> w.reserve(capacity + 1));
      w.close();
    } finally {
      freeDescriptor(ptrs);
    }
  }

  private static final String HUGE_VECTOR_THRESHOLD =
      "spark.sql.inMemoryColumnarStorage.hugeVectorThreshold";
  private static final String HUGE_VECTOR_RESERVE_RATIO =
      "spark.sql.inMemoryColumnarStorage.hugeVectorReserveRatio";

  private static void withHugeVectorConf(String threshold, String ratio, Runnable body) {
    SQLConf conf = SQLConf.get();
    String previousThreshold = conf.getConfString(HUGE_VECTOR_THRESHOLD, null);
    String previousRatio = conf.getConfString(HUGE_VECTOR_RESERVE_RATIO, null);
    try {
      conf.setConfString(HUGE_VECTOR_THRESHOLD, threshold);
      conf.setConfString(HUGE_VECTOR_RESERVE_RATIO, ratio);
      body.run();
    } finally {
      if (previousThreshold == null) {
        conf.unsetConf(HUGE_VECTOR_THRESHOLD);
      } else {
        conf.setConfString(HUGE_VECTOR_THRESHOLD, previousThreshold);
      }
      if (previousRatio == null) {
        conf.unsetConf(HUGE_VECTOR_RESERVE_RATIO);
      } else {
        conf.setConfString(HUGE_VECTOR_RESERVE_RATIO, previousRatio);
      }
    }
  }

  private static void assertHugeVectorRejected(String threshold) {
    long[] ptrs = allocDescriptor(4, 16);
    try {
      withHugeVectorConf(
          threshold,
          "1.2",
          () -> {
            UnsupportedOperationException rootError =
                assertThrows(
                    UnsupportedOperationException.class,
                    () -> {
                      try (VeloxWritableColumnVector ignored =
                          new VeloxWritableColumnVector(ptrs[0], 0L, 4, DataTypes.IntegerType)) {
                        fail("Expected huge-vector rejection before construction");
                      }
                    });
            assertTrue(rootError.getMessage().contains("hugeVector"), rootError.getMessage());
            UnsupportedOperationException childError =
                assertThrows(
                    UnsupportedOperationException.class,
                    () -> {
                      try (VeloxWritableColumnVector ignored =
                          new VeloxWritableColumnVector(
                              ptrs[0], 0L, 4, DataTypes.IntegerType, 0L, new int[] {0})) {
                        fail("Expected huge-vector rejection before construction");
                      }
                    });
            assertTrue(childError.getMessage().contains("hugeVector"), childError.getMessage());
          });
    } finally {
      freeDescriptor(ptrs);
    }
  }

  @Test
  public void zeroHugeVectorThresholdRejectsWriterConstruction() {
    assertHugeVectorRejected("0");
  }

  @Test
  public void positiveHugeVectorThresholdRejectsWriterConstruction() {
    assertHugeVectorRejected("1");
  }

  @Test
  public void hugeVectorThresholdRejectsEvenBelowTheThreshold() {
    assertHugeVectorRejected("4096");
  }

  @Test
  public void disabledHugeVectorThresholdDoesNotTreatReserveRatioAsAnEnableSwitch() {
    long[] ptrs = allocDescriptor(4, 16);
    try {
      withHugeVectorConf(
          "-1",
          "3.0",
          () -> {
            try (VeloxWritableColumnVector cv =
                new VeloxWritableColumnVector(ptrs[0], 0L, 4, DataTypes.IntegerType)) {
              cv.putInt(0, 37);
              assertEquals(37, cv.getInt(0));
              assertEquals(0L, cv.ownerHandle());
            }
          });
    } finally {
      freeDescriptor(ptrs);
    }
  }

  @Test
  public void hugeVectorRejectionPrecedesTypeEncodingAndNativeAllocation() {
    // NullType has no writer encoding, so neither branch can reach a JNI allocation.
    withHugeVectorConf(
        "0",
        "1.2",
        () -> {
          UnsupportedOperationException error =
              assertThrows(
                  UnsupportedOperationException.class,
                  () -> new VeloxWritableColumnVector(1, DataTypes.NullType));
          assertTrue(error.getMessage().contains("hugeVector"), error.getMessage());
        });
  }
}
