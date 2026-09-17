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

import org.apache.spark.sql.execution.vectorized.OnHeapColumnVector;
import org.apache.spark.sql.execution.vectorized.WritableColumnVector;
import org.apache.spark.sql.types.ArrayType;
import org.apache.spark.sql.types.BinaryType;
import org.apache.spark.sql.types.BooleanType;
import org.apache.spark.sql.types.ByteType;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DateType;
import org.apache.spark.sql.types.Decimal;
import org.apache.spark.sql.types.DecimalType;
import org.apache.spark.sql.types.DoubleType;
import org.apache.spark.sql.types.FloatType;
import org.apache.spark.sql.types.IntegerType;
import org.apache.spark.sql.types.LongType;
import org.apache.spark.sql.types.MapType;
import org.apache.spark.sql.types.ShortType;
import org.apache.spark.sql.types.StringType;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.types.TimestampType;
import org.apache.spark.unsafe.Platform;
import org.apache.spark.unsafe.types.UTF8String;

import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * A Spark 4.1 {@link WritableColumnVector} backed by native Velox vectors for test expression I/O.
 *
 * <p>Read support below describes {@link VeloxColumnVector}/{@link VeloxColumnarReadRow}; write
 * support describes this class and {@link VeloxColumnarRow}. Typed readers, writers and logical
 * result adapters are distinct: the table does not authorize reinterpreting an unsupported type.
 *
 * <table>
 *   <caption>Logical types, native layout and supported access paths</caption>
 *   <tr><th>Spark type</th><th>Velox type / physical layout</th><th>Read</th><th>Write</th></tr>
 *   <tr><td>Boolean</td><td>BOOLEAN / packed bits</td><td>getBoolean</td><td>putBoolean</td></tr>
 *   <tr><td>Byte, Short, Integer, Long</td>
 *       <td>TINYINT, SMALLINT, INTEGER, BIGINT / 1, 2, 4, 8 bytes</td>
 *       <td>Typed getters</td><td>Typed put methods</td></tr>
 *   <tr><td>Float, Double</td><td>REAL, DOUBLE / 4, 8 bytes</td>
 *       <td>Typed getters</td><td>Typed put methods</td></tr>
 *   <tr><td>Date</td><td>DATE / int32 epoch days</td><td>getInt</td><td>putInt</td></tr>
 *   <tr><td>Timestamp</td><td>TIMESTAMP / int64 seconds + uint64 nanoseconds</td>
 *       <td>getLong reconstructs microseconds</td><td>putTimestampMicros only</td></tr>
 *   <tr><td>Decimal</td><td>DECIMAL / unscaled int64 or int128</td>
 *       <td>getDecimal</td><td>putDecimal</td></tr>
 *   <tr><td>String, Binary</td>
 *       <td>VARCHAR, VARBINARY / 16-byte StringView; up to 12 bytes inline</td>
 *       <td>Borrowed UTF8String / copied byte[]</td><td>putByteArray + finishStringColumn</td></tr>
 *   <tr><td>Array, Map</td><td>ARRAY, MAP / int32 offsets and sizes + child vectors</td>
 *       <td>ColumnarArray / ColumnarMap</td><td>putArray and child writers; row.update</td></tr>
 *   <tr><td>Struct</td><td>ROW / validity and field vectors</td>
 *       <td>Child row view</td><td>Child writers; row.update</td></tr>
 *   <tr><td>CalendarInterval</td>
 *       <td>CALENDAR_INTERVAL / packed int32 months, int32 days, int64 micros</td>
 *       <td>Supported packed-field reader</td><td>Unsupported</td></tr>
 *   <tr><td>Null</td><td>UNKNOWN / validity only</td><td>Supported null reader</td>
 *       <td>Unsupported</td></tr>
 *   <tr><td>Other types, including Variant and spatial types</td><td>No bridge mapping</td>
 *       <td>Unsupported</td><td>Unsupported</td></tr>
 * </table>
 *
 * <p>Generic putLong/appendLong do not write Timestamp's 16-byte representation; use the dedicated
 * method. String/Binary appendByteArray is unsupported; use putByteArray and finalize explicitly.
 * Root capacity is fixed; existing nested-child growth remains supported. Spark's huge-vector
 * reset/shrink optimization is unsupported and rejected at construction when enabled.
 *
 * <h3>Copy limitations</h3>
 *
 * <p>VeloxColumnarRow.copy supports non-null scalar writer types, not complex values.
 * VeloxColumnarReadRow.copy also delegates supported nested copies, except collection elements that
 * are structs containing CalendarInterval. Direct CalendarInterval reads and copies remain
 * supported. Borrowed string views require the backing vector to remain alive; writes copy the
 * supplied bytes into native storage rather than transferring ownership of the source.
 *
 * <h3>Null encoding</h3>
 *
 * Velox's validity bitmap uses {@code 1 = valid, 0 = null} (LSB-first within each byte), the
 * opposite of Arrow. On allocation every row is pre-initialised to valid (all bytes = 0xFF) by the
 * native {@link VeloxColumnHandleJniWrapper#allocateNestedOutput} call. {@link #putNull} clears the
 * relevant bit and increments the inherited {@code nullCount} field; {@link #putNotNull} sets the
 * bit and decrements {@code nullCount}.
 *
 * <h3>Ownership and close() contract</h3>
 *
 * <ul>
 *   <li>The production constructor (with JNI) obtains an {@code ownerHandle} from the native
 *       ObjectStore and a descriptor address. {@link #close()} releases the vector via {@link
 *       VeloxColumnHandleJniWrapper#releaseOutput} and frees the descriptor via {@link
 *       VeloxColumnHandleJniWrapper#freeDescriptor}.
 *   <li>The package-private test-only constructor accepts pre-allocated off-heap addresses; {@code
 *       ownerHandle = 0} signals that {@link #close()} is a safe no-op for native resources (the
 *       test manages memory directly).
 * </ul>
 *
 * <h3>Descriptor layout (VeloxColumnHandle)</h3>
 *
 * <pre>
 * offset  size  field
 *  0       8    length
 *  8       8    nullCount
 * 16       8    nBuffers
 * 24       8    nChildren
 * 32       8    buffers  -> pointer to array of nBuffers 8-byte addresses
 * 40       8    children -> pointer to array of nChildren 8-byte addresses
 * </pre>
 */
public class VeloxWritableColumnVector extends WritableColumnVector {

  // ---- Descriptor field offsets ----

  /** Byte offset of the {@code buffers} pointer in the VeloxColumnHandle struct. */
  private static final int H_BUFFERS = 32;

  /** Byte offset of the {@code children} pointer in the VeloxColumnHandle struct. */
  private static final int H_CHILDREN = 40;

  /** Empty int array constant used as the default child path for root vectors. */
  private static final int[] EMPTY_INT_ARRAY = new int[0];

  // ---- Instance fields ----

  /** Native address of the root VeloxColumnHandle descriptor (0 for the test-only path). */
  private final long descAddr;

  /** Native address of the Velox validity bitmap (buffers[0]). Never 0 for fixed-width types. */
  private long nullsAddr;

  /**
   * Native address of the Velox values buffer (buffers[1]) for flat scalar types. {@code 0L} for
   * {@link ArrayType} columns (no flat values buffer at the root level).
   */
  private long valuesAddr;

  /**
   * Native address of the int32 offsets buffer (buffers[1]) for {@link ArrayType} columns. {@code
   * 0L} for flat scalar types.
   */
  private long offsetsAddr;

  /**
   * Native address of the int32 sizes buffer (buffers[2]) for {@link ArrayType} columns. {@code 0L}
   * for flat scalar types.
   */
  private long sizesAddr;

  /**
   * Child writable column for {@link ArrayType} columns (wraps the child descriptor at
   * children[0]). {@code null} for flat scalar types.
   */
  private VeloxWritableColumnVector childColumn;

  /**
   * ObjectStore owner handle from {@link VeloxColumnHandleJniWrapper#allocateNestedOutput}. {@code
   * 0} indicates the test-only path where no native JNI allocation was performed.
   */
  private long ownerHandle;

  /**
   * {@code true} iff this instance allocated the native resources via JNI and is responsible for
   * freeing them. {@code false} for the test-only constructor where the caller manages memory.
   */
  private final boolean ownsNativeResources;

  /**
   * Owner handle of the ROOT vector in the nested tree, used with growChild. 0L if growth
   * unsupported.
   */
  private final long rootOwnerHandle;

  /** Path from root to this child (empty for root vectors). */
  private final int[] childPath;

  /** Guards against double-close. */
  private boolean closed = false;

  // ---- VARCHAR / StringView chunk state ----
  // Used only when isVarchar is true. Chunk memory is allocated via native JNI
  // (allocateStringChunk) for long (>12 byte) strings; the inline path writes
  // directly into the StringView slot.

  /** {@code true} iff this vector holds VARCHAR (StringView, 16 bytes/slot) data. */
  private final boolean isVarchar;

  /**
   * {@code true} iff this vector holds TIMESTAMP (Velox {@code Timestamp}, 16 bytes/slot) data.
   * When {@code true}, {@link #getLong} reconstructs micros from the 16-byte {sec, nanos} struct
   * instead of using the generic 8-byte long read.
   */
  private final boolean isTimestamp;

  /** DECIMAL precision, or {@code 0} for non-decimal vectors. */
  private final int decimalPrecision;

  /** DECIMAL scale, or {@code 0} for non-decimal vectors. */
  private final int decimalScale;

  /** Raw address of the current native string-data chunk (0 if no chunk allocated yet). */
  private long curChunkAddr = 0L;

  /** Usable capacity in bytes of the current string-data chunk (0 if none). */
  private long curChunkCap = 0L;

  /** Number of bytes written into the current chunk so far. */
  private long curChunkOffset = 0L;

  /**
   * Per-chunk used-byte counts in allocation order. Populated by {@link #ensureChunk} on rollover
   * and by {@link #finishStringColumn} for the last chunk. Passed to {@link
   * VeloxColumnHandleJniWrapper#finalizeStringColumn} at the end.
   */
  private final List<Long> chunkUsed = new ArrayList<>();

  // ---- Constructors ----

  /**
   * Production constructor: allocates a native Velox vector via JNI.
   *
   * <p>Calls {@link VeloxColumnHandleJniWrapper#allocateNestedOutput} with a DFS-encoded type
   * representation for both flat and nested types. Nested descriptors are wired up into
   * native-backed child {@link VeloxWritableColumnVector} instances.
   *
   * @param capacity number of rows to pre-allocate.
   * @param type Spark {@link DataType}; must be a supported type.
   */
  public VeloxWritableColumnVector(int capacity, DataType type) {
    super(capacity, type);
    rejectHugeVectorOptimization();
    final long[] result =
        VeloxColumnHandleJniWrapper.allocateNestedOutput(encodeType(type), capacity);
    this.descAddr = result[0];
    this.ownerHandle = result[1];
    this.ownsNativeResources = true;
    this.rootOwnerHandle = result[1];
    this.childPath = EMPTY_INT_ARRAY;
    long buffersPtr = Platform.getLong(null, descAddr + H_BUFFERS);
    this.nullsAddr = Platform.getLong(null, buffersPtr);
    if (type instanceof ArrayType || type instanceof MapType) {
      this.valuesAddr = 0L;
      this.offsetsAddr = Platform.getLong(null, buffersPtr + 8L);
      this.sizesAddr = Platform.getLong(null, buffersPtr + 16L);
      if (type instanceof ArrayType) {
        this.childColumn = buildArrayChild((ArrayType) type, descAddr, result[1], EMPTY_INT_ARRAY);
        if (childColumn != null) {
          childColumns[0] = childColumn;
        }
      } else {
        this.childColumn = null;
        buildMapChildren((MapType) type, descAddr, result[1], EMPTY_INT_ARRAY);
      }
    } else if (type instanceof StructType) {
      this.valuesAddr = 0L;
      this.offsetsAddr = 0L;
      this.sizesAddr = 0L;
      this.childColumn = null;
      buildStructChildren((StructType) type, descAddr, result[1], EMPTY_INT_ARRAY);
    } else {
      this.valuesAddr = Platform.getLong(null, buffersPtr + 8L);
      this.offsetsAddr = 0L;
      this.sizesAddr = 0L;
      this.childColumn = null;
    }
    this.isVarchar = type instanceof StringType || type instanceof BinaryType;
    this.isTimestamp = type instanceof TimestampType;
    if (type instanceof DecimalType) {
      DecimalType d = (DecimalType) type;
      this.decimalPrecision = d.precision();
      this.decimalScale = d.scale();
    } else {
      this.decimalPrecision = 0;
      this.decimalScale = 0;
    }
  }

  /**
   * Test-only constructor: wraps pre-allocated off-heap addresses without performing any JNI call.
   *
   * <p>Supports both flat scalar types and {@link ArrayType} columns. For flat types the descriptor
   * must have {@code nBuffers=2} ({@code buffers[0]}=nulls, {@code buffers[1]}=values). For {@link
   * ArrayType} the descriptor must have {@code nBuffers=3} ({@code buffers[0]}=nulls, {@code
   * buffers[1]}=offsets, {@code buffers[2]}=sizes) and {@code nChildren=1} pointing to the child
   * flat INT descriptor. The child descriptor's {@code length} field is read to determine the child
   * capacity.
   *
   * <p>The caller retains ownership of all native memory; {@link #close()} is a safe no-op for
   * native resources when {@code ownerHandle == 0}.
   *
   * @param descAddr native address of a hand-crafted VeloxColumnHandle descriptor.
   * @param ownerHandle 0 to signal test-only mode (no native JNI allocation).
   * @param capacity number of rows.
   * @param type Spark {@link DataType}.
   */
  VeloxWritableColumnVector(long descAddr, long ownerHandle, int capacity, DataType type) {
    this(descAddr, ownerHandle, false, capacity, type, ownerHandle, EMPTY_INT_ARRAY);
  }

  /**
   * Package-private test-only constructor for a NESTED-child-like vector: wraps a pre-allocated
   * descriptor while letting the test supply an explicit {@code rootOwnerHandle} and {@code
   * childPath}. Combined with an override of {@link #allocateStringChunkNative}/{@link
   * #finalizeStringColumnNative}, this lets a JVM-only test exercise the childPath-aware
   * string-chunk routing without loading the native library.
   *
   * @param descAddr native address of a hand-crafted VeloxColumnHandle descriptor.
   * @param ownerHandle 0 to signal test-only mode (no native JNI allocation for this node).
   * @param capacity number of rows/element slots.
   * @param type Spark {@link DataType}.
   * @param rootOwnerHandle simulated ObjectStore handle of the root vector.
   * @param childPath simulated steps from root to this child (non-empty for nested children).
   */
  VeloxWritableColumnVector(
      long descAddr,
      long ownerHandle,
      int capacity,
      DataType type,
      long rootOwnerHandle,
      int[] childPath) {
    this(descAddr, ownerHandle, false, capacity, type, rootOwnerHandle, childPath);
  }

  /**
   * Private full constructor: initialises all fields from a pre-allocated descriptor tree.
   *
   * <p>Called by both the package-private test-only constructor (via delegation) and by the
   * recursive child-construction helpers ({@link #buildArrayChild}, {@link #buildMapChildren},
   * {@link #buildStructChildren}).
   *
   * <p>The {@code super(capacity, type)} call triggers Spark's {@link
   * WritableColumnVector#reserveNewColumn} for each logical child, populating the inherited {@code
   * childColumns[]} array with managed on-heap vectors. The constructor body then replaces those
   * placeholders with native-backed children built from the real descriptor tree (via the {@code
   * buildXxx} helpers). Replaced placeholders do not own native buffers.
   *
   * @param descAddr native address of the VeloxColumnHandle descriptor for this column.
   * @param ownerHandle ObjectStore handle (0 for test-only or child vectors).
   * @param ownsNativeResources {@code true} only for the root production vector.
   * @param capacity number of rows (or elements for child vectors).
   * @param type Spark {@link DataType} of this column.
   * @param rootOwnerHandle owner handle of the root vector (for {@link #reserveInternal} growth).
   * @param childPath steps from root to this child vector (empty for roots).
   */
  private VeloxWritableColumnVector(
      long descAddr,
      long ownerHandle,
      boolean ownsNativeResources,
      int capacity,
      DataType type,
      long rootOwnerHandle,
      int[] childPath) {
    super(capacity, type);
    rejectHugeVectorOptimization();
    this.descAddr = descAddr;
    this.ownerHandle = ownerHandle;
    this.ownsNativeResources = ownsNativeResources;
    this.rootOwnerHandle = rootOwnerHandle;
    this.childPath = childPath;
    long buffersPtr = Platform.getLong(null, descAddr + H_BUFFERS);
    this.nullsAddr = Platform.getLong(null, buffersPtr);
    if (type instanceof ArrayType || type instanceof MapType) {
      // ARRAY/MAP: buffers[1]=offsets, buffers[2]=sizes; children contain element(s)
      this.valuesAddr = 0L;
      this.offsetsAddr = Platform.getLong(null, buffersPtr + 8L);
      this.sizesAddr = Platform.getLong(null, buffersPtr + 16L);
      if (type instanceof ArrayType) {
        this.childColumn = buildArrayChild((ArrayType) type, descAddr, rootOwnerHandle, childPath);
        if (childColumn != null) {
          childColumns[0] = childColumn;
        }
      } else {
        // MAP: childColumn unused; keys=childColumns[0], values=childColumns[1]
        this.childColumn = null;
        buildMapChildren((MapType) type, descAddr, rootOwnerHandle, childPath);
      }
    } else if (type instanceof StructType) {
      // ROW: buffers[0]=nulls only; children 1:1 with fields
      this.valuesAddr = 0L;
      this.offsetsAddr = 0L;
      this.sizesAddr = 0L;
      this.childColumn = null;
      buildStructChildren((StructType) type, descAddr, rootOwnerHandle, childPath);
    } else {
      // Flat scalar (INTEGER, BIGINT, SMALLINT, TINYINT, REAL, DOUBLE, VARCHAR/STRING)
      this.valuesAddr = Platform.getLong(null, buffersPtr + 8L);
      this.offsetsAddr = 0L;
      this.sizesAddr = 0L;
      this.childColumn = null;
    }
    this.isVarchar = type instanceof StringType || type instanceof BinaryType;
    this.isTimestamp = type instanceof TimestampType;
    if (type instanceof DecimalType) {
      DecimalType d = (DecimalType) type;
      this.decimalPrecision = d.precision();
      this.decimalScale = d.scale();
    } else {
      this.decimalPrecision = 0;
      this.decimalScale = 0;
    }
  }

  private void rejectHugeVectorOptimization() {
    if (hugeVectorThreshold >= 0) {
      throw new UnsupportedOperationException(
          "VeloxWritableColumnVector does not support hugeVectorThreshold >= 0");
    }
  }

  // ---- Public getters ----

  /** ObjectStore owner handle for this column's native Velox vector (0 in test-only mode). */
  public long ownerHandle() {
    return ownerHandle;
  }

  // ---- Null handling ----
  // Velox validity bitmap: 1 = valid, 0 = null; LSB-first within each byte.

  /**
   * {@inheritDoc}
   *
   * <p>Reads the Velox validity bitmap: a cleared bit at position {@code rowId} means null.
   */
  @Override
  public boolean isNullAt(int rowId) {
    if (nullsAddr == 0L) {
      return false;
    }
    byte b = Platform.getByte(null, nullsAddr + (rowId >> 3));
    return (b & (1 << (rowId & 7))) == 0;
  }

  /**
   * {@inheritDoc}
   *
   * <p>Clears the validity bit for {@code rowId} and increments {@code nullCount} if the row was
   * previously valid. For VARCHAR columns, also writes {@code size=0} into the StringView slot at
   * {@code valuesAddr + rowId * 16} so the read side sees an empty (zero-size) view alongside the
   * cleared null bit.
   */
  @Override
  public void putNull(int rowId) {
    // For VARCHAR: zero the size field in the StringView slot so the read side sees
    // a size-0 view (the null bit is the authoritative null indicator, but a clean slot
    // avoids stale data being mis-read if the null bitmap is ever bypassed).
    if (isVarchar && valuesAddr != 0L) {
      Platform.putInt(null, valuesAddr + (long) rowId * 16, 0);
    }
    long byteAddr = nullsAddr + (rowId >> 3);
    byte b = Platform.getByte(null, byteAddr);
    int bitMask = 1 << (rowId & 7);
    if ((b & bitMask) != 0) {
      // Row was valid; mark null and track the count.
      Platform.putByte(null, byteAddr, (byte) (b & ~bitMask));
      numNulls++;
    }
  }

  /**
   * {@inheritDoc}
   *
   * <p>Clears the validity bit for each row in {@code [rowId, rowId + count)}.
   */
  @Override
  public void putNulls(int rowId, int count) {
    for (int i = 0; i < count; i++) {
      putNull(rowId + i);
    }
  }

  /**
   * {@inheritDoc}
   *
   * <p>Sets the validity bit for {@code rowId} and decrements {@code numNulls} if the row was
   * previously null.
   */
  @Override
  public void putNotNull(int rowId) {
    long byteAddr = nullsAddr + (rowId >> 3);
    byte b = Platform.getByte(null, byteAddr);
    int bitMask = 1 << (rowId & 7);
    if ((b & bitMask) == 0) {
      // Row was null; mark valid and update the count.
      Platform.putByte(null, byteAddr, (byte) (b | bitMask));
      numNulls--;
    }
  }

  /**
   * {@inheritDoc}
   *
   * <p>Sets the validity bit for each row in {@code [rowId, rowId + count)}.
   */
  @Override
  public void putNotNulls(int rowId, int count) {
    for (int i = 0; i < count; i++) {
      putNotNull(rowId + i);
    }
  }

  // ---- Fixed-width scalar put methods ----

  /** {@inheritDoc} */
  @Override
  public void putInt(int rowId, int value) {
    Platform.putInt(null, valuesAddr + (long) rowId * 4, value);
  }

  /**
   * {@inheritDoc} Generic long writes, including appendLong, do not support Timestamp; use {@link
   * #putTimestampMicros(int, long)}.
   */
  @Override
  public void putLong(int rowId, long value) {
    Platform.putLong(null, valuesAddr + (long) rowId * 8, value);
  }

  /** {@inheritDoc} */
  @Override
  public void putShort(int rowId, short value) {
    Platform.putShort(null, valuesAddr + (long) rowId * 2, value);
  }

  /** {@inheritDoc} */
  @Override
  public void putByte(int rowId, byte value) {
    Platform.putByte(null, valuesAddr + (long) rowId, value);
  }

  /** {@inheritDoc} */
  @Override
  public void putFloat(int rowId, float value) {
    Platform.putFloat(null, valuesAddr + (long) rowId * 4, value);
  }

  /** {@inheritDoc} */
  @Override
  public void putDouble(int rowId, double value) {
    Platform.putDouble(null, valuesAddr + (long) rowId * 8, value);
  }

  /**
   * Writes a Spark microseconds-since-epoch timestamp into the Velox TIMESTAMP buffer.
   *
   * <p>Velox {@code Timestamp} is a 16-byte struct: {@code int64 seconds} at offset 0, {@code
   * uint64 nanos} at offset 8. The conversion uses floor-division and floor-modulo to handle
   * pre-epoch (negative) microsecond values correctly:
   *
   * <ul>
   *   <li>{@code sec = Math.floorDiv(micros, 1_000_000L)}
   *   <li>{@code nanos = Math.floorMod(micros, 1_000_000L) * 1_000L}
   * </ul>
   *
   * @param rowId zero-based row index; must be in {@code [0, capacity)}.
   * @param micros Spark microseconds-since-epoch (may be negative for pre-epoch timestamps).
   */
  public void putTimestampMicros(int rowId, long micros) {
    long base = valuesAddr + (long) rowId * 16;
    long sec = Math.floorDiv(micros, 1_000_000L);
    long nanos = Math.floorMod(micros, 1_000_000L) * 1_000L;
    Platform.putLong(null, base, sec);
    Platform.putLong(null, base + 8, nanos);
  }

  /**
   * Writes a long-decimal int128 unscaled value into the Velox HUGEINT buffer at {@code rowId}.
   *
   * <p>The int128 is stored <b>little-endian, two's complement</b> (16 bytes/row): the low 64 bits
   * ({@code lo}) at offset 0 and the high 64 bits ({@code hi}) at offset 8. The {@code (lo, hi)}
   * pair must be produced by {@link DecimalAccessor#bigIntegerToInt128(java.math.BigInteger)},
   * which is the exact inverse of the read-side {@link DecimalAccessor#int128ToBigInteger(long,
   * long)}.
   *
   * @param rowId zero-based row index; must be in {@code [0, capacity)}.
   * @param lo low 64 bits of the int128 unscaled value.
   * @param hi high 64 bits of the int128 unscaled value.
   */
  public void putDecimal128(int rowId, long lo, long hi) {
    long base = valuesAddr + (long) rowId * 16;
    Platform.putLong(null, base, lo);
    Platform.putLong(null, base + 8, hi);
  }

  /** {@inheritDoc} */
  @Override
  public int getInt(int rowId) {
    return Platform.getInt(null, valuesAddr + (long) rowId * 4);
  }

  /** {@inheritDoc} */
  @Override
  public long getLong(int rowId) {
    if (isTimestamp) {
      // TIMESTAMP is stored as Velox Timestamp: {int64 sec @ +0, uint64 nanos @ +8} (16B/slot).
      // Reconstruct Spark micros-since-epoch: sec * 1_000_000 + nanos / 1_000.
      long base = valuesAddr + (long) rowId * 16;
      long sec = Platform.getLong(null, base);
      long nanos = Platform.getLong(null, base + 8);
      return sec * 1_000_000L + nanos / 1_000L;
    }
    return Platform.getLong(null, valuesAddr + (long) rowId * 8);
  }

  /** {@inheritDoc} */
  @Override
  public Decimal getDecimal(int rowId, int precision, int scale) {
    if (isNullAt(rowId)) {
      return null;
    }
    // Velox short decimals use int64 slots, including precision <= 9.
    if (precision <= Decimal.MAX_LONG_DIGITS()) {
      long unscaled = Platform.getLong(null, valuesAddr + (long) rowId * 8);
      return Decimal.createUnsafe(unscaled, precision, scale);
    }
    long base = valuesAddr + (long) rowId * 16;
    long lo = Platform.getLong(null, base);
    long hi = Platform.getLong(null, base + 8);
    BigInteger unscaled = DecimalAccessor.int128ToBigInteger(lo, hi);
    return Decimal.apply(new java.math.BigDecimal(unscaled, scale), precision, scale);
  }

  /** Reads a decimal using this vector's declared precision and scale. */
  public Decimal getDecimal(int rowId) {
    return getDecimal(rowId, decimalPrecision, decimalScale);
  }

  /** {@inheritDoc} */
  @Override
  public void putDecimal(int rowId, Decimal value, int precision) {
    if (precision <= Decimal.MAX_LONG_DIGITS()) {
      putLong(rowId, value.toUnscaledLong());
    } else {
      long[] loHi = DecimalAccessor.bigIntegerToInt128(value.toJavaBigDecimal().unscaledValue());
      putDecimal128(rowId, loHi[0], loHi[1]);
    }
  }

  /** {@inheritDoc} */
  @Override
  public short getShort(int rowId) {
    return Platform.getShort(null, valuesAddr + (long) rowId * 2);
  }

  /** {@inheritDoc} */
  @Override
  public byte getByte(int rowId) {
    return Platform.getByte(null, valuesAddr + (long) rowId);
  }

  /** {@inheritDoc} */
  @Override
  public float getFloat(int rowId) {
    return Platform.getFloat(null, valuesAddr + (long) rowId * 4);
  }

  /** {@inheritDoc} */
  @Override
  public double getDouble(int rowId) {
    return Platform.getDouble(null, valuesAddr + (long) rowId * 8);
  }

  // ---- Boolean (bit-packed, LSB-first) ----

  /**
   * {@inheritDoc}
   *
   * <p>Reads a bit-packed BOOLEAN value (1 bit/row, LSB-first) from the Velox values buffer. The
   * byte is at {@code valuesAddr + (rowId >> 3)} and the bit is {@code rowId & 7}.
   */
  @Override
  public boolean getBoolean(int rowId) {
    byte b = Platform.getByte(null, valuesAddr + (rowId >> 3));
    return (b & (1 << (rowId & 7))) != 0;
  }

  /**
   * {@inheritDoc}
   *
   * <p>Writes a bit-packed BOOLEAN value (1 bit/row, LSB-first) into the Velox values buffer. The
   * enclosing byte at {@code valuesAddr + (rowId >> 3)} is read-modify-written to set/clear bit
   * {@code rowId & 7}.
   */
  @Override
  public void putBoolean(int rowId, boolean value) {
    long addr = valuesAddr + (rowId >> 3);
    int b = Platform.getByte(null, addr) & 0xFF;
    int mask = 1 << (rowId & 7);
    b = value ? (b | mask) : (b & ~mask);
    Platform.putByte(null, addr, (byte) b);
  }

  /**
   * {@inheritDoc}
   *
   * @throws UnsupportedOperationException always -- deferred to Task 12.
   */
  @Override
  public void putBooleans(int rowId, int count, boolean value) {
    throw new UnsupportedOperationException(
        "VeloxWritableColumnVector.putBooleans: BOOLEAN bit-packed storage -- Task 12");
  }

  /**
   * {@inheritDoc}
   *
   * @throws UnsupportedOperationException always -- deferred to Task 12.
   */
  @Override
  public void putBooleans(int rowId, byte src) {
    throw new UnsupportedOperationException(
        "VeloxWritableColumnVector.putBooleans(byte): BOOLEAN bit-packed storage -- Task 12");
  }

  // ---- Bulk put stubs (Task 12) ----

  /**
   * {@inheritDoc}
   *
   * @throws UnsupportedOperationException always -- deferred to Task 12.
   */
  @Override
  public void putBytes(int rowId, int count, byte value) {
    throw new UnsupportedOperationException(
        "VeloxWritableColumnVector.putBytes: bulk put -- Task 12");
  }

  /**
   * {@inheritDoc}
   *
   * @throws UnsupportedOperationException always -- deferred to Task 12.
   */
  @Override
  public void putBytes(int rowId, int count, byte[] src, int srcIndex) {
    throw new UnsupportedOperationException(
        "VeloxWritableColumnVector.putBytes(array): bulk put -- Task 12");
  }

  /**
   * {@inheritDoc}
   *
   * @throws UnsupportedOperationException always -- deferred to Task 12.
   */
  @Override
  public void putShorts(int rowId, int count, short value) {
    throw new UnsupportedOperationException(
        "VeloxWritableColumnVector.putShorts: bulk put -- Task 12");
  }

  /**
   * {@inheritDoc}
   *
   * @throws UnsupportedOperationException always -- deferred to Task 12.
   */
  @Override
  public void putShorts(int rowId, int count, short[] src, int srcIndex) {
    throw new UnsupportedOperationException(
        "VeloxWritableColumnVector.putShorts(array): bulk put -- Task 12");
  }

  /**
   * {@inheritDoc}
   *
   * @throws UnsupportedOperationException always -- deferred to Task 12.
   */
  @Override
  public void putShorts(int rowId, int count, byte[] src, int srcIndex) {
    throw new UnsupportedOperationException(
        "VeloxWritableColumnVector.putShorts(bytes): bulk put -- Task 12");
  }

  /**
   * {@inheritDoc}
   *
   * @throws UnsupportedOperationException always -- deferred to Task 12.
   */
  @Override
  public void putInts(int rowId, int count, int value) {
    throw new UnsupportedOperationException(
        "VeloxWritableColumnVector.putInts: bulk put -- Task 12");
  }

  /**
   * {@inheritDoc}
   *
   * @throws UnsupportedOperationException always -- deferred to Task 12.
   */
  @Override
  public void putInts(int rowId, int count, int[] src, int srcIndex) {
    throw new UnsupportedOperationException(
        "VeloxWritableColumnVector.putInts(array): bulk put -- Task 12");
  }

  /**
   * {@inheritDoc}
   *
   * @throws UnsupportedOperationException always -- deferred to Task 12.
   */
  @Override
  public void putInts(int rowId, int count, byte[] src, int srcIndex) {
    throw new UnsupportedOperationException(
        "VeloxWritableColumnVector.putInts(bytes): bulk put -- Task 12");
  }

  /**
   * {@inheritDoc}
   *
   * @throws UnsupportedOperationException always -- deferred to Task 12.
   */
  @Override
  public void putIntsLittleEndian(int rowId, int count, byte[] src, int srcIndex) {
    throw new UnsupportedOperationException(
        "VeloxWritableColumnVector.putIntsLittleEndian: bulk put -- Task 12");
  }

  /**
   * {@inheritDoc}
   *
   * @throws UnsupportedOperationException always -- deferred to Task 12.
   */
  @Override
  public void putLongs(int rowId, int count, long value) {
    throw new UnsupportedOperationException(
        "VeloxWritableColumnVector.putLongs: bulk put -- Task 12");
  }

  /**
   * {@inheritDoc}
   *
   * @throws UnsupportedOperationException always -- deferred to Task 12.
   */
  @Override
  public void putLongs(int rowId, int count, long[] src, int srcIndex) {
    throw new UnsupportedOperationException(
        "VeloxWritableColumnVector.putLongs(array): bulk put -- Task 12");
  }

  /**
   * {@inheritDoc}
   *
   * @throws UnsupportedOperationException always -- deferred to Task 12.
   */
  @Override
  public void putLongs(int rowId, int count, byte[] src, int srcIndex) {
    throw new UnsupportedOperationException(
        "VeloxWritableColumnVector.putLongs(bytes): bulk put -- Task 12");
  }

  /**
   * {@inheritDoc}
   *
   * @throws UnsupportedOperationException always -- deferred to Task 12.
   */
  @Override
  public void putLongsLittleEndian(int rowId, int count, byte[] src, int srcIndex) {
    throw new UnsupportedOperationException(
        "VeloxWritableColumnVector.putLongsLittleEndian: bulk put -- Task 12");
  }

  /**
   * {@inheritDoc}
   *
   * @throws UnsupportedOperationException always -- deferred to Task 12.
   */
  @Override
  public void putFloats(int rowId, int count, float value) {
    throw new UnsupportedOperationException(
        "VeloxWritableColumnVector.putFloats: bulk put -- Task 12");
  }

  /**
   * {@inheritDoc}
   *
   * @throws UnsupportedOperationException always -- deferred to Task 12.
   */
  @Override
  public void putFloats(int rowId, int count, float[] src, int srcIndex) {
    throw new UnsupportedOperationException(
        "VeloxWritableColumnVector.putFloats(array): bulk put -- Task 12");
  }

  /**
   * {@inheritDoc}
   *
   * @throws UnsupportedOperationException always -- deferred to Task 12.
   */
  @Override
  public void putFloats(int rowId, int count, byte[] src, int srcIndex) {
    throw new UnsupportedOperationException(
        "VeloxWritableColumnVector.putFloats(bytes): bulk put -- Task 12");
  }

  /**
   * {@inheritDoc}
   *
   * @throws UnsupportedOperationException always -- deferred to Task 12.
   */
  @Override
  public void putFloatsLittleEndian(int rowId, int count, byte[] src, int srcIndex) {
    throw new UnsupportedOperationException(
        "VeloxWritableColumnVector.putFloatsLittleEndian: bulk put -- Task 12");
  }

  /**
   * {@inheritDoc}
   *
   * @throws UnsupportedOperationException always -- deferred to Task 12.
   */
  @Override
  public void putDoubles(int rowId, int count, double value) {
    throw new UnsupportedOperationException(
        "VeloxWritableColumnVector.putDoubles: bulk put -- Task 12");
  }

  /**
   * {@inheritDoc}
   *
   * @throws UnsupportedOperationException always -- deferred to Task 12.
   */
  @Override
  public void putDoubles(int rowId, int count, double[] src, int srcIndex) {
    throw new UnsupportedOperationException(
        "VeloxWritableColumnVector.putDoubles(array): bulk put -- Task 12");
  }

  /**
   * {@inheritDoc}
   *
   * @throws UnsupportedOperationException always -- deferred to Task 12.
   */
  @Override
  public void putDoubles(int rowId, int count, byte[] src, int srcIndex) {
    throw new UnsupportedOperationException(
        "VeloxWritableColumnVector.putDoubles(bytes): bulk put -- Task 12");
  }

  /**
   * {@inheritDoc}
   *
   * @throws UnsupportedOperationException always -- deferred to Task 12.
   */
  @Override
  public void putDoublesLittleEndian(int rowId, int count, byte[] src, int srcIndex) {
    throw new UnsupportedOperationException(
        "VeloxWritableColumnVector.putDoublesLittleEndian: bulk put -- Task 12");
  }

  // ---- Array / nested type implementation ----

  /**
   * Writes the {@code offset} and {@code length} for an array row into the Velox offsets and sizes
   * buffers. Both buffers store {@code int32} values at {@code rowId * 4} bytes.
   *
   * <p>After calling this method, the caller should write the actual element values into the child
   * column obtained via {@link #getChildColumn()}.
   *
   * @param rowId row index (0-based).
   * @param offset start index of this row's elements in the flat child column.
   * @param length number of elements for this row.
   * @throws UnsupportedOperationException if this vector is not an ARRAY type.
   */
  @Override
  public void putArray(int rowId, int offset, int length) {
    if (offsetsAddr == 0L) {
      throw new UnsupportedOperationException(
          "VeloxWritableColumnVector.putArray: not an ARRAY type (offsetsAddr == 0)");
    }
    Platform.putInt(null, offsetsAddr + (long) rowId * 4, offset);
    Platform.putInt(null, sizesAddr + (long) rowId * 4, length);
  }

  /**
   * Returns the start offset (into the child column) for the array at {@code rowId}. Reads from the
   * Velox offsets buffer ({@code buffers[1]} of an ARRAY descriptor).
   *
   * @param rowId row index (0-based).
   * @return the int32 offset value, or {@code 0} if this vector is not an ARRAY type.
   */
  @Override
  public int getArrayOffset(int rowId) {
    if (offsetsAddr == 0L) {
      return 0;
    }
    return Platform.getInt(null, offsetsAddr + (long) rowId * 4);
  }

  /**
   * Returns the number of elements for the array at {@code rowId}. Reads from the Velox sizes
   * buffer ({@code buffers[2]} of an ARRAY descriptor).
   *
   * @param rowId row index (0-based).
   * @return the int32 size value, or {@code 0} if this vector is not an ARRAY type.
   */
  @Override
  public int getArrayLength(int rowId) {
    if (sizesAddr == 0L) {
      return 0;
    }
    return Platform.getInt(null, sizesAddr + (long) rowId * 4);
  }

  /**
   * Returns the child {@link VeloxWritableColumnVector} for this ARRAY-typed column.
   *
   * <p>The child vector wraps the flat INT child descriptor ({@code children[0]}) and supports
   * writing element values via {@link #putInt(int, int)} etc.
   *
   * @return the child writable column, or {@code null} if this vector is a flat scalar type.
   */
  public VeloxWritableColumnVector getChildColumn() {
    return childColumn;
  }

  /**
   * Writes a string value as a Velox {@code StringView} into the values buffer.
   *
   * <p>The StringView layout (16 bytes per slot at {@code valuesAddr + rowId * 16}):
   *
   * <pre>
   * [0..4)  int32  size       -- byte length of the string
   * [4..8)  4 bytes prefix    -- first 4 bytes of string data
   * [8..16) inline remainder (if size &le; 12) OR native char* pointer (if size &gt; 12)
   * </pre>
   *
   * <p><b>Inline (size &le; 12):</b> the full string occupies bytes {@code slot+4..slot+4+size}.
   * The 12-byte region from {@code slot+4} to {@code slot+16} is zeroed first, then {@code size}
   * bytes are copied starting at {@code slot+4}.
   *
   * <p><b>Out-of-line (size &gt; 12):</b> a native string-data chunk is obtained via {@link
   * #ensureChunk}, the full string bytes are copied into the chunk, the first 4 bytes go into the
   * prefix field at {@code slot+4}, and the chunk address is stored as a pointer at {@code slot+8}.
   *
   * <p>This method is the exact inverse of {@link FlatAccessor#getUTF8String} on the read side.
   * String/Binary appendByteArray is unsupported because StringView storage has no Spark byte-child
   * append layout; use this indexed write method instead.
   *
   * @param rowId zero-based row index.
   * @param value source byte array.
   * @param offset starting offset within {@code value}.
   * @param count number of bytes to write.
   * @return 0 (StringView columns do not use an offset-based byte buffer model).
   * @throws UnsupportedOperationException if this vector is not a VARCHAR column.
   */
  @Override
  public int putByteArray(int rowId, byte[] value, int offset, int count) {
    if (!isVarchar) {
      throw new UnsupportedOperationException(
          "VeloxWritableColumnVector.putByteArray: "
              + "only VARCHAR (StringType) columns are supported");
    }
    long slot = valuesAddr + (long) rowId * 16;
    // [0..4): size field
    Platform.putInt(null, slot, count);
    if (count <= 12) {
      // Inline path: zero the 12-byte data region (prefix + remainder) then copy.
      Platform.putInt(null, slot + 4, 0);
      Platform.putLong(null, slot + 8, 0L);
      if (count > 0) {
        Platform.copyMemory(value, Platform.BYTE_ARRAY_OFFSET + offset, null, slot + 4, count);
      }
    } else {
      // Out-of-line path: write full string into a native chunk; store prefix + pointer.
      ensureChunk(count);
      long dst = curChunkAddr + curChunkOffset;
      Platform.copyMemory(value, Platform.BYTE_ARRAY_OFFSET + offset, null, dst, count);
      // Prefix: first 4 bytes at slot+4.
      Platform.copyMemory(value, Platform.BYTE_ARRAY_OFFSET + offset, null, slot + 4, 4);
      // Pointer to full string data at slot+8.
      Platform.putLong(null, slot + 8, dst);
      curChunkOffset += count;
    }
    putNotNull(rowId);
    return 0;
  }

  /**
   * Ensures that a native string-data chunk with at least {@code need} free bytes is available.
   *
   * <p>If no chunk exists or the current chunk lacks capacity, the current chunk's used byte count
   * is recorded in {@link #chunkUsed}, the chunk state is reset, and a new chunk is obtained via
   * {@link VeloxColumnHandleJniWrapper#allocateStringChunk}.
   *
   * <p>In test-only mode ({@code ownerHandle == 0}) this method cannot allocate a new chunk; if the
   * injected chunk is exhausted (or no chunk was injected), an {@link IllegalStateException} is
   * thrown to prevent a silent out-of-bounds native write or stale-pointer reuse. The test must
   * pre-inject a chunk large enough for all out-of-line strings via {@link #injectChunkForTest}.
   *
   * @param need minimum number of bytes required in the chunk.
   * @throws IllegalStateException in test-only mode when the injected chunk is exhausted or no
   *     chunk has been injected.
   */
  private void ensureChunk(int need) {
    if (curChunkAddr == 0L || curChunkOffset + need > curChunkCap) {
      if (curChunkAddr != 0L) {
        // Record used bytes for the chunk that is now full / being replaced, then
        // reset state so a stale pointer cannot be used after this point.
        chunkUsed.add(curChunkOffset);
        curChunkAddr = 0L;
        curChunkCap = 0L;
        curChunkOffset = 0L;
      }
      // Top-level VARCHAR (real ownerHandle) and nested VARCHAR children (real
      // rootOwnerHandle + non-empty childPath) allocate via JNI. Pure test-only mode
      // (both handles 0) cannot allocate and relies on injectChunkForTest.
      if (ownerHandle == 0L && (rootOwnerHandle == 0L || childPath.length == 0)) {
        throw new IllegalStateException(
            "VeloxWritableColumnVector: test-mode string chunk exhausted or not injected; "
                + "inject a sufficiently large chunk via injectChunkForTest before writing"
                + " out-of-line strings");
      }
      long[] c = allocateStringChunkNative(need);
      curChunkAddr = c[0];
      curChunkCap = c[1];
      curChunkOffset = 0L;
    }
  }

  /**
   * Allocates a native string-data chunk for this column, routing to the correct JNI entry point:
   *
   * <ul>
   *   <li>Top-level VARCHAR ({@code ownerHandle != 0}): {@link
   *       VeloxColumnHandleJniWrapper#allocateStringChunk} against this vector's own owner handle.
   *   <li>Nested VARCHAR child ({@code ownerHandle == 0} but {@code rootOwnerHandle != 0} with a
   *       non-empty {@code childPath}): {@link VeloxColumnHandleJniWrapper#allocateStringChunkAt},
   *       navigating from the root vector down {@code childPath} to this flat child.
   * </ul>
   *
   * <p>Isolated into an overridable method so JVM-only unit tests can inject a synthetic chunk
   * without loading the native library (mirrors {@link #growChildNative}).
   *
   * @param need minimum number of bytes required in the chunk.
   * @return {@code long[2]}: {@code [0]} raw chunk address, {@code [1]} usable capacity in bytes.
   */
  protected long[] allocateStringChunkNative(int need) {
    if (ownerHandle != 0L) {
      return VeloxColumnHandleJniWrapper.allocateStringChunk(ownerHandle, need);
    }
    return VeloxColumnHandleJniWrapper.allocateStringChunkAt(rootOwnerHandle, childPath, need);
  }

  /**
   * Finalizes all string-data chunks after all row values have been written.
   *
   * <p>Must be called exactly once after the last {@link #putByteArray} for this column and before
   * the column is assembled into a batch. Records the used-byte count for the current (last) chunk
   * and calls {@link VeloxColumnHandleJniWrapper#finalizeStringColumn} so that Velox trims each
   * chunk's buffer size to the actual bytes written.
   *
   * <p>In test-only mode ({@code ownerHandle == 0}) the JNI call is skipped (the test manages
   * native memory directly).
   */
  public void finishStringColumn() {
    if (curChunkAddr != 0L) {
      chunkUsed.add(curChunkOffset);
      curChunkAddr = 0L;
    }
    // Top-level VARCHAR and nested VARCHAR children finalize via JNI so Velox trims each
    // chunk to the bytes written. Pure test-only mode (both handles 0) skips the JNI call.
    if (ownerHandle != 0L || (rootOwnerHandle != 0L && childPath.length > 0)) {
      long[] used = new long[chunkUsed.size()];
      for (int i = 0; i < used.length; i++) {
        used[i] = chunkUsed.get(i);
      }
      finalizeStringColumnNative(used);
    }
  }

  /**
   * Finalizes this column's native string-data chunks, routing to the correct JNI entry point (top
   * -level owner handle vs. childPath-aware). Isolated into an overridable method so JVM-only unit
   * tests can capture the call without loading the native library.
   *
   * @param used per-chunk used-byte counts in allocation order.
   */
  protected void finalizeStringColumnNative(long[] used) {
    if (ownerHandle != 0L) {
      VeloxColumnHandleJniWrapper.finalizeStringColumn(ownerHandle, used);
    } else {
      VeloxColumnHandleJniWrapper.finalizeStringColumnAt(rootOwnerHandle, childPath, used);
    }
  }

  /**
   * Package-private test helper: inject a pre-allocated native memory region as the current string
   * chunk, bypassing the JNI {@link VeloxColumnHandleJniWrapper#allocateStringChunk} call.
   *
   * <p>Only valid in test-only mode ({@code ownerHandle == 0}). Allows unit tests to verify the
   * StringView write layout without loading the native library.
   *
   * @param addr raw address of the pre-allocated chunk buffer.
   * @param cap usable capacity in bytes of the buffer.
   */
  void injectChunkForTest(long addr, long cap) {
    this.curChunkAddr = addr;
    this.curChunkCap = cap;
    this.curChunkOffset = 0L;
  }

  /**
   * Returns the string at {@code rowId} as a zero-copy {@link UTF8String} backed by native memory.
   *
   * <p>For VARCHAR columns, reads the 16-byte StringView slot at {@code valuesAddr + rowId * 16}
   * using the same layout as {@link FlatAccessor#getUTF8String}:
   *
   * <ul>
   *   <li>Inline ({@code size <= 12}): the full string content starts at {@code slot + 4}.
   *   <li>Out-of-line ({@code size > 12}): the pointer at {@code slot + 8} is the {@code char*}
   *       base; {@link UTF8String#fromAddress} is used for a zero-copy view.
   * </ul>
   *
   * <p>This method is the exact read-side mirror of {@link #putByteArray}; together they form the
   * StringView round-trip self-check.
   *
   * @param rowId zero-based row index.
   * @return a zero-copy {@link UTF8String} backed by native memory.
   * @throws UnsupportedOperationException if this vector is not a VARCHAR column.
   */
  @Override
  public UTF8String getUTF8String(int rowId) {
    if (!isVarchar) {
      throw new UnsupportedOperationException(
          "VeloxWritableColumnVector.getUTF8String: "
              + "variable-width read not supported (Task 12/13)");
    }
    long sv = valuesAddr + (long) rowId * 16;
    int size = Platform.getInt(null, sv);
    if (size <= 12) {
      // Inline: full string data starts at the prefix field (offset 4).
      return UTF8String.fromAddress(null, sv + 4, size);
    }
    // Out-of-line: last 8 bytes hold a native pointer to the character data.
    long ptr = Platform.getLong(null, sv + 8);
    return UTF8String.fromAddress(null, ptr, size);
  }

  /**
   * Reads back binary bytes written at {@code rowId} as a heap-allocated {@code byte[]}.
   *
   * <p>Reads the 16-byte StringView slot at {@code valuesAddr + rowId * 16} using the same layout
   * as {@link FlatAccessor#getBinary} on the read side:
   *
   * <ul>
   *   <li>Inline ({@code size <= 12}): full binary content starts at {@code slot + 4}.
   *   <li>Out-of-line ({@code size > 12}): pointer at {@code slot + 8} is the native data buffer.
   * </ul>
   *
   * <p>This method is only valid for VARBINARY ({@link BinaryType}) columns; it also works for
   * VARCHAR ({@link StringType}) columns as a byte-level read (layout is identical).
   *
   * @param rowId zero-based row index.
   * @return a heap-resident copy of the binary bytes.
   * @throws UnsupportedOperationException if this is not a StringView column.
   */
  @Override
  public byte[] getBinary(int rowId) {
    if (!isVarchar) {
      throw new UnsupportedOperationException(
          "VeloxWritableColumnVector.getBinary: only VARBINARY/VARCHAR (StringView) columns "
              + "are supported");
    }
    long sv = valuesAddr + (long) rowId * 16;
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
   * {@inheritDoc}
   *
   * @throws UnsupportedOperationException always -- variable-width string read not supported (Task
   *     12/13).
   */
  @Override
  protected UTF8String getBytesAsUTF8String(int rowId, int count) {
    throw new UnsupportedOperationException(
        "VeloxWritableColumnVector.getBytesAsUTF8String: not supported (Task 12/13)");
  }

  /**
   * {@inheritDoc}
   *
   * @throws UnsupportedOperationException always -- deferred to Task 12.
   */
  @Override
  public ByteBuffer getByteBuffer(int rowId, int count) {
    throw new UnsupportedOperationException(
        "VeloxWritableColumnVector.getByteBuffer: deferred to Task 12");
  }

  /**
   * Computes the byte size of the null bitmap for {@code capacity} rows (1 bit per row), rounded up
   * to a multiple of 8 bytes with a floor of 8. Uses long arithmetic so it stays correct up to
   * {@link Integer#MAX_VALUE} rows (int {@code capacity + 7} would overflow within 7 of INT_MAX).
   *
   * @param capacity number of rows/elements
   * @return positive byte size of the validity buffer
   */
  static long nullsByteSize(int capacity) {
    long bytes = ((long) capacity + 7) / 8; // 1 bit per row, rounded up
    long padded = (bytes + 7) & ~7L; // pad to 8-byte multiple
    return Math.max(8L, padded);
  }

  /** Creates managed columns for Spark's constructor bookkeeping and dictionary IDs. */
  @Override
  public WritableColumnVector reserveNewColumn(int capacity, DataType type) {
    return new OnHeapColumnVector(capacity, type);
  }

  // ---- Dictionary id stub ----

  /**
   * {@inheritDoc}
   *
   * @return 0 -- dictionary encoding is not used for write-side flat vectors.
   */
  @Override
  public int getDictId(int rowId) {
    return 0;
  }

  // ---- Buffer lifecycle ----

  /**
   * Spark 4.1 lifecycle hook.
   *
   * <p>No-op: buffer ownership belongs to the native ObjectStore entry tracked by {@code
   * ownerHandle}. Memory is freed in {@link #close()}.
   */
  protected void releaseMemory() {
    // no-op: buffers are owned by the native Velox vector via ownerHandle / descAddr.
  }

  /**
   * {@inheritDoc}
   *
   * <p>For root-level vectors and test-only vectors (no native {@code rootOwnerHandle}), growth
   * throws {@link UnsupportedOperationException} since buffer reallocation requires native JNI
   * support. For child vectors with a valid {@code rootOwnerHandle} and non-empty {@code
   * childPath}, delegates to {@link VeloxColumnHandleJniWrapper#growChild} and refreshes the
   * write-side buffer addresses from the returned values.
   *
   * @throws UnsupportedOperationException if {@code newCapacity > capacity} and growth is not
   *     supported (root or test-only mode).
   */
  @Override
  protected void reserveInternal(int newCapacity) {
    if (newCapacity <= capacity) {
      // Same or smaller capacity: no-op (buffers already allocated).
      return;
    }
    if (rootOwnerHandle == 0L || childPath.length == 0) {
      // Growth is not supported for root vectors or in test-only mode (ownerHandle=0).
      // Root-level row capacity is fixed at allocateNestedOutput time; only nested
      // element children can be grown via growChild.
      throw new UnsupportedOperationException(
          "VeloxWritableColumnVector.reserveInternal: buffer growth not supported for"
              + " root or test-only mode (childPath="
              + Arrays.toString(childPath)
              + ", rootOwnerHandle="
              + rootOwnerHandle
              + ")");
    }
    // Grow the child via native; stale pointer resolved by using the refreshed addresses.
    // Parent ARRAY/MAP offsets/sizes buffers cannot go stale here because:
    //   - growChild resizes only the TARGET child identified by childPath;
    //   - parent-level offsets/sizes are managed by the parent's own descriptor and are only
    //     updated when the parent itself is resized (via a separate growChild call on the parent,
    //     which is not triggered here); write ordering ensures parents are written before children.
    long[] refreshed = growChildNative(newCapacity);
    // Refresh write pointers from the returned addresses. The returned array is a DFS
    // pre-order flattening whose per-node block depends on THIS vector's type:
    //   leaf  = [nulls, values]
    //   ARRAY/MAP = [nulls, offsets, sizes]
    //   ROW   = [nulls, <each field child's block, recursively>]
    // For a ROW target, RowVector::resize reallocated the field-child buffers; we MUST
    // refresh every field child's addresses AND bump its capacity here, otherwise a
    // subsequent putX on a field child (whose capacity looks large enough to no-op reserve)
    // would write through a dangling pre-resize pointer -> native heap UAF.
    int consumed = refreshFrom(refreshed, 0, newCapacity);
    assert consumed == refreshed.length
        : "growChild returned " + refreshed.length + " addrs but consumed " + consumed;
  }

  /**
   * Grows this child's native Velox buffers to {@code newCapacity} and returns the refreshed
   * address layout (see {@link #refreshFrom}). Isolated into an overridable method so unit tests
   * can inject a synthetic layout that mimics {@code growChild} without loading the native library.
   *
   * @param newCapacity the requested new capacity.
   * @return the flattened DFS address blocks for this vector's subtree.
   */
  protected long[] growChildNative(int newCapacity) {
    return VeloxColumnHandleJniWrapper.growChild(rootOwnerHandle, childPath, newCapacity);
  }

  /**
   * Refreshes this vector's native buffer addresses (and, for ROW, all descendant field children
   * recursively) from the flattened {@code addrs} array produced by {@link
   * VeloxColumnHandleJniWrapper#growChild}, and sets {@code capacity} to {@code newCapacity}.
   *
   * @param addrs flattened DFS pre-order address blocks.
   * @param idx start index of this vector's block within {@code addrs}.
   * @param newCapacity the post-grow capacity to record for this vector (and, recursively, for each
   *     field child of a ROW -- RowVector::resize grows all field children to the same size).
   * @return the index immediately following this vector's block (one past the last consumed).
   */
  private int refreshFrom(long[] addrs, int idx, int newCapacity) {
    DataType dt = dataType();
    this.nullsAddr = addrs[idx++];
    if (dt instanceof StructType) {
      // ROW: nulls consumed above; each field child follows in field order.
      StructType st = (StructType) dt;
      for (int i = 0; i < st.length(); i++) {
        VeloxWritableColumnVector fieldCol = (VeloxWritableColumnVector) getChild(i);
        idx = fieldCol.refreshFrom(addrs, idx, newCapacity);
      }
    } else if (dt instanceof ArrayType || dt instanceof MapType) {
      this.offsetsAddr = addrs[idx++];
      this.sizesAddr = addrs[idx++];
    } else {
      // Flat leaf.
      this.valuesAddr = addrs[idx++];
    }
    this.capacity = newCapacity;
    return idx;
  }

  // ---- Resource management ----

  /**
   * Releases native resources held by this vector.
   *
   * <p>When {@code ownsNativeResources} is {@code true} (production constructor):
   *
   * <ul>
   *   <li>Releases the Velox vector from the ObjectStore via {@link
   *       VeloxColumnHandleJniWrapper#releaseOutput(long)}.
   *   <li>Frees the descriptor tree via {@link VeloxColumnHandleJniWrapper#freeDescriptor(long)}.
   * </ul>
   *
   * <p>When {@code ownsNativeResources} is {@code false} (test-only constructor), this method is a
   * safe no-op for native JNI resources; the test is responsible for freeing off-heap memory.
   *
   * <p>In all cases string-chunk state ({@code curChunkAddr}, {@code curChunkCap}, {@code
   * curChunkOffset}, {@code chunkUsed}) is cleared to prevent stale accounting on a closed or
   * reused instance.
   *
   * <p>Idempotent: subsequent calls after the first are ignored.
   */
  @Override
  public void close() {
    if (!closed) {
      if (ownsNativeResources) {
        if (ownerHandle != 0L) {
          VeloxColumnHandleJniWrapper.releaseOutput(ownerHandle);
          ownerHandle = 0L;
        }
        if (descAddr != 0L) {
          VeloxColumnHandleJniWrapper.freeDescriptor(descAddr);
        }
      }
      // Reset string-chunk accounting so a closed/reused instance does not retain
      // stale chunk addresses or double-count used bytes.
      curChunkAddr = 0L;
      curChunkCap = 0L;
      curChunkOffset = 0L;
      chunkUsed.clear();
      // releaseMemory() is a no-op; superclass cleanup only drops Java-side resources.
      childColumn = null;
      super.close();
      closed = true;
    }
  }

  // ---- Recursive child-construction helpers ----

  /**
   * Builds the element child for an ARRAY vector from the descriptor tree.
   *
   * @param type the ARRAY type
   * @param descAddr native address of the ARRAY descriptor
   * @param rootOwnerHandle owner handle of the root vector (propagated to child)
   * @param parentPath path from root to the ARRAY vector (child will append index 0)
   * @return a native-backed child {@link VeloxWritableColumnVector}, or {@code null} if {@code
   *     childrenPtr == 0}
   */
  private VeloxWritableColumnVector buildArrayChild(
      ArrayType type, long descAddr, long rootOwnerHandle, int[] parentPath) {
    long childrenPtr = Platform.getLong(null, descAddr + H_CHILDREN);
    if (childrenPtr == 0L) {
      return null;
    }
    long childDescAddr = Platform.getLong(null, childrenPtr);
    int elemCap = (int) Platform.getLong(null, childDescAddr);
    int[] childPath = appendPath(parentPath, 0);
    return new VeloxWritableColumnVector(
        childDescAddr, 0L, false, elemCap, type.elementType(), rootOwnerHandle, childPath);
  }

  /**
   * Populates {@code childColumns[0]} (keys) and {@code childColumns[1]} (values) for a MAP vector.
   * No-op if {@code childrenPtr == 0}.
   *
   * @param type the MAP type
   * @param descAddr native address of the MAP descriptor
   * @param rootOwnerHandle owner handle of the root vector (propagated to children)
   * @param parentPath path from root to the MAP vector
   */
  private void buildMapChildren(
      MapType type, long descAddr, long rootOwnerHandle, int[] parentPath) {
    long childrenPtr = Platform.getLong(null, descAddr + H_CHILDREN);
    if (childrenPtr == 0L) {
      return;
    }
    long keysDescAddr = Platform.getLong(null, childrenPtr);
    int keysCap = (int) Platform.getLong(null, keysDescAddr);
    int[] keysPath = appendPath(parentPath, 0);
    childColumns[0] =
        new VeloxWritableColumnVector(
            keysDescAddr, 0L, false, keysCap, type.keyType(), rootOwnerHandle, keysPath);

    long valsDescAddr = Platform.getLong(null, childrenPtr + 8L);
    int valsCap = (int) Platform.getLong(null, valsDescAddr);
    int[] valsPath = appendPath(parentPath, 1);
    childColumns[1] =
        new VeloxWritableColumnVector(
            valsDescAddr, 0L, false, valsCap, type.valueType(), rootOwnerHandle, valsPath);
  }

  /**
   * Replaces Spark's placeholder {@code childColumns[i]} entries with native-backed child vectors
   * for each field of a StructType. No-op if {@code childrenPtr == 0}.
   *
   * @param type the STRUCT type
   * @param descAddr native address of the STRUCT descriptor
   * @param rootOwnerHandle owner handle of the root vector (propagated to children)
   * @param parentPath path from root to the STRUCT vector
   */
  private void buildStructChildren(
      StructType type, long descAddr, long rootOwnerHandle, int[] parentPath) {
    long childrenPtr = Platform.getLong(null, descAddr + H_CHILDREN);
    if (childrenPtr == 0L) {
      return;
    }
    for (int i = 0; i < type.length(); i++) {
      long fieldDescAddr = Platform.getLong(null, childrenPtr + (long) i * 8);
      int fieldCap = (int) Platform.getLong(null, fieldDescAddr);
      DataType fieldType = type.fields()[i].dataType();
      int[] fieldPath = appendPath(parentPath, i);
      childColumns[i] =
          new VeloxWritableColumnVector(
              fieldDescAddr, 0L, false, fieldCap, fieldType, rootOwnerHandle, fieldPath);
    }
  }

  /**
   * Returns a new path array that is {@code parent} extended by one step {@code index}.
   *
   * @param parent the parent path (may be empty)
   * @param index the index to append
   * @return a new array of length {@code parent.length + 1}
   */
  private static int[] appendPath(int[] parent, int index) {
    int[] result = Arrays.copyOf(parent, parent.length + 1);
    result[parent.length] = index;
    return result;
  }

  /**
   * Encodes a Spark {@link DataType} as a DFS pre-order {@code int[]} array for {@link
   * VeloxColumnHandleJniWrapper#allocateNestedOutput}.
   *
   * <p>Encoding scheme:
   *
   * <ul>
   *   <li>0=INTEGER, 1=BIGINT, 2=SMALLINT, 3=TINYINT, 4=REAL, 5=DOUBLE, 6=VARCHAR
   *   <li>7=ARRAY followed by element type
   *   <li>8=MAP followed by key type then value type
   *   <li>9=ROW followed by nFields then each field type
   *   <li>10=DATE, 11=VARBINARY, 12=BOOLEAN, 13=TIMESTAMP
   *   <li>14=DECIMAL followed by precision then scale
   * </ul>
   *
   * @param type the Spark DataType to encode; must be a supported type or nested type whose leaves
   *     are all supported types.
   * @return DFS pre-order encoded int array.
   * @throws UnsupportedOperationException if {@code type} is not a supported type.
   */
  static int[] encodeType(DataType type) {
    List<Integer> acc = new ArrayList<>();
    encodeTypeInto(type, acc);
    int[] result = new int[acc.size()];
    for (int i = 0; i < result.length; i++) {
      result[i] = acc.get(i);
    }
    return result;
  }

  private static void encodeTypeInto(DataType type, List<Integer> acc) {
    if (type instanceof IntegerType) {
      acc.add(0);
    } else if (type instanceof LongType) {
      acc.add(1);
    } else if (type instanceof ShortType) {
      acc.add(2);
    } else if (type instanceof ByteType) {
      acc.add(3);
    } else if (type instanceof FloatType) {
      acc.add(4);
    } else if (type instanceof DoubleType) {
      acc.add(5);
    } else if (type instanceof StringType) {
      acc.add(6);
    } else if (type instanceof DateType) {
      acc.add(10);
    } else if (type instanceof BinaryType) {
      acc.add(11);
    } else if (type instanceof BooleanType) {
      acc.add(12);
    } else if (type instanceof TimestampType) {
      acc.add(13);
    } else if (type instanceof DecimalType) {
      // Velox chooses BIGINT or HUGEINT storage from the encoded decimal precision.
      DecimalType d = (DecimalType) type;
      acc.add(14);
      acc.add(d.precision());
      acc.add(d.scale());
    } else if (type instanceof ArrayType) {
      acc.add(7);
      encodeTypeInto(((ArrayType) type).elementType(), acc);
    } else if (type instanceof MapType) {
      acc.add(8);
      encodeTypeInto(((MapType) type).keyType(), acc);
      encodeTypeInto(((MapType) type).valueType(), acc);
    } else if (type instanceof StructType) {
      StructType st = (StructType) type;
      acc.add(9);
      acc.add(st.length());
      for (int i = 0; i < st.length(); i++) {
        encodeTypeInto(st.fields()[i].dataType(), acc);
      }
    } else {
      throw new UnsupportedOperationException(
          "VeloxWritableColumnVector.encodeType: unsupported type: " + type);
    }
  }
}
