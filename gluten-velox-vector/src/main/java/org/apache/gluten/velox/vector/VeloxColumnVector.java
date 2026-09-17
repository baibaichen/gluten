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

import org.apache.spark.sql.types.ArrayType;
import org.apache.spark.sql.types.BinaryType;
import org.apache.spark.sql.types.BooleanType;
import org.apache.spark.sql.types.ByteType;
import org.apache.spark.sql.types.CalendarIntervalType;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.DateType;
import org.apache.spark.sql.types.Decimal;
import org.apache.spark.sql.types.DecimalType;
import org.apache.spark.sql.types.DoubleType;
import org.apache.spark.sql.types.FloatType;
import org.apache.spark.sql.types.IntegerType;
import org.apache.spark.sql.types.LongType;
import org.apache.spark.sql.types.MapType;
import org.apache.spark.sql.types.NullType;
import org.apache.spark.sql.types.ShortType;
import org.apache.spark.sql.types.StringType;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.types.TimestampType;
import org.apache.spark.sql.vectorized.ColumnVector;
import org.apache.spark.sql.vectorized.ColumnarArray;
import org.apache.spark.sql.vectorized.ColumnarMap;
import org.apache.spark.unsafe.Platform;
import org.apache.spark.unsafe.types.UTF8String;

import java.util.function.LongConsumer;

/**
 * A zero-copy Spark {@link ColumnVector} backed by a native Velox column descriptor.
 *
 * <p>Reads scalar values directly from off-heap native memory via {@link Platform} (i.e. {@code
 * sun.misc.Unsafe}). No vector data is copied; only the small fixed-size descriptor header (48
 * bytes) is snapshotted during construction via {@link #importFromNative}.
 *
 * <h3>Descriptor layout</h3>
 *
 * The {@code VeloxColumnHandle} descriptor is a flat struct at the address returned by {@link
 * VeloxColumnHandleJniWrapper#exportDescriptor}:
 *
 * <pre>
 * offset  size  field
 *  0       8    length      -- number of rows
 *  8       8    nullCount   -- pre-computed null count (negative = unknown)
 * 16       8    nBuffers    -- number of buffer pointers that follow
 * 24       8    nChildren   -- number of child descriptor pointers
 * 32       8    buffers     -- pointer to array of nBuffers 8-byte addresses
 * 40       8    children    -- pointer to array of nChildren 8-byte addresses
 * </pre>
 *
 * <h3>Ownership and close() contract</h3>
 *
 * <ul>
 *   <li>The root {@code VeloxColumnVector} owns the descriptor tree (rootDescAddr != 0).
 *   <li>{@link #close()} releases the descriptor tree via {@link
 *       VeloxColumnHandleJniWrapper#freeDescriptor}. It does <em>not</em> touch the underlying
 *       Velox vector data; vector lifetime is managed by the native side.
 *   <li>Child nodes are constructed with {@code rootDescAddr = 0} so that {@link #close()} on a
 *       child is a no-op at the descriptor level.
 * </ul>
 */
public final class VeloxColumnVector extends ColumnVector {

  // -------------------------------------------------------------------------
  // Descriptor field offsets (bytes from descriptor base address)
  // -------------------------------------------------------------------------

  /** Byte offset of the {@code length} field in the descriptor. */
  private static final int H_LENGTH = 0;

  /** Byte offset of the {@code nullCount} field in the descriptor. */
  private static final int H_NULLCOUNT = 8;

  /** Byte offset of the {@code nBuffers} field in the descriptor. */
  private static final int H_NBUFFERS = 16;

  /** Byte offset of the {@code nChildren} field in the descriptor. */
  private static final int H_NCHILDREN = 24;

  /** Byte offset of the {@code buffers} pointer field in the descriptor. */
  private static final int H_BUFFERS = 32;

  /** Byte offset of the {@code children} pointer field in the descriptor. */
  private static final int H_CHILDREN = 40;

  // -------------------------------------------------------------------------
  // Instance fields
  // -------------------------------------------------------------------------

  /** Number of rows in this column, as reported by the {@code length} field of the descriptor. */
  private final long length;

  /** Type-specific reader that provides zero-copy access to native values. */
  private final VeloxColumnAccessor accessor;

  /**
   * Child column vectors (for nested types). Empty for flat scalars, which is the only encoding
   * supported in this task.
   */
  private final VeloxColumnVector[] children;

  /**
   * Address of the root descriptor allocated by {@link
   * VeloxColumnHandleJniWrapper#exportDescriptor}. Non-zero only for the root node; {@code 0} for
   * child nodes so that {@link #close()} does not double-free the descriptor.
   */
  private final long rootDescAddr;

  // -------------------------------------------------------------------------
  // Private constructor
  // -------------------------------------------------------------------------

  private VeloxColumnVector(
      DataType type,
      long length,
      VeloxColumnAccessor accessor,
      VeloxColumnVector[] children,
      long rootDescAddr) {
    super(type);
    this.length = length;
    this.accessor = accessor;
    this.children = children;
    this.rootDescAddr = rootDescAddr;
  }

  // -------------------------------------------------------------------------
  // Factory method
  // -------------------------------------------------------------------------

  /**
   * Package-private: imports a native descriptor as a zero-copy read-only view <em>without</em>
   * taking ownership of the descriptor. {@link #close()} will be a no-op at the descriptor level.
   *
   * <p>Use this in tests where the descriptor is managed externally (e.g. allocated via {@link
   * org.apache.spark.unsafe.Platform#allocateMemory} in a test helper). Calling code is responsible
   * for freeing the descriptor after the returned vector is done being used.
   *
   * @param descAddr native address of the {@code VeloxColumnHandle} descriptor
   * @param type Spark {@link DataType} describing the element type of this column
   * @return a new {@link VeloxColumnVector} backed by native memory, with {@code rootDescAddr=0}
   */
  static VeloxColumnVector importFromNativeView(long descAddr, DataType type) {
    return importFromNativeInternal(descAddr, type);
  }

  /**
   * Snapshot a native descriptor and return a {@link VeloxColumnVector} backed by it.
   *
   * <p>Reads the six fixed fields from the descriptor at {@code descAddr} via {@link
   * Platform#getLong}, constructs the appropriate {@link VeloxColumnAccessor} for {@code type}, and
   * recursively builds child vectors for nested types ({@link ArrayType}, {@link MapType}, {@link
   * StructType}).
   *
   * <p>The returned vector <em>owns</em> the descriptor: {@link #close()} will release it via
   * {@link VeloxColumnHandleJniWrapper#freeDescriptor}. Child vectors are created with {@code
   * rootDescAddr = 0} so that {@link #close()} on a child is a no-op at the descriptor level (the
   * root's {@link #close()} manages the entire descriptor tree). If construction fails, the root
   * descriptor tree is released before the exception is rethrown.
   *
   * @param descAddr native address of the root {@code VeloxColumnHandle} descriptor; must be a
   *     valid non-zero address obtained from {@link VeloxColumnHandleJniWrapper#exportDescriptor}
   * @param type Spark {@link DataType} describing the element type of this column
   * @return a new {@link VeloxColumnVector} backed by native memory
   * @throws IllegalArgumentException if {@code descAddr} is zero
   * @throws UnsupportedOperationException if {@code type} is not yet supported
   */
  public static VeloxColumnVector importFromNative(long descAddr, DataType type) {
    return importFromNative(descAddr, type, VeloxColumnHandleJniWrapper::freeDescriptor);
  }

  static VeloxColumnVector importFromNative(
      long descAddr, DataType type, LongConsumer freeOnFailure) {
    if (descAddr == 0L) {
      throw new IllegalArgumentException("Descriptor address must not be zero");
    }
    try {
      // Build borrowed children first; this entry point alone owns failure cleanup.
      VeloxColumnVector view = importFromNativeInternal(descAddr, type);
      return new VeloxColumnVector(type, view.length, view.accessor, view.children, descAddr);
    } catch (RuntimeException | Error e) {
      freeOnFailure.accept(descAddr);
      throw e;
    }
  }

  /**
   * Internal recursive factory for borrowed descriptor views.
   *
   * @param descAddr native address of the {@code VeloxColumnHandle} descriptor
   * @param type Spark {@link DataType} for this column
   * @return a new {@link VeloxColumnVector} backed by native memory
   */
  private static VeloxColumnVector importFromNativeInternal(long descAddr, DataType type) {
    if (descAddr == 0L) {
      throw new IllegalArgumentException("Descriptor address must not be zero");
    }
    // Snapshot all six descriptor fields per the VeloxColumnHandle layout spec.
    long length = Platform.getLong(null, descAddr + H_LENGTH);
    long nullCount = Platform.getLong(null, descAddr + H_NULLCOUNT);
    long nBuffers = Platform.getLong(null, descAddr + H_NBUFFERS);
    long nChildren = Platform.getLong(null, descAddr + H_NCHILDREN);
    long buffersPtr = Platform.getLong(null, descAddr + H_BUFFERS);
    long childrenPtr = Platform.getLong(null, descAddr + H_CHILDREN);

    if (type instanceof NullType) {
      if (length < 0
          || length > Integer.MAX_VALUE
          || (nullCount != -1 && nullCount != length)
          || nBuffers != 2
          || nChildren != 0
          || childrenPtr != 0L
          || buffersPtr == 0L) {
        throw new IllegalArgumentException("Invalid NullType descriptor");
      }
      long nullsAddr = Platform.getLong(null, buffersPtr);
      long valuesAddr = Platform.getLong(null, buffersPtr + 8);
      if (valuesAddr != 0L || (length > 0 && nullsAddr == 0L)) {
        throw new IllegalArgumentException("NullType requires a null bitmap and no values buffer");
      }
      // Reuse the validity-only reader: inherited data getters throw without reading address 0.
      VeloxColumnAccessor accessor = new StructAccessor(nullsAddr, length);
      for (int row = 0; row < length; row++) {
        if (!accessor.isNullAt(row)) {
          throw new IllegalArgumentException("NullType contains a non-null value at row " + row);
        }
      }
      return new VeloxColumnVector(type, length, accessor, new VeloxColumnVector[0], 0L);
    }

    if (type instanceof CalendarIntervalType) {
      if (length < 0
          || length > Integer.MAX_VALUE
          || nullCount < -1
          || nullCount > length
          || nBuffers != 2
          || nChildren != 0
          || childrenPtr != 0L
          || buffersPtr == 0L) {
        throw new IllegalArgumentException("Invalid CalendarInterval descriptor");
      }
      long nullsAddr = Platform.getLong(null, buffersPtr);
      long valuesAddr = Platform.getLong(null, buffersPtr + 8);
      if (nullCount > 0 && nullsAddr == 0L) {
        throw new IllegalArgumentException("CalendarInterval nulls require a bitmap");
      }
      VeloxColumnAccessor accessor = new StructAccessor(nullsAddr, nullCount);
      if (valuesAddr == 0L) {
        for (int row = 0; row < length; row++) {
          if (!accessor.isNullAt(row)) {
            throw new IllegalArgumentException("Non-null CalendarInterval has no values buffer");
          }
        }
      }
      // getInterval reads these logical fields; native descriptors stay packed.
      VeloxColumnVector[] fields = new VeloxColumnVector[3];
      for (int field = 0; field < fields.length; field++) {
        fields[field] =
            new VeloxColumnVector(
                field == 2 ? DataTypes.LongType : DataTypes.IntegerType,
                length,
                new CalendarIntervalAccessor(nullsAddr, valuesAddr, nullCount, field * 4),
                new VeloxColumnVector[0],
                0L);
      }
      return new VeloxColumnVector(type, length, accessor, fields, 0L);
    }

    // buffers[0] = validity bitmap (may be 0 = all valid).
    long nullsAddr = nBuffers >= 1 ? Platform.getLong(null, buffersPtr) : 0L;

    if (type instanceof ArrayType) {
      ArrayType arrayType = (ArrayType) type;
      // buffers[1] = offsets (int32 per row), buffers[2] = sizes (int32 per row).
      long offsetsAddr = nBuffers >= 2 ? Platform.getLong(null, buffersPtr + 8) : 0L;
      long sizesAddr = nBuffers >= 3 ? Platform.getLong(null, buffersPtr + 16) : 0L;
      // children[0] = element child descriptor.
      long childDescAddr = Platform.getLong(null, childrenPtr);
      VeloxColumnVector childVec = importFromNativeInternal(childDescAddr, arrayType.elementType());
      ArrayAccessor accessor =
          new ArrayAccessor(nullsAddr, offsetsAddr, sizesAddr, nullCount, childVec);
      return new VeloxColumnVector(type, length, accessor, new VeloxColumnVector[] {childVec}, 0L);
    }

    if (type instanceof MapType) {
      MapType mapType = (MapType) type;
      // buffers[1] = offsets (int32 per row), buffers[2] = sizes (int32 per row).
      long offsetsAddr = nBuffers >= 2 ? Platform.getLong(null, buffersPtr + 8) : 0L;
      long sizesAddr = nBuffers >= 3 ? Platform.getLong(null, buffersPtr + 16) : 0L;
      // children[0] = keys, children[1] = values.
      long keyDescAddr = Platform.getLong(null, childrenPtr);
      long valDescAddr = Platform.getLong(null, childrenPtr + 8);
      VeloxColumnVector keyVec = importFromNativeInternal(keyDescAddr, mapType.keyType());
      VeloxColumnVector valVec = importFromNativeInternal(valDescAddr, mapType.valueType());
      MapAccessor accessor =
          new MapAccessor(nullsAddr, offsetsAddr, sizesAddr, nullCount, keyVec, valVec);
      return new VeloxColumnVector(
          type, length, accessor, new VeloxColumnVector[] {keyVec, valVec}, 0L);
    }

    if (type instanceof StructType) {
      StructType structType = (StructType) type;
      StructField[] fields = structType.fields();
      VeloxColumnVector[] childVectors = new VeloxColumnVector[fields.length];
      for (int i = 0; i < fields.length; i++) {
        long childDescAddr = Platform.getLong(null, childrenPtr + (long) i * 8);
        childVectors[i] = importFromNativeInternal(childDescAddr, fields[i].dataType());
      }
      StructAccessor accessor = new StructAccessor(nullsAddr, nullCount);
      return new VeloxColumnVector(type, length, accessor, childVectors, 0L);
    }

    // Flat scalar types.
    long valuesAddr = nBuffers >= 2 ? Platform.getLong(null, buffersPtr + 8) : 0L;
    VeloxColumnAccessor accessor = buildAccessor(type, nullsAddr, valuesAddr, nullCount);
    return new VeloxColumnVector(type, length, accessor, new VeloxColumnVector[0], 0L);
  }

  /**
   * Constructs a {@link VeloxColumnAccessor} appropriate for {@code type}.
   *
   * @param type Spark DataType
   * @param nullsAddr native address of the validity bitmap (0 = all valid)
   * @param valuesAddr native address of the packed values buffer
   * @param nullCount pre-computed null count from the descriptor
   * @return a concrete accessor
   * @throws UnsupportedOperationException for types not yet mapped to an accessor
   */
  private static VeloxColumnAccessor buildAccessor(
      DataType type, long nullsAddr, long valuesAddr, long nullCount) {
    if (type instanceof IntegerType
        || type instanceof LongType
        || type instanceof ShortType
        || type instanceof ByteType
        || type instanceof FloatType
        || type instanceof DoubleType
        || type instanceof BooleanType
        || type instanceof StringType
        || type instanceof DateType
        || type instanceof BinaryType) {
      return new FlatAccessor(nullsAddr, valuesAddr, nullCount);
    }
    if (type instanceof TimestampType) {
      return new TimestampAccessor(nullsAddr, valuesAddr, nullCount);
    }
    if (type instanceof DecimalType) {
      DecimalType d = (DecimalType) type;
      return new DecimalAccessor(d.precision(), d.scale(), nullsAddr, valuesAddr, nullCount);
    }
    throw new UnsupportedOperationException("Unsupported type: " + type);
  }

  // -------------------------------------------------------------------------
  // ColumnVector abstract method implementations
  // -------------------------------------------------------------------------

  /** {@inheritDoc} */
  @Override
  public boolean hasNull() {
    return accessor.hasNull();
  }

  /**
   * {@inheritDoc}
   *
   * <p>Preserves the accessor's count, including {@code -1} when unknown. Per-row nullability is
   * authoritative via {@link #isNullAt(int)}; this method does not scan the validity bitmap.
   */
  @Override
  public int numNulls() {
    return (int) accessor.numNulls();
  }

  /** {@inheritDoc} */
  @Override
  public boolean isNullAt(int rowId) {
    return accessor.isNullAt(rowId);
  }

  /** {@inheritDoc} */
  @Override
  public boolean getBoolean(int rowId) {
    return accessor.getBoolean(rowId);
  }

  /** {@inheritDoc} */
  @Override
  public byte getByte(int rowId) {
    return accessor.getByte(rowId);
  }

  /** {@inheritDoc} */
  @Override
  public short getShort(int rowId) {
    return accessor.getShort(rowId);
  }

  /** {@inheritDoc} */
  @Override
  public int getInt(int rowId) {
    return accessor.getInt(rowId);
  }

  /** {@inheritDoc} */
  @Override
  public long getLong(int rowId) {
    return accessor.getLong(rowId);
  }

  /** {@inheritDoc} */
  @Override
  public float getFloat(int rowId) {
    return accessor.getFloat(rowId);
  }

  /** {@inheritDoc} */
  @Override
  public double getDouble(int rowId) {
    return accessor.getDouble(rowId);
  }

  /** {@inheritDoc} */
  @Override
  public Decimal getDecimal(int rowId, int precision, int scale) {
    return accessor.getDecimal(rowId, precision, scale);
  }

  /** {@inheritDoc} */
  @Override
  public UTF8String getUTF8String(int rowId) {
    return accessor.getUTF8String(rowId);
  }

  /** {@inheritDoc} */
  @Override
  public byte[] getBinary(int rowId) {
    return accessor.getBinary(rowId);
  }

  /** {@inheritDoc} */
  @Override
  public ColumnarArray getArray(int rowId) {
    return accessor.getArray(rowId);
  }

  /** {@inheritDoc} */
  @Override
  public ColumnarMap getMap(int rowId) {
    return accessor.getMap(rowId);
  }

  /** {@inheritDoc} */
  @Override
  public ColumnVector getChild(int ordinal) {
    return children[ordinal];
  }

  // -------------------------------------------------------------------------
  // Resource management
  // -------------------------------------------------------------------------

  /**
   * Releases resources held by this vector.
   *
   * <p>Only the root node releases the descriptor tree via {@link
   * VeloxColumnHandleJniWrapper#freeDescriptor}. Underlying Velox vector data remains native-owned.
   */
  @Override
  public void close() {
    if (rootDescAddr != 0L) {
      VeloxColumnHandleJniWrapper.freeDescriptor(rootDescAddr);
    }
  }
}
