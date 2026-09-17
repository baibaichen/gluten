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
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;
import org.apache.spark.sql.types.*;
import org.apache.spark.sql.vectorized.ColumnarArray;
import org.apache.spark.sql.vectorized.ColumnarMap;
import org.apache.spark.unsafe.types.CalendarInterval;
import org.apache.spark.unsafe.types.GeographyVal;
import org.apache.spark.unsafe.types.GeometryVal;
import org.apache.spark.unsafe.types.UTF8String;
import org.apache.spark.unsafe.types.VariantVal;

/**
 * A null-safe {@link InternalRow} backed by an array of {@link VeloxColumnVector}s.
 *
 * <p>This row is used by {@link VeloxInputBatch#getRow(int)} to expose a single row of a native
 * Velox batch for JVM-side projection. It is <em>reused</em>: the {@link #rowId} field is updated
 * by {@link VeloxInputBatch#getRow(int)} before the row is passed to any expression.
 *
 * <h3>Null-safety (constraint C1)</h3>
 *
 * Every getter checks {@link #isNullAt(int)} <em>first</em> and returns {@code null} (or the
 * appropriate zero/default for primitives) when the value is null. This mirrors the pattern in
 * {@code ArrowColumnarRow.get} (ArrowColumnarRow.scala:120-122) and fixes the interpreted
 * projection path in Spark 4.1 where {@code ColumnarBatchRow.get(int, DataType)} does NOT perform a
 * null check, causing null inputs to silently return 0 or garbage.
 */
public final class VeloxColumnarReadRow extends InternalRow {

  /** The columns backing this row view. */
  private final VeloxColumnVector[] columns;

  /**
   * The current row index. Updated by {@link VeloxInputBatch#getRow(int)} before each projection.
   * Package-private so that {@link VeloxInputBatch} can set it directly without an extra method
   * call overhead.
   */
  int rowId;

  /**
   * Constructs a reusable read row over the given column vectors.
   *
   * @param columns per-column vectors; must match the schema used by the owning {@link
   *     VeloxInputBatch}
   */
  VeloxColumnarReadRow(VeloxColumnVector[] columns) {
    this.columns = columns;
  }

  // ---------------------------------------------------------------------------
  // InternalRow contract
  // ---------------------------------------------------------------------------

  @Override
  public VariantVal getVariant(int ordinal) {
    throw new UnsupportedOperationException("getVariant");
  }

  @Override
  public GeographyVal getGeography(int ordinal) {
    throw new UnsupportedOperationException("getGeography");
  }

  @Override
  public GeometryVal getGeometry(int ordinal) {
    throw new UnsupportedOperationException("getGeometry");
  }

  /** {@inheritDoc} */
  @Override
  public int numFields() {
    return columns.length;
  }

  /**
   * {@inheritDoc}
   *
   * <p>Returns {@code true} iff the value at {@code ordinal} for the current row is null.
   */
  @Override
  public boolean isNullAt(int ordinal) {
    return columns[ordinal].isNullAt(rowId);
  }

  /**
   * {@inheritDoc}
   *
   * <p>Null-checks first (C1): returns {@code false} for null values.
   */
  @Override
  public boolean getBoolean(int ordinal) {
    if (columns[ordinal].isNullAt(rowId)) return false;
    return columns[ordinal].getBoolean(rowId);
  }

  /**
   * {@inheritDoc}
   *
   * <p>Null-checks first (C1): returns {@code 0} for null values.
   */
  @Override
  public byte getByte(int ordinal) {
    if (columns[ordinal].isNullAt(rowId)) return 0;
    return columns[ordinal].getByte(rowId);
  }

  /**
   * {@inheritDoc}
   *
   * <p>Null-checks first (C1): returns {@code 0} for null values.
   */
  @Override
  public short getShort(int ordinal) {
    if (columns[ordinal].isNullAt(rowId)) return 0;
    return columns[ordinal].getShort(rowId);
  }

  /**
   * {@inheritDoc}
   *
   * <p>Null-checks first (C1): returns {@code 0} for null values.
   */
  @Override
  public int getInt(int ordinal) {
    if (columns[ordinal].isNullAt(rowId)) return 0;
    return columns[ordinal].getInt(rowId);
  }

  /**
   * {@inheritDoc}
   *
   * <p>Null-checks first (C1): returns {@code 0L} for null values.
   */
  @Override
  public long getLong(int ordinal) {
    if (columns[ordinal].isNullAt(rowId)) return 0L;
    return columns[ordinal].getLong(rowId);
  }

  /**
   * {@inheritDoc}
   *
   * <p>Null-checks first (C1): returns {@code 0.0f} for null values.
   */
  @Override
  public float getFloat(int ordinal) {
    if (columns[ordinal].isNullAt(rowId)) return 0.0f;
    return columns[ordinal].getFloat(rowId);
  }

  /**
   * {@inheritDoc}
   *
   * <p>Null-checks first (C1): returns {@code 0.0} for null values.
   */
  @Override
  public double getDouble(int ordinal) {
    if (columns[ordinal].isNullAt(rowId)) return 0.0;
    return columns[ordinal].getDouble(rowId);
  }

  /**
   * {@inheritDoc}
   *
   * <p>Null-checks first (C1): returns {@code null} for null values.
   */
  @Override
  public Decimal getDecimal(int ordinal, int precision, int scale) {
    if (columns[ordinal].isNullAt(rowId)) return null;
    return columns[ordinal].getDecimal(rowId, precision, scale);
  }

  /**
   * {@inheritDoc}
   *
   * <p>Null-checks first (C1): returns {@code null} for null values.
   */
  @Override
  public UTF8String getUTF8String(int ordinal) {
    if (columns[ordinal].isNullAt(rowId)) return null;
    return columns[ordinal].getUTF8String(rowId);
  }

  /**
   * {@inheritDoc}
   *
   * <p>Null-checks first (C1): returns {@code null} for null values.
   */
  @Override
  public byte[] getBinary(int ordinal) {
    if (columns[ordinal].isNullAt(rowId)) return null;
    return columns[ordinal].getBinary(rowId);
  }

  /**
   * {@inheritDoc}
   *
   * <p>Null-checks first (C1): returns {@code null} for null values.
   */
  @Override
  public CalendarInterval getInterval(int ordinal) {
    if (columns[ordinal].isNullAt(rowId)) return null;
    return columns[ordinal].getInterval(rowId);
  }

  /**
   * {@inheritDoc}
   *
   * <p>Null-checks first (C1): returns {@code null} for null values.
   */
  @Override
  public InternalRow getStruct(int ordinal, int numFields) {
    if (columns[ordinal].isNullAt(rowId)) return null;
    // Delegate to a child-row view backed by the struct column's child vectors.
    VeloxColumnVector structCol = columns[ordinal];
    VeloxColumnVector[] childCols = new VeloxColumnVector[numFields];
    for (int i = 0; i < numFields; i++) {
      childCols[i] = (VeloxColumnVector) structCol.getChild(i);
    }
    VeloxColumnarReadRow childRow = new VeloxColumnarReadRow(childCols);
    childRow.rowId = rowId;
    return childRow;
  }

  /**
   * {@inheritDoc}
   *
   * <p>Null-checks first (C1): returns {@code null} for null values.
   */
  @Override
  public ColumnarArray getArray(int ordinal) {
    if (columns[ordinal].isNullAt(rowId)) return null;
    return columns[ordinal].getArray(rowId);
  }

  /**
   * {@inheritDoc}
   *
   * <p>Null-checks first (C1): returns {@code null} for null values.
   */
  @Override
  public ColumnarMap getMap(int ordinal) {
    if (columns[ordinal].isNullAt(rowId)) return null;
    return columns[ordinal].getMap(rowId);
  }

  /**
   * {@inheritDoc}
   *
   * <p>Dispatches to the appropriate typed getter based on {@code dataType}, null-checking first
   * (C1). Mirrors {@code ArrowColumnarRow.get(int, DataType)} null-check semantics.
   */
  @Override
  public Object get(int ordinal, DataType dataType) {
    // C1: check null first (mirrors ArrowColumnarRow.scala:120-122)
    if (isNullAt(ordinal)) return null;
    if (dataType instanceof BooleanType) return getBoolean(ordinal);
    if (dataType instanceof ByteType) return getByte(ordinal);
    if (dataType instanceof ShortType) return getShort(ordinal);
    if (dataType instanceof IntegerType) return getInt(ordinal);
    if (dataType instanceof DateType) return getInt(ordinal);
    if (dataType instanceof LongType) return getLong(ordinal);
    // TimestampType is stored as micros-since-epoch (a long), mirroring the DateType->getInt gap:
    // VeloxInputBatch admits TimestampType, so get()/copy() must handle it (interpreted + copy
    // paths).
    if (dataType instanceof TimestampType) return getLong(ordinal);
    if (dataType instanceof FloatType) return getFloat(ordinal);
    if (dataType instanceof DoubleType) return getDouble(ordinal);
    if (dataType instanceof StringType) return getUTF8String(ordinal);
    if (dataType instanceof BinaryType) return getBinary(ordinal);
    if (dataType instanceof CalendarIntervalType) return getInterval(ordinal);
    if (dataType instanceof DecimalType) {
      DecimalType dt = (DecimalType) dataType;
      return getDecimal(ordinal, dt.precision(), dt.scale());
    }
    if (dataType instanceof ArrayType) return getArray(ordinal);
    if (dataType instanceof MapType) return getMap(ordinal);
    if (dataType instanceof StructType) return getStruct(ordinal, ((StructType) dataType).size());
    throw new UnsupportedOperationException(
        "VeloxColumnarReadRow.get: unsupported data type: " + dataType);
  }

  // ---------------------------------------------------------------------------
  // Mutation operations — not supported for a read-only row
  // ---------------------------------------------------------------------------

  /** {@inheritDoc} Unsupported — this row is read-only. */
  @Override
  public void update(int ordinal, Object value) {
    throw new UnsupportedOperationException("VeloxColumnarReadRow is read-only");
  }

  /** {@inheritDoc} Unsupported — this row is read-only. */
  @Override
  public void setNullAt(int ordinal) {
    throw new UnsupportedOperationException("VeloxColumnarReadRow is read-only");
  }

  /**
   * {@inheritDoc}
   *
   * <p>Returns a deep copy of the current row as a {@code GenericInternalRow}. Variable-length and
   * complex types (String, Array, Map, Struct) are deep-copied via their own {@code .copy()} so
   * that the returned row owns independent heap memory and does not retain references into native
   * off-heap Velox batch memory (which may be freed after the batch is closed). Fixed-width
   * primitives are copied by value. Null fields remain null.
   *
   * <p>Copies of collection elements that are structs containing CalendarInterval are unsupported.
   * Direct CalendarInterval reads and copies remain supported.
   */
  @Override
  public InternalRow copy() {
    Object[] values = new Object[columns.length];
    for (int i = 0; i < columns.length; i++) {
      values[i] = InternalRow.copyValue(get(i, columns[i].dataType()));
    }
    return new GenericInternalRow(values);
  }
}
