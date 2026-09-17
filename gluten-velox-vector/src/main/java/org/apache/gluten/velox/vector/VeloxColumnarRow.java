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
import org.apache.spark.sql.catalyst.util.ArrayData;
import org.apache.spark.sql.catalyst.util.MapData;
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
import org.apache.spark.sql.vectorized.ColumnarArray;
import org.apache.spark.sql.vectorized.ColumnarMap;
import org.apache.spark.unsafe.types.CalendarInterval;
import org.apache.spark.unsafe.types.GeographyVal;
import org.apache.spark.unsafe.types.GeometryVal;
import org.apache.spark.unsafe.types.UTF8String;
import org.apache.spark.unsafe.types.VariantVal;

import java.util.IdentityHashMap;

/**
 * A write-view {@link InternalRow} backed by an array of {@link VeloxWritableColumnVector}s.
 *
 * <p>This row is used by JVM-side projections (e.g. a Spark {@code MutableProjection}) to write UDF
 * results directly into native Velox output columns row by row. See {@link
 * VeloxWritableColumnVector} for logical/native types, physical layouts and separate read/write and
 * copy support boundaries.
 *
 * <p>The design mirrors {@code ArrowColumnarRow} (ArrowColumnarRow.scala) for the set/update path:
 * each setter delegates to the corresponding {@code putX(rowId, value)} method on the backing
 * column vector, and {@link #update} dispatches by the column's runtime {@link DataType}.
 *
 * <p>Usage:
 *
 * <pre>{@code
 * VeloxColumnarRow row = new VeloxColumnarRow(columns);
 * for (int r = 0; r < numRows; r++) {
 *   row.rowId = r;
 *   projection.target(row).apply(inputRow);  // MutableProjection writes via setX / setUTF8String
 * }
 * row.finishWriteRow();  // call ONCE after all rows; flushes VARCHAR string chunks
 * }</pre>
 *
 * <h3>VARCHAR / StringView flush</h3>
 *
 * After all rows have been written, call {@link #finishWriteRow()} exactly once before assembling
 * the batch. This flushes any pending string-data chunks in VARCHAR columns by calling {@link
 * VeloxWritableColumnVector#finishStringColumn()} on each VARCHAR column.
 *
 * <h3>Null encoding</h3>
 *
 * {@link #setNullAt} delegates to {@link VeloxWritableColumnVector#putNull}, which clears the
 * relevant bit in the Velox validity bitmap and increments the null count.
 *
 * <h3>Thread safety</h3>
 *
 * This class is <em>not</em> thread-safe. Each thread must use its own row instance.
 */
public final class VeloxColumnarRow extends InternalRow {

  /** The columns backing this write-view row. */
  private final VeloxWritableColumnVector[] columns;

  /**
   * The current row index within the backing column vectors. Must be set by the caller before
   * invoking any setter. Package-private so that owning batch builders can update it directly.
   */
  public int rowId;

  /**
   * Running write cursors for child columns of complex-type output columns. Each child column
   * (accessed via {@link VeloxWritableColumnVector#getChildColumn()} for ARRAY or {@link
   * VeloxWritableColumnVector#getChild(int)} for MAP/STRUCT) tracks its next-available element slot
   * across all rows in the batch. Keys are child column identities; values are {@code int[1]} boxes
   * for mutability.
   */
  private final IdentityHashMap<VeloxWritableColumnVector, int[]> cursors = new IdentityHashMap<>();

  /**
   * Constructs a write-view row over the given writable column vectors.
   *
   * @param columns per-column writable vectors; must match the schema of the output batch. The
   *     caller retains ownership of the column vectors and must close them when no longer needed.
   */
  public VeloxColumnarRow(VeloxWritableColumnVector[] columns) {
    this.columns = columns;
  }

  // ---------------------------------------------------------------------------
  // InternalRow contract -- structural
  // ---------------------------------------------------------------------------

  /** {@inheritDoc} Returns the number of columns in this row. */
  @Override
  public int numFields() {
    return columns.length;
  }

  /**
   * {@inheritDoc}
   *
   * <p>Returns a heap-resident {@link GenericInternalRow} containing the current row's values.
   * Supports Boolean, fixed-width numeric types, Date, Timestamp, Decimal, String and Binary.
   * Non-null complex values are unsupported; see {@link VeloxWritableColumnVector} for the separate
   * read/write and copy support boundaries.
   */
  @Override
  public InternalRow copy() {
    Object[] values = new Object[columns.length];
    for (int i = 0; i < columns.length; i++) {
      if (isNullAt(i)) {
        values[i] = null;
      } else {
        DataType dt = columns[i].dataType();
        if (dt instanceof BooleanType) {
          values[i] = getBoolean(i);
        } else if (dt instanceof IntegerType) {
          values[i] = getInt(i);
        } else if (dt instanceof LongType) {
          values[i] = getLong(i);
        } else if (dt instanceof ShortType) {
          values[i] = getShort(i);
        } else if (dt instanceof ByteType) {
          values[i] = getByte(i);
        } else if (dt instanceof FloatType) {
          values[i] = getFloat(i);
        } else if (dt instanceof DoubleType) {
          values[i] = getDouble(i);
        } else if (dt instanceof DateType) {
          // DateType is stored as epoch-day int32, same physical layout as IntegerType.
          values[i] = getInt(i);
        } else if (dt instanceof StringType) {
          // Return a heap copy of the string so the GenericInternalRow is self-contained.
          values[i] = getUTF8String(i).copy();
        } else if (dt instanceof BinaryType) {
          // getBinary returns a fresh heap byte[], so no extra copy is needed.
          values[i] = getBinary(i);
        } else if (dt instanceof TimestampType) {
          // TimestampType is stored as micros-since-epoch (long), backed by a 16B Velox Timestamp.
          // getLong reconstructs micros via sec*1_000_000 + nanos/1_000.
          values[i] = getLong(i);
        } else if (dt instanceof DecimalType) {
          DecimalType d = (DecimalType) dt;
          values[i] = getDecimal(i, d.precision(), d.scale());
        } else {
          throw new UnsupportedOperationException("VeloxColumnarRow.copy: unsupported type: " + dt);
        }
      }
    }
    return new GenericInternalRow(values);
  }

  // ---------------------------------------------------------------------------
  // InternalRow contract -- read side (for optional self-check after writing)
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

  /**
   * {@inheritDoc}
   *
   * <p>Returns {@code true} iff the value at {@code ordinal} for the current {@link #rowId} is null
   * according to the Velox validity bitmap.
   */
  @Override
  public boolean isNullAt(int ordinal) {
    return columns[ordinal].isNullAt(rowId);
  }

  /**
   * {@inheritDoc}
   *
   * <p>Reads back the bit-packed BOOLEAN value written at {@code ordinal} for the current {@link
   * #rowId}.
   */
  @Override
  public boolean getBoolean(int ordinal) {
    return columns[ordinal].getBoolean(rowId);
  }

  /**
   * {@inheritDoc}
   *
   * <p>Reads back the byte value written at {@code ordinal} for the current {@link #rowId}. Useful
   * for post-write self-checks.
   */
  @Override
  public byte getByte(int ordinal) {
    return columns[ordinal].getByte(rowId);
  }

  /**
   * {@inheritDoc}
   *
   * <p>Reads back the short value written at {@code ordinal} for the current {@link #rowId}.
   */
  @Override
  public short getShort(int ordinal) {
    return columns[ordinal].getShort(rowId);
  }

  /**
   * {@inheritDoc}
   *
   * <p>Reads back the int value written at {@code ordinal} for the current {@link #rowId}.
   */
  @Override
  public int getInt(int ordinal) {
    return columns[ordinal].getInt(rowId);
  }

  /**
   * {@inheritDoc}
   *
   * <p>Reads back the long value written at {@code ordinal} for the current {@link #rowId}.
   */
  @Override
  public long getLong(int ordinal) {
    return columns[ordinal].getLong(rowId);
  }

  /**
   * {@inheritDoc}
   *
   * <p>Reads back the float value written at {@code ordinal} for the current {@link #rowId}.
   */
  @Override
  public float getFloat(int ordinal) {
    return columns[ordinal].getFloat(rowId);
  }

  /**
   * {@inheritDoc}
   *
   * <p>Reads back the double value written at {@code ordinal} for the current {@link #rowId}.
   */
  @Override
  public double getDouble(int ordinal) {
    return columns[ordinal].getDouble(rowId);
  }

  /** {@inheritDoc} Reads back the DECIMAL value written at {@code ordinal} for self-checks. */
  @Override
  public Decimal getDecimal(int ordinal, int precision, int scale) {
    return columns[ordinal].getDecimal(rowId);
  }

  /**
   * {@inheritDoc}
   *
   * <p>Reads back the string value written at {@code ordinal} for the current {@link #rowId}.
   * Delegates to {@link VeloxWritableColumnVector#getUTF8String} which reads the StringView layout
   * (inline for size &le; 12, out-of-line pointer for size &gt; 12). Useful for post-write
   * self-checks. The returned {@link UTF8String} is backed by native memory; callers must call
   * {@link UTF8String#copy()} if a heap-resident copy is needed.
   *
   * @throws UnsupportedOperationException if the column at {@code ordinal} is not VARCHAR.
   */
  @Override
  public UTF8String getUTF8String(int ordinal) {
    return columns[ordinal].getUTF8String(rowId);
  }

  /**
   * {@inheritDoc}
   *
   * <p>Reads back the binary bytes written at {@code ordinal} for the current {@link #rowId}.
   * Delegates to {@link VeloxWritableColumnVector#getBinary}, which reads the Velox StringView
   * layout (inline for size &le; 12, out-of-line pointer for size &gt; 12). Useful for post-write
   * self-checks. The returned {@code byte[]} is heap-resident.
   *
   * @throws UnsupportedOperationException if the column at {@code ordinal} is not a VARBINARY
   *     column.
   */
  @Override
  public byte[] getBinary(int ordinal) {
    return columns[ordinal].getBinary(rowId);
  }

  /** {@inheritDoc} Not supported in this fixed-width-only version; always throws. */
  @Override
  public CalendarInterval getInterval(int ordinal) {
    throw new UnsupportedOperationException(
        "VeloxColumnarRow.getInterval: interval type not supported in fixed-width version");
  }

  /** {@inheritDoc} Not supported in this fixed-width-only version; always throws. */
  @Override
  public InternalRow getStruct(int ordinal, int numFields) {
    throw new UnsupportedOperationException(
        "VeloxColumnarRow.getStruct: struct type not supported in fixed-width version");
  }

  /** {@inheritDoc} Not supported in this fixed-width-only version; always throws. */
  @Override
  public ColumnarArray getArray(int ordinal) {
    throw new UnsupportedOperationException(
        "VeloxColumnarRow.getArray: array type not supported in fixed-width version");
  }

  /** {@inheritDoc} Not supported in this fixed-width-only version; always throws. */
  @Override
  public ColumnarMap getMap(int ordinal) {
    throw new UnsupportedOperationException(
        "VeloxColumnarRow.getMap: map type not supported in fixed-width version");
  }

  /**
   * {@inheritDoc}
   *
   * <p>Dispatches to the appropriate typed getter based on {@code dataType}. Supports fixed-width
   * types (INTEGER, BIGINT, SMALLINT, TINYINT, REAL, DOUBLE) and VARCHAR (STRING).
   */
  @Override
  public Object get(int ordinal, DataType dataType) {
    if (isNullAt(ordinal)) return null;
    if (dataType instanceof IntegerType) return getInt(ordinal);
    if (dataType instanceof LongType) return getLong(ordinal);
    if (dataType instanceof ShortType) return getShort(ordinal);
    if (dataType instanceof ByteType) return getByte(ordinal);
    if (dataType instanceof FloatType) return getFloat(ordinal);
    if (dataType instanceof DoubleType) return getDouble(ordinal);
    if (dataType instanceof DateType) return getInt(ordinal);
    if (dataType instanceof BooleanType) return getBoolean(ordinal);
    if (dataType instanceof StringType) return getUTF8String(ordinal);
    if (dataType instanceof BinaryType) return getBinary(ordinal);
    if (dataType instanceof TimestampType) return getLong(ordinal);
    if (dataType instanceof DecimalType) {
      DecimalType d = (DecimalType) dataType;
      return getDecimal(ordinal, d.precision(), d.scale());
    }
    throw new UnsupportedOperationException(
        "VeloxColumnarRow.get: unsupported data type: " + dataType);
  }

  // ---------------------------------------------------------------------------
  // InternalRow contract -- write side
  // ---------------------------------------------------------------------------

  /**
   * {@inheritDoc}
   *
   * <p>Marks the value at {@code ordinal} for the current {@link #rowId} as null by clearing the
   * corresponding bit in the Velox validity bitmap. Mirrors {@code ArrowColumnarRow.setNullAt}.
   */
  @Override
  public void setNullAt(int ordinal) {
    columns[ordinal].putNull(rowId);
  }

  /**
   * {@inheritDoc}
   *
   * <p>Dispatches to the appropriate typed setter based on the column's {@link DataType}. Supports
   * flat types (INTEGER, BIGINT, SMALLINT, TINYINT, REAL, DOUBLE, STRING) and nested types (ARRAY,
   * MAP, STRUCT) whose leaves are flat types. Passing {@code null} calls {@link #setNullAt}.
   *
   * <p>For ARRAY: the value must be an {@link ArrayData}; elements are appended to the child column
   * starting at the current cursor for that child, then {@link VeloxWritableColumnVector#putArray}
   * records the row's offset and length.
   *
   * <p>For MAP: the value must be a {@link MapData}; keys and values are appended to their
   * respective child columns.
   *
   * <p>For STRUCT: the value must be an {@link InternalRow}; each field is written to the
   * corresponding child column at {@link #rowId}.
   */
  @Override
  public void update(int ordinal, Object value) {
    if (value == null) {
      setNullAt(ordinal);
      return;
    }
    DataType dt = columns[ordinal].dataType();
    writeValue(columns[ordinal], dt, value, rowId);
  }

  /** {@inheritDoc} Delegates to {@link VeloxWritableColumnVector#putBoolean}. */
  @Override
  public void setBoolean(int ordinal, boolean value) {
    columns[ordinal].putBoolean(rowId, value);
  }

  /** {@inheritDoc} Delegates to {@link VeloxWritableColumnVector#putByte}. */
  @Override
  public void setByte(int ordinal, byte value) {
    columns[ordinal].putByte(rowId, value);
  }

  /** {@inheritDoc} Delegates to {@link VeloxWritableColumnVector#putShort}. */
  @Override
  public void setShort(int ordinal, short value) {
    columns[ordinal].putShort(rowId, value);
  }

  /** {@inheritDoc} Delegates to {@link VeloxWritableColumnVector#putInt}. */
  @Override
  public void setInt(int ordinal, int value) {
    columns[ordinal].putInt(rowId, value);
  }

  /**
   * {@inheritDoc}
   *
   * <p>For {@link TimestampType} columns, delegates to {@link
   * VeloxWritableColumnVector#putTimestampMicros} to decompose {@code value} (Spark
   * microseconds-since-epoch) into the Velox 16-byte {@code {sec, nanos}} struct.
   *
   * <p>For all other {@link LongType}-backed columns, delegates to {@link
   * VeloxWritableColumnVector#putLong}.
   *
   * <p>Spark's {@code MutableProjection} calls this typed setter (rather than {@link #update(int,
   * Object)}) for all {@code long}-valued column types, including {@code TimestampType} whose
   * internal representation is microseconds.
   */
  @Override
  public void setLong(int ordinal, long value) {
    if (columns[ordinal].dataType() instanceof TimestampType) {
      columns[ordinal].putTimestampMicros(rowId, value);
    } else {
      columns[ordinal].putLong(rowId, value);
    }
  }

  /** {@inheritDoc} Delegates to {@link VeloxWritableColumnVector#putFloat}. */
  @Override
  public void setFloat(int ordinal, float value) {
    columns[ordinal].putFloat(rowId, value);
  }

  /** {@inheritDoc} Delegates to {@link VeloxWritableColumnVector#putDouble}. */
  @Override
  public void setDouble(int ordinal, double value) {
    columns[ordinal].putDouble(rowId, value);
  }

  /**
   * {@inheritDoc}
   *
   * <p>Spark's {@code MutableProjection} calls this typed setter (rather than {@link #update(int,
   * Object)}) for {@link DecimalType} columns. Delegates to {@link
   * VeloxWritableColumnVector#putDecimal} using the column's declared precision.
   *
   * @param ordinal column index.
   * @param value the decimal value (a {@code null} writes a SQL NULL).
   * @param precision the target precision supplied by Spark (informational; the column's own
   *     declared {@link DecimalType} drives the short/long backing choice).
   */
  @Override
  public void setDecimal(int ordinal, Decimal value, int precision) {
    if (value == null) {
      setNullAt(ordinal);
      return;
    }
    DataType dt = columns[ordinal].dataType();
    if (dt instanceof DecimalType) {
      columns[ordinal].putDecimal(rowId, value, ((DecimalType) dt).precision());
    } else {
      throw new UnsupportedOperationException(
          "VeloxColumnarRow.setDecimal: non-decimal column type: " + dt);
    }
  }

  /**
   * Writes a UTF-8 string value into the VARCHAR column at {@code ordinal} for the current {@link
   * #rowId}.
   *
   * <p>Converts the {@link UTF8String} to a byte array and delegates to {@link
   * VeloxWritableColumnVector#putByteArray}, which writes a Velox {@code StringView} slot (inline
   * for size &le; 12, out-of-line chunk pointer for size &gt; 12).
   *
   * <p>Note: {@code setUTF8String} was removed from {@link InternalRow} in Spark 4.1. This method
   * remains as a direct-call convenience for {@code StringType} columns.
   *
   * @param ordinal zero-based column index; the backing column must be a VARCHAR column.
   * @param value the string to write; must not be {@code null} (use {@link #setNullAt} for nulls).
   * @throws UnsupportedOperationException if the backing column is not a VARCHAR column.
   */
  public void setUTF8String(int ordinal, UTF8String value) {
    byte[] b = value.getBytes();
    columns[ordinal].putByteArray(rowId, b, 0, b.length);
  }

  /**
   * Finalizes all pending string-data chunks after the last row has been written.
   *
   * <p>Recursively traverses the column tree and calls {@link
   * VeloxWritableColumnVector#finishStringColumn()} on every VARCHAR/VARBINARY column (including
   * nested VARCHAR/VARBINARY children inside ARRAY/MAP/STRUCT columns).
   *
   * <p>Must be called exactly <em>once</em> after the entire row-write loop and before assembling
   * the batch via {@code makeVeloxBatch}.
   */
  public void finishWriteRow() {
    for (VeloxWritableColumnVector col : columns) {
      finishColumn(col);
    }
  }

  /**
   * Recursively finalizes string chunks for a column and all its descendants.
   *
   * @param col the column to finalize; may be any type.
   */
  private static void finishColumn(VeloxWritableColumnVector col) {
    DataType dt = col.dataType();
    if (dt instanceof StringType || dt instanceof BinaryType) {
      col.finishStringColumn();
    } else if (dt instanceof ArrayType) {
      VeloxWritableColumnVector child = col.getChildColumn();
      if (child != null) {
        finishColumn(child);
      }
    } else if (dt instanceof MapType) {
      finishColumn((VeloxWritableColumnVector) col.getChild(0));
      finishColumn((VeloxWritableColumnVector) col.getChild(1));
    } else if (dt instanceof StructType) {
      StructType st = (StructType) dt;
      for (int i = 0; i < st.length(); i++) {
        finishColumn((VeloxWritableColumnVector) col.getChild(i));
      }
    }
    // flat non-VARCHAR: no-op
  }

  // ---------------------------------------------------------------------------
  // Nested write helpers
  // ---------------------------------------------------------------------------

  /**
   * Returns the current append cursor for a child column vector.
   *
   * @param child the child column; identity-keyed in {@link #cursors}.
   * @return current append position (next empty slot index in the child).
   */
  private int getCursor(VeloxWritableColumnVector child) {
    int[] box = cursors.get(child);
    return (box == null) ? 0 : box[0];
  }

  /**
   * Updates the append cursor for a child column vector.
   *
   * @param child the child column; identity-keyed in {@link #cursors}.
   * @param pos new cursor value (next empty slot index).
   */
  private void setCursor(VeloxWritableColumnVector child, int pos) {
    int[] box = cursors.get(child);
    if (box == null) {
      box = new int[] {pos};
      cursors.put(child, box);
    } else {
      box[0] = pos;
    }
  }

  /**
   * Writes a single value of the given type into a column at the specified row id. Handles all
   * supported leaf types and nested types (ARRAY, MAP, STRUCT) recursively.
   *
   * <p>Callers must ensure {@code value != null} before calling this method; use {@link
   * VeloxWritableColumnVector#putNull} for nulls.
   *
   * @param col column vector to write into.
   * @param dt Spark data type of the value.
   * @param value the non-null value to write.
   * @param rowId row index (or element index for nested children).
   */
  private void writeValue(VeloxWritableColumnVector col, DataType dt, Object value, int rowId) {
    if (dt instanceof IntegerType) {
      col.putInt(rowId, (Integer) value);
    } else if (dt instanceof DateType) {
      col.putInt(rowId, (Integer) value);
    } else if (dt instanceof LongType) {
      col.putLong(rowId, (Long) value);
    } else if (dt instanceof ShortType) {
      col.putShort(rowId, (Short) value);
    } else if (dt instanceof ByteType) {
      col.putByte(rowId, (Byte) value);
    } else if (dt instanceof FloatType) {
      col.putFloat(rowId, (Float) value);
    } else if (dt instanceof DoubleType) {
      col.putDouble(rowId, (Double) value);
    } else if (dt instanceof BooleanType) {
      col.putBoolean(rowId, (Boolean) value);
    } else if (dt instanceof StringType) {
      UTF8String s = (UTF8String) value;
      byte[] bytes = s.getBytes();
      col.putByteArray(rowId, bytes, 0, bytes.length);
    } else if (dt instanceof BinaryType) {
      byte[] b = (byte[]) value;
      col.putByteArray(rowId, b, 0, b.length);
    } else if (dt instanceof TimestampType) {
      col.putTimestampMicros(rowId, (Long) value);
    } else if (dt instanceof DecimalType) {
      col.putDecimal(rowId, (Decimal) value, ((DecimalType) dt).precision());
    } else if (dt instanceof ArrayType) {
      writeArray(col, (ArrayData) value, (ArrayType) dt, rowId);
    } else if (dt instanceof MapType) {
      writeMap(col, (MapData) value, (MapType) dt, rowId);
    } else if (dt instanceof StructType) {
      writeStruct(col, (InternalRow) value, (StructType) dt, rowId);
    } else {
      throw new UnsupportedOperationException(
          "VeloxColumnarRow.writeValue: unsupported type: " + dt);
    }
  }

  /**
   * Writes an {@link ArrayData} value into an ARRAY-typed column for the given row.
   *
   * <p>Elements are appended to the column's child starting at the current cursor position. The
   * child is grown via {@link VeloxWritableColumnVector#reserve} if needed. After writing all
   * elements, {@link VeloxWritableColumnVector#putArray} records the row's offset and size.
   *
   * @param col ARRAY-typed column to write into.
   * @param arrData the array data value.
   * @param dt the ARRAY type.
   * @param rowId row index within {@code col}.
   */
  private void writeArray(
      VeloxWritableColumnVector col, ArrayData arrData, ArrayType dt, int rowId) {
    VeloxWritableColumnVector child = col.getChildColumn();
    int startOffset = getCursor(child);
    int numElems = arrData.numElements();
    int needed = startOffset + numElems;
    // Grow the child if needed; reserve provides geometric growth and is safe here
    // because elementsAppended = 0 so reset() is a no-op for our null tracking.
    child.reserve(needed);
    DataType elemType = dt.elementType();
    for (int i = 0; i < numElems; i++) {
      int elemRowId = startOffset + i;
      if (arrData.isNullAt(i)) {
        child.putNull(elemRowId);
      } else {
        writeValue(child, elemType, arrData.get(i, elemType), elemRowId);
      }
    }
    col.putArray(rowId, startOffset, numElems);
    setCursor(child, needed);
  }

  /**
   * Writes a {@link MapData} value into a MAP-typed column for the given row.
   *
   * <p>Keys and values are appended to their respective children starting at the current cursor
   * position (which must be the same for both). After writing, {@link
   * VeloxWritableColumnVector#putArray} records the offset and entry count.
   *
   * @param col MAP-typed column to write into.
   * @param mapData the map data value.
   * @param dt the MAP type.
   * @param rowId row index within {@code col}.
   */
  private void writeMap(VeloxWritableColumnVector col, MapData mapData, MapType dt, int rowId) {
    VeloxWritableColumnVector keyChild = (VeloxWritableColumnVector) col.getChild(0);
    VeloxWritableColumnVector valChild = (VeloxWritableColumnVector) col.getChild(1);
    int startOffset = getCursor(keyChild);
    int numEntries = mapData.numElements();
    int needed = startOffset + numEntries;
    keyChild.reserve(needed);
    valChild.reserve(needed);
    ArrayData keys = mapData.keyArray();
    ArrayData values = mapData.valueArray();
    DataType keyType = dt.keyType();
    DataType valType = dt.valueType();
    for (int i = 0; i < numEntries; i++) {
      int elemRowId = startOffset + i;
      if (keys.isNullAt(i)) {
        keyChild.putNull(elemRowId);
      } else {
        writeValue(keyChild, keyType, keys.get(i, keyType), elemRowId);
      }
      if (values.isNullAt(i)) {
        valChild.putNull(elemRowId);
      } else {
        writeValue(valChild, valType, values.get(i, valType), elemRowId);
      }
    }
    col.putArray(rowId, startOffset, numEntries);
    setCursor(keyChild, needed);
    setCursor(valChild, needed);
  }

  /**
   * Writes an {@link InternalRow} value into a STRUCT-typed column for the given row.
   *
   * <p>Each field is written to the corresponding child column at {@code rowId} (ROW vectors have
   * 1:1 alignment: field {@code i} of row {@code r} lives at position {@code r} in {@code
   * childColumns[i]}).
   *
   * @param col STRUCT-typed column to write into.
   * @param row the struct row value.
   * @param dt the STRUCT type.
   * @param rowId row index within all field children.
   */
  private void writeStruct(
      VeloxWritableColumnVector col, InternalRow row, StructType dt, int rowId) {
    for (int i = 0; i < dt.length(); i++) {
      VeloxWritableColumnVector fieldCol = (VeloxWritableColumnVector) col.getChild(i);
      // Ensure the field child has capacity for rowId. For a top-level struct rowId < numRows so
      // this is a no-op; for a struct nested in an ARRAY/MAP, rowId = elemRowId can exceed numRows
      // and the field child (sized only for numRows) must be grown before writing to avoid an OOB
      // native write. Mirrors the child.reserve pattern in writeArray/writeMap.
      fieldCol.reserve(rowId + 1);
      DataType fieldType = dt.fields()[i].dataType();
      if (row.isNullAt(i)) {
        fieldCol.putNull(rowId);
      } else {
        writeValue(fieldCol, fieldType, row.get(i, fieldType), rowId);
      }
    }
  }
}
