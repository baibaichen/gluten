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

import org.apache.spark.sql.types.Decimal;
import org.apache.spark.sql.vectorized.ColumnarArray;
import org.apache.spark.sql.vectorized.ColumnarMap;
import org.apache.spark.unsafe.types.UTF8String;

/**
 * Abstract base class for reading a single Velox column without copying data.
 *
 * <p>Concrete subclasses override only the getters that their encoding supports. All unimplemented
 * getters throw {@link UnsupportedOperationException} by default, so subclasses are not forced to
 * provide stubs for every type.
 *
 * <p>Null semantics follow the Velox convention: a validity bitmap where {@code 1} means valid and
 * {@code 0} means null (opposite of Arrow). When no nulls bitmap is present ({@code nullsAddr==0}),
 * all rows are valid.
 */
public abstract class VeloxColumnAccessor {

  /**
   * Returns {@code true} if the value at {@code rowId} is null.
   *
   * @param rowId zero-based row index within this column
   * @return {@code true} if the row is null
   */
  public abstract boolean isNullAt(int rowId);

  /**
   * Returns {@code true} if this column contains at least one null value.
   *
   * @return {@code true} when any row is null
   */
  public abstract boolean hasNull();

  /**
   * Returns the number of null values in this column, or {@code -1} if unknown.
   *
   * @return null count, or {@code -1} if not tracked
   */
  public abstract long numNulls();

  /**
   * Returns the {@code int} value at {@code rowId}.
   *
   * @param rowId zero-based row index
   * @return integer value
   * @throws UnsupportedOperationException if this column's type does not support int access
   */
  public int getInt(int rowId) {
    throw new UnsupportedOperationException("getInt");
  }

  /**
   * Returns the {@code long} value at {@code rowId}.
   *
   * @param rowId zero-based row index
   * @return long value
   * @throws UnsupportedOperationException if this column's type does not support long access
   */
  public long getLong(int rowId) {
    throw new UnsupportedOperationException("getLong");
  }

  /**
   * Returns the {@code short} value at {@code rowId}.
   *
   * @param rowId zero-based row index
   * @return short value
   * @throws UnsupportedOperationException if this column's type does not support short access
   */
  public short getShort(int rowId) {
    throw new UnsupportedOperationException("getShort");
  }

  /**
   * Returns the {@code byte} value at {@code rowId}.
   *
   * @param rowId zero-based row index
   * @return byte value
   * @throws UnsupportedOperationException if this column's type does not support byte access
   */
  public byte getByte(int rowId) {
    throw new UnsupportedOperationException("getByte");
  }

  /**
   * Returns the {@code float} value at {@code rowId}.
   *
   * @param rowId zero-based row index
   * @return float value
   * @throws UnsupportedOperationException if this column's type does not support float access
   */
  public float getFloat(int rowId) {
    throw new UnsupportedOperationException("getFloat");
  }

  /**
   * Returns the {@code double} value at {@code rowId}.
   *
   * @param rowId zero-based row index
   * @return double value
   * @throws UnsupportedOperationException if this column's type does not support double access
   */
  public double getDouble(int rowId) {
    throw new UnsupportedOperationException("getDouble");
  }

  /**
   * Returns the {@link UTF8String} value at {@code rowId}.
   *
   * @param rowId zero-based row index
   * @return UTF8String value (may be a zero-copy view into native memory)
   * @throws UnsupportedOperationException if this column's type does not support string access
   */
  public UTF8String getUTF8String(int rowId) {
    throw new UnsupportedOperationException("getUTF8String");
  }

  /**
   * Returns the {@code boolean} value at {@code rowId}.
   *
   * @param rowId zero-based row index
   * @return boolean value
   * @throws UnsupportedOperationException if this column's type does not support boolean access
   */
  public boolean getBoolean(int rowId) {
    throw new UnsupportedOperationException("getBoolean");
  }

  /**
   * Returns a {@link ColumnarArray} view for the array-typed value at {@code rowId}.
   *
   * @param rowId zero-based row index
   * @return a zero-copy {@link ColumnarArray} over the child column
   * @throws UnsupportedOperationException if this column's type does not support array access
   */
  public ColumnarArray getArray(int rowId) {
    throw new UnsupportedOperationException("getArray");
  }

  /**
   * Returns a {@link ColumnarMap} view for the map-typed value at {@code rowId}.
   *
   * @param rowId zero-based row index
   * @return a zero-copy {@link ColumnarMap} over the key and value child columns
   * @throws UnsupportedOperationException if this column's type does not support map access
   */
  public ColumnarMap getMap(int rowId) {
    throw new UnsupportedOperationException("getMap");
  }

  /**
   * Returns the raw binary bytes at {@code rowId} as a newly allocated {@code byte[]}.
   *
   * @param rowId zero-based row index
   * @return a {@code byte[]} containing the binary value
   * @throws UnsupportedOperationException if this column's type does not support binary access
   */
  public byte[] getBinary(int rowId) {
    throw new UnsupportedOperationException("getBinary");
  }

  /**
   * Returns the {@link Decimal} value at {@code rowId}.
   *
   * @param rowId zero-based row index
   * @param precision decimal precision (total number of significant digits)
   * @param scale decimal scale (number of digits after the decimal point)
   * @return decimal value
   * @throws UnsupportedOperationException if this column's type does not support decimal access
   */
  public Decimal getDecimal(int rowId, int precision, int scale) {
    throw new UnsupportedOperationException("getDecimal");
  }
}
