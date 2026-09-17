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

import org.apache.spark.unsafe.Platform;

/**
 * A {@link VeloxColumnAccessor} for Velox struct-encoded columns.
 *
 * <p>Struct values in Spark's columnar API are read via {@link VeloxColumnVector#getChild(int)}
 * (which Spark's {@code ColumnarRow} calls internally). This accessor handles only the struct's own
 * validity bitmap; individual field values are accessed through child {@link VeloxColumnVector}s.
 *
 * <p>Note: {@code getStruct(int)} in Spark 4.1 is declared {@code final} in {@link
 * org.apache.spark.sql.vectorized.ColumnVector} and delegates to {@code getChild}; do not override
 * it.
 *
 * <p>Package-private — callers should use {@link VeloxColumnVector} as the public entry point.
 */
class StructAccessor extends VeloxColumnAccessor {

  /** Native address of the Velox validity bitmap, or {@code 0} when all rows are valid. */
  private final long nullsAddr;

  /**
   * Pre-computed null count. {@code 0} = no nulls; negative = unknown (bitmap must be consulted at
   * query time).
   */
  private final long nullCount;

  /**
   * Constructs a StructAccessor from raw descriptor fields.
   *
   * @param nullsAddr native address of the validity bitmap (0 = all valid)
   * @param nullCount pre-computed null count ({@code < 0} means unknown)
   */
  StructAccessor(long nullsAddr, long nullCount) {
    this.nullsAddr = nullsAddr;
    this.nullCount = nullCount;
  }

  /**
   * {@inheritDoc}
   *
   * <p>Reads bit {@code rowId} from the Velox validity bitmap. Returns {@code false} (valid) when
   * no bitmap is present.
   */
  @Override
  public boolean isNullAt(int rowId) {
    if (nullsAddr == 0L) {
      return false;
    }
    byte b = Platform.getByte(null, nullsAddr + (rowId >> 3));
    return (b & (1 << (rowId & 7))) == 0;
  }

  /** {@inheritDoc} */
  @Override
  public boolean hasNull() {
    return nullCount > 0 || (nullCount < 0 && nullsAddr != 0L);
  }

  /** {@inheritDoc} */
  @Override
  public long numNulls() {
    return nullCount >= 0 ? nullCount : -1L;
  }

  // All other getters (getInt, getLong, getArray, getMap, etc.) inherit the
  // UnsupportedOperationException-throwing defaults from VeloxColumnAccessor.
  // Struct field values are read by accessing child column vectors via
  // VeloxColumnVector.getChild(int), not through this accessor.
}
