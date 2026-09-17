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

import org.apache.spark.sql.vectorized.ColumnarArray;
import org.apache.spark.unsafe.Platform;

/**
 * A {@link VeloxColumnAccessor} for Velox array-encoded columns.
 *
 * <p>Reads element ranges from two parallel int32 buffers:
 *
 * <ul>
 *   <li><b>offsets</b> ({@code buffers[1]}): the start index (in the child column) of each array
 *       row.
 *   <li><b>sizes</b> ({@code buffers[2]}): the number of elements in each array row.
 * </ul>
 *
 * <p>The element values themselves are stored in the {@code child} {@link VeloxColumnVector}. A
 * {@link ColumnarArray} view is returned without copying any data.
 *
 * <p>Package-private — callers should use {@link VeloxColumnVector} as the public entry point.
 */
class ArrayAccessor extends VeloxColumnAccessor {

  /** Native address of the Velox validity bitmap, or {@code 0} when all rows are valid. */
  private final long nullsAddr;

  /** Native address of the int32 offsets buffer (one entry per row). */
  private final long offsetsAddr;

  /** Native address of the int32 sizes buffer (one entry per row). */
  private final long sizesAddr;

  /**
   * Pre-computed null count. {@code 0} = no nulls; negative = unknown (bitmap must be consulted at
   * query time).
   */
  private final long nullCount;

  /** Child column vector holding the flattened element values. */
  final VeloxColumnVector child;

  /**
   * Constructs an ArrayAccessor from raw descriptor fields.
   *
   * @param nullsAddr native address of the validity bitmap (0 = all valid)
   * @param offsetsAddr native address of the int32 offsets buffer
   * @param sizesAddr native address of the int32 sizes buffer
   * @param nullCount pre-computed null count ({@code < 0} means unknown)
   * @param child the child {@link VeloxColumnVector} holding flattened element values
   */
  ArrayAccessor(
      long nullsAddr, long offsetsAddr, long sizesAddr, long nullCount, VeloxColumnVector child) {
    this.nullsAddr = nullsAddr;
    this.offsetsAddr = offsetsAddr;
    this.sizesAddr = sizesAddr;
    this.nullCount = nullCount;
    this.child = child;
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

  /**
   * {@inheritDoc}
   *
   * <p>Reads {@code start} from the offsets buffer and {@code len} from the sizes buffer (both at
   * {@code rowId * 4} bytes), then returns a zero-copy {@link ColumnarArray} view into the child
   * column.
   *
   * <p>The length is always read from the sizes buffer — never derived via offset subtraction —
   * because Velox stores explicit sizes alongside offsets.
   */
  @Override
  public ColumnarArray getArray(int rowId) {
    int start = Platform.getInt(null, offsetsAddr + (long) rowId * 4);
    int len = Platform.getInt(null, sizesAddr + (long) rowId * 4);
    return new ColumnarArray(child, start, len);
  }
}
