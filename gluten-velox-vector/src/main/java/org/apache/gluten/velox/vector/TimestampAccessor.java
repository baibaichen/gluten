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
 * A {@link VeloxColumnAccessor} for Velox {@code TIMESTAMP} columns.
 *
 * <p>Velox stores each timestamp as a 16-byte struct:
 *
 * <pre>
 * offset  size  field
 *  0       8    int64   seconds  — seconds since Unix epoch (may be negative for pre-epoch)
 *  8       8    uint64  nanos    — nanosecond sub-second offset in [0, 999_999_999]
 * </pre>
 *
 * <p>Spark's {@code TimestampType} internal representation is a signed {@code int64} count of
 * microseconds since the Unix epoch. The conversion is:
 *
 * <pre>
 *   micros = seconds * 1_000_000L + nanos / 1_000L
 * </pre>
 *
 * <p>Sub-microsecond precision is truncated (not rounded): {@code nanos=1500} yields {@code
 * nanos/1000 = 1} microsecond, discarding the 500 ns remainder.
 *
 * <p>The null-handling pattern mirrors {@link FlatAccessor}: validity bitmap is optional (address
 * {@code 0} means all rows are valid, {@code 1} in a bit means valid, {@code 0} means null).
 *
 * <p>Package-private — callers should use {@link VeloxColumnVector} as the public entry point.
 */
class TimestampAccessor extends VeloxColumnAccessor {

  /** Native address of the Velox validity bitmap, or {@code 0} when no nulls are present. */
  final long nullsAddr;

  /**
   * Native address of the packed 16-byte-per-row values buffer. Each slot is a {@code {int64
   * seconds, uint64 nanos}} pair.
   */
  final long valuesAddr;

  /**
   * Pre-computed null count from the descriptor. A value of {@code 0} means no nulls; a negative
   * value means the count was not stored and must be derived from the bitmap at query time.
   */
  final long nullCount;

  /**
   * Constructs a TimestampAccessor from raw descriptor fields.
   *
   * @param nullsAddr native address of the validity bitmap (0 = all valid)
   * @param valuesAddr native address of the 16-byte-per-row values buffer
   * @param nullCount pre-computed null count ({@code < 0} means unknown)
   */
  TimestampAccessor(long nullsAddr, long valuesAddr, long nullCount) {
    this.nullsAddr = nullsAddr;
    this.valuesAddr = valuesAddr;
    this.nullCount = nullCount;
  }

  /**
   * {@inheritDoc}
   *
   * <p>Reads bit {@code rowId} from the Velox validity bitmap. If no bitmap is present the row is
   * always valid (returns {@code false}).
   */
  @Override
  public boolean isNullAt(int rowId) {
    if (nullsAddr == 0L) {
      return false;
    }
    // Velox validity bitmap: 1 = valid, 0 = null; LSB-first within each byte.
    byte b = Platform.getByte(null, nullsAddr + (rowId >> 3));
    boolean valid = (b & (1 << (rowId & 7))) != 0;
    return !valid;
  }

  /**
   * {@inheritDoc}
   *
   * <p>Returns {@code true} when the stored null count is positive, or when no null count is stored
   * but a validity bitmap exists (meaning some rows may be null).
   */
  @Override
  public boolean hasNull() {
    return nullCount > 0 || (nullCount < 0 && nullsAddr != 0L);
  }

  /**
   * {@inheritDoc}
   *
   * @return the stored null count, or {@code -1} when not available
   */
  @Override
  public long numNulls() {
    return nullCount >= 0 ? nullCount : -1L;
  }

  /**
   * {@inheritDoc}
   *
   * <p>Reads the 16-byte Velox {@code Timestamp} struct at {@code valuesAddr + rowId * 16} and
   * converts it to Spark's microseconds-since-epoch representation.
   *
   * <p>Conversion: {@code micros = seconds * 1_000_000L + nanos / 1_000L}. Sub-microsecond
   * precision is truncated. Pre-epoch timestamps (negative {@code seconds}) are handled correctly
   * by the arithmetic since {@code nanos} is always non-negative.
   *
   * @param rowId zero-based row index
   * @return microseconds since the Unix epoch (Spark TimestampType internal value)
   */
  @Override
  public long getLong(int rowId) {
    long base = valuesAddr + (long) rowId * 16;
    long seconds = Platform.getLong(null, base);
    // nanos is uint64 in Velox, but is always in [0, 999_999_999] so the signed
    // Java long interpretation is identical to the unsigned value.
    long nanos = Platform.getLong(null, base + 8);
    return seconds * 1_000_000L + nanos / 1_000L;
  }
}
