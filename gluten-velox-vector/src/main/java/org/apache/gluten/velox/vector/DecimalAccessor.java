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
import org.apache.spark.unsafe.Platform;

import java.math.BigInteger;

/**
 * A {@link VeloxColumnAccessor} for Velox {@code DECIMAL} columns.
 *
 * <p>Velox DECIMAL is a logical type over a physical integer backing, selected by precision:
 *
 * <ul>
 *   <li><b>Short decimal</b> ({@code precision <= }{@link Decimal#MAX_LONG_DIGITS()}{@code == 18}):
 *       backed by {@code BIGINT}. Each row is an int64 unscaled value (8 bytes). Read directly and
 *       wrapped via {@link Decimal#createUnsafe(long, int, int)} with no {@link BigInteger}
 *       allocation.
 *   <li><b>Long decimal</b> ({@code precision > 18}): backed by {@code HUGEINT}. Each row is an
 *       int128 unscaled value (16 bytes) stored <b>little-endian, two's complement</b>. The low 64
 *       bits sit at offset 0 and the high 64 bits at offset 8. See {@link #int128ToBigInteger(long,
 *       long)} for the sign-correct conversion.
 * </ul>
 *
 * <p>The null-handling pattern mirrors {@link FlatAccessor}: the validity bitmap is optional
 * (address {@code 0} means all rows are valid; a set bit means valid, a clear bit means null).
 *
 * <p>Package-private -- callers should use {@link VeloxColumnVector} as the public entry point.
 */
class DecimalAccessor extends VeloxColumnAccessor {

  /** Decimal precision (total number of significant digits). */
  final int precision;

  /** Decimal scale (number of digits after the decimal point). */
  final int scale;

  /** Native address of the Velox validity bitmap, or {@code 0} when no nulls are present. */
  final long nullsAddr;

  /** Native address of the packed unscaled-value buffer (8B/row short, 16B/row long). */
  final long valuesAddr;

  /**
   * Pre-computed null count from the descriptor. A value of {@code 0} means no nulls; a negative
   * value means the count was not stored and must be derived from the bitmap at query time.
   */
  final long nullCount;

  /**
   * Constructs a DecimalAccessor from raw descriptor fields.
   *
   * @param precision decimal precision
   * @param scale decimal scale
   * @param nullsAddr native address of the validity bitmap (0 = all valid)
   * @param valuesAddr native address of the packed unscaled-value buffer
   * @param nullCount pre-computed null count ({@code < 0} means unknown)
   */
  DecimalAccessor(int precision, int scale, long nullsAddr, long valuesAddr, long nullCount) {
    this.precision = precision;
    this.scale = scale;
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
   * <p>Reads the unscaled value from native memory and wraps it as a Spark {@link Decimal}. The
   * {@code precision}/{@code scale} arguments (supplied by Spark from the column's declared type)
   * match this accessor's own {@link #precision}/{@link #scale}.
   *
   * @param rowId zero-based row index
   * @param p decimal precision (from the Spark {@code DecimalType})
   * @param s decimal scale (from the Spark {@code DecimalType})
   * @return the decimal value at {@code rowId}
   */
  @Override
  public Decimal getDecimal(int rowId, int p, int s) {
    if (precision <= Decimal.MAX_LONG_DIGITS()) {
      // Short decimal: int64 unscaled, 8 bytes/row.
      long unscaled = Platform.getLong(null, valuesAddr + (long) rowId * 8);
      return Decimal.createUnsafe(unscaled, precision, scale);
    }
    // Long decimal: int128 unscaled, 16 bytes/row, little-endian two's complement.
    long base = valuesAddr + (long) rowId * 16;
    long lo = Platform.getLong(null, base);
    long hi = Platform.getLong(null, base + 8);
    BigInteger unscaled = int128ToBigInteger(lo, hi);
    return Decimal.apply(new java.math.BigDecimal(unscaled, scale), precision, scale);
  }

  /**
   * Converts a Velox int128 (little-endian two's complement) split into two signed {@code long}
   * halves into a sign-correct {@link BigInteger}.
   *
   * <p>Velox HUGEINT stores the value little-endian, so {@code lo} holds the least-significant 64
   * bits and {@code hi} the most-significant 64 bits (including the sign bit). We assemble a
   * <b>big-endian</b> 16-byte two's-complement array -- {@code [hi MSB..LSB][lo MSB..LSB]} -- and
   * hand it to {@link BigInteger#BigInteger(byte[])}, which interprets a big-endian
   * two's-complement byte array with the correct sign for negatives.
   *
   * @param lo low 64 bits (offset 0 in native memory)
   * @param hi high 64 bits (offset 8 in native memory)
   * @return the signed unscaled value as a {@link BigInteger}
   */
  static BigInteger int128ToBigInteger(long lo, long hi) {
    byte[] be = new byte[16];
    for (int i = 0; i < 8; i++) {
      be[7 - i] = (byte) (hi >>> (8 * i)); // high 64 bits occupy the leading (MSB) 8 bytes
      be[15 - i] = (byte) (lo >>> (8 * i)); // low 64 bits occupy the trailing (LSB) 8 bytes
    }
    return new BigInteger(be);
  }

  /**
   * Converts a signed unscaled {@link BigInteger} into a Velox int128 (little-endian two's
   * complement) split into two {@code long} halves {@code [lo, hi]}. This is the <b>exact
   * inverse</b> of {@link #int128ToBigInteger(long, long)}.
   *
   * <p>The low half comes from {@link BigInteger#longValue()}; an arithmetic right shift yields the
   * high half, preserving two's-complement sign extension.
   *
   * @param bi the signed unscaled value (magnitude {@code < 2^127}; fits in 16 signed bytes)
   * @return {@code long[2] = {lo, hi}} -- low 64 bits then high 64 bits
   */
  static long[] bigIntegerToInt128(BigInteger bi) {
    return new long[] {bi.longValue(), bi.shiftRight(64).longValue()};
  }
}
