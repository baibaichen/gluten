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
import org.apache.spark.unsafe.types.UTF8String;

/**
 * A {@link VeloxColumnAccessor} for flat (non-nested) Velox encodings.
 *
 * <p>Reads directly from native memory via {@link Platform} (i.e. {@code sun.misc.Unsafe}) with
 * zero data copies. The layout assumed is:
 *
 * <ul>
 *   <li><b>Validity bitmap</b> (optional, Velox convention): 1 bit per row, LSB-first within each
 *       byte, {@code 1 = valid}, {@code 0 = null}. When {@code nullsAddr == 0} all rows are valid.
 *   <li><b>Values buffer</b>: packed fixed-width primitives (4 bytes for int/float, 8 bytes for
 *       long/double, 2 bytes for short, 1 byte for byte). Booleans are bit-packed using the same
 *       layout as the validity bitmap.
 * </ul>
 *
 * <p>Package-private — callers should use {@link VeloxColumnVector} as the public entry point.
 */
class FlatAccessor extends VeloxColumnAccessor {

  /** Native address of the Velox validity bitmap, or {@code 0} when no nulls are present. */
  final long nullsAddr;

  /** Native address of the packed values buffer. */
  final long valuesAddr;

  /**
   * Pre-computed null count from the descriptor. A value of {@code 0} means no nulls; a negative
   * value means the count was not stored and must be derived from the bitmap at query time.
   */
  final long nullCount;

  /**
   * Constructs a FlatAccessor from raw descriptor fields.
   *
   * @param nullsAddr native address of the validity bitmap (0 = all valid)
   * @param valuesAddr native address of the values buffer
   * @param nullCount pre-computed null count ({@code < 0} means unknown)
   */
  FlatAccessor(long nullsAddr, long valuesAddr, long nullCount) {
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
   * <p>Reads a 4-byte integer from the values buffer at offset {@code rowId * 4}.
   */
  @Override
  public int getInt(int rowId) {
    return Platform.getInt(null, valuesAddr + (long) rowId * 4);
  }

  /**
   * {@inheritDoc}
   *
   * <p>Reads an 8-byte long from the values buffer at offset {@code rowId * 8}.
   */
  @Override
  public long getLong(int rowId) {
    return Platform.getLong(null, valuesAddr + (long) rowId * 8);
  }

  /**
   * {@inheritDoc}
   *
   * <p>Reads a 2-byte short from the values buffer at offset {@code rowId * 2}.
   */
  @Override
  public short getShort(int rowId) {
    return Platform.getShort(null, valuesAddr + (long) rowId * 2);
  }

  /**
   * {@inheritDoc}
   *
   * <p>Reads a single byte from the values buffer at offset {@code rowId}.
   */
  @Override
  public byte getByte(int rowId) {
    return Platform.getByte(null, valuesAddr + (long) rowId);
  }

  /**
   * {@inheritDoc}
   *
   * <p>Reads a 4-byte float from the values buffer at offset {@code rowId * 4}.
   */
  @Override
  public float getFloat(int rowId) {
    return Platform.getFloat(null, valuesAddr + (long) rowId * 4);
  }

  /**
   * {@inheritDoc}
   *
   * <p>Reads an 8-byte double from the values buffer at offset {@code rowId * 8}.
   */
  @Override
  public double getDouble(int rowId) {
    return Platform.getDouble(null, valuesAddr + (long) rowId * 8);
  }

  /**
   * {@inheritDoc}
   *
   * <p>Reads a bit-packed boolean from the values buffer. Velox stores booleans using the same
   * bit-layout as the validity bitmap (1 bit per row, LSB-first within each byte).
   */
  @Override
  public boolean getBoolean(int rowId) {
    byte b = Platform.getByte(null, valuesAddr + (rowId >> 3));
    return (b & (1 << (rowId & 7))) != 0;
  }

  /**
   * {@inheritDoc}
   *
   * <p>Reads a Velox {@code StringView} (16 bytes) from the values buffer and returns a zero-copy
   * {@link UTF8String} view into native memory.
   *
   * <p>A {@code StringView} is a 16-byte struct:
   *
   * <pre>
   * [0..4)  int32  size       — byte length of the string
   * [4..8)  4 bytes prefix    — first 4 chars (used for comparisons)
   * [8..16) 8 bytes inline/ptr:
   *           if size &le; 12: remaining inline chars (prefix + these = full string)
   *           if size &gt; 12: 8-byte native pointer to the out-of-line character data
   * </pre>
   *
   * <p>For inline strings ({@code size <= 12}) the full string content sits contiguously starting
   * at {@code svAddr + 4} (prefix field). For long strings the pointer at {@code svAddr + 8} points
   * directly to the character data. Both paths use {@link UTF8String#fromAddress} for a zero-copy
   * view.
   *
   * @param rowId zero-based row index
   * @return a zero-copy {@link UTF8String} backed by native memory
   */
  @Override
  public UTF8String getUTF8String(int rowId) {
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
   * {@inheritDoc}
   *
   * <p>Reads a Velox {@code StringView} (16 bytes) from the values buffer and returns the raw
   * binary bytes as a newly allocated {@code byte[]}.
   *
   * <p>Velox VARBINARY uses the same 16-byte {@code StringView} layout as VARCHAR:
   *
   * <pre>
   * [0..4)  int32  size       — byte length of the binary value
   * [4..8)  4 bytes prefix    — first 4 bytes (used for comparisons)
   * [8..16) 8 bytes inline/ptr:
   *           if size &le; 12: remaining inline bytes (prefix + these = full value)
   *           if size &gt; 12: 8-byte native pointer to the out-of-line data buffer
   * </pre>
   *
   * <p>For inline values ({@code size <= 12}) the full byte content sits contiguously starting at
   * {@code svAddr + 4} (prefix field). For long values the pointer at {@code svAddr + 8} points
   * directly to the data. Both paths copy the bytes into a new {@code byte[]} via {@link
   * Platform#copyMemory}.
   *
   * @param rowId zero-based row index
   * @return a {@code byte[]} containing the binary value
   */
  @Override
  public byte[] getBinary(int rowId) {
    long sv = valuesAddr + (long) rowId * 16;
    int size = Platform.getInt(null, sv);
    byte[] out = new byte[size];
    long src = (size <= 12) ? (sv + 4) : Platform.getLong(null, sv + 8);
    Platform.copyMemory(null, src, out, Platform.BYTE_ARRAY_OFFSET, size);
    return out;
  }
}
