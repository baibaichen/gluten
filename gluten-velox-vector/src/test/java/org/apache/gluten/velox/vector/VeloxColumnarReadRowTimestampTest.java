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
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.unsafe.Platform;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

/**
 * TDD tests for TimestampType read via {@link VeloxColumnarReadRow#get(int,
 * org.apache.spark.sql.types.DataType)}.
 *
 * <p>{@link VeloxInputBatch} admits TimestampType onto the zero-copy read path, but before the
 * TimestampType branch is added to {@link VeloxColumnarReadRow#get}, calling {@code row.get(0,
 * DataTypes.TimestampType)} falls through to {@code throw new UnsupportedOperationException}. That
 * is reachable on the interpreted MutableProjection fallback and on any {@code copy()} call (the
 * codegen path uses getLong and masks it). This mirrors the DATE gap in {@link
 * VeloxColumnarReadRowDateTest}.
 *
 * <p>RED (before fix): get(0, TimestampType) / copy() throw. GREEN (after fix): the Spark internal
 * micros-since-epoch value (seconds * 1_000_000 + nanos / 1_000) is returned.
 *
 * <p>No native library is loaded. Descriptors are built in Java-allocated off-heap memory via
 * {@link Platform}. Memory is freed manually after each test.
 */
public class VeloxColumnarReadRowTimestampTest {

  /**
   * Allocates a VeloxColumnHandle descriptor for a flat TIMESTAMP column. Each row is a 16-byte
   * struct {@code [seconds:int64, nanos:int64]} (matching {@link TimestampAccessor#getLong}).
   *
   * @param seconds per-row seconds since the Unix epoch
   * @param nanos per-row sub-second nanoseconds in [0, 999_999_999]
   * @return {@code long[3] = {desc, buffers, values}} — all freed by caller
   */
  private static long[] allocTimestampDescriptor(long[] seconds, long[] nanos) {
    long values = Platform.allocateMemory((long) seconds.length * 16);
    for (int i = 0; i < seconds.length; i++) {
      Platform.putLong(null, values + (long) i * 16, seconds[i]);
      Platform.putLong(null, values + (long) i * 16 + 8, nanos[i]);
    }

    long buffers = Platform.allocateMemory(16);
    Platform.putLong(null, buffers, 0L); // nulls = nullptr (no nulls)
    Platform.putLong(null, buffers + 8, values);

    long desc = Platform.allocateMemory(48);
    Platform.putLong(null, desc, seconds.length); // length
    Platform.putLong(null, desc + 8, 0L); // nullCount = 0
    Platform.putLong(null, desc + 16, 2L); // nBuffers
    Platform.putLong(null, desc + 24, 0L); // nChildren
    Platform.putLong(null, desc + 32, buffers); // buffers ptr
    Platform.putLong(null, desc + 40, 0L); // children ptr

    return new long[] {desc, buffers, values};
  }

  /**
   * Verifies that {@link VeloxColumnarReadRow#get} with {@code DataTypes.TimestampType} returns the
   * micros-since-epoch value rather than throwing {@link UnsupportedOperationException}.
   */
  @Test
  public void getTimestampReturnsMicros() {
    // row0: epoch. row1: 1_000_000s + 1_500_000ns -> 1_000_000_000_000 + 1_500 = ...001_500 micros
    //   (sub-micro 500ns truncated). row2: pre-epoch negative seconds.
    long[] seconds = {0L, 1_000_000L, -5L};
    long[] nanos = {0L, 1_500_000L, 0L};
    long[] ptrs = allocTimestampDescriptor(seconds, nanos);
    try {
      VeloxColumnVector cv = VeloxColumnVector.importFromNative(ptrs[0], DataTypes.TimestampType);
      VeloxColumnarReadRow row = new VeloxColumnarReadRow(new VeloxColumnVector[] {cv});

      row.rowId = 0;
      assertEquals(0L, row.get(0, DataTypes.TimestampType), "epoch must round-trip to 0 micros");

      row.rowId = 1;
      assertEquals(
          1_000_000L * 1_000_000L + 1_500L,
          row.get(0, DataTypes.TimestampType),
          "seconds+nanos must convert to micros (sub-micro truncated)");

      row.rowId = 2;
      assertEquals(
          -5L * 1_000_000L,
          row.get(0, DataTypes.TimestampType),
          "pre-epoch seconds must round-trip");

      // Also verify getLong works (used by codegen path — should already pass).
      row.rowId = 1;
      assertEquals(
          1_000_000L * 1_000_000L + 1_500L,
          row.getLong(0),
          "getLong must still work for TIMESTAMP");
    } finally {
      Platform.freeMemory(ptrs[2]); // values
      Platform.freeMemory(ptrs[1]); // buffers
      Platform.freeMemory(ptrs[0]); // desc
    }
  }

  /**
   * Verifies that {@link VeloxColumnarReadRow#copy()} on a TIMESTAMP-column row preserves the
   * micros value. copy() delegates fixed-width types to get(i, dt), so the same TimestampType
   * branch in get() fixes the copy path.
   */
  @Test
  public void copyTimestampColumnPreservesMicros() {
    long[] seconds = {1_000_000L, 0L};
    long[] nanos = {0L, 0L};
    long[] ptrs = allocTimestampDescriptor(seconds, nanos);
    try {
      VeloxColumnVector cv = VeloxColumnVector.importFromNative(ptrs[0], DataTypes.TimestampType);
      VeloxColumnarReadRow row = new VeloxColumnarReadRow(new VeloxColumnVector[] {cv});

      row.rowId = 0;
      InternalRow copied = row.copy();
      assertEquals(
          1_000_000L * 1_000_000L,
          copied.getLong(0),
          "copy() of TIMESTAMP row must preserve micros value");

      row.rowId = 1;
      InternalRow copied2 = row.copy();
      assertEquals(0L, copied2.getLong(0), "copy() of epoch TIMESTAMP row must return 0");
    } finally {
      Platform.freeMemory(ptrs[2]);
      Platform.freeMemory(ptrs[1]);
      Platform.freeMemory(ptrs[0]);
    }
  }

  /** Verifies that {@link VeloxColumnarReadRow#get} returns {@code null} for a null TIMESTAMP. */
  @Test
  public void getTimestampNullReturnsNull() {
    long values = Platform.allocateMemory(16);
    Platform.putLong(null, values, 12345L); // any value — should be masked by null
    Platform.putLong(null, values + 8, 0L);

    long nullsBitmap = Platform.allocateMemory(8);
    Platform.putLong(null, nullsBitmap, 0L); // all bits 0 -> all null

    long buffers = Platform.allocateMemory(16);
    Platform.putLong(null, buffers, nullsBitmap);
    Platform.putLong(null, buffers + 8, values);

    long desc = Platform.allocateMemory(48);
    Platform.putLong(null, desc, 1L); // length = 1
    Platform.putLong(null, desc + 8, 1L); // nullCount = 1
    Platform.putLong(null, desc + 16, 2L); // nBuffers
    Platform.putLong(null, desc + 24, 0L); // nChildren
    Platform.putLong(null, desc + 32, buffers);
    Platform.putLong(null, desc + 40, 0L);

    try {
      VeloxColumnVector cv = VeloxColumnVector.importFromNative(desc, DataTypes.TimestampType);
      VeloxColumnarReadRow row = new VeloxColumnarReadRow(new VeloxColumnVector[] {cv});

      row.rowId = 0;
      assertNull(
          row.get(0, DataTypes.TimestampType), "null TIMESTAMP slot must return null from get()");
    } finally {
      Platform.freeMemory(desc);
      Platform.freeMemory(buffers);
      Platform.freeMemory(nullsBitmap);
      Platform.freeMemory(values);
    }
  }
}
