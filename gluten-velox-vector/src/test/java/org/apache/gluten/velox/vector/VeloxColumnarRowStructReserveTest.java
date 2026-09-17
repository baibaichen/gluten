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
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.unsafe.Platform;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Regression test for Bug 1: {@code VeloxColumnarRow.writeStruct} must grow (reserve) each STRUCT
 * field child before writing at {@code rowId}, so that a struct nested inside an ARRAY / MAP --
 * where {@code rowId} is a cumulative element index that can exceed the batch {@code numRows} --
 * does not write past the field child's buffer (heap OOB).
 *
 * <p>The batch {@code numRows} is 2, but a struct field is written at {@code rowId = 5} (mimicking
 * {@code elemRowId = startOffset + i} for an array of 6 structs in row 0). Under the buggy code
 * {@code writeStruct} never reserves the field child, so its capacity stays at the batch {@code
 * numRows} and the {@code putInt(5, ...)} is an out-of-bounds write. The fix reserves {@code rowId
 * + 1} on each field child (mirroring {@code writeArray}/{@code writeMap}).
 *
 * <p>Observation is on the field child's {@code reserve} bookkeeping (recorded capacity), which is
 * deterministic and does not depend on whether the OOB heap write happens to land in mapped memory.
 */
class VeloxColumnarRowStructReserveTest {

  /**
   * A STRUCT-typed writable column whose single {@code int} field child is a {@link
   * RecordingChild}. Overrides {@link #getChild(int)} so {@code writeStruct} sees the recording
   * child. Backed by a minimal hand-built descriptor (nulls only) via the test-only constructor.
   */
  private static final class StructColumn extends VeloxWritableColumnVector {
    private final RecordingChild child;

    StructColumn(long descAddr, RecordingChild child, StructType type) {
      super(descAddr, 0L, /* capacity= */ 2, type);
      this.child = child;
    }

    @Override
    public org.apache.spark.sql.execution.vectorized.WritableColumnVector getChild(int ordinal) {
      return child;
    }
  }

  /**
   * An {@code int} field child that records the maximum capacity it was reserved to, overriding
   * {@code reserveInternal} so the test does not need native {@code growChild}. Values are written
   * into a generously over-allocated off-heap buffer so an (unfixed) OOB {@code putInt} is
   * memory-safe for the test process while still being observable via {@link #reservedCapacity}.
   */
  private static final class RecordingChild extends VeloxWritableColumnVector {
    int reservedCapacity;

    RecordingChild(long descAddr, int capacity) {
      super(descAddr, 0L, capacity, DataTypes.IntegerType);
      this.reservedCapacity = capacity;
    }

    @Override
    protected void reserveInternal(int newCapacity) {
      if (newCapacity > reservedCapacity) {
        reservedCapacity = newCapacity;
      }
      // Emulate a successful native growth without touching JNI: capacity now covers newCapacity.
      this.capacity = newCapacity;
    }
  }

  private static long allocFlatDesc(int capacity, long valueBytes) {
    long nullsBytes = VeloxWritableColumnVector.nullsByteSize(capacity);
    long nulls = Platform.allocateMemory(nullsBytes);
    for (long i = 0; i < nullsBytes; i++) {
      Platform.putByte(null, nulls + i, (byte) 0xFF);
    }
    long values = Platform.allocateMemory(Math.max(8L, valueBytes));
    long buffers = Platform.allocateMemory(16);
    Platform.putLong(null, buffers, nulls);
    Platform.putLong(null, buffers + 8, values);
    long desc = Platform.allocateMemory(48);
    Platform.putLong(null, desc, capacity);
    Platform.putLong(null, desc + 8, 0L);
    Platform.putLong(null, desc + 16, 2L);
    Platform.putLong(null, desc + 24, 0L);
    Platform.putLong(null, desc + 32, buffers);
    Platform.putLong(null, desc + 40, 0L);
    return desc;
  }

  private static long allocStructDesc(int capacity) {
    long nullsBytes = VeloxWritableColumnVector.nullsByteSize(capacity);
    long nulls = Platform.allocateMemory(nullsBytes);
    for (long i = 0; i < nullsBytes; i++) {
      Platform.putByte(null, nulls + i, (byte) 0xFF);
    }
    long buffers = Platform.allocateMemory(8);
    Platform.putLong(null, buffers, nulls);
    long desc = Platform.allocateMemory(48);
    Platform.putLong(null, desc, capacity);
    Platform.putLong(null, desc + 8, 0L);
    Platform.putLong(null, desc + 16, 1L);
    Platform.putLong(null, desc + 24, 0L);
    Platform.putLong(null, desc + 32, buffers);
    Platform.putLong(null, desc + 40, 0L);
    return desc;
  }

  @Test
  void writeStructReservesFieldChildBeyondNumRows() {
    int numRows = 2;
    StructType structType =
        DataTypes.createStructType(
            new org.apache.spark.sql.types.StructField[] {
              DataTypes.createStructField("f", DataTypes.IntegerType, true)
            });

    // Field child capacity == numRows (2), but over-allocate the real values buffer to 16 ints so
    // an unfixed OOB putInt at index 5 stays inside mapped test memory.
    long childDesc = allocFlatDesc(numRows, 16L * Integer.BYTES);
    RecordingChild child = new RecordingChild(childDesc, numRows);

    long structDesc = allocStructDesc(numRows);
    StructColumn structCol = new StructColumn(structDesc, child, structType);

    VeloxColumnarRow row = new VeloxColumnarRow(new VeloxWritableColumnVector[] {structCol});

    // Simulate the array<struct> element write: elemRowId = 5 >= numRows(2).
    int elemRowId = 5;
    row.rowId = elemRowId;
    InternalRow structVal = new GenericInternalRow(new Object[] {42});
    row.update(0, structVal);

    // The field child must have been reserved to cover elemRowId (>= elemRowId + 1). Under the
    // buggy code writeStruct never reserved it, so reservedCapacity would still be numRows(2).
    assertTrue(
        child.reservedCapacity >= elemRowId + 1,
        "field child must be grown to cover elemRowId; got capacity="
            + child.reservedCapacity
            + " but needed >= "
            + (elemRowId + 1));
    // Value round-trips correctly at the out-of-numRows slot.
    assertEquals(42, child.getInt(elemRowId));
  }
}
