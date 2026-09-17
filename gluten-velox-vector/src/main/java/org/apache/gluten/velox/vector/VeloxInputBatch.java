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
import org.apache.spark.sql.types.*;

import java.util.function.LongConsumer;

/**
 * Wraps a native Velox batch's columns as zero-copy {@link VeloxColumnVector}s and exposes a
 * null-safe {@link InternalRow} for the projection.
 *
 * <p>Key design constraints:
 *
 * <ul>
 *   <li><b>C1 null-safety:</b> {@link #getRow(int)} returns a {@link VeloxColumnarReadRow} that
 *       checks {@code isNullAt} before every getter -- mirroring {@code ArrowColumnarRow.get}
 *       (ArrowColumnarRow.scala:120-122). Callers MUST NOT use {@code ColumnarBatch.getRow} with
 *       these vectors, as Spark 4.1's {@code ColumnarBatchRow.get} does not null-check in the
 *       interpreted projection path.
 *   <li><b>Import gate:</b> {@link #canImport(StructType)} whitelists exactly the types that the
 *       native read side can export:
 *       BOOLEAN/INT/LONG/SHORT/BYTE/FLOAT/DOUBLE/STRING/DATE/BINARY/TIMESTAMP/DECIMAL/NULL/
 *       CALENDAR_INTERVAL as leaves, plus ARRAY/MAP/STRUCT recursively.
 * </ul>
 */
public final class VeloxInputBatch implements AutoCloseable {

  private final VeloxColumnVector[] cols;
  private final int numRows;
  private final VeloxColumnarReadRow row;

  private VeloxInputBatch(VeloxColumnVector[] cols, int numRows) {
    this.cols = cols;
    this.numRows = numRows;
    this.row = new VeloxColumnarReadRow(cols);
  }

  // ---------------------------------------------------------------------------
  // Import gate
  // ---------------------------------------------------------------------------

  /**
   * Returns {@code true} iff every field in {@code schema} (recursively) is a read-side importable
   * type.
   *
   * <p>Importable leaves: BOOLEAN, INT, LONG, SHORT, BYTE, FLOAT, DOUBLE, STRING, DATE, BINARY,
   * TIMESTAMP, DECIMAL, NULL, CALENDAR_INTERVAL. Importable containers: ARRAY, MAP, STRUCT
   * (recursively).
   *
   * @param schema Spark {@link StructType} to check
   * @return {@code true} if all fields are importable
   */
  public static boolean canImport(StructType schema) {
    for (StructField f : schema.fields()) {
      if (!importable(f.dataType())) return false;
    }
    return true;
  }

  private static boolean importable(DataType t) {
    if (t instanceof BooleanType
        || t instanceof IntegerType
        || t instanceof LongType
        || t instanceof ShortType
        || t instanceof ByteType
        || t instanceof FloatType
        || t instanceof DoubleType
        || t instanceof StringType
        || t instanceof DateType
        || t instanceof BinaryType
        || t instanceof TimestampType
        || t instanceof DecimalType
        || t instanceof CalendarIntervalType
        || t instanceof NullType) {
      return true;
    }
    if (t instanceof ArrayType) return importable(((ArrayType) t).elementType());
    if (t instanceof MapType) {
      MapType m = (MapType) t;
      return importable(m.keyType()) && importable(m.valueType());
    }
    if (t instanceof StructType) return canImport((StructType) t);
    // Other types (e.g. UDTs) -> not importable
    return false;
  }

  // ---------------------------------------------------------------------------
  // Factory method
  // ---------------------------------------------------------------------------

  /**
   * Wraps a native Velox batch's columns as zero-copy {@link VeloxColumnVector}s.
   *
   * <p>Calls {@link VeloxColumnHandleJniWrapper#exportBatchColumnDescriptors} to obtain per-column
   * descriptor addresses, then constructs one {@link VeloxColumnVector} per column via {@link
   * VeloxColumnVector#importFromNative}. The returned {@link VeloxInputBatch} owns the vectors;
   * calling {@link #close()} frees each column's descriptor.
   *
   * @param batchHandle native ColumnarBatch handle (from {@code ColumnarBatches.getNativeHandle})
   * @param schema Spark schema of the batch; must have the same number of columns as the batch
   * @param numRows number of rows in the batch
   * @return a new {@link VeloxInputBatch} ready for row-by-row projection
   * @throws IllegalStateException if column count in the batch does not match {@code schema}
   */
  public static VeloxInputBatch wrap(long batchHandle, StructType schema, int numRows) {
    long[] descs = VeloxColumnHandleJniWrapper.exportBatchColumnDescriptors(batchHandle);
    StructField[] fields = schema.fields();
    if (descs.length != fields.length) {
      freeUnimportedDescriptors(descs, 0, VeloxColumnHandleJniWrapper::freeDescriptor);
      throw new IllegalStateException(
          "VeloxInputBatch.wrap: column count mismatch: descriptors="
              + descs.length
              + " schema="
              + fields.length);
    }
    VeloxColumnVector[] cols = new VeloxColumnVector[descs.length];
    int consumed = 0;
    try {
      for (int i = 0; i < descs.length; i++) {
        // importFromNative owns this descriptor even when construction fails.
        consumed = i + 1;
        cols[i] = VeloxColumnVector.importFromNative(descs[i], fields[i].dataType());
      }
    } catch (RuntimeException | Error e) {
      // Close any already-imported columns (each frees its own descriptor tree on close).
      for (VeloxColumnVector col : cols) {
        if (col != null) {
          col.close();
        }
      }
      // The failed import released its own tree; only untouched descriptors remain caller-owned.
      freeUnimportedDescriptors(descs, consumed, VeloxColumnHandleJniWrapper::freeDescriptor);
      throw e;
    }
    return new VeloxInputBatch(cols, numRows);
  }

  /**
   * Frees every untouched native descriptor {@code descs[fromIndex..N-1]} via {@code freeFn}.
   * Successful imports are owned by their vectors; a failed import releases its own descriptor.
   * Only descriptors whose import was never attempted remain caller-owned after a failure.
   *
   * @param descs the full array of native descriptor addresses
   * @param fromIndex index of the first untouched descriptor (inclusive)
   * @param freeFn raw descriptor free (normally {@link
   *     VeloxColumnHandleJniWrapper#freeDescriptor(long)})
   */
  static void freeUnimportedDescriptors(long[] descs, int fromIndex, LongConsumer freeFn) {
    for (int j = fromIndex; j < descs.length; j++) {
      freeFn.accept(descs[j]);
    }
  }

  // ---------------------------------------------------------------------------
  // Row access
  // ---------------------------------------------------------------------------

  /**
   * Returns the null-safe {@link InternalRow} for row {@code i} (reused, with {@code rowId} set).
   *
   * <p>The returned row checks {@code isNullAt} before every getter, mirroring {@code
   * ArrowColumnarRow.get} null-check semantics (ArrowColumnarRow.scala:120-122). Do NOT use {@code
   * ColumnarBatch.getRow} with these vectors instead -- see class-level Javadoc for constraint C1.
   *
   * @param i zero-based row index
   * @return a reused {@link VeloxColumnarReadRow} positioned at row {@code i}
   */
  public InternalRow getRow(int i) {
    row.rowId = i;
    return row;
  }

  /** Returns the number of rows in this batch. */
  public int numRows() {
    return numRows;
  }

  // ---------------------------------------------------------------------------
  // Resource management
  // ---------------------------------------------------------------------------

  /**
   * Closes each {@link VeloxColumnVector}, freeing the per-column read descriptors.
   *
   * <p>After this call the batch must not be used.
   */
  @Override
  public void close() {
    for (VeloxColumnVector c : cols) {
      c.close();
    }
  }
}
