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

import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.unsafe.Platform;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * TDD tests for DATE (int32, days since epoch) zero-copy read via {@link VeloxColumnVector}.
 *
 * <p>Velox DATE columns are physically stored as int32 (days since epoch), identical in memory
 * layout to INT32. These tests verify that {@link VeloxColumnVector} routes {@link
 * org.apache.spark.sql.types.DateType} to the existing {@link FlatAccessor} and that {@link
 * VeloxInputBatch#canImport} admits schemas containing DateType leaves.
 *
 * <p>No native library is loaded. Descriptors are built in Java-allocated off-heap memory via
 * {@link Platform} (i.e. {@code sun.misc.Unsafe}). Memory is freed manually after each test rather
 * than via {@link VeloxColumnVector#close()} to avoid invoking the native {@link
 * VeloxColumnHandleJniWrapper#freeDescriptor} on Java-allocated addresses.
 */
public class VeloxColumnVectorDateTest {

  /**
   * Verifies that a flat DATE column (int32 values {0, 19000, -1}) can be read correctly as
   * days-since-epoch integers via {@link VeloxColumnVector#getInt}.
   *
   * <p>Layout:
   *
   * <ul>
   *   <li>3 int32 values: 0 (epoch), 19000 (~2022-01-13), -1 (one day before epoch)
   *   <li>No validity bitmap (nulls pointer = 0, nullCount = 0)
   * </ul>
   */
  @Test
  public void dateColumnReadsInt32DaysViaUnsafe() {
    int[] dates = {0, 19000, -1};
    long values = Platform.allocateMemory(dates.length * 4);
    for (int i = 0; i < dates.length; i++) {
      Platform.putInt(null, values + (long) i * 4, dates[i]);
    }

    long buffers = Platform.allocateMemory(2 * 8);
    Platform.putLong(null, buffers, 0L); // nulls = nullptr
    Platform.putLong(null, buffers + 8, values); // values

    long h = Platform.allocateMemory(48);
    Platform.putLong(null, h + 0, (long) dates.length); // length
    Platform.putLong(null, h + 8, 0L); // nullCount = 0
    Platform.putLong(null, h + 16, 2L); // nBuffers
    Platform.putLong(null, h + 24, 0L); // nChildren
    Platform.putLong(null, h + 32, buffers); // buffers ptr
    Platform.putLong(null, h + 40, 0L); // children ptr

    // importFromNative with DateType — must NOT throw UnsupportedOperationException
    VeloxColumnVector cv = VeloxColumnVector.importFromNative(h, DataTypes.DateType);

    assertEquals(0, cv.getInt(0));
    assertEquals(19000, cv.getInt(1));
    assertEquals(-1, cv.getInt(2));
    assertFalse(cv.hasNull());

    // Free manually — do NOT call cv.close() as that would try to native-free h.
    Platform.freeMemory(h);
    Platform.freeMemory(buffers);
    Platform.freeMemory(values);
  }

  /**
   * Verifies that {@link VeloxInputBatch#canImport} returns {@code true} for a schema containing a
   * single DateType leaf.
   */
  @Test
  public void canImportDateSchema() {
    StructType schema = new StructType().add("d", DataTypes.DateType);
    assertTrue(VeloxInputBatch.canImport(schema));
  }
}
