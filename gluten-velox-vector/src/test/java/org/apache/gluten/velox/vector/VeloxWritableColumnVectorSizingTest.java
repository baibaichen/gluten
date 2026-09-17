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

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Pure-arithmetic tests for null-bitmap sizing near {@link Integer#MAX_VALUE}, without allocating
 * memory.
 */
class VeloxWritableColumnVectorSizingTest {

  @Test
  void nullsByteSizeIsPositiveAndCorrectNearIntMax() {
    // Bug 6: with int arithmetic, (capacity + 7) overflows to a negative int within 7 of INT_MAX,
    // producing a bogus (often negative) validity-buffer size.
    int capacity = Integer.MAX_VALUE - 3;
    long bytes = VeloxWritableColumnVector.nullsByteSize(capacity);

    // 1 bit per row rounded up to whole bytes, then padded to a multiple of 8.
    long rawBytes = ((long) capacity + 7) / 8;
    long expected = (rawBytes + 7) & ~7L;
    assertEquals(expected, bytes);
    assertTrue(bytes > 0, "null-bitmap byte size must be positive");
    assertTrue(bytes >= rawBytes, "must cover 1 bit per row");
  }

  @Test
  void nullsByteSizeFloorAndSmallValues() {
    assertEquals(8L, VeloxWritableColumnVector.nullsByteSize(0));
    assertEquals(8L, VeloxWritableColumnVector.nullsByteSize(1));
    assertEquals(8L, VeloxWritableColumnVector.nullsByteSize(64)); // 8 bytes exactly
    assertEquals(16L, VeloxWritableColumnVector.nullsByteSize(65)); // 9 bytes -> pad to 16
  }
}
