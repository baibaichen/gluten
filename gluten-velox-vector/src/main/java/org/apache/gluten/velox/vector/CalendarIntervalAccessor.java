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
 * Reads a logical field of Velox CalendarInterval::pack() without copying its native data.
 * Little-endian int128 slots contain int32 months, int32 days, and int64 microseconds.
 */
final class CalendarIntervalAccessor extends FlatAccessor {
  private final int fieldOffset;

  CalendarIntervalAccessor(long nullsAddr, long valuesAddr, long nullCount, int fieldOffset) {
    super(nullsAddr, valuesAddr, nullCount);
    this.fieldOffset = fieldOffset;
  }

  @Override
  public int getInt(int rowId) {
    return Platform.getInt(null, address(rowId));
  }

  @Override
  public long getLong(int rowId) {
    return Platform.getLong(null, address(rowId));
  }

  private long address(int rowId) {
    if (valuesAddr == 0L) {
      throw new IllegalStateException("Non-null CalendarInterval has no values buffer");
    }
    return valuesAddr + (long) rowId * 16 + fieldOffset;
  }
}
