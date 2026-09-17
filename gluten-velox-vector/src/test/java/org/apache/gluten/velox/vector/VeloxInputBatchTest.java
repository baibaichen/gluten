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
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/** Unit tests for the {@link VeloxInputBatch#canImport} import gate (pure Java, no native). */
class VeloxInputBatchTest {

  @Test
  void canImportSupportedLeaves() {
    StructType s =
        new StructType()
            .add("a", DataTypes.IntegerType)
            .add("b", DataTypes.StringType)
            .add("null", DataTypes.NullType)
            .add("interval", DataTypes.CalendarIntervalType)
            .add("c", DataTypes.createArrayType(DataTypes.LongType));
    assertTrue(VeloxInputBatch.canImport(s));
  }

  @Test
  void rejectsUnsupportedLeaf() {
    StructType s = new StructType().add("a", DataTypes.TimestampNTZType);
    assertFalse(VeloxInputBatch.canImport(s));
  }

  @Test
  void rejectsUnsupportedInNested() {
    StructType s = new StructType().add("a", DataTypes.createArrayType(DataTypes.TimestampNTZType));
    assertFalse(VeloxInputBatch.canImport(s));
  }

  @Test
  void canImportNestedNullLeaves() {
    StructType s =
        new StructType()
            .add("array", DataTypes.createArrayType(DataTypes.NullType))
            .add("map", DataTypes.createMapType(DataTypes.StringType, DataTypes.NullType))
            .add("struct", new StructType().add("null", DataTypes.NullType));
    assertTrue(VeloxInputBatch.canImport(s));
  }

  @Test
  void canImportNestedCalendarIntervals() {
    StructType s =
        new StructType()
            .add("array", DataTypes.createArrayType(DataTypes.CalendarIntervalType))
            .add(
                "map",
                DataTypes.createMapType(DataTypes.StringType, DataTypes.CalendarIntervalType))
            .add("struct", new StructType().add("interval", DataTypes.CalendarIntervalType));
    assertTrue(VeloxInputBatch.canImport(s));
  }
}
