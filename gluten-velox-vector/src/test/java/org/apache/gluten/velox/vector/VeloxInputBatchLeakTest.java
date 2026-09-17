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

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Unit test for the mid-loop descriptor-leak cleanup in {@link VeloxInputBatch#wrap} (Bug 2). Uses
 * a tracking free function so no native handles are required: the fake records which descriptor
 * addresses were freed.
 */
class VeloxInputBatchLeakTest {

  @Test
  void freesEveryUnimportedDescriptorFromFailureIndexOnward() {
    // Simulate a batch of 5 descriptors where import failed at index 2 (so 0,1 became columns and
    // are freed by their own close(); 2,3,4 were never imported and must be freed raw here).
    long[] descs = {10L, 11L, 12L, 13L, 14L};
    List<Long> freed = new ArrayList<>();

    VeloxInputBatch.freeUnimportedDescriptors(descs, 2, freed::add);

    assertEquals(List.of(12L, 13L, 14L), freed);
  }

  @Test
  void freesAllWhenFirstImportFails() {
    long[] descs = {10L, 11L, 12L};
    List<Long> freed = new ArrayList<>();

    VeloxInputBatch.freeUnimportedDescriptors(descs, 0, freed::add);

    assertEquals(List.of(10L, 11L, 12L), freed);
  }

  @Test
  void freesNothingWhenAllImported() {
    long[] descs = {10L, 11L, 12L};
    List<Long> freed = new ArrayList<>();

    // fromIndex == length: every descriptor became a column, nothing to free raw (no double-free).
    VeloxInputBatch.freeUnimportedDescriptors(descs, descs.length, freed::add);

    assertEquals(List.of(), freed);
  }
}
