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
#include "vector/VeloxColumnHandle.h"
#include <gtest/gtest.h>
#include <jni.h>
#include <cstring>
#include <limits>
#include "utils/ObjectStore.h"
#include "vector/VeloxColumnHandleExport.h"
#include "velox/type/CalendarInterval.h"
#include "velox/type/StringView.h"
#include "velox/type/Timestamp.h"
#include "velox/vector/tests/utils/VectorTestBase.h"

using namespace gluten;
using namespace facebook::velox;

TEST(VeloxColumnHandleTest, LayoutIsFixed48Bytes) {
  EXPECT_EQ(sizeof(VeloxColumnHandle), 48u);
  EXPECT_EQ(offsetof(VeloxColumnHandle, length), 0u);
  EXPECT_EQ(offsetof(VeloxColumnHandle, nullCount), 8u);
  EXPECT_EQ(offsetof(VeloxColumnHandle, nBuffers), 16u);
  EXPECT_EQ(offsetof(VeloxColumnHandle, nChildren), 24u);
  EXPECT_EQ(offsetof(VeloxColumnHandle, buffers), 32u);
  EXPECT_EQ(offsetof(VeloxColumnHandle, children), 40u);
}

TEST(VeloxColumnHandleTest, AllocAndReleaseNoLeak) {
  auto* h = allocColumnHandle(/*nBuffers=*/2, /*nChildren=*/1);
  EXPECT_EQ(h->nBuffers, 2);
  EXPECT_EQ(h->nChildren, 1);
  ASSERT_NE(h->buffers, nullptr);
  ASSERT_NE(h->children, nullptr);
  freeColumnHandleShallow(h); // frees h + its buffers/children arrays (not child trees)
}

// ---- ExportTest: FLAT primitive leaf export ----

class ExportTest : public testing::Test, public test::VectorTestBase {
 protected:
  static void SetUpTestCase() {
    memory::MemoryManager::testingSetInstance(memory::MemoryManager::Options{});
  }
};

TEST_F(ExportTest, FlatIntLeaf) {
  auto vec = makeFlatVector<int32_t>({10, 20, 30, 40, 50});
  auto* h = exportVector(vec);
  EXPECT_EQ(h->length, 5);
  EXPECT_EQ(h->nBuffers, 2);
  EXPECT_EQ(h->nChildren, 0);
  EXPECT_EQ(h->buffers[0], nullptr); // no nulls -> nullptr
  ASSERT_NE(h->buffers[1], nullptr);
  auto* values = reinterpret_cast<const int32_t*>(h->buffers[1]);
  EXPECT_EQ(values[2], 30);
  freeColumnHandleShallow(h);
}

TEST_F(ExportTest, FlatIntWithNullsExposesBitmap) {
  auto vec = makeNullableFlatVector<int32_t>({10, std::nullopt, 30});
  auto* h = exportVector(vec);
  ASSERT_NE(h->buffers[0], nullptr); // nulls bitmap present
  freeColumnHandleShallow(h);
}

TEST_F(ExportTest, CalendarIntervalPackedLeafPreservesFieldsAndNulls) {
  const std::vector<CalendarInterval> expected = {
      {0, 0, 0},
      {14, 40, 5400000001LL},
      {-1, 5, -1000001},
      {std::numeric_limits<int32_t>::min(), std::numeric_limits<int32_t>::max(), std::numeric_limits<int64_t>::min()},
      {std::numeric_limits<int32_t>::max(), std::numeric_limits<int32_t>::min(), std::numeric_limits<int64_t>::max()}};
  std::vector<std::optional<int128_t>> packed;
  for (const auto& interval : expected) {
    packed.push_back(interval.pack());
  }
  packed.push_back(std::nullopt);
  auto vec = makeNullableFlatVector<int128_t>(packed, CALENDAR_INTERVAL());
  EXPECT_TRUE(classifyColumn(vec.get()));
  const auto baseline = veloxColumnHandleLiveCount().load(std::memory_order_relaxed);
  auto* h = exportVector(vec);
  EXPECT_EQ(h->length, packed.size());
  EXPECT_EQ(h->nBuffers, 2);
  EXPECT_EQ(h->nChildren, 0);
  EXPECT_EQ(h->buffers[0], vec->rawNulls());
  EXPECT_EQ(h->buffers[1], vec->rawValues());
  const auto* values = static_cast<const char*>(h->buffers[1]);
  for (size_t i = 0; i < expected.size(); ++i) {
    int32_t months;
    int32_t days;
    int64_t micros;
    std::memcpy(&months, values + i * 16, 4);
    std::memcpy(&days, values + i * 16 + 4, 4);
    std::memcpy(&micros, values + i * 16 + 8, 8);
    EXPECT_EQ(months, expected[i].months);
    EXPECT_EQ(days, expected[i].days);
    EXPECT_EQ(micros, expected[i].microseconds);
  }
  EXPECT_TRUE(bits::isBitNull(static_cast<const uint64_t*>(h->buffers[0]), expected.size()));
  freeColumnHandleTree(h);
  EXPECT_EQ(veloxColumnHandleLiveCount().load(std::memory_order_relaxed), baseline);
  EXPECT_TRUE(vec->type()->isCalendarInterval());
  EXPECT_EQ(CalendarInterval::unpack(vec->valueAt(2)), expected[2]);

  auto empty = BaseVector::create(CALENDAR_INTERVAL(), 0, pool());
  h = exportVector(empty);
  EXPECT_EQ(h->length, 0);
  EXPECT_EQ(h->nBuffers, 2);
  EXPECT_EQ(h->nChildren, 0);
  freeColumnHandleTree(h);
  EXPECT_EQ(veloxColumnHandleLiveCount().load(std::memory_order_relaxed), baseline);
}

TEST_F(ExportTest, NestedCalendarIntervalLeavesRemainBorrowed) {
  auto intervals = makeNullableFlatVector<int128_t>(
      {CalendarInterval(-14, 40, -86400000001LL).pack(), std::nullopt}, CALENDAR_INTERVAL());
  auto arrays = makeArrayVector({0, 2}, intervals);
  auto maps = makeMapVector({0, 2}, makeFlatVector<int32_t>({1, 2}), intervals);
  auto row = makeRowVector({intervals, arrays, maps});
  EXPECT_TRUE(classifyColumn(row.get()));
  const auto baseline = veloxColumnHandleLiveCount().load(std::memory_order_relaxed);
  auto* h = exportVector(row);
  for (auto* leaf : {h->children[0], h->children[1]->children[0], h->children[2]->children[1]}) {
    EXPECT_EQ(leaf->nBuffers, 2);
    EXPECT_EQ(leaf->nChildren, 0);
    EXPECT_EQ(leaf->buffers[1], intervals->rawValues());
    EXPECT_TRUE(bits::isBitNull(static_cast<const uint64_t*>(leaf->buffers[0]), 1));
  }
  EXPECT_EQ(static_cast<const int32_t*>(h->children[1]->buffers[2])[1], 0);
  freeColumnHandleTree(h);
  EXPECT_EQ(veloxColumnHandleLiveCount().load(std::memory_order_relaxed), baseline);
  EXPECT_EQ(CalendarInterval::unpack(intervals->valueAt(0)), CalendarInterval(-14, 40, -86400000001LL));
}

TEST_F(ExportTest, FlatUnknownLeafHasOnlyNulls) {
  for (vector_size_t size : {0, 1, 9}) {
    SCOPED_TRACE(size);
    auto vec = BaseVector::create(UNKNOWN(), size, pool());
    EXPECT_TRUE(classifyColumn(vec.get()));
    const auto baseline = veloxColumnHandleLiveCount().load(std::memory_order_relaxed);
    auto* h = exportVector(vec);
    EXPECT_EQ(h->length, size);
    EXPECT_EQ(h->nullCount, size);
    EXPECT_EQ(h->nBuffers, 2);
    EXPECT_EQ(h->nChildren, 0);
    EXPECT_EQ(h->buffers[0], vec->rawNulls());
    EXPECT_EQ(h->buffers[1], nullptr);
    for (vector_size_t i = 0; i < size; ++i) {
      EXPECT_TRUE(bits::isBitNull(static_cast<const uint64_t*>(h->buffers[0]), i));
    }
    freeColumnHandleTree(h);
    EXPECT_EQ(veloxColumnHandleLiveCount().load(std::memory_order_relaxed), baseline);
    EXPECT_EQ(BaseVector::countNulls(vec->nulls(), size), size);
  }
}

TEST_F(ExportTest, NestedUnknownLeavesAndEmptyArrays) {
  auto unknowns = BaseVector::create(UNKNOWN(), 3, pool());
  auto arrays = makeArrayVector({0, 2, 2}, unknowns); // [null,null], [], [null]
  auto map = makeMapVector({0, 2, 2}, makeFlatVector<int32_t>({1, 2, 3}), unknowns);
  auto row = makeRowVector({unknowns, arrays, map});
  EXPECT_TRUE(classifyColumn(row.get()));
  const auto baseline = veloxColumnHandleLiveCount().load(std::memory_order_relaxed);
  auto* h = exportVector(row);
  for (auto* leaf : {h->children[0], h->children[1]->children[0], h->children[2]->children[1]}) {
    EXPECT_EQ(leaf->length, 3);
    EXPECT_EQ(leaf->nullCount, 3);
    EXPECT_EQ(leaf->nBuffers, 2);
    EXPECT_EQ(leaf->buffers[0], unknowns->rawNulls());
    EXPECT_EQ(leaf->buffers[1], nullptr);
  }
  EXPECT_EQ(static_cast<const int32_t*>(h->children[1]->buffers[2])[1], 0);
  freeColumnHandleTree(h);
  EXPECT_EQ(veloxColumnHandleLiveCount().load(std::memory_order_relaxed), baseline);

  auto nested = makeArrayVector({0, 2}, arrays);
  EXPECT_TRUE(classifyColumn(nested.get()));
  h = exportVector(nested);
  EXPECT_EQ(h->children[0]->children[0]->nullCount, 3);
  EXPECT_EQ(h->children[0]->children[0]->buffers[1], nullptr);
  EXPECT_EQ(static_cast<const int32_t*>(h->children[0]->buffers[2])[1], 0);
  freeColumnHandleTree(h);
  EXPECT_EQ(veloxColumnHandleLiveCount().load(std::memory_order_relaxed), baseline);

  auto empty = makeArrayVector({0, 0}, BaseVector::create(UNKNOWN(), 0, pool()));
  h = exportVector(empty);
  EXPECT_EQ(h->children[0]->length, 0);
  EXPECT_EQ(h->children[0]->nullCount, 0);
  EXPECT_EQ(h->children[0]->buffers[1], nullptr);
  EXPECT_EQ(static_cast<const int32_t*>(h->buffers[2])[0], 0);
  EXPECT_EQ(static_cast<const int32_t*>(h->buffers[2])[1], 0);
  freeColumnHandleTree(h);
  EXPECT_EQ(veloxColumnHandleLiveCount().load(std::memory_order_relaxed), baseline);
}

TEST_F(ExportTest, NonNullUnknownFailsWithoutLeaking) {
  auto invalid = BaseVector::create(UNKNOWN(), 3, pool());
  invalid->setNull(1, false);
  const auto baseline = veloxColumnHandleLiveCount().load(std::memory_order_relaxed);
  EXPECT_THROW(exportVector(invalid), VeloxException);
  auto row = makeRowVector({makeFlatVector<int32_t>({1, 2, 3}), invalid});
  EXPECT_THROW(exportVector(row), VeloxException);
  EXPECT_EQ(veloxColumnHandleLiveCount().load(std::memory_order_relaxed), baseline);
}

TEST_F(ExportTest, ArrayOfInt) {
  // row0=[10,20], row1=NULL, row2=[30,40,50]
  auto elements = makeFlatVector<int32_t>({10, 20, 30, 40, 50});
  auto arr = makeArrayVector({0, 2, 2}, elements, /*nullRows=*/{1});
  auto* h = exportVector(arr);
  EXPECT_EQ(h->length, 3);
  EXPECT_EQ(h->nBuffers, 3);
  EXPECT_EQ(h->nChildren, 1);
  auto* offsets = reinterpret_cast<const int32_t*>(h->buffers[1]);
  auto* sizes = reinterpret_cast<const int32_t*>(h->buffers[2]);
  EXPECT_EQ(offsets[2], 2);
  EXPECT_EQ(sizes[2], 3); // read directly from rawSizes, not offset subtraction
  auto* child = h->children[0];
  EXPECT_EQ(child->length, 5);
  auto* vals = reinterpret_cast<const int32_t*>(child->buffers[1]);
  EXPECT_EQ(vals[2], 30);
  freeColumnHandleTree(h); // recursively frees child descriptors then h
}

TEST_F(ExportTest, RowStruct) {
  auto c0 = makeFlatVector<int32_t>({1, 2, 3});
  auto c1 = makeFlatVector<int64_t>({10, 20, 30});
  auto row = makeRowVector({c0, c1});
  auto* h = exportVector(row);
  EXPECT_EQ(h->nBuffers, 1);
  EXPECT_EQ(h->nChildren, 2);
  EXPECT_EQ(h->children[0]->length, 3);
  EXPECT_EQ(h->children[1]->length, 3);
  freeColumnHandleTree(h);
}

TEST_F(ExportTest, MapIntInt) {
  auto keys = makeFlatVector<int32_t>({1, 2, 3});
  auto vals = makeFlatVector<int32_t>({10, 20, 30});
  auto map = makeMapVector({0, 2}, keys, vals); // 2 rows: row0=[1->10,2->20], row1=[3->30]
  auto* h = exportVector(map);
  EXPECT_EQ(h->nBuffers, 3);
  EXPECT_EQ(h->nChildren, 2);
  EXPECT_EQ(h->length, 2); // map has 2 rows
  EXPECT_EQ(h->children[0]->length, 3); // keys vector has 3 entries
  EXPECT_EQ(h->children[1]->length, 3); // values vector has 3 entries
  freeColumnHandleTree(h);
}

TEST_F(ExportTest, StringViewLeaf) {
  // Build a flat VARCHAR vector with two inline strings and one out-of-line string.
  // StringView stores strings <= 12 bytes inline; longer strings use an external pointer.
  auto vec = makeFlatVector<StringView>({"hi"_sv, "world"_sv, "this is a long string"_sv});
  auto* h = exportVector(vec);
  EXPECT_EQ(h->length, 3);
  EXPECT_EQ(h->nBuffers, 2);
  EXPECT_EQ(h->nChildren, 0);
  // buffers[1] is the raw StringView array base (16 bytes per entry).
  ASSERT_NE(h->buffers[1], nullptr);
  auto* svs = reinterpret_cast<const StringView*>(h->buffers[1]);
  EXPECT_EQ(svs[0].size(), 2u); // "hi" has 2 chars
  EXPECT_TRUE(svs[0].isInline()); // short string — inline
  EXPECT_FALSE(svs[2].isInline()); // "this is a long string" (21 chars) — out-of-line pointer
  freeColumnHandleTree(h); // frees only descriptor memory, not vector data
}

TEST_F(ExportTest, FreeDescriptorTreeNoLeakNoDoubleFree) {
  // Build ARRAY<INT>; arr (VectorPtr) owns the vector tree.
  auto elements = makeFlatVector<int32_t>({10, 20, 30, 40, 50});
  auto arr = makeArrayVector({0, 2, 2}, elements, /*nullRows=*/{1});
  auto* root = exportVector(arr);
  // freeColumnHandleTree frees ONLY descriptor memory recursively; must NOT touch the vector.
  freeColumnHandleTree(root);
  // arr still valid — vector not touched by descriptor free.
  EXPECT_EQ(arr->size(), 3);
}

// ---- classifyColumn: 4 TDD tests added in Task 10 ----

TEST_F(ExportTest, DictionaryColumnNotClassifiable) {
  // A DICTIONARY-encoded vector is not exportable by exportVector; classifyColumn returns false.
  auto base = makeFlatVector<int32_t>({1, 2, 3});
  auto indices = makeIndices({0, 1, 2});
  auto dict = wrapInDictionary(indices, 3, base);
  EXPECT_FALSE(classifyColumn(dict.get()));
}

TEST_F(ExportTest, NestedFlatArrayClassifiable) {
  // ARRAY<INT> where elements are FLAT: fully exportable, classifyColumn returns true.
  auto elements = makeFlatVector<int32_t>({10, 20, 30, 40, 50});
  auto arr = makeArrayVector({0, 2, 2}, elements, /*nullRows=*/{1});
  EXPECT_TRUE(classifyColumn(arr.get()));
}

TEST_F(ExportTest, BooleanLeafClassifiable) {
  // FLAT BOOLEAN is now exportable (bit-packed values buffer); classifyColumn returns true.
  auto vec = makeFlatVector<bool>({true, false, true});
  EXPECT_TRUE(classifyColumn(vec.get()));
}

TEST_F(ExportTest, FlatBooleanLeafBitPacked) {
  // Build a FLAT BOOLEAN vector spanning byte boundaries: rows 0,7,8,9,15 true, others false,
  // with one null (row 3). Export it and read buffers[1] as a bit-packed values buffer:
  // bit r lives at byte (r>>3), bit position (r&7), LSB-first -- matching Java FlatAccessor.
  // Pre-fix RED: rawValuesAsVoid throws VELOX_UNSUPPORTED for BOOLEAN.
  std::vector<std::optional<bool>> input(16, false);
  input[0] = true;
  input[7] = true;
  input[8] = true;
  input[9] = true;
  input[15] = true;
  input[3] = std::nullopt; // null row
  auto vec = makeNullableFlatVector<bool>(input);

  auto* h = exportVector(vec);
  EXPECT_EQ(h->length, 16);
  EXPECT_EQ(h->nBuffers, 2);
  EXPECT_EQ(h->nChildren, 0);
  ASSERT_NE(h->buffers[0], nullptr); // nulls bitmap present (row 3 null)
  ASSERT_NE(h->buffers[1], nullptr); // bit-packed values buffer

  const auto* valueBytes = reinterpret_cast<const uint8_t*>(h->buffers[1]);
  auto readBit = [&](int r) -> bool { return (valueBytes[r >> 3] & (1 << (r & 7))) != 0; };
  for (int r = 0; r < 16; ++r) {
    if (r == 3) {
      continue; // null row: value undefined
    }
    EXPECT_EQ(readBit(r), input[r].value()) << "mismatch at row " << r;
  }

  EXPECT_TRUE(classifyColumn(vec.get())); // BOOLEAN leaf is now classifiable/exportable
  freeColumnHandleShallow(h);
}

TEST_F(ExportTest, DictChildMakesWholeArrayUnclassifiable) {
  // ARRAY whose elements child is DICTIONARY-encoded: the whole column is not exportable.
  auto base = makeFlatVector<int32_t>({10, 20, 30, 40, 50});
  auto indices = makeIndices({0, 1, 2, 3, 4});
  auto dictElements = wrapInDictionary(indices, 5, base);
  // makeArrayVector accepts any VectorPtr as elements, including DICTIONARY-encoded.
  auto arr = makeArrayVector({0, 2}, dictElements, /*nullRows=*/{});
  EXPECT_FALSE(classifyColumn(arr.get()));
}

// ---- ExportLeakSafety: exception-safety / no descriptor node leaks (#6) ----
//
// Fix #6 adds try/catch around the child-export recursion in the ARRAY/ROW/MAP
// branches of exportVector, calling freeColumnHandleTree(parent) before rethrowing
// so no parent descriptor node leaks when a child export throws.
//
// We must use children whose encoding hits the `default:` VELOX_UNSUPPORTED case
// in exportVector's encoding switch — this throws WITHOUT calling allocColumnHandle,
// so the only node that can leak is the parent.  DICTIONARY encoding is ideal:
// it is a valid Velox encoding that exportVector rejects at the switch level.
//
// NOTE: we do NOT use FLAT BOOLEAN children because exportFlat() allocates its
// own descriptor node *before* calling rawValuesAsVoid() which throws — that
// is a separate pre-existing issue in exportFlat that fix #6 does not address.
//
// TDD contract:
//   GREEN  on current code  (try/catch + freeColumnHandleTree in ARRAY/ROW/MAP)
//   RED    on pre-fix code  (028b0fed — no try/catch, parent node leaks after throw)
//
// Leak detection uses veloxColumnHandleLiveCount() — the header-only atomic seam
// added alongside these tests (incremented in allocColumnHandle, decremented in
// freeColumnHandleShallow).  Using difference-from-baseline avoids interference
// from parallel tests or other in-flight allocations.

TEST_F(ExportTest, ExportLeakSafety_ArrayOfDictionary) {
  // ARRAY<DICT(INT)>: the ARRAY branch allocates a parent node (nBuffers=3,
  // nChildren=1), then calls exportVector(elements) on a DICTIONARY-encoded child.
  // The child hits exportVector's `default:` case and throws VELOX_UNSUPPORTED
  // WITHOUT allocating any descriptor node.
  // Fixed  → catch frees parent → count == baseline (no leak).
  // Pre-fix → parent leaks     → count == baseline + 1.
  auto base = makeFlatVector<int32_t>({1, 2, 3});
  auto indices = makeIndices({0, 1, 2});
  auto dictElements = wrapInDictionary(indices, 3, base);
  // makeArrayVector accepts any VectorPtr (including DICTIONARY-encoded) as elements.
  auto arr = makeArrayVector({0, 2}, dictElements, /*nullRows=*/{});

  int64_t baseline = veloxColumnHandleLiveCount().load(std::memory_order_relaxed);
  EXPECT_THROW(exportVector(arr), facebook::velox::VeloxException);
  int64_t after = veloxColumnHandleLiveCount().load(std::memory_order_relaxed);
  EXPECT_EQ(after, baseline) << "Descriptor node leaked after ARRAY<DICT> export exception "
                             << "(count delta = " << (after - baseline) << ")";
}

TEST_F(ExportTest, ExportLeakSafety_RowOfDictionary) {
  // ROW<DICT(INT)>: single-field struct whose child is DICTIONARY-encoded.
  // The ROW branch allocates a parent node (nBuffers=1, nChildren=1), then iterates
  // children; the DICTIONARY child throws WITHOUT allocating any node.
  // Fixed  → catch frees parent → count == baseline.
  // Pre-fix → parent leaks     → count == baseline + 1.
  auto base = makeFlatVector<int32_t>({10, 20, 30});
  auto indices = makeIndices({0, 1, 2});
  auto dictCol = wrapInDictionary(indices, 3, base);
  auto row = makeRowVector({dictCol});

  int64_t baseline = veloxColumnHandleLiveCount().load(std::memory_order_relaxed);
  EXPECT_THROW(exportVector(row), facebook::velox::VeloxException);
  int64_t after = veloxColumnHandleLiveCount().load(std::memory_order_relaxed);
  EXPECT_EQ(after, baseline) << "Descriptor node leaked after ROW<DICT> export exception "
                             << "(count delta = " << (after - baseline) << ")";
}

TEST_F(ExportTest, ExportLeakSafety_MapWithDictionaryValue) {
  // MAP<INT, DICT(INT)>: keys are FLAT INTEGER (child[0] exports successfully),
  // values are DICTIONARY-encoded (child[1] throws without allocating a node).
  // The MAP branch allocates a parent node (nBuffers=3, nChildren=2) and then the
  // successful keys export allocates one more node before the values throw.
  // Fixed  → catch calls freeColumnHandleTree(parent) which recursively frees the
  //           already-attached keys child and then the parent → count == baseline.
  // Pre-fix → parent + keys child both leak → count == baseline + 2.
  auto keys = makeFlatVector<int32_t>({1, 2, 3});
  auto vbase = makeFlatVector<int32_t>({10, 20, 30});
  auto vindices = makeIndices({0, 1, 2});
  auto dictVals = wrapInDictionary(vindices, 3, vbase);
  auto map = makeMapVector({0, 2}, keys, dictVals); // 2 rows

  int64_t baseline = veloxColumnHandleLiveCount().load(std::memory_order_relaxed);
  EXPECT_THROW(exportVector(map), facebook::velox::VeloxException);
  int64_t after = veloxColumnHandleLiveCount().load(std::memory_order_relaxed);
  EXPECT_EQ(after, baseline) << "Descriptor node(s) leaked after MAP<INT,DICT> export exception "
                             << "(count delta = " << (after - baseline) << ")";
}

// ---- ExportLeakSafety: exportFlat exception-safety / no descriptor node leaks (#9) ----
//
// Bug #9: exportFlat() calls allocColumnHandle() before rawValuesAsVoid(), which throws
// VELOX_UNSUPPORTED for unsupported TypeKinds (TIMESTAMP, DECIMAL, etc.).  When
// rawValuesAsVoid() throws, the already-allocated descriptor node leaks because nothing
// frees it on the exception path.
//
// Fix: resolve rawValuesAsVoid() BEFORE allocColumnHandle() so that if it throws, no
// descriptor node has yet been allocated -- exportFlat remains exception-safe.
//
// NOTE: BOOLEAN and TIMESTAMP are no longer usable here -- both are now SUPPORTED read-export
// kinds in rawValuesAsVoid (BOOLEAN bit-packed; TIMESTAMP added by a later Wave-3 commit). We use
// a non-decimal FLAT HUGEINT vector, which hits rawValuesAsVoid's `default:` VELOX_UNSUPPORTED
// throw, to keep exercising the pre-alloc throw path.
//
// TDD contract:
//   RED  on pre-fix exportFlat  -> liveCount delta == 1 (node allocated, then leaked)
//   GREEN on fixed  exportFlat  -> liveCount delta == 0 (no allocation before the throw)

TEST_F(ExportTest, ExportLeakSafety_FlatUnsupportedNoLeak) {
  // Non-decimal FLAT HUGEINT is excluded by rawValuesAsVoid() via VELOX_UNSUPPORTED.
  // exportFlat() is called directly from exportVector()'s FLAT branch.
  // Pre-fix: allocColumnHandle() runs first -> node leaks on the exception path (delta=1).
  // Fixed:   rawValuesAsVoid() throws before allocColumnHandle() runs -> delta=0.
  auto vec = BaseVector::create(HUGEINT(), 4, pool());
  EXPECT_FALSE(classifyColumn(vec.get()));

  int64_t baseline = veloxColumnHandleLiveCount().load(std::memory_order_relaxed);
  EXPECT_THROW(exportVector(vec), facebook::velox::VeloxException);
  int64_t after = veloxColumnHandleLiveCount().load(std::memory_order_relaxed);
  EXPECT_EQ(after, baseline) << "Descriptor node leaked in exportFlat for FLAT HUGEINT "
                             << "(count delta = " << (after - baseline) << "); "
                             << "rawValuesAsVoid() must be called before allocColumnHandle()";
}

TEST_F(ExportTest, AllocateOutputIntRoundTrip) {
  // Allocate a writable INTEGER vector with 4 rows.
  auto [desc, ownerHandle] = gluten::allocateNestedOutput(facebook::velox::INTEGER(), 4);

  ASSERT_NE(desc, nullptr);
  EXPECT_EQ(desc->length, 4);
  EXPECT_EQ(desc->nBuffers, 2);
  EXPECT_EQ(desc->nChildren, 0);

  // buffers[1] = values (writable via const_cast — we own the vector).
  ASSERT_NE(desc->buffers[1], nullptr);
  auto* vals = const_cast<int32_t*>(reinterpret_cast<const int32_t*>(desc->buffers[1]));
  vals[0] = 100;
  vals[1] = 200;
  vals[2] = 300;
  vals[3] = 400;
  EXPECT_EQ(vals[0], 100);
  EXPECT_EQ(vals[1], 200);
  EXPECT_EQ(vals[2], 300);
  EXPECT_EQ(vals[3], 400);

  // buffers[0] = nulls; all rows initialised to valid (each byte == 0xFF).
  ASSERT_NE(desc->buffers[0], nullptr);
  const auto* nullBytes = reinterpret_cast<const uint8_t*>(desc->buffers[0]);
  // 4 rows fit in the first byte; all 4 bits should be set.
  EXPECT_EQ(nullBytes[0], 0xFF);

  // Free the descriptor (shallow — does not touch vector data owned by the ObjectStore).
  freeColumnHandleShallow(desc);

  // Releasing the ObjectStore entry must not crash (vector and buffers are freed here).
  gluten::ObjectStore::release(ownerHandle);
}

TEST_F(ExportTest, AllocateArrayIntOutputRoundTrip) {
  // Allocate a writable ARRAY<INT> vector: 2 rows, 5 total element slots.
  // row0 = [10, 20]   → offset=0, size=2
  // row1 = [30, 40, 50] → offset=2, size=3
  auto [desc, ownerHandle] = gluten::allocateNestedOutput(ARRAY(INTEGER()), 2);
  gluten::growChild(ownerHandle, {0}, 5);
  freeColumnHandleTree(desc);
  desc = exportVector(*gluten::ObjectStore::retrieve<VectorPtr>(ownerHandle));

  ASSERT_NE(desc, nullptr);
  EXPECT_EQ(desc->length, 2); // 2 rows
  EXPECT_EQ(desc->nBuffers, 3); // nulls, offsets, sizes
  EXPECT_EQ(desc->nChildren, 1);

  // Root nulls: both rows valid → bits 0 and 1 must be set.
  ASSERT_NE(desc->buffers[0], nullptr);
  const auto* rootNullBytes = reinterpret_cast<const uint8_t*>(desc->buffers[0]);
  EXPECT_EQ(rootNullBytes[0] & 0x03u, 0x03u);

  // Write offsets and sizes for row0 and row1.
  ASSERT_NE(desc->buffers[1], nullptr);
  ASSERT_NE(desc->buffers[2], nullptr);
  auto* offsets = const_cast<int32_t*>(reinterpret_cast<const int32_t*>(desc->buffers[1]));
  auto* sizes = const_cast<int32_t*>(reinterpret_cast<const int32_t*>(desc->buffers[2]));
  offsets[0] = 0;
  sizes[0] = 2;
  offsets[1] = 2;
  sizes[1] = 3;

  // Write element ints via the child descriptor.
  ASSERT_NE(desc->children[0], nullptr);
  auto* childDesc = desc->children[0];
  EXPECT_EQ(childDesc->length, 5);
  EXPECT_EQ(childDesc->nBuffers, 2);
  EXPECT_EQ(childDesc->nChildren, 0);

  // Child nulls: all 5 elements valid.
  ASSERT_NE(childDesc->buffers[0], nullptr);
  const auto* childNullBytes = reinterpret_cast<const uint8_t*>(childDesc->buffers[0]);
  EXPECT_EQ(childNullBytes[0] & 0x1Fu, 0x1Fu); // bits 0-4 set

  ASSERT_NE(childDesc->buffers[1], nullptr);
  auto* childVals = const_cast<int32_t*>(reinterpret_cast<const int32_t*>(childDesc->buffers[1]));
  childVals[0] = 10;
  childVals[1] = 20;
  childVals[2] = 30;
  childVals[3] = 40;
  childVals[4] = 50;

  // Read back and verify.
  EXPECT_EQ(offsets[0], 0);
  EXPECT_EQ(sizes[0], 2);
  EXPECT_EQ(offsets[1], 2);
  EXPECT_EQ(sizes[1], 3);
  EXPECT_EQ(childVals[0], 10);
  EXPECT_EQ(childVals[1], 20);
  EXPECT_EQ(childVals[2], 30);
  EXPECT_EQ(childVals[3], 40);
  EXPECT_EQ(childVals[4], 50);

  // Free descriptor tree and release ObjectStore entry.
  freeColumnHandleTree(desc);
  gluten::ObjectStore::release(ownerHandle);
}

// ---- NestedOutputTest: allocateNestedOutput (P4 Task 1) ----
//
// Tests that allocateNestedOutput recursively builds the correct Velox vector tree
// and exports a matching writable descriptor tree for arbitrary nested types.
//
// TDD contract (RED → GREEN):
//   RED  on pre-implementation code  → allocateNestedOutput undeclared.
//   GREEN on implementation          → all assertions pass.
//
// C1 unwrap: retrieve<VectorPtr> returns shared_ptr<VectorPtr>;
//   dereference once to get VectorPtr.
//
// These tests run under ExportTest (same fixture) to share the already-initialized
// MemoryManager and outputPool() static. A separate fixture with its own
// testingSetInstance call would destroy the existing MemoryManager, invalidating
// the process-static pool references held by outputPool().

TEST_F(ExportTest, AllocateNestedArrayVarchar) {
  // Allocate a writable ARRAY<VARCHAR> tree with 4 rows.
  auto [desc, owner] = gluten::allocateNestedOutput(ARRAY(VARCHAR()), 4);

  // C1 unwrap: double-wrapped owner → retrieve<VectorPtr> + dereference.
  facebook::velox::VectorPtr v = *gluten::ObjectStore::retrieve<VectorPtr>(static_cast<gluten::ObjectHandle>(owner));

  // Root vector must be an ARRAY.
  ASSERT_EQ(v->typeKind(), TypeKind::ARRAY);

  // Root descriptor: nBuffers=3 (nulls/offsets/sizes), nChildren=1 (elements).
  EXPECT_EQ(desc->nBuffers, 3);
  EXPECT_EQ(desc->nChildren, 1);
  EXPECT_EQ(desc->length, 4);
  ASSERT_NE(desc->buffers[0], nullptr); // nulls pre-allocated
  ASSERT_NE(desc->buffers[1], nullptr); // offsets
  ASSERT_NE(desc->buffers[2], nullptr); // sizes

  // Elements child: VARCHAR leaf — nBuffers=2, nChildren=0.
  ASSERT_NE(desc->children[0], nullptr);
  EXPECT_EQ(desc->children[0]->nChildren, 0);
  EXPECT_EQ(desc->children[0]->nBuffers, 2);

  // The underlying elements vector has typeKind VARCHAR.
  EXPECT_EQ(v->asUnchecked<ArrayVector>()->elements()->typeKind(), TypeKind::VARCHAR);

  gluten::ObjectStore::release(static_cast<gluten::ObjectHandle>(owner));
  gluten::freeColumnHandleTree(desc);
}

TEST_F(ExportTest, AllocateNestedRowIntArrayInt) {
  // Allocate a writable ROW(INT, ARRAY(INT)) tree with 3 rows.
  // This exercises multi-child (ROW) and nested recursion (ARRAY inside ROW).
  auto rowType = ROW({"col0", "col1"}, {INTEGER(), ARRAY(INTEGER())});
  auto [desc, owner] = gluten::allocateNestedOutput(rowType, 3);

  // C1 unwrap.
  facebook::velox::VectorPtr v = *gluten::ObjectStore::retrieve<VectorPtr>(static_cast<gluten::ObjectHandle>(owner));

  // Root vector must be a ROW.
  ASSERT_EQ(v->typeKind(), TypeKind::ROW);

  // Root descriptor: nBuffers=1 (nulls), nChildren=2 (col0, col1).
  EXPECT_EQ(desc->nBuffers, 1);
  EXPECT_EQ(desc->nChildren, 2);
  EXPECT_EQ(desc->length, 3);
  ASSERT_NE(desc->buffers[0], nullptr); // root nulls pre-allocated

  // col0: INT leaf — nBuffers=2, nChildren=0.
  ASSERT_NE(desc->children[0], nullptr);
  EXPECT_EQ(desc->children[0]->nBuffers, 2);
  EXPECT_EQ(desc->children[0]->nChildren, 0);
  ASSERT_NE(desc->children[0]->buffers[0], nullptr); // nulls
  ASSERT_NE(desc->children[0]->buffers[1], nullptr); // values

  // col1: ARRAY(INT) — nBuffers=3, nChildren=1.
  ASSERT_NE(desc->children[1], nullptr);
  EXPECT_EQ(desc->children[1]->nBuffers, 3);
  EXPECT_EQ(desc->children[1]->nChildren, 1);
  ASSERT_NE(desc->children[1]->buffers[0], nullptr); // nulls
  ASSERT_NE(desc->children[1]->buffers[1], nullptr); // offsets
  ASSERT_NE(desc->children[1]->buffers[2], nullptr); // sizes

  // col1's elements child: INT leaf — nBuffers=2, nChildren=0.
  ASSERT_NE(desc->children[1]->children[0], nullptr);
  EXPECT_EQ(desc->children[1]->children[0]->nChildren, 0);
  EXPECT_EQ(desc->children[1]->children[0]->nBuffers, 2);

  // Cross-check underlying RowVector children.
  auto* rowVec = v->asUnchecked<RowVector>();
  EXPECT_EQ(rowVec->childAt(0)->typeKind(), TypeKind::INTEGER);
  EXPECT_EQ(rowVec->childAt(1)->typeKind(), TypeKind::ARRAY);
  EXPECT_EQ(rowVec->childAt(1)->asUnchecked<ArrayVector>()->elements()->typeKind(), TypeKind::INTEGER);

  gluten::ObjectStore::release(static_cast<gluten::ObjectHandle>(owner));
  gluten::freeColumnHandleTree(desc);
}

TEST_F(ExportTest, AllocateVarcharAndChunk) {
  // Step 1: allocate a writable VARCHAR output vector with 4 rows.
  auto [desc, owner] = gluten::allocateNestedOutput(VARCHAR(), 4);

  ASSERT_NE(desc, nullptr);
  EXPECT_EQ(desc->length, 4);
  EXPECT_EQ(desc->nBuffers, 2);
  EXPECT_EQ(desc->nChildren, 0);
  // buffers[1] is the raw StringView slot array base (16 bytes per entry).
  ASSERT_NE(desc->buffers[1], nullptr);

  // Step 2: C1 unwrap — retrieve<VectorPtr> returns shared_ptr<VectorPtr>; dereference.
  auto ownerPtr = gluten::ObjectStore::retrieve<facebook::velox::VectorPtr>(static_cast<gluten::ObjectHandle>(owner));
  ASSERT_NE(ownerPtr, nullptr);
  facebook::velox::VectorPtr v = *ownerPtr;
  ASSERT_EQ(v->typeKind(), TypeKind::VARCHAR);

  // Step 3: allocate a string chunk — must return a non-null address and
  // a capacity >= the requested minBytes.
  auto [addr, cap] = gluten::allocateStringChunk(owner, 100);
  EXPECT_NE(addr, 0);
  EXPECT_GE(cap, 100);

  // Step 4: finalize — one chunk registered, used 0 bytes.
  gluten::finalizeStringColumn(owner, {0});

  // Step 5: release resources (no crash = correct ownership/no double-free).
  gluten::ObjectStore::release(static_cast<gluten::ObjectHandle>(owner));
  gluten::freeColumnHandleTree(desc);
}

// ============================================================
// Bug 1 regression: NewLongArray-returns-null (JVM OOM) on flat allocation must
//   (a) never call SetLongArrayRegion on a null array (UB), (b) not leak the saved
//   descriptor tree / ObjectStore vector, and (c) return nullptr safely.
//
// This test lives in the ExportTest fixture (not the JNI guard fixture) on purpose:
// allocateNestedOutput() allocates from the process-static outputPool(), whose pool
// references are bound to the MemoryManager created by ExportTest::SetUpTestCase.
// A fixture that calls testingSetInstance() again would destroy that MemoryManager
// and dangle the cached pool, so the real allocation path must be exercised here.
//
// A minimal mock JNIEnv forces NewLongArray -> nullptr. On the pre-fix code the
// function blindly called SetLongArrayRegion on the null array (mkSetLongArrayRegion
// asserts arr != null => RED) and leaked the descriptor tree (live count grows => RED).
// ============================================================
extern "C" JNIEXPORT jlongArray JNICALL
Java_org_apache_gluten_velox_vector_VeloxColumnHandleJniWrapper_allocateNestedOutput(
    JNIEnv*,
    jclass,
    jintArray typeEncoding,
    jint rowCapacity);

namespace {
struct Bug1MockState {
  bool forceNewLongArrayNull{false};
};
Bug1MockState g_bug1Mock;
jlong g_bug1FakeArrayStore[8];

jsize JNICALL bug1GetArrayLength(JNIEnv*, jarray) {
  return 1;
}

void JNICALL bug1GetIntArrayRegion(JNIEnv*, jintArray arr, jsize start, jsize len, jint* buf) {
  std::memcpy(buf, reinterpret_cast<const jint*>(arr) + start, len * sizeof(jint));
}

jlongArray JNICALL bug1NewLongArray(JNIEnv*, jsize) {
  if (g_bug1Mock.forceNewLongArrayNull) {
    return nullptr; // simulate JVM OOM
  }
  return reinterpret_cast<jlongArray>(&g_bug1FakeArrayStore[0]);
}

void JNICALL bug1SetLongArrayRegion(JNIEnv*, jlongArray arr, jsize start, jsize len, const jlong* buf) {
  ASSERT_NE(arr, nullptr) << "SetLongArrayRegion must never be called on a null array";
  for (jsize i = 0; i < len && (start + i) < 8; ++i) {
    reinterpret_cast<jlong*>(arr)[start + i] = buf[i];
  }
}

JNIEnv* bug1MockEnv() {
  static JNINativeInterface_ funcs;
  static JNIEnv_ envObj;
  static bool ready = false;
  if (!ready) {
    std::memset(&funcs, 0, sizeof(funcs));
    funcs.GetArrayLength = bug1GetArrayLength;
    funcs.GetIntArrayRegion = bug1GetIntArrayRegion;
    funcs.NewLongArray = bug1NewLongArray;
    funcs.SetLongArrayRegion = bug1SetLongArrayRegion;
    envObj.functions = &funcs;
    ready = true;
  }
  return &envObj;
}
} // namespace

TEST_F(ExportTest, AllocateOutputNewLongArrayNullNoLeak) {
  JNIEnv* env = bug1MockEnv();

  const int64_t liveBefore = gluten::veloxColumnHandleLiveCount().load(std::memory_order_relaxed);

  g_bug1Mock.forceNewLongArrayNull = true;

  jlongArray ret = reinterpret_cast<jlongArray>(static_cast<uintptr_t>(0xDEAD));
  // Encoding [1] => BIGINT: a real descriptor tree + ObjectStore vector are allocated
  // before NewLongArray is reached, so the failure path must free both.
  jint typeCode = 1;
  EXPECT_NO_THROW({
    ret = Java_org_apache_gluten_velox_vector_VeloxColumnHandleJniWrapper_allocateNestedOutput(
        env, /*clazz=*/nullptr, reinterpret_cast<jintArray>(&typeCode), /*rowCapacity=*/8);
  });

  g_bug1Mock.forceNewLongArrayNull = false;

  // (c) safe default return.
  EXPECT_EQ(ret, nullptr) << "allocateNestedOutput must return nullptr when NewLongArray fails";

  // (b) no descriptor-node leak: the tree allocated before the failure must be freed,
  //     so the live count returns to its pre-call baseline.
  const int64_t liveAfter = gluten::veloxColumnHandleLiveCount().load(std::memory_order_relaxed);
  EXPECT_EQ(liveAfter, liveBefore) << "descriptor tree leaked on NewLongArray-null failure path";
}

// ============================================================
// R4 regression: NewLongArray-returns-null (JVM OOM) on allocateNestedOutput must
// release the ObjectStore-saved output vector on the failure path. Pre-fix, it freed
// only the descriptor tree and forgot to release the saved VectorPtr, permanently
// pinning the nested Velox vector (and all its backing buffers) in outputStore().
//
// The descriptor-node live count (veloxColumnHandleLiveCount) is NOT sufficient to
// observe this leak: the descriptor tree IS freed on both the pre-fix and fixed paths.
// The leaked resource is the native Velox vector held by the ObjectStore, whose backing
// buffers are allocated from the module-level output MemoryPool (a child of the process
// MemoryManager).  We therefore observe the leak through the global MemoryManager's total
// allocated bytes: on the pre-fix path the pinned vector keeps its buffers alive, so the
// byte total stays elevated after the JNI call; on the fixed path the release() drops the
// vector and its buffers, returning the total to its pre-call baseline.
//
// This test lives in the ExportTest fixture for the same reason as the flat allocation one:
// allocateNestedOutput() uses the process-static outputStore()/outputPool() bound to the
// MemoryManager created by ExportTest::SetUpTestCase.
// ============================================================
namespace {
// DFS pre-order type encoding for ARRAY<INT>: [7 (ARRAY), 0 (INTEGER)].
jint g_nestedTypeEncoding[2] = {7, 0};

jsize JNICALL nestedGetArrayLength(JNIEnv*, jarray arr) {
  // Only the typeEncoding int[] length is ever queried in this test.
  (void)arr;
  return 2;
}

void JNICALL nestedGetIntArrayRegion(JNIEnv*, jintArray arr, jsize start, jsize len, jint* buf) {
  (void)arr;
  for (jsize i = 0; i < len; ++i) {
    buf[i] = g_nestedTypeEncoding[start + i];
  }
}

jlongArray JNICALL nestedNewLongArray(JNIEnv*, jsize) {
  return nullptr; // simulate JVM OOM at NewLongArray(2)
}

void JNICALL nestedSetLongArrayRegion(JNIEnv*, jlongArray arr, jsize, jsize, const jlong*) {
  ASSERT_NE(arr, nullptr) << "SetLongArrayRegion must never be called on a null array";
}

JNIEnv* nestedMockEnv() {
  static JNINativeInterface_ funcs;
  static JNIEnv_ envObj;
  static bool ready = false;
  if (!ready) {
    std::memset(&funcs, 0, sizeof(funcs));
    funcs.GetArrayLength = nestedGetArrayLength;
    funcs.GetIntArrayRegion = nestedGetIntArrayRegion;
    funcs.NewLongArray = nestedNewLongArray;
    funcs.SetLongArrayRegion = nestedSetLongArrayRegion;
    envObj.functions = &funcs;
    ready = true;
  }
  return &envObj;
}
} // namespace

TEST_F(ExportTest, AllocateNestedOutputNewLongArrayNullNoLeak) {
  JNIEnv* env = nestedMockEnv();

  // typeEncoding is never dereferenced by the mock env; a non-null token suffices.
  jintArray typeEncoding = reinterpret_cast<jintArray>(static_cast<uintptr_t>(0xBEEF));

  const int64_t bytesBefore = memory::memoryManager()->getTotalBytes();

  jlongArray ret = reinterpret_cast<jlongArray>(static_cast<uintptr_t>(0xDEAD));
  EXPECT_NO_THROW({
    ret = Java_org_apache_gluten_velox_vector_VeloxColumnHandleJniWrapper_allocateNestedOutput(
        env, /*clazz=*/nullptr, typeEncoding, /*rowCapacity=*/8);
  });

  // (a) safe default return.
  EXPECT_EQ(ret, nullptr) << "allocateNestedOutput must return nullptr when NewLongArray fails";

  // (b) no native-vector leak: the ObjectStore-saved nested vector (and its backing
  //     buffers) must be released on the failure path, so the process MemoryManager's
  //     total allocated bytes return to their pre-call baseline.
  const int64_t bytesAfter = memory::memoryManager()->getTotalBytes();
  EXPECT_EQ(bytesAfter, bytesBefore) << "nested output vector leaked on NewLongArray-null failure path (byte delta = "
                                     << (bytesAfter - bytesBefore) << ")";
}
