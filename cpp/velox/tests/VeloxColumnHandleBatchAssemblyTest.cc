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

#include <gtest/gtest.h>
#include <cstring>
#include <string>
#include "compute/VeloxExpressionEvaluator.h"
#include "memory/VeloxColumnarBatch.h"
#include "substrait/extended_expression.pb.h"
#include "utils/ObjectStore.h"
#include "vector/VeloxColumnHandle.h"
#include "vector/VeloxColumnHandleExport.h"
#include "velox/functions/sparksql/registration/Register.h"
#include "velox/vector/tests/utils/VectorTestBase.h"

using namespace facebook::velox;

/**
 * Test fixture for makeVeloxBatch batch-assembly tests.
 *
 * Inherits VectorTestBase for Velox helpers and initialises the Velox
 * MemoryManager once per test-case (required for allocateNestedOutput).
 */
class VeloxColumnHandleBatchAssemblyTest : public testing::Test, public test::VectorTestBase {
 protected:
  static void SetUpTestCase() {
    memory::MemoryManager::testingSetInstance(memory::MemoryManager::Options{});
    functions::sparksql::registerFunctions("");
  }
};

TEST_F(VeloxColumnHandleBatchAssemblyTest, ZeroRowStringBatch) {
  auto [descriptor, owner] = gluten::allocateNestedOutput(VARCHAR(), 0);
  EXPECT_EQ(descriptor->length, 0);
  gluten::finalizeStringColumn(owner, {});
  auto store = gluten::ObjectStore::create();
  auto handle = gluten::makeVeloxBatch(
      {owner}, {"input"}, [&](std::shared_ptr<gluten::ColumnarBatch> batch) { return store->save(std::move(batch)); });
  auto batch = store->retrieveOwned<gluten::ColumnarBatch>(handle);
  EXPECT_EQ(batch->numColumns(), 1);
  EXPECT_EQ(batch->numRows(), 0);
  gluten::ObjectStore::release(owner);
  gluten::freeColumnHandleTree(descriptor);
  EXPECT_EQ(batch->numRows(), 0);
  gluten::ObjectStore::release(handle);
}

TEST_F(VeloxColumnHandleBatchAssemblyTest, ZeroColumnBatch) {
  auto store = gluten::ObjectStore::create();
  auto handle = gluten::makeVeloxBatch(
      {}, {}, [&](std::shared_ptr<gluten::ColumnarBatch> batch) { return store->save(std::move(batch)); });
  auto batch = store->retrieveOwned<gluten::ColumnarBatch>(handle);
  EXPECT_EQ(batch->numColumns(), 0);
  EXPECT_EQ(batch->numRows(), 0);
  gluten::ObjectStore::release(handle);
}

TEST_F(VeloxColumnHandleBatchAssemblyTest, RejectsLiveBatchAndEvaluatorOwnerHandles) {
  auto store = gluten::ObjectStore::create();
  auto batch = std::make_shared<gluten::VeloxColumnarBatch>(makeRowVector({makeFlatVector<int32_t>({1})}));
  auto batchHandle = store->save(std::static_pointer_cast<gluten::ColumnarBatch>(batch));
  ::substrait::ExtendedExpression expression;
  expression.mutable_base_schema()->mutable_struct_();
  auto* reference = expression.add_referred_expr();
  reference->add_output_names("result");
  reference->mutable_expression()->mutable_literal()->set_string("value");
  auto bytes = expression.SerializeAsString();
  auto evaluatorHandle = store->save(std::make_shared<gluten::VeloxExpressionEvaluator>(
      pool_, core::QueryCtx::create(), reinterpret_cast<const uint8_t*>(bytes.data()), bytes.size()));
  for (auto wrongHandle : {batchHandle, evaluatorHandle}) {
    bool saved = false;
    EXPECT_THROW(
        gluten::makeVeloxBatch(
            {wrongHandle},
            {"invalid"},
            [&](std::shared_ptr<gluten::ColumnarBatch> output) {
              saved = true;
              return store->save(std::move(output));
            }),
        gluten::GlutenException);
    EXPECT_FALSE(saved);
    EXPECT_THROW(gluten::allocateStringChunk(wrongHandle, 16), gluten::GlutenException);
    EXPECT_THROW(gluten::finalizeStringColumn(wrongHandle, {}), gluten::GlutenException);
  }
  EXPECT_THROW(gluten::exportBatchColumns(evaluatorHandle), gluten::GlutenException);
  EXPECT_NE(gluten::ObjectStore::retrieveChecked<gluten::ColumnarBatch>(batchHandle), nullptr);
  EXPECT_NE(gluten::ObjectStore::retrieveChecked<gluten::VeloxExpressionEvaluator>(evaluatorHandle), nullptr);
  gluten::ObjectStore::release(batchHandle);
  gluten::ObjectStore::release(evaluatorHandle);
}

TEST_F(VeloxColumnHandleBatchAssemblyTest, DirectStringsFeedExpressionEvaluatorWithoutArrow) {
  auto [descriptor, owner] = gluten::allocateNestedOutput(VARCHAR(), 4);
  auto* values = const_cast<StringView*>(static_cast<const StringView*>(descriptor->buffers[1]));
  values[0] = StringView("0123456789");
  values[1] = StringView("trim  ");
  bits::setNull(const_cast<uint64_t*>(static_cast<const uint64_t*>(descriptor->buffers[0])), 2, true);
  const std::string longString(64, 'x');
  auto [address, capacity] = gluten::allocateStringChunk(owner, longString.size());
  ASSERT_GE(capacity, longString.size());
  auto* chunk = reinterpret_cast<char*>(address);
  std::memcpy(chunk, longString.data(), longString.size());
  values[3] = StringView(chunk, longString.size());
  gluten::finalizeStringColumn(owner, {static_cast<int64_t>(longString.size())});

  auto store = gluten::ObjectStore::create();
  auto inputHandle = gluten::makeVeloxBatch({owner}, {"input_0"}, [&](std::shared_ptr<gluten::ColumnarBatch> batch) {
    return store->save(std::move(batch));
  });
  auto input = store->retrieveOwned<gluten::ColumnarBatch>(inputHandle);
  gluten::ObjectStore::release(owner);
  gluten::freeColumnHandleTree(descriptor);

  ::substrait::ExtendedExpression expression;
  expression.mutable_base_schema()->add_names("input_0");
  expression.mutable_base_schema()->mutable_struct_()->add_types()->mutable_string();
  auto* mapping = expression.add_extensions()->mutable_extension_function();
  mapping->set_function_anchor(1);
  mapping->set_name("rtrim:str");
  auto* ref = expression.add_referred_expr();
  ref->add_output_names("result");
  auto* function = ref->mutable_expression()->mutable_scalar_function();
  function->set_function_reference(1);
  function->mutable_output_type()->mutable_string();
  auto* field = function->add_arguments()->mutable_value()->mutable_selection();
  field->mutable_root_reference();
  field->mutable_direct_reference()->mutable_struct_field()->set_field(0);
  auto bytes = expression.SerializeAsString();
  std::shared_ptr<gluten::VeloxColumnarBatch> output;
  {
    gluten::VeloxExpressionEvaluator evaluator(
        pool_, core::QueryCtx::create(), reinterpret_cast<const uint8_t*>(bytes.data()), bytes.size());
    output = evaluator.evaluate(input);
  }
  gluten::ObjectStore::release(inputHandle);
  input.reset();
  test::assertEqualVectors(
      makeNullableFlatVector<std::string>({"0123456789", "trim", std::nullopt, longString}),
      output->getRowVector()->childAt(0));
}

/**
 * MakeBatchFromTwoIntColumns: allocates two owned output vectors (INT, BIGINT),
 * writes values, assembles them into a native Velox batch via makeVeloxBatch
 * (using a local ObjectStore passed as the saveFn), and verifies:
 *   1. The batch is a VeloxColumnarBatch with 2 columns.
 *   2. Column values are correct (both columns checked).
 *   3. After releasing ownerA and ownerB, the batch's column data is still
 *      readable for BOTH columns (RowVector holds its own reference to each
 *      child VectorPtr).
 */
TEST_F(VeloxColumnHandleBatchAssemblyTest, MakeBatchFromTwoIntColumns) {
  auto [descA, ownerA] = gluten::allocateNestedOutput(INTEGER(), 3);
  auto [descB, ownerB] = gluten::allocateNestedOutput(BIGINT(), 3);

  // C1 unwrap: the store holds shared_ptr<VectorPtr>; dereference to get VectorPtr.
  VectorPtr va = *gluten::ObjectStore::retrieve<facebook::velox::VectorPtr>(static_cast<gluten::ObjectHandle>(ownerA));
  VectorPtr vb = *gluten::ObjectStore::retrieve<facebook::velox::VectorPtr>(static_cast<gluten::ObjectHandle>(ownerB));

  va->asFlatVector<int32_t>()->set(0, 7);
  vb->asFlatVector<int64_t>()->set(0, 77);
  va->resize(3);
  vb->resize(3);

  // Use a LOCAL ObjectStore as the save destination, matching the pipeline
  // contract where makeVeloxBatch saves into the runtime's store.
  auto store = gluten::ObjectStore::create();
  int64_t batchHandle = gluten::makeVeloxBatch(
      {ownerA, ownerB}, {"a", "b"}, [&store](std::shared_ptr<gluten::ColumnarBatch> b) -> int64_t {
        return static_cast<int64_t>(store->save(std::move(b)));
      });

  // ObjectStore::retrieve decodes the store ID from batchHandle and finds `store`.
  auto cb = gluten::ObjectStore::retrieve<gluten::ColumnarBatch>(static_cast<gluten::ObjectHandle>(batchHandle));
  auto vcb = std::dynamic_pointer_cast<gluten::VeloxColumnarBatch>(cb);
  ASSERT_NE(vcb, nullptr);

  auto rv = vcb->getRowVector();
  EXPECT_EQ(rv->childrenSize(), 2u);
  EXPECT_EQ(rv->childAt(0)->asFlatVector<int32_t>()->valueAt(0), 7);
  EXPECT_EQ(rv->childAt(1)->asFlatVector<int64_t>()->valueAt(0), 77);

  // Release owner handles; the RowVector's shared_ptr references keep each
  // child vector alive -- no premature free, no double-free.
  gluten::ObjectStore::release(static_cast<gluten::ObjectHandle>(ownerA));
  gluten::ObjectStore::release(static_cast<gluten::ObjectHandle>(ownerB));
  gluten::freeColumnHandleTree(descA);
  gluten::freeColumnHandleTree(descB);

  // Re-retrieve the batch and confirm BOTH columns are still readable after owner release.
  auto rv2 = std::dynamic_pointer_cast<gluten::VeloxColumnarBatch>(
                 gluten::ObjectStore::retrieve<gluten::ColumnarBatch>(static_cast<gluten::ObjectHandle>(batchHandle)))
                 ->getRowVector();
  EXPECT_EQ(rv2->childAt(0)->asFlatVector<int32_t>()->valueAt(0), 7);
  EXPECT_EQ(rv2->childAt(1)->asFlatVector<int64_t>()->valueAt(0), 77);

  gluten::ObjectStore::release(static_cast<gluten::ObjectHandle>(batchHandle));
}

/**
 * GrowArrayElements: allocates ARRAY<INT> with initial capacity 2, grows the elements
 * child to 100 via growChild({0}), and verifies:
 *   1. The elements child size is >= 100.
 *   2. The returned addresses are non-zero (valid mutable buffer pointers).
 *   3. The returned valuesAddr matches the post-grow mutableRawValues pointer, confirming
 *      the address is refreshed after potential reallocation so Java can re-fetch write
 *      pointers correctly.
 *
 * TDD RED: growChild is undeclared until the implementation is added in
 * VeloxColumnHandleExport.h / VeloxColumnHandleExport.cc.
 */
TEST_F(VeloxColumnHandleBatchAssemblyTest, GrowArrayElements) {
  // Allocate ARRAY<INT> with only 2 initial element slots.
  auto [desc, owner] = gluten::allocateNestedOutput(ARRAY(INTEGER()), 2);

  // Grow the elements child (childPath={0} navigates ARRAY->elements()) to 100 slots.
  // C1 ownerHandle is int64_t (the ObjectStore handle, not a pointer).
  auto addrs = gluten::growChild(static_cast<int64_t>(owner), {0}, 100);

  // C1 unwrap: retrieve<VectorPtr> returns shared_ptr<VectorPtr>; dereference once.
  facebook::velox::VectorPtr v =
      *gluten::ObjectStore::retrieve<facebook::velox::VectorPtr>(static_cast<gluten::ObjectHandle>(owner));

  // 1. Elements child must have grown to at least 100.
  EXPECT_GE(v->asUnchecked<ArrayVector>()->elements()->size(), 100u);

  // 2. Returned addresses must be non-zero (FLAT leaf: [nullsAddr, valuesAddr], 2 longs).
  ASSERT_EQ(addrs.size(), 2u);
  EXPECT_NE(addrs[0], static_cast<int64_t>(0)); // nullsAddr
  EXPECT_NE(addrs[1], static_cast<int64_t>(0)); // valuesAddr

  // 3. Returned valuesAddr must match the child's current mutableRawValues, confirming
  //    the descriptor is post-reallocation and usable for direct writes via Unsafe.
  auto* elems = v->asUnchecked<ArrayVector>()->elements()->asUnchecked<FlatVector<int32_t>>();
  EXPECT_EQ(addrs[1], reinterpret_cast<int64_t>(elems->mutableRawValues()));

  gluten::ObjectStore::release(static_cast<gluten::ObjectHandle>(owner));
  gluten::freeColumnHandleTree(desc);
}

/**
 * GrowChildPreservesExistingNulls: regression test for a null-bitmap clobber bug in
 * growChild.  Before growing, we mark elements child row 0 as NULL.  After growChild
 * grows the child (triggering resize + null-bitmap re-init), row 0 MUST still read as
 * null.  The buggy implementation memset the ENTIRE bitmap [0,newCapacity) to 0xFF
 * (all-valid), clobbering the pre-existing null -> silent data corruption.
 *
 * RED (buggy memset): elems->isNullAt(0) == false -> assertion fails.
 * GREEN (fillBits over [oldSize,newCapacity)): elems->isNullAt(0) == true -> passes.
 */
TEST_F(VeloxColumnHandleBatchAssemblyTest, GrowChildPreservesExistingNulls) {
  // Allocate ARRAY<INT> with 2 initial element slots.
  auto [desc, owner] = gluten::allocateNestedOutput(ARRAY(INTEGER()), 2);

  facebook::velox::VectorPtr v =
      *gluten::ObjectStore::retrieve<facebook::velox::VectorPtr>(static_cast<gluten::ObjectHandle>(owner));

  // Mark elements child row 0 as NULL before the grow (row 0 < oldSize==2).
  auto& elems = v->asUnchecked<ArrayVector>()->elements();
  ASSERT_GE(elems->size(), 1u);
  elems->setNull(0, true);
  ASSERT_TRUE(elems->isNullAt(0));

  // Grow the elements child to 100 slots -- triggers resize + null-bitmap re-init.
  gluten::growChild(static_cast<int64_t>(owner), {0}, 100);

  // Re-fetch (resize may have replaced buffers) and assert row 0 is STILL null.
  auto& elemsAfter = v->asUnchecked<ArrayVector>()->elements();
  EXPECT_GE(elemsAfter->size(), 100u);
  EXPECT_TRUE(elemsAfter->isNullAt(0)) << "growChild clobbered a pre-existing null bit at row 0";

  gluten::ObjectStore::release(static_cast<gluten::ObjectHandle>(owner));
  gluten::freeColumnHandleTree(desc);
}

/**
 * GrowRowChildReturnsFieldAddrs: regression test for a use-after-free introduced by an
 * earlier incomplete fix.  For ARRAY<ROW<INT>>, writeArray grows the ROW element child via
 * a single growChild({0}, newCapacity).  Velox's RowVector::resize RECURSIVELY resizes and
 * REALLOCATES each field child's value/nulls buffers.  If growChild returns only the ROW's
 * own nulls address, the Java field-child objects keep their STALE valuesAddr and the
 * subsequent putInt writes land in the freed/reallocated field buffer -> native heap UAF.
 *
 * The fix: for a ROW target, growChild appends, after the ROW nulls address, each field
 * child's refreshed address block (leaf: [nulls, values]; array/map: [nulls, offsets,
 * sizes]; nested row: [nulls, <recursive field blocks>]).  This test asserts the returned
 * vector contains the field child's CURRENT (post-reallocation) mutableRawValues pointer.
 *
 * RED (returns only [rowNulls], size==1): the field values addr is absent -> assertion
 *   fails (addrs.size()==1, and no entry equals the fresh values pointer).
 * GREEN (returns [rowNulls, fieldNulls, fieldValues]): addrs[2] == fresh values pointer.
 */
TEST_F(VeloxColumnHandleBatchAssemblyTest, GrowRowChildReturnsFieldAddrs) {
  // ARRAY<ROW<INT>> with only 2 initial element (row) slots.
  auto [desc, owner] = gluten::allocateNestedOutput(ARRAY(ROW({"f"}, {INTEGER()})), 2);

  // Grow the ROW element child (childPath={0} navigates ARRAY->elements()) to 100 slots.
  // This triggers RowVector::resize which reallocates the INT field child buffers.
  auto addrs = gluten::growChild(static_cast<int64_t>(owner), {0}, 100);

  facebook::velox::VectorPtr v =
      *gluten::ObjectStore::retrieve<facebook::velox::VectorPtr>(static_cast<gluten::ObjectHandle>(owner));
  auto* rowChild = v->asUnchecked<ArrayVector>()->elements()->asUnchecked<RowVector>();
  EXPECT_GE(rowChild->size(), 100u);

  // Fresh field-child buffers after the grow.
  auto* field = rowChild->childAt(0)->asUnchecked<FlatVector<int32_t>>();
  const int64_t freshFieldNulls = reinterpret_cast<int64_t>(field->mutableRawNulls());
  const int64_t freshFieldValues = reinterpret_cast<int64_t>(field->mutableRawValues());

  // ROW layout: [rowNulls, fieldNulls, fieldValues] for a single INT field.
  ASSERT_EQ(addrs.size(), 3u) << "growChild must return ROW nulls + each field child's addr block";
  EXPECT_NE(addrs[0], static_cast<int64_t>(0)); // ROW nulls
  EXPECT_EQ(addrs[1], freshFieldNulls); // field nulls (fresh)
  EXPECT_EQ(addrs[2], freshFieldValues); // field values (fresh, post-reallocation)

  // Sanity: a direct write through the returned fresh pointer lands in valid memory and
  // reads back correctly (this is exactly what Java does via Unsafe.putInt).
  reinterpret_cast<int32_t*>(addrs[2])[99] = 4242;
  EXPECT_EQ(field->valueAt(99), 4242);

  gluten::ObjectStore::release(static_cast<gluten::ObjectHandle>(owner));
  gluten::freeColumnHandleTree(desc);
}

/**
 * GrowVarcharChildRefreshesSlotAddr: memory-safety regression test for the nested VARCHAR
 * growth path (the use-after-free guard).  For ARRAY<VARCHAR>, when the element count exceeds
 * the child's initial capacity, writeArray grows the VARCHAR element child via a single
 * growChild({0}, newCapacity).  Velox's FlatVector<StringView>::resize resizes the 16-byte
 * StringView SLOT buffer, which may REALLOCATE.  Correctness requires ALL of:
 *   (a) growChild returns the FRESH slot (values) pointer for the VARCHAR child (post-resize),
 *   (b) already-written StringViews -- inline AND those holding an absolute char* into the
 *       pinned, append-only string chunk -- are PRESERVED across the resize and still read back
 *       correctly through the refreshed slot pointer (the chunk never moves), and
 *   (c) the NEW slots [oldSize, newCapacity) are value-initialized so a StringView written at a
 *       grown index reads back correctly while appending into the SAME pinned chunk.
 *
 * Any of: a stale slot pointer, resize not preserving the views, uninitialized new slots, or a
 * lost chunk cursor is a native heap UAF / corruption.
 *
 * RED (e.g. growChild returns a stale pre-resize slot pointer, or resize drops the views):
 *   assertion (a) or (b) fails / reads garbage.
 * GREEN (FLAT branch re-fetches mutableRawValues after resize; Velox resize preserves slots and
 *   value-initializes the new ones): all read-backs match.
 */
TEST_F(VeloxColumnHandleBatchAssemblyTest, GrowVarcharChildRefreshesSlotAddr) {
  // ARRAY<VARCHAR> with only 2 initial element slots.
  auto [desc, owner] = gluten::allocateNestedOutput(ARRAY(VARCHAR()), 2);

  facebook::velox::VectorPtr v =
      *gluten::ObjectStore::retrieve<facebook::velox::VectorPtr>(static_cast<gluten::ObjectHandle>(owner));
  auto* elems = v->asUnchecked<ArrayVector>()->elements()->asUnchecked<FlatVector<StringView>>();
  ASSERT_GE(elems->size(), 2u);

  // Allocate a pinned string-data chunk on the element child (childPath {0}) for out-of-line
  // (>12 byte) strings -- mirrors the Java out-of-line write path.
  auto chunkPair = gluten::allocateStringChunkAt(static_cast<int64_t>(owner), {0}, 4096);
  const int64_t chunkAddr = chunkPair.first;
  ASSERT_NE(chunkAddr, static_cast<int64_t>(0));
  ASSERT_GE(chunkPair.second, static_cast<int64_t>(4096));
  char* chunk = reinterpret_cast<char*>(chunkAddr);

  // Write long (>12 byte) StringViews EXACTLY as Java does: raw 16-byte slot
  // [4B size][4B prefix][8B absolute char* into the pinned chunk].
  const std::string s0 = "row-zero-string-well-over-twelve-bytes";
  const std::string s1 = "row-one-string-also-well-over-twelve-bytes";
  int64_t chunkOff = 0;
  auto writeLongSlot = [&](char* slotBase, int idx, const std::string& s) {
    std::memcpy(chunk + chunkOff, s.data(), s.size());
    char* slot = slotBase + static_cast<size_t>(idx) * 16;
    const int32_t sz = static_cast<int32_t>(s.size());
    std::memcpy(slot, &sz, 4);
    std::memcpy(slot + 4, s.data(), 4);
    const int64_t ptr = reinterpret_cast<int64_t>(chunk + chunkOff);
    std::memcpy(slot + 8, &ptr, 8);
    chunkOff += static_cast<int64_t>(s.size());
  };
  char* preSlots = reinterpret_cast<char*>(elems->mutableRawValues());
  writeLongSlot(preSlots, 0, s0);
  writeLongSlot(preSlots, 1, s1);

  // Sanity: reads back before the grow.
  ASSERT_EQ(elems->valueAt(0).getString(), s0);
  ASSERT_EQ(elems->valueAt(1).getString(), s1);

  // Grow the VARCHAR element child from 2 -> 100.  Resizes the StringView slot buffer
  // (may reallocate); must preserve existing views and value-initialize the new slots.
  auto addrs = gluten::growChild(static_cast<int64_t>(owner), {0}, 100);

  auto* elemsAfter = v->asUnchecked<ArrayVector>()->elements()->asUnchecked<FlatVector<StringView>>();
  EXPECT_GE(elemsAfter->size(), 100u);

  // (a) FLAT VARCHAR block is [nulls, values]; values must be the FRESH slot base.
  ASSERT_EQ(addrs.size(), 2u);
  EXPECT_NE(addrs[0], static_cast<int64_t>(0)); // nulls
  EXPECT_EQ(addrs[1], reinterpret_cast<int64_t>(elemsAfter->mutableRawValues()))
      << "growChild must return the post-reallocation StringView slot base for the VARCHAR child";

  // (b) Previously-written StringViews survive the resize and still read back through the
  //     refreshed slot pointer -- the char* pointers into the pinned chunk remain valid.
  EXPECT_EQ(elemsAfter->valueAt(0).getString(), s0) << "grow dropped/reallocated an existing StringView";
  EXPECT_EQ(elemsAfter->valueAt(1).getString(), s1) << "grow dropped/reallocated an existing StringView";

  // (c) A new StringView written at a grown index reads back correctly, appending into the SAME
  //     pinned chunk (cursor continuity) via the FRESH slot base returned by growChild.
  const std::string s99 = "grown-index-string-longer-than-twelve-bytes";
  char* freshSlots = reinterpret_cast<char*>(addrs[1]);
  writeLongSlot(freshSlots, 99, s99);
  EXPECT_EQ(elemsAfter->valueAt(99).getString(), s99) << "new grown-index StringView slot is corrupt";

  gluten::ObjectStore::release(static_cast<gluten::ObjectHandle>(owner));
  gluten::freeColumnHandleTree(desc);
}

/**
 * GrowBooleanChildRefreshesBitBuffer: an ARRAY<BOOLEAN> element child that grows past its initial
 * nested capacity must (a) have growChild return the FRESH bit-packed values pointer, (b) preserve
 * the already-written bits [0, oldSize), and (c) expose the new bits [oldSize, newCapacity) as
 * FALSE (not garbage/stale) so a grown index not yet written reads back as false, while a bit
 * written at a grown index (spanning byte boundaries) reads back correctly.
 *
 * BOOLEAN is bit-packed (1 bit/row, LSB-first).  FlatVector<bool>::resize does NOT value-initialize
 * the grown region, so growChild explicitly clears the new bits to FALSE; this test locks that in.
 *
 * The test forces the deterministic no-reallocation resize path by growing only within the
 * initial buffer's byte capacity and poisoning the whole buffer to all-ones (0xFF) first: without
 * the new-bit clear, the grown bits would read back as `true` (RED); with it, they read false.
 */
TEST_F(VeloxColumnHandleBatchAssemblyTest, GrowBooleanChildRefreshesBitBuffer) {
  // ARRAY<BOOLEAN> with only 2 initial element slots.
  auto [desc, owner] = gluten::allocateNestedOutput(ARRAY(BOOLEAN()), 2);

  facebook::velox::VectorPtr v =
      *gluten::ObjectStore::retrieve<facebook::velox::VectorPtr>(static_cast<gluten::ObjectHandle>(owner));
  auto* elems = v->asUnchecked<ArrayVector>()->elements()->asUnchecked<FlatVector<bool>>();
  ASSERT_GE(elems->size(), 2u);

  // Bit helpers mirroring Java VeloxWritableColumnVector.putBoolean/getBoolean (LSB-first).
  auto setBit = [](uint64_t* base, int r, bool val) {
    uint8_t* p = reinterpret_cast<uint8_t*>(base) + (r >> 3);
    const uint8_t m = static_cast<uint8_t>(1 << (r & 7));
    *p = val ? static_cast<uint8_t>(*p | m) : static_cast<uint8_t>(*p & ~m);
  };
  auto getBit = [](const uint64_t* base, int r) {
    const uint8_t* p = reinterpret_cast<const uint8_t*>(base) + (r >> 3);
    return (*p & (1 << (r & 7))) != 0;
  };

  // Force allocation of the bit-packed values buffer and poison the ENTIRE underlying buffer to
  // all-ones so any bit the grow leaves untouched would read back as `true` (garbage) -- this is
  // what makes the missing new-bit clear a DETERMINISTIC failure.
  uint64_t* preBits = elems->mutableRawValues<uint64_t>();
  const size_t bufBytes = elems->values()->capacity();
  ASSERT_GE(bufBytes, 2u) << "need >= 2 bytes to span a byte boundary within initial capacity";
  std::memset(preBits, 0xFF, bufBytes);

  // Write the two in-capacity bits exactly as Java would: bit 0 = true, bit 1 = false.
  setBit(preBits, 0, true);
  setBit(preBits, 1, false);
  ASSERT_TRUE(getBit(preBits, 0));
  ASSERT_FALSE(getBit(preBits, 1));

  // Grow within the initial buffer's byte capacity => resize takes the no-reallocation path, so the
  // poisoned bytes are retained and the new-bit clear is observable.  The grown capacity spans many
  // bytes (>= 16 bits).
  const int grownCap = static_cast<int>(bufBytes * 8);
  ASSERT_GE(grownCap, 16);
  auto addrs = gluten::growChild(static_cast<int64_t>(owner), {0}, grownCap);

  auto* elemsAfter = v->asUnchecked<ArrayVector>()->elements()->asUnchecked<FlatVector<bool>>();
  EXPECT_GE(elemsAfter->size(), static_cast<vector_size_t>(grownCap));

  // (a) FLAT BOOLEAN block is [nulls, values]; values must be the fresh bit-buffer base.
  ASSERT_EQ(addrs.size(), 2u);
  EXPECT_NE(addrs[0], static_cast<int64_t>(0)); // nulls
  EXPECT_EQ(addrs[1], reinterpret_cast<int64_t>(elemsAfter->mutableRawValues<uint64_t>()))
      << "growChild must return the post-resize bit-packed values base for the BOOLEAN child";

  uint64_t* postBits = reinterpret_cast<uint64_t*>(addrs[1]);

  // (b) Previously-written bits survive the resize.
  EXPECT_TRUE(getBit(postBits, 0)) << "existing true bit lost after grow";
  EXPECT_FALSE(getBit(postBits, 1)) << "existing false bit corrupted after grow";

  // (c) New bits [oldSize=2, grownCap) must read back FALSE (the new-bit clear), not the poisoned
  //     all-ones garbage.  This is the RED->GREEN assertion for the growth new-bit init.
  for (int r = 2; r < grownCap; ++r) {
    EXPECT_FALSE(getBit(postBits, r)) << "grown-index bit " << r << " not cleared to false";
  }

  // (d) Bits written at grown indices spanning byte boundaries read back correctly, and their
  //     false neighbors stay false.
  setBit(postBits, 8, true);
  setBit(postBits, 15, true);
  setBit(postBits, grownCap - 1, true);
  EXPECT_TRUE(getBit(postBits, 8));
  EXPECT_TRUE(getBit(postBits, 15));
  EXPECT_TRUE(getBit(postBits, grownCap - 1));
  EXPECT_FALSE(getBit(postBits, 9)) << "grown-index neighbor bit corrupted";
  EXPECT_FALSE(getBit(postBits, 14)) << "grown-index neighbor bit corrupted";
  EXPECT_TRUE(getBit(postBits, 0)) << "existing bit clobbered by grown writes";

  gluten::ObjectStore::release(static_cast<gluten::ObjectHandle>(owner));
  gluten::freeColumnHandleTree(desc);
}

/**
 * GrowFlatTimestampZeroInitsNewSlots: an ARRAY<TIMESTAMP> element child that grows past its
 * initial nested capacity must value-initialize the grown slots [oldSize, newCapacity) to ZERO
 * (0 seconds + 0 nanos = epoch), NOT leave them holding garbage while marking them VALID.
 *
 * FlatVector<Timestamp>::resize forwards std::nullopt to resizeValues and does NOT
 * value-initialize the grown region (unlike FlatVector<StringView>::resize).  So without an
 * explicit zero-init in growChild, the trailing valid-marked Timestamp slots hold stale bytes ->
 * a positional consumer (unnest/explode/full-child copy) reads garbage micros as non-null data.
 *
 * The test forces the deterministic no-reallocation resize path by growing only within the
 * initial buffer's element capacity and poisoning the whole values buffer to 0xFF first: without
 * the zero-init, the grown Timestamp slots would read back as the poisoned garbage (RED); with it
 * they read back as Timestamp(0, 0) (GREEN).  Element 0 (written pre-grow) must be preserved.
 */
TEST_F(VeloxColumnHandleBatchAssemblyTest, GrowFlatTimestampZeroInitsNewSlots) {
  // ARRAY<TIMESTAMP> with only 2 initial element slots.
  auto [desc, owner] = gluten::allocateNestedOutput(ARRAY(TIMESTAMP()), 2);

  facebook::velox::VectorPtr v =
      *gluten::ObjectStore::retrieve<facebook::velox::VectorPtr>(static_cast<gluten::ObjectHandle>(owner));
  {
    auto* elems0 = v->asUnchecked<ArrayVector>()->elements()->asUnchecked<FlatVector<Timestamp>>();
    // cppSizeInBytes() (the memset element stride used by the fix) must be 16 for TIMESTAMP.
    ASSERT_EQ(elems0->type()->cppSizeInBytes(), 16u);
  }

  // First grow to a modest capacity to force allocation of a large backing buffer whose byte
  // capacity (AlignedBuffer rounds up) exceeds the requested element count -- this gives spare
  // slots we can poison and then reach via a SECOND grow WITHOUT reallocation, making the missing
  // zero-init a DETERMINISTIC failure (a plain reallocation could hand back zeroed pages).
  gluten::growChild(static_cast<int64_t>(owner), {0}, 8);
  auto* elems = v->asUnchecked<ArrayVector>()->elements()->asUnchecked<FlatVector<Timestamp>>();

  const size_t bufBytes = elems->values()->capacity();
  const int elemCap = static_cast<int>(bufBytes / sizeof(Timestamp));
  ASSERT_GT(elemCap, 8) << "need spare buffer slots beyond size() to exercise no-realloc grow";

  // Poison the ENTIRE underlying buffer to 0xFF so any slot the second grow leaves untouched reads
  // back as garbage.
  Timestamp* preVals = elems->mutableRawValues();
  std::memset(preVals, 0xFF, bufBytes);

  // Write a known Timestamp at element 0 (in the live range).
  const Timestamp known(1234567, 890);
  preVals[0] = known;
  ASSERT_EQ(preVals[0], known);

  const vector_size_t oldSize = elems->size(); // == 8, the range grown into by the second grow
  // Grow to the buffer's full element capacity => no reallocation, poisoned bytes retained.
  const int grownCap = elemCap;
  auto addrs = gluten::growChild(static_cast<int64_t>(owner), {0}, grownCap);

  auto* elemsAfter = v->asUnchecked<ArrayVector>()->elements()->asUnchecked<FlatVector<Timestamp>>();
  EXPECT_GE(elemsAfter->size(), static_cast<vector_size_t>(grownCap));

  // FLAT TIMESTAMP block is [nulls, values]; values must be the fresh slot base.
  ASSERT_EQ(addrs.size(), 2u);
  EXPECT_NE(addrs[0], static_cast<int64_t>(0)); // nulls
  EXPECT_EQ(addrs[1], reinterpret_cast<int64_t>(elemsAfter->mutableRawValues()))
      << "growChild must return the post-resize values base for the TIMESTAMP child";

  const Timestamp* postVals = reinterpret_cast<const Timestamp*>(addrs[1]);

  // Existing element 0 survives the resize.
  EXPECT_EQ(postVals[0], known) << "existing Timestamp value lost after grow";

  // Grown slots [oldSize, grownCap) must read back Timestamp(0,0), not the poisoned garbage.
  const Timestamp zero(0, 0);
  for (int r = oldSize; r < grownCap; ++r) {
    EXPECT_EQ(postVals[r], zero) << "grown-index Timestamp slot " << r << " not zero-initialized";
  }

  // Grown slots are marked VALID (not-null).
  for (int r = oldSize; r < grownCap; ++r) {
    EXPECT_FALSE(elemsAfter->isNullAt(r)) << "grown-index slot " << r << " unexpectedly null";
  }

  gluten::ObjectStore::release(static_cast<gluten::ObjectHandle>(owner));
  gluten::freeColumnHandleTree(desc);
}

/**
 * MakeBatchRejectsMismatchedColumnSizes: verifies that makeVeloxBatch throws a
 * VeloxException when two columns have different sizes.
 *
 * RED->GREEN: without the VELOX_CHECK_EQ in makeVeloxBatch this test fails
 * (no exception thrown); with the check it passes.
 */
TEST_F(VeloxColumnHandleBatchAssemblyTest, MakeBatchRejectsMismatchedColumnSizes) {
  // Column A: capacity 3, size 3.
  auto [descA, ownerA] = gluten::allocateNestedOutput(INTEGER(), 3);
  // Column B: capacity 2, size 2 -- deliberately mismatched.
  auto [descB, ownerB] = gluten::allocateNestedOutput(INTEGER(), 2);

  auto store = gluten::ObjectStore::create();
  auto saveFn = [&store](std::shared_ptr<gluten::ColumnarBatch> b) -> int64_t {
    return static_cast<int64_t>(store->save(std::move(b)));
  };

  EXPECT_THROW(gluten::makeVeloxBatch({ownerA, ownerB}, {"a", "b"}, saveFn), facebook::velox::VeloxException);

  gluten::ObjectStore::release(static_cast<gluten::ObjectHandle>(ownerA));
  gluten::ObjectStore::release(static_cast<gluten::ObjectHandle>(ownerB));
  gluten::freeColumnHandleTree(descA);
  gluten::freeColumnHandleTree(descB);
}
