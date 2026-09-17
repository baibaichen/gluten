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
#include "memory/VeloxColumnarBatch.h"
#include "utils/ObjectStore.h"
#include "vector/VeloxColumnHandle.h"
#include "vector/VeloxColumnHandleExport.h"
#include "velox/type/CalendarInterval.h"
#include "velox/vector/tests/utils/VectorTestBase.h"

using namespace facebook::velox;

/**
 * Test fixture for exportBatchColumns tests.
 *
 * Mirrors ExportTest in VeloxColumnHandleTest.cc: inherits VectorTestBase for
 * makeFlatVector/makeRowVector helpers and initialises the Velox MemoryManager
 * once per test-case.
 */
class VeloxColumnHandleBatchTest : public testing::Test, public test::VectorTestBase {
 protected:
  static void SetUpTestCase() {
    memory::MemoryManager::testingSetInstance(memory::MemoryManager::Options{});
  }
};

/**
 * ExportTwoColumnBatch: builds a RowVector with two columns (INT, BIGINT), wraps
 * it in a VeloxColumnarBatch saved in a local ObjectStore, calls exportBatchColumns,
 * and verifies that two read-side descriptors are returned with the correct length
 * and nChildren == 0 (flat primitive leaves).
 *
 * The descriptors are views: freeColumnHandleTree frees only descriptor memory;
 * the underlying Velox buffers remain owned by the batch/rowVector.
 */
TEST_F(VeloxColumnHandleBatchTest, ExportTwoColumnBatch) {
  // Build RowVector: col0 INT [1,2,3], col1 BIGINT [10,20,30].
  auto c0 = makeFlatVector<int32_t>({1, 2, 3});
  auto c1 = makeFlatVector<int64_t>({10, 20, 30});
  auto rowVec = makeRowVector({c0, c1});

  // Wrap in a VeloxColumnarBatch and save into a local ObjectStore.
  auto batch = std::make_shared<gluten::VeloxColumnarBatch>(rowVec);
  auto store = gluten::ObjectStore::create();
  int64_t handle = store->save(std::static_pointer_cast<gluten::ColumnarBatch>(batch));

  // Export per-column read-side descriptors.
  auto descs = gluten::exportBatchColumns(handle);

  ASSERT_EQ(descs.size(), 2u);

  // Both columns have 3 rows.
  EXPECT_EQ(descs[0]->length, 3);
  EXPECT_EQ(descs[1]->length, 3);

  // Flat primitive leaves — no children.
  EXPECT_EQ(descs[0]->nChildren, 0);
  EXPECT_EQ(descs[1]->nChildren, 0);

  // Values buffers are non-null.
  ASSERT_NE(descs[0]->buffers[1], nullptr);
  ASSERT_NE(descs[1]->buffers[1], nullptr);

  // Spot-check values: col0[2]==3, col1[2]==30.
  EXPECT_EQ(reinterpret_cast<const int32_t*>(descs[0]->buffers[1])[2], 3);
  EXPECT_EQ(reinterpret_cast<const int64_t*>(descs[1]->buffers[1])[2], 30);

  // Free only descriptor memory — vectors are still alive in the batch.
  for (auto* d : descs) {
    gluten::freeColumnHandleTree(d);
  }

  // Releasing the batch handle must not crash (store keeps the batch alive until here).
  gluten::ObjectStore::release(handle);

  // Vector data still valid after descriptor free — row vector was not touched.
  EXPECT_EQ(rowVec->size(), 3);
}

/**
 * ExportBatchColumnsInvalidHandle: passing an unknown handle must not silently
 * succeed.  The implementation is expected to throw.  We accept any std::exception
 * subclass here because the ObjectStore may throw GlutenException on an invalid
 * handle before the VELOX_CHECK ever fires.
 */
TEST_F(VeloxColumnHandleBatchTest, ExportBatchColumnsInvalidHandle) {
  // kInvalidObjectHandle (-1) is never saved; retrieve returns nullptr or throws.
  EXPECT_THROW(gluten::exportBatchColumns(gluten::kInvalidObjectHandle), std::exception);
}

TEST_F(VeloxColumnHandleBatchTest, ConstantStringAndAllNullOutputs) {
  const std::string value(64, 'x');
  for (vector_size_t size : {0, 1, 4}) {
    for (auto isNull : {false, true}) {
      SCOPED_TRACE("size=" + std::to_string(size) + ", null=" + std::to_string(isNull));
      auto base = makeNullableFlatVector<std::string>(
          {isNull ? std::optional<std::string>{} : std::optional<std::string>{value}});
      auto column = BaseVector::wrapInConstant(size, 0, base);
      auto row = makeRowVector({column});
      auto batch = std::make_shared<gluten::VeloxColumnarBatch>(row);
      auto store = gluten::ObjectStore::create();
      auto handle = store->save(std::static_pointer_cast<gluten::ColumnarBatch>(batch));
      auto descriptors = gluten::exportBatchColumns(handle);
      auto repeated = gluten::exportBatchColumns(handle);
      ASSERT_EQ(descriptors.size(), 1);
      EXPECT_EQ(descriptors[0]->length, size);
      EXPECT_EQ(descriptors[0]->buffers[1], repeated[0]->buffers[1]);
      EXPECT_EQ(
          descriptors[0]->buffers[1],
          batch->getFlattenedRowVector()->childAt(0)->asFlatVector<StringView>()->rawValues());
      EXPECT_EQ(batch->getRowVector().get(), row.get());
      EXPECT_EQ(row->childAt(0).get(), column.get());
      EXPECT_EQ(column->encoding(), VectorEncoding::Simple::CONSTANT);

      batch.reset();
      row.reset();
      column.reset();
      base.reset();
      for (vector_size_t index = 0; index < size; ++index) {
        if (isNull) {
          ASSERT_NE(descriptors[0]->buffers[0], nullptr);
          EXPECT_TRUE(bits::isBitNull(static_cast<const uint64_t*>(descriptors[0]->buffers[0]), index));
        } else {
          EXPECT_EQ(static_cast<const StringView*>(descriptors[0]->buffers[1])[index].str(), value);
        }
      }
      gluten::freeColumnHandleTree(descriptors[0]);
      gluten::freeColumnHandleTree(repeated[0]);
      gluten::ObjectStore::release(handle);
    }
  }
}

TEST_F(VeloxColumnHandleBatchTest, MixedEncodingsKeepFlatBuffersAndOriginalVectors) {
  auto flat = makeFlatVector<int32_t>({1, 2, 3});
  auto base = makeFlatVector<std::string>({"zero-long-string", "one-long-string"});
  auto dictionary = wrapInDictionary(makeIndices({1, 0, 1}), base);
  auto nestedEncoded = makeRowVector({"encoded"}, {dictionary});
  auto nestedFlatValues = makeFlatVector<std::string>({"a", "b", "c"});
  auto nestedFlat = makeRowVector({"flat"}, {nestedFlatValues});
  auto row = makeRowVector({flat, dictionary, nestedEncoded, nestedFlat});
  auto batch = std::make_shared<gluten::VeloxColumnarBatch>(row);
  auto store = gluten::ObjectStore::create();
  auto handle = store->save(std::static_pointer_cast<gluten::ColumnarBatch>(batch));

  auto descriptors = gluten::exportBatchColumns(handle);
  ASSERT_EQ(descriptors.size(), 4);
  EXPECT_EQ(descriptors[0]->buffers[1], flat->rawValues());
  EXPECT_EQ(descriptors[3]->children[0]->buffers[1], nestedFlatValues->rawValues());
  EXPECT_EQ(batch->getRowVector().get(), row.get());
  EXPECT_EQ(row->childAt(1).get(), dictionary.get());
  EXPECT_EQ(row->childAt(2).get(), nestedEncoded.get());
  EXPECT_EQ(dictionary->encoding(), VectorEncoding::Simple::DICTIONARY);
  EXPECT_EQ(nestedEncoded->childAt(0)->encoding(), VectorEncoding::Simple::DICTIONARY);
  const std::vector<std::string> expected = {"one-long-string", "zero-long-string", "one-long-string"};
  for (vector_size_t index = 0; index < 3; ++index) {
    EXPECT_EQ(static_cast<const StringView*>(descriptors[1]->buffers[1])[index].str(), expected[index]);
    EXPECT_EQ(static_cast<const StringView*>(descriptors[2]->children[0]->buffers[1])[index].str(), expected[index]);
  }
  test::assertEqualVectors(makeFlatVector<int32_t>({1, 2, 3}), flat);
  test::assertEqualVectors(makeFlatVector<std::string>({"zero-long-string", "one-long-string"}), base);
  for (auto* descriptor : descriptors) {
    gluten::freeColumnHandleTree(descriptor);
  }
  gluten::ObjectStore::release(handle);
}

TEST_F(VeloxColumnHandleBatchTest, ConstantUnknownOutputsKeepBatchOwnership) {
  for (vector_size_t size : {0, 1, 9}) {
    SCOPED_TRACE(size);
    auto column = BaseVector::createNullConstant(UNKNOWN(), size, pool());
    auto row = makeRowVector({column});
    auto batch = std::make_shared<gluten::VeloxColumnarBatch>(row);
    auto store = gluten::ObjectStore::create();
    auto handle = store->save(std::static_pointer_cast<gluten::ColumnarBatch>(batch));
    const auto baseline = gluten::veloxColumnHandleLiveCount().load(std::memory_order_relaxed);
    auto descriptors = gluten::exportBatchColumns(handle);
    auto repeated = gluten::exportBatchColumns(handle);
    ASSERT_EQ(descriptors.size(), 1);
    ASSERT_EQ(repeated.size(), 1);
    EXPECT_EQ(descriptors[0]->length, size);
    EXPECT_EQ(descriptors[0]->nullCount, size);
    EXPECT_EQ(descriptors[0]->buffers[1], nullptr);
    EXPECT_EQ(descriptors[0]->buffers[0], repeated[0]->buffers[0]);
    EXPECT_EQ(row->childAt(0).get(), column.get());
    EXPECT_EQ(column->encoding(), VectorEncoding::Simple::CONSTANT);
    batch.reset();
    row.reset();
    column.reset();
    for (vector_size_t i = 0; i < size; ++i) {
      EXPECT_TRUE(bits::isBitNull(static_cast<const uint64_t*>(descriptors[0]->buffers[0]), i));
    }
    gluten::freeColumnHandleTree(descriptors[0]);
    gluten::freeColumnHandleTree(repeated[0]);
    EXPECT_EQ(gluten::veloxColumnHandleLiveCount().load(std::memory_order_relaxed), baseline);
    gluten::ObjectStore::release(handle);
  }
}

TEST_F(VeloxColumnHandleBatchTest, ConstantCalendarIntervalOutputsKeepBatchOwnership) {
  const auto expected = CalendarInterval(-14, 40, -86400000001LL).pack();
  for (vector_size_t size : {0, 1, 3}) {
    for (bool isNull : {false, true}) {
      SCOPED_TRACE("size=" + std::to_string(size) + ", null=" + std::to_string(isNull));
      auto base = makeNullableFlatVector<int128_t>(
          {isNull ? std::optional<int128_t>{} : std::optional<int128_t>{expected}}, CALENDAR_INTERVAL());
      auto column = BaseVector::wrapInConstant(size, 0, base);
      auto row = makeRowVector({column});
      auto batch = std::make_shared<gluten::VeloxColumnarBatch>(row);
      auto store = gluten::ObjectStore::create();
      auto handle = store->save(std::static_pointer_cast<gluten::ColumnarBatch>(batch));
      const auto baseline = gluten::veloxColumnHandleLiveCount().load(std::memory_order_relaxed);
      auto descriptors = gluten::exportBatchColumns(handle);
      auto repeated = gluten::exportBatchColumns(handle);
      ASSERT_EQ(descriptors.size(), 1);
      ASSERT_EQ(repeated.size(), 1);
      EXPECT_EQ(descriptors[0]->length, size);
      EXPECT_EQ(descriptors[0]->nChildren, 0);
      EXPECT_EQ(descriptors[0]->buffers[1], repeated[0]->buffers[1]);
      EXPECT_TRUE(batch->getFlattenedRowVector()->childAt(0)->type()->isCalendarInterval());
      EXPECT_EQ(row->childAt(0).get(), column.get());
      EXPECT_EQ(column->encoding(), VectorEncoding::Simple::CONSTANT);
      batch.reset();
      row.reset();
      column.reset();
      base.reset();
      for (vector_size_t i = 0; i < size; ++i) {
        if (isNull) {
          EXPECT_TRUE(bits::isBitNull(static_cast<const uint64_t*>(descriptors[0]->buffers[0]), i));
        } else {
          EXPECT_EQ(static_cast<const int128_t*>(descriptors[0]->buffers[1])[i], expected);
        }
      }
      gluten::freeColumnHandleTree(descriptors[0]);
      gluten::freeColumnHandleTree(repeated[0]);
      EXPECT_EQ(gluten::veloxColumnHandleLiveCount().load(std::memory_order_relaxed), baseline);
      gluten::ObjectStore::release(handle);
    }
  }
}
