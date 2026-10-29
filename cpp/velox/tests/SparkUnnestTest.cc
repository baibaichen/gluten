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

#include "operators/plannodes/SparkUnnest.h"
#include <gtest/gtest.h>
#include <optional>
#include "velox/common/memory/Memory.h"
#include "velox/core/PlanNode.h"
#include "velox/core/QueryCtx.h"
#include "velox/exec/Task.h"
#include "velox/vector/ComplexVector.h"
#include "velox/vector/DecodedVector.h"
#include "velox/vector/FlatVector.h"

namespace gluten {
namespace {
using namespace facebook::velox;

class SparkUnnestTest : public ::testing::Test {
 protected:
  static void SetUpTestSuite() {
    memory::MemoryManager::testingSetInstance(memory::MemoryManager::Options{});
    exec::Operator::registerOperator(std::make_unique<SparkUnnestTranslator>());
  }

  void SetUp() override {
    pool_ = memory::memoryManager()->addLeafPool("spark-unnest-test");
  }

  std::vector<RowVectorPtr> runPlan(const core::PlanNodePtr& plan, int32_t maxBatchRows) {
    auto query = core::QueryCtx::create(
        nullptr,
        core::QueryConfig{
            {{core::QueryConfig::kPreferredOutputBatchRows, std::to_string(maxBatchRows)},
             {core::QueryConfig::kUnnestSplitOutput, "true"}}});
    auto task = exec::Task::create(
        "spark-unnest-task-" + std::to_string(taskCounter_++),
        core::PlanFragment{plan},
        0,
        std::move(query),
        exec::Task::ExecutionMode::kSerial);
    std::vector<RowVectorPtr> results;
    while (!task->isFinished()) {
      auto future = ContinueFuture::makeEmpty();
      auto output = task->next(&future);
      if (future.valid()) {
        std::move(future).wait();
        continue;
      }
      if (!output) {
        break;
      }
      EXPECT_GT(output->size(), 0);
      EXPECT_LE(output->size(), maxBatchRows);
      auto copied = BaseVector::create<RowVector>(output->type(), output->size(), pool_.get());
      copied->copy(output.get(), 0, 0, output->size());
      results.push_back(std::move(copied));
    }
    return results;
  }

  ArrayVectorPtr makeIntArrayVector(const std::vector<std::optional<std::vector<std::optional<int32_t>>>>& rows) {
    std::vector<std::optional<int32_t>> flatElements;
    for (const auto& row : rows) {
      if (row.has_value()) {
        flatElements.insert(flatElements.end(), row->begin(), row->end());
      }
    }
    auto elements = BaseVector::create<FlatVector<int32_t>>(INTEGER(), flatElements.size(), pool_.get());
    for (size_t i = 0; i < flatElements.size(); ++i) {
      if (flatElements[i].has_value()) {
        elements->set(i, *flatElements[i]);
      } else {
        elements->setNull(i, true);
      }
    }
    auto array = BaseVector::create<ArrayVector>(ARRAY(INTEGER()), rows.size(), pool_.get());
    array->setElements(elements);
    vector_size_t offset = 0;
    for (size_t r = 0; r < rows.size(); ++r) {
      if (!rows[r].has_value()) {
        array->setOffsetAndSize(r, offset, 0);
        array->setNull(r, true);
      } else {
        const auto len = static_cast<vector_size_t>(rows[r]->size());
        array->setOffsetAndSize(r, offset, len);
        offset += len;
      }
    }
    return array;
  }

  std::shared_ptr<memory::MemoryPool> pool_;
  inline static int taskCounter_ = 0;
};

TEST_F(SparkUnnestTest, innerArrayContiguousAndEmptyGaps) {
  auto ids = BaseVector::create<FlatVector<int64_t>>(BIGINT(), 5, pool_.get());
  for (int i = 0; i < 5; ++i) {
    ids->set(i, 100 + i);
  }
  auto arrays = makeIntArrayVector({
      std::nullopt,
      std::vector<std::optional<int32_t>>{},
      std::vector<std::optional<int32_t>>{10, std::nullopt, 30},
      std::vector<std::optional<int32_t>>{},
      std::vector<std::optional<int32_t>>{40, 50},
  });
  auto input = std::make_shared<RowVector>(
      pool_.get(), ROW({"id", "a"}, {BIGINT(), ARRAY(INTEGER())}), nullptr, 5, std::vector<VectorPtr>{ids, arrays});
  auto source = std::make_shared<core::ValuesNode>("values", std::vector<RowVectorPtr>{input});
  auto repId = std::make_shared<core::FieldAccessTypedExpr>(BIGINT(), "id");
  auto unnestA = std::make_shared<core::FieldAccessTypedExpr>(ARRAY(INTEGER()), "a");
  auto plan = std::make_shared<SparkUnnestNode>(
      "spark-unnest",
      std::vector<core::FieldAccessTypedExprPtr>{repId},
      std::vector<core::FieldAccessTypedExprPtr>{unnestA},
      /*withOrdinality=*/false,
      /*isOuter=*/false,
      /*inlineStruct=*/false,
      ROW({"id", "col"}, {BIGINT(), INTEGER()}),
      source);

  const std::vector<std::pair<int64_t, std::optional<int32_t>>> expected{
      {102, 10},
      {102, std::nullopt},
      {102, 30},
      {104, 40},
      {104, 50},
  };

  for (int32_t batchSize : {1, 2, 3, 10}) {
    auto batches = runPlan(plan, batchSize);
    size_t idx = 0;
    for (const auto& batch : batches) {
      DecodedVector idDec(*batch->childAt(0));
      DecodedVector colDec(*batch->childAt(1));
      for (vector_size_t r = 0; r < batch->size(); ++r) {
        ASSERT_LT(idx, expected.size());
        EXPECT_EQ(idDec.valueAt<int64_t>(r), expected[idx].first);
        EXPECT_EQ(colDec.isNullAt(r), !expected[idx].second.has_value());
        if (expected[idx].second.has_value()) {
          EXPECT_EQ(colDec.valueAt<int32_t>(r), *expected[idx].second);
        }
        ++idx;
      }
    }
    EXPECT_EQ(idx, expected.size());
  }
}

TEST_F(SparkUnnestTest, innerMapExpansion) {
  auto keys = BaseVector::create<FlatVector<int32_t>>(INTEGER(), 3, pool_.get());
  auto vals = BaseVector::create<FlatVector<int64_t>>(BIGINT(), 3, pool_.get());
  keys->set(0, 1);
  keys->set(1, 2);
  keys->set(2, 3);
  vals->set(0, 10);
  vals->setNull(1, true);
  vals->set(2, 30);

  auto mapVec = BaseVector::create<MapVector>(MAP(INTEGER(), BIGINT()), 3, pool_.get());
  mapVec->setKeysAndValues(keys, vals);
  mapVec->setOffsetAndSize(0, 0, 2);
  mapVec->setOffsetAndSize(1, 2, 0);
  mapVec->setNull(1, true);
  mapVec->setOffsetAndSize(2, 2, 1);

  auto input = std::make_shared<RowVector>(
      pool_.get(), ROW({"m"}, {MAP(INTEGER(), BIGINT())}), nullptr, 3, std::vector<VectorPtr>{mapVec});
  auto source = std::make_shared<core::ValuesNode>("values", std::vector<RowVectorPtr>{input});
  auto unnestM = std::make_shared<core::FieldAccessTypedExpr>(MAP(INTEGER(), BIGINT()), "m");
  auto plan = std::make_shared<SparkUnnestNode>(
      "spark-unnest",
      std::vector<core::FieldAccessTypedExprPtr>{},
      std::vector<core::FieldAccessTypedExprPtr>{unnestM},
      /*withOrdinality=*/false,
      /*isOuter=*/false,
      /*inlineStruct=*/false,
      ROW({"key", "value"}, {INTEGER(), BIGINT()}),
      source);

  auto batches = runPlan(plan, 2);
  ASSERT_EQ(batches.size(), 2);
  EXPECT_EQ(batches[0]->size(), 2);
  EXPECT_EQ(batches[1]->size(), 1);
}

TEST_F(SparkUnnestTest, posExplodeInnerAndOuterAndNullsWritable) {
  auto ids = BaseVector::create<FlatVector<int64_t>>(BIGINT(), 4, pool_.get());
  for (int i = 0; i < 4; ++i) {
    ids->set(i, i + 1);
  }
  auto arrays = makeIntArrayVector({
      std::nullopt,
      std::vector<std::optional<int32_t>>{},
      std::vector<std::optional<int32_t>>{std::nullopt, 7},
      std::vector<std::optional<int32_t>>{8, 9, 10},
  });
  auto input = std::make_shared<RowVector>(
      pool_.get(), ROW({"id", "a"}, {BIGINT(), ARRAY(INTEGER())}), nullptr, 4, std::vector<VectorPtr>{ids, arrays});
  auto source = std::make_shared<core::ValuesNode>("values", std::vector<RowVectorPtr>{input});
  auto repId = std::make_shared<core::FieldAccessTypedExpr>(BIGINT(), "id");
  auto unnestA = std::make_shared<core::FieldAccessTypedExpr>(ARRAY(INTEGER()), "a");

  for (bool isOuter : {false, true}) {
    auto plan = std::make_shared<SparkUnnestNode>(
        "spark-unnest",
        std::vector<core::FieldAccessTypedExprPtr>{repId},
        std::vector<core::FieldAccessTypedExprPtr>{unnestA},
        /*withOrdinality=*/true,
        isOuter,
        /*inlineStruct=*/false,
        ROW({"id", "pos", "col"}, {BIGINT(), INTEGER(), INTEGER()}),
        source);

    struct RowOut {
      int64_t id;
      std::optional<int32_t> pos;
      std::optional<int32_t> col;
    };
    std::vector<RowOut> expected;
    if (isOuter) {
      expected.push_back({1, std::nullopt, std::nullopt});
      expected.push_back({2, std::nullopt, std::nullopt});
    }
    expected.push_back({3, 0, std::nullopt});
    expected.push_back({3, 1, 7});
    expected.push_back({4, 0, 8});
    expected.push_back({4, 1, 9});
    expected.push_back({4, 2, 10});

    for (int32_t batchSize : {2, 3, 10}) {
      auto batches = runPlan(plan, batchSize);
      size_t idx = 0;
      for (const auto& batch : batches) {
        ASSERT_EQ(batch->childrenSize(), 3);
        // Verify pos and col never share the same nulls BufferPtr and pos nulls are writable.
        if (batch->childAt(1)->nulls() && batch->childAt(2)->nulls()) {
          EXPECT_NE(batch->childAt(1)->nulls().get(), batch->childAt(2)->nulls().get());
        }
        if (batch->childAt(1)->nulls()) {
          EXPECT_TRUE(batch->childAt(1)->isNullsWritable());
        }
        DecodedVector idDec(*batch->childAt(0));
        DecodedVector posDec(*batch->childAt(1));
        DecodedVector colDec(*batch->childAt(2));
        for (vector_size_t r = 0; r < batch->size(); ++r) {
          ASSERT_LT(idx, expected.size());
          EXPECT_EQ(idDec.valueAt<int64_t>(r), expected[idx].id);
          EXPECT_EQ(posDec.isNullAt(r), !expected[idx].pos.has_value());
          if (expected[idx].pos.has_value()) {
            EXPECT_EQ(posDec.valueAt<int32_t>(r), *expected[idx].pos);
          }
          EXPECT_EQ(colDec.isNullAt(r), !expected[idx].col.has_value());
          if (expected[idx].col.has_value()) {
            EXPECT_EQ(colDec.valueAt<int32_t>(r), *expected[idx].col);
          }
          ++idx;
        }
      }
      EXPECT_EQ(idx, expected.size());
    }
  }
}

TEST_F(SparkUnnestTest, multiColumnStackInnerAndOuter) {
  auto a0 = makeIntArrayVector({
      std::vector<std::optional<int32_t>>{1, 2},
      std::vector<std::optional<int32_t>>{},
  });
  auto a1 = makeIntArrayVector({
      std::vector<std::optional<int32_t>>{10},
      std::vector<std::optional<int32_t>>{},
  });
  auto input = std::make_shared<RowVector>(
      pool_.get(), ROW({"a0", "a1"}, {ARRAY(INTEGER()), ARRAY(INTEGER())}), nullptr, 2, std::vector<VectorPtr>{a0, a1});
  auto source = std::make_shared<core::ValuesNode>("values", std::vector<RowVectorPtr>{input});
  auto u0 = std::make_shared<core::FieldAccessTypedExpr>(ARRAY(INTEGER()), "a0");
  auto u1 = std::make_shared<core::FieldAccessTypedExpr>(ARRAY(INTEGER()), "a1");
  auto plan = std::make_shared<SparkUnnestNode>(
      "spark-unnest",
      std::vector<core::FieldAccessTypedExprPtr>{},
      std::vector<core::FieldAccessTypedExprPtr>{u0, u1},
      /*withOrdinality=*/false,
      /*isOuter=*/true,
      /*inlineStruct=*/false,
      ROW({"col0", "col1"}, {INTEGER(), INTEGER()}),
      source);

  auto batches = runPlan(plan, 2);
  std::vector<std::pair<std::optional<int32_t>, std::optional<int32_t>>> actual;
  for (const auto& batch : batches) {
    DecodedVector c0(*batch->childAt(0));
    DecodedVector c1(*batch->childAt(1));
    for (vector_size_t r = 0; r < batch->size(); ++r) {
      actual.emplace_back(
          c0.isNullAt(r) ? std::nullopt : std::make_optional(c0.valueAt<int32_t>(r)),
          c1.isNullAt(r) ? std::nullopt : std::make_optional(c1.valueAt<int32_t>(r)));
    }
  }
  const std::vector<std::pair<std::optional<int32_t>, std::optional<int32_t>>> expected{
      {1, 10},
      {2, std::nullopt},
      {std::nullopt, std::nullopt},
  };
  EXPECT_EQ(actual, expected);
}

TEST_F(SparkUnnestTest, inlineContiguousAndMixedNullStructs) {
  auto f0 = BaseVector::create<FlatVector<int32_t>>(INTEGER(), 3, pool_.get());
  auto f1 = BaseVector::create<FlatVector<int64_t>>(BIGINT(), 3, pool_.get());
  f0->set(0, 1);
  f0->set(1, 2);
  f0->setNull(2, true);
  f1->set(0, 100);
  f1->set(1, 200);
  f1->set(2, 300);

  auto structType = ROW({"x", "y"}, {INTEGER(), BIGINT()});
  auto structVec = std::make_shared<RowVector>(pool_.get(), structType, nullptr, 3, std::vector<VectorPtr>{f0, f1});
  structVec->setNull(1, true); // Second struct element is NULL.

  auto arrayVec = BaseVector::create<ArrayVector>(ARRAY(structType), 3, pool_.get());
  arrayVec->setElements(structVec);
  arrayVec->setOffsetAndSize(0, 0, 2);
  arrayVec->setOffsetAndSize(1, 2, 0);
  arrayVec->setNull(1, true);
  arrayVec->setOffsetAndSize(2, 2, 1);

  auto input = std::make_shared<RowVector>(
      pool_.get(), ROW({"s"}, {ARRAY(structType)}), nullptr, 3, std::vector<VectorPtr>{arrayVec});
  auto source = std::make_shared<core::ValuesNode>("values", std::vector<RowVectorPtr>{input});
  auto unnestS = std::make_shared<core::FieldAccessTypedExpr>(ARRAY(structType), "s");

  auto plan = std::make_shared<SparkUnnestNode>(
      "spark-unnest",
      std::vector<core::FieldAccessTypedExprPtr>{},
      std::vector<core::FieldAccessTypedExprPtr>{unnestS},
      /*withOrdinality=*/false,
      /*isOuter=*/true,
      /*inlineStruct=*/true,
      structType,
      source);

  auto batches = runPlan(plan, 2);
  std::vector<std::pair<std::optional<int32_t>, std::optional<int64_t>>> actual;
  for (const auto& batch : batches) {
    DecodedVector xDec(*batch->childAt(0));
    DecodedVector yDec(*batch->childAt(1));
    for (vector_size_t r = 0; r < batch->size(); ++r) {
      actual.emplace_back(
          xDec.isNullAt(r) ? std::nullopt : std::make_optional(xDec.valueAt<int32_t>(r)),
          yDec.isNullAt(r) ? std::nullopt : std::make_optional(yDec.valueAt<int64_t>(r)));
    }
  }
  const std::vector<std::pair<std::optional<int32_t>, std::optional<int64_t>>> expected{
      {1, 100},
      {std::nullopt, std::nullopt}, // NULL struct element
      {std::nullopt, std::nullopt}, // outer padding row
      {std::nullopt, 300}, // struct with NULL field x
  };
  EXPECT_EQ(actual, expected);
}

TEST_F(SparkUnnestTest, inlineDictionaryEncodedStructAndEmptyStruct) {
  // 1. Dictionary-encoded RowVector with a ConstantVector child field.
  auto f0 = BaseVector::createConstant(INTEGER(), 42, 2, pool_.get());
  auto structType = ROW({"c"}, {INTEGER()});
  auto baseRow = std::make_shared<RowVector>(pool_.get(), structType, nullptr, 2, std::vector<VectorPtr>{f0});
  auto dictIndices = allocateIndices(3, pool_.get());
  auto* rawDictIndices = dictIndices->asMutable<vector_size_t>();
  rawDictIndices[0] = 0;
  rawDictIndices[1] = 1;
  rawDictIndices[2] = 0;
  auto dictNulls = allocateNulls(3, pool_.get());
  bits::setNull(dictNulls->asMutable<uint64_t>(), 1, true);
  auto dictElements = BaseVector::wrapInDictionary(dictNulls, dictIndices, 3, baseRow);

  auto arrayVec = BaseVector::create<ArrayVector>(ARRAY(structType), 1, pool_.get());
  arrayVec->setElements(dictElements);
  arrayVec->setOffsetAndSize(0, 0, 3);

  auto input = std::make_shared<RowVector>(
      pool_.get(), ROW({"s"}, {ARRAY(structType)}), nullptr, 1, std::vector<VectorPtr>{arrayVec});
  auto source = std::make_shared<core::ValuesNode>("values", std::vector<RowVectorPtr>{input});
  auto unnestS = std::make_shared<core::FieldAccessTypedExpr>(ARRAY(structType), "s");
  auto plan = std::make_shared<SparkUnnestNode>(
      "spark-unnest",
      std::vector<core::FieldAccessTypedExprPtr>{},
      std::vector<core::FieldAccessTypedExprPtr>{unnestS},
      /*withOrdinality=*/false,
      /*isOuter=*/false,
      /*inlineStruct=*/true,
      structType,
      source);

  auto batches = runPlan(plan, 2);
  std::vector<std::optional<int32_t>> actual;
  for (const auto& batch : batches) {
    DecodedVector cDec(*batch->childAt(0));
    for (vector_size_t r = 0; r < batch->size(); ++r) {
      actual.push_back(cDec.isNullAt(r) ? std::nullopt : std::make_optional(cDec.valueAt<int32_t>(r)));
    }
  }
  const std::vector<std::optional<int32_t>> expected{42, std::nullopt, 42};
  EXPECT_EQ(actual, expected);

  // 2. 0-field ROW<> preserves cardinality for replicated columns.
  auto emptyRowType = ROW({}, {});
  auto emptyStructElements =
      std::make_shared<RowVector>(pool_.get(), emptyRowType, nullptr, 3, std::vector<VectorPtr>{});
  auto emptyArrayVec = BaseVector::create<ArrayVector>(ARRAY(emptyRowType), 2, pool_.get());
  emptyArrayVec->setElements(emptyStructElements);
  emptyArrayVec->setOffsetAndSize(0, 0, 2);
  emptyArrayVec->setOffsetAndSize(1, 2, 1);
  auto idCol = BaseVector::create<FlatVector<int64_t>>(BIGINT(), 2, pool_.get());
  idCol->set(0, 7);
  idCol->set(1, 8);
  auto emptyInput = std::make_shared<RowVector>(
      pool_.get(),
      ROW({"id", "s"}, {BIGINT(), ARRAY(emptyRowType)}),
      nullptr,
      2,
      std::vector<VectorPtr>{idCol, emptyArrayVec});
  auto emptySource = std::make_shared<core::ValuesNode>("values2", std::vector<RowVectorPtr>{emptyInput});
  auto repId = std::make_shared<core::FieldAccessTypedExpr>(BIGINT(), "id");
  auto unnestEmpty = std::make_shared<core::FieldAccessTypedExpr>(ARRAY(emptyRowType), "s");
  auto emptyPlan = std::make_shared<SparkUnnestNode>(
      "spark-unnest-empty",
      std::vector<core::FieldAccessTypedExprPtr>{repId},
      std::vector<core::FieldAccessTypedExprPtr>{unnestEmpty},
      /*withOrdinality=*/false,
      /*isOuter=*/false,
      /*inlineStruct=*/true,
      ROW({"id"}, {BIGINT()}),
      emptySource);
  auto emptyBatches = runPlan(emptyPlan, 2);
  std::vector<int64_t> actualIds;
  for (const auto& batch : emptyBatches) {
    DecodedVector idDec(*batch->childAt(0));
    for (vector_size_t r = 0; r < batch->size(); ++r) {
      actualIds.push_back(idDec.valueAt<int64_t>(r));
    }
  }
  const std::vector<int64_t> expectedIds{7, 7, 8};
  EXPECT_EQ(actualIds, expectedIds);
}

} // namespace
} // namespace gluten
