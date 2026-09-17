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

#include "compute/VeloxRuntime.h"

#include <mutex>

#include <gtest/gtest.h>
#include "compute/VeloxBackend.h"
#include "compute/VeloxExpressionEvaluator.h"
#include "config/VeloxConfig.h"
#include "memory.pb.h"
#include "memory/VeloxColumnarBatch.h"
#include "substrait/extended_expression.pb.h"
#include "threads/ThreadInitializer.h"
#include "velox/vector/tests/utils/VectorMaker.h"

namespace gluten {

namespace {
void ensureVeloxBackendCreated() {
  static std::once_flag initialized;
  std::call_once(initialized, [] { VeloxBackend::create(AllocationListener::noop(), {}); });
}
} // namespace

class DummyMemoryManager final : public MemoryManager {
 public:
  DummyMemoryManager(const std::string& kind) : MemoryManager(kind){};

  arrow::MemoryPool* defaultArrowMemoryPool() override {
    throw GlutenException("Not yet implemented");
  }
  std::shared_ptr<arrow::MemoryPool> getOrCreateArrowMemoryPool(const std::string& name) override {
    throw GlutenException("Not yet implemented");
  }
  const MemoryUsageStats collectMemoryUsageStats() const override {
    throw GlutenException("Not yet implemented");
  }
  const int64_t shrink(int64_t size) override {
    throw GlutenException("Not yet implemented");
  }
  void hold() override {
    throw GlutenException("Not yet implemented");
  }
};

inline static const std::string kDummyBackendKind{"dummy"};

class DummyThreadManager final : public ThreadManager {
 public:
  explicit DummyThreadManager(const std::string& kind) : ThreadManager(kind), initializer_(ThreadInitializer::noop()) {}

  ThreadInitializer* getThreadInitializer() override {
    return initializer_.get();
  }

 private:
  std::shared_ptr<ThreadInitializer> initializer_;
};

class DummyRuntime final : public Runtime {
 public:
  DummyRuntime(
      const std::string& kind,
      DummyMemoryManager* mm,
      ThreadManager* tm,
      const std::unordered_map<std::string, std::string>& conf)
      : Runtime(kind, mm, tm, conf) {}

  void parsePlan(const uint8_t* data, int32_t size) override {}

  void parseSplitInfo(const uint8_t* data, int32_t size, int32_t idx) override {}

  std::shared_ptr<ResultIterator> createResultIterator(
      const std::string& spillDir,
      const std::vector<std::shared_ptr<ResultIterator>>& inputs) override {
    auto resIter = std::make_unique<DummyResultIterator>();
    auto iter = std::make_shared<ResultIterator>(std::move(resIter));
    return iter;
  }

  void noMoreSplits(ResultIterator* iter) override {
    // Do nothing.
  }

  MemoryManager* memoryManager() override {
    throw GlutenException("Not yet implemented");
  }
  std::shared_ptr<ColumnarBatch> createOrGetEmptySchemaBatch(int32_t numRows) override {
    throw GlutenException("Not yet implemented");
  }
  std::shared_ptr<ColumnarToRowConverter> createColumnar2RowConverter(int64_t column2RowMemThreshold) override {
    throw GlutenException("Not yet implemented");
  }
  std::shared_ptr<RowToColumnarConverter> createRow2ColumnarConverter(struct ArrowSchema* cSchema) override {
    throw GlutenException("Not yet implemented");
  }
  std::shared_ptr<ShuffleWriter> createShuffleWriter(
      int32_t numPartitions,
      const std::shared_ptr<PartitionWriter>& partitionWriter,
      const std::shared_ptr<ShuffleWriterOptions>&) override {
    throw GlutenException("Not yet implemented");
  }
  Metrics* getMetrics(ColumnarBatchIterator* rawIter, int64_t exportNanos) override {
    static Metrics m(0, R"({"orderedNodeIds":[],"omittedNodeIds":[],"loadLazyVectorTime":0,"nodeStats":{}})");
    return &m;
  }
  std::shared_ptr<ShuffleReader> createShuffleReader(
      std::shared_ptr<arrow::Schema> schema,
      const std::shared_ptr<ShuffleReaderOptions>& options) override {
    throw GlutenException("Not yet implemented");
  }
  std::unique_ptr<ColumnarBatchSerializer> createColumnarBatchSerializer(struct ArrowSchema* cSchema) override {
    throw GlutenException("Not yet implemented");
  }
  std::shared_ptr<ColumnarBatch> select(std::shared_ptr<ColumnarBatch>, const std::vector<int32_t>&) override {
    throw GlutenException("Not yet implemented");
  }
  std::string planString(bool details, const std::unordered_map<std::string, std::string>& sessionConf) override {
    throw GlutenException("Not yet implemented");
  }

 private:
  class DummyResultIterator : public ColumnarBatchIterator {
   public:
    std::shared_ptr<ColumnarBatch> next() override {
      if (!hasNext_) {
        return nullptr;
      }
      hasNext_ = false;

      return gluten::createZeroColumnBatch(1);
    }

   private:
    bool hasNext_ = true;
  };
};

static Runtime* dummyRuntimeFactory(
    const std::string& kind,
    MemoryManager* mm,
    ThreadManager* tm,
    const std::unordered_map<std::string, std::string> conf) {
  return new DummyRuntime(kind, dynamic_cast<DummyMemoryManager*>(mm), tm, conf);
}

static void dummyRuntimeReleaser(Runtime* runtime) {
  delete runtime;
}

TEST(TestRuntime, CreateRuntime) {
  Runtime::registerFactory(kDummyBackendKind, dummyRuntimeFactory, dummyRuntimeReleaser);
  DummyMemoryManager mm(kDummyBackendKind);
  DummyThreadManager tm(kDummyBackendKind);
  auto runtime = Runtime::create(kDummyBackendKind, &mm, &tm);
  ASSERT_EQ(typeid(*runtime), typeid(DummyRuntime));
  Runtime::release(runtime);
}

TEST(TestRuntime, CreateVeloxRuntime) {
  ensureVeloxBackendCreated();
  auto mm = MemoryManager::create(kVeloxBackendKind, AllocationListener::noop());
  auto tm = ThreadManager::create(kVeloxBackendKind, ThreadInitializer::noop());
  auto runtime = Runtime::create(kVeloxBackendKind, mm, tm);
  ASSERT_EQ(typeid(*runtime), typeid(VeloxRuntime));
  Runtime::release(runtime);
  ThreadManager::release(tm);
}

TEST(TestRuntime, ExpressionInputsFromDifferentRuntime) {
  ensureVeloxBackendCreated();
  std::shared_ptr<MemoryManager> mm(
      MemoryManager::create(kVeloxBackendKind, AllocationListener::noop()), MemoryManager::release);
  std::shared_ptr<ThreadManager> tm(
      ThreadManager::create(kVeloxBackendKind, ThreadInitializer::noop()), ThreadManager::release);
  const std::unordered_map<std::string, std::string> conf = {{kSessionTimezone, "UTC"}};
  std::shared_ptr<Runtime> inputRuntime(Runtime::create(kVeloxBackendKind, mm.get(), tm.get(), conf), Runtime::release);
  std::shared_ptr<Runtime> expressionRuntime(
      Runtime::create(kVeloxBackendKind, mm.get(), tm.get(), conf), Runtime::release);
  auto* veloxRuntime = dynamic_cast<VeloxRuntime*>(expressionRuntime.get());
  ASSERT_NE(veloxRuntime, nullptr);
  auto* veloxMm = dynamic_cast<VeloxMemoryManager*>(mm.get());
  ASSERT_NE(veloxMm, nullptr);
  facebook::velox::test::VectorMaker maker(veloxMm->getLeafMemoryPool().get());

  for (auto withColumn : {false, true}) {
    ::substrait::ExtendedExpression expression;
    auto* schema = expression.mutable_base_schema();
    schema->mutable_struct_();
    auto* reference = expression.add_referred_expr();
    reference->add_output_names("result");
    if (withColumn) {
      schema->add_names("input");
      schema->mutable_struct_()->add_types()->mutable_string();
      auto* field = reference->mutable_expression()->mutable_selection();
      field->mutable_root_reference();
      field->mutable_direct_reference()->mutable_struct_field()->set_field(0);
    } else {
      reference->mutable_expression()->mutable_literal()->set_string("literal");
    }
    auto bytes = expression.SerializeAsString();
    auto evaluatorHandle = expressionRuntime->saveObject(
        veloxRuntime->compileExpression(reinterpret_cast<const uint8_t*>(bytes.data()), bytes.size()));
    auto evaluator = expressionRuntime->retrieveObject<VeloxExpressionEvaluator>(evaluatorHandle);
    EXPECT_ANY_THROW(inputRuntime->retrieveObject<VeloxExpressionEvaluator>(evaluatorHandle));
    EXPECT_ANY_THROW(ObjectStore::retrieveChecked<ColumnarBatch>(evaluatorHandle));

    for (auto size : {0, 3}) {
      std::shared_ptr<ColumnarBatch> input;
      if (withColumn) {
        input = std::make_shared<VeloxColumnarBatch>(maker.rowVector(
            {"source_name"},
            {maker.flatVector<std::string>(size, [](auto row) { return "row" + std::to_string(row); })}));
      } else {
        input = inputRuntime->createOrGetEmptySchemaBatch(size);
      }
      auto inputHandle = inputRuntime->saveObject(input);
      EXPECT_ANY_THROW(expressionRuntime->retrieveObject<ColumnarBatch>(inputHandle));
      auto output = evaluator->evaluate(ObjectStore::retrieveChecked<ColumnarBatch>(inputHandle));
      EXPECT_EQ(output->numRows(), size);
      EXPECT_EQ(output->numColumns(), 1);
      ObjectStore::release(inputHandle);
      input.reset();
      EXPECT_ANY_THROW(ObjectStore::retrieveChecked<ColumnarBatch>(inputHandle));
      auto* values =
          output->getRowVector()->childAt(0)->as<facebook::velox::SimpleVector<facebook::velox::StringView>>();
      for (auto row = 0; row < size; ++row) {
        EXPECT_EQ(values->valueAt(row).str(), withColumn ? "row" + std::to_string(row) : "literal");
      }
    }
    ObjectStore::release(evaluatorHandle);
    EXPECT_ANY_THROW(expressionRuntime->retrieveObject<VeloxExpressionEvaluator>(evaluatorHandle));
  }
}

TEST(TestRuntime, GetResultIterator) {
  DummyMemoryManager mm(kDummyBackendKind);
  DummyThreadManager tm(kDummyBackendKind);
  auto runtime =
      std::make_shared<DummyRuntime>(kDummyBackendKind, &mm, &tm, std::unordered_map<std::string, std::string>());
  auto iter = runtime->createResultIterator("/tmp/test-spill", {});
  runtime->noMoreSplits(iter.get());
  ASSERT_TRUE(iter->hasNext());
  auto next = iter->next();
  ASSERT_NE(next, nullptr);
  ASSERT_FALSE(iter->hasNext());
  next = iter->next();
  ASSERT_EQ(next, nullptr);
}

} // namespace gluten
