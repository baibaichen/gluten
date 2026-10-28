# Task 1: `SparkUnnestNode` 骨架、单列 `Array / Map` 连续快路径与 C++ UT

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 创建 `SparkUnnestNode`、`SparkUnnest`、`SparkUnnestTranslator` 的基础实现，完成保留列复制（含单行 `ConstantVector` 快路径）与单列 `Array / Map` 的 `inner` 展开（含 `forEachNotNull` 扫描、`contiguousOutputEnd_` O(1) 范围提取与 `base->slice` 零拷贝切片），并编写首个 C++ GoogleTest 套件 `SparkUnnestTest.cc` 验证。

**Architecture:** 在 `cpp/velox/operators/plannodes/` 下实现自定义 `PlanNode` 与 `Operator`，在 `VeloxBackend::init` 注册 `SparkUnnestTranslator`，并在 `cpp/velox/tests/CMakeLists.txt` 中通过 `add_velox_test(spark_unnest_test SOURCES SparkUnnestTest.cc)` 注册独立 C++ 单元测试。

**Tech Stack:** C++17, Velox (`core::PlanNode`, `exec::Operator`, `DecodedVector`), GoogleTest.

---

### Files

- Create: `cpp/velox/operators/plannodes/SparkUnnest.h`
- Create: `cpp/velox/operators/plannodes/SparkUnnest.cc`
- Create: `cpp/velox/tests/SparkUnnestTest.cc`
- Modify: `cpp/velox/CMakeLists.txt:185`
- Modify: `cpp/velox/compute/VeloxBackend.cc:195-210`
- Modify: `cpp/velox/tests/CMakeLists.txt:135`

---

- [ ] **Step 1: 创建 `cpp/velox/operators/plannodes/SparkUnnest.h`**

```cpp
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

#pragma once

#include <optional>
#include "velox/core/PlanNode.h"
#include "velox/exec/Operator.h"

namespace gluten {

class SparkUnnestNode final : public facebook::velox::core::PlanNode {
 public:
  SparkUnnestNode(
      const facebook::velox::core::PlanNodeId& id,
      std::vector<facebook::velox::core::FieldAccessTypedExprPtr> replicateVariables,
      std::vector<facebook::velox::core::FieldAccessTypedExprPtr> unnestVariables,
      bool withOrdinality,
      bool isOuter,
      bool inlineStruct,
      facebook::velox::RowTypePtr outputType,
      facebook::velox::core::PlanNodePtr source);

  const facebook::velox::RowTypePtr& outputType() const override {
    return outputType_;
  }

  const std::vector<facebook::velox::core::PlanNodePtr>& sources() const override {
    return sources_;
  }

  const std::vector<facebook::velox::core::FieldAccessTypedExprPtr>& replicateVariables() const {
    return replicateVariables_;
  }

  const std::vector<facebook::velox::core::FieldAccessTypedExprPtr>& unnestVariables() const {
    return unnestVariables_;
  }

  bool withOrdinality() const {
    return withOrdinality_;
  }

  bool isOuter() const {
    return isOuter_;
  }

  bool inlineStruct() const {
    return inlineStruct_;
  }

  bool supportsBarrier() const override {
    return true;
  }

  std::string_view name() const override {
    return "SparkUnnest";
  }

  folly::dynamic serialize() const override {
    VELOX_UNSUPPORTED("SparkUnnestNode is not serializable");
  }

 private:
  void addDetails(std::stringstream& stream) const override;

  const std::vector<facebook::velox::core::FieldAccessTypedExprPtr> replicateVariables_;
  const std::vector<facebook::velox::core::FieldAccessTypedExprPtr> unnestVariables_;
  const bool withOrdinality_;
  const bool isOuter_;
  const bool inlineStruct_;
  const facebook::velox::RowTypePtr outputType_;
  const std::vector<facebook::velox::core::PlanNodePtr> sources_;
};

class SparkUnnest final : public facebook::velox::exec::Operator {
 public:
  SparkUnnest(
      int32_t operatorId,
      facebook::velox::exec::DriverCtx* driverCtx,
      const std::shared_ptr<const SparkUnnestNode>& unnestNode);

  bool needsInput() const override {
    return input_ == nullptr && !noMoreInput_;
  }

  void addInput(facebook::velox::RowVectorPtr input) override;

  facebook::velox::RowVectorPtr getOutput() override;

  facebook::velox::exec::BlockingReason isBlocked(facebook::velox::ContinueFuture* /*future*/) override {
    return facebook::velox::exec::BlockingReason::kNotBlocked;
  }

  bool isFinished() override;

 private:
  struct RowRange {
    facebook::velox::vector_size_t startInputRow{0};
    facebook::velox::vector_size_t numInputRows{0};
    std::optional<facebook::velox::vector_size_t> lastInnerRowEnd;
    facebook::velox::vector_size_t numInnerRows{0};
    bool hasEmptyUnnestValue{false};

    template <typename Func>
    void forEachRow(
        Func&& func,
        const facebook::velox::vector_size_t* rawMaxSizes,
        facebook::velox::vector_size_t firstInnerRowStart) const {
      const auto firstInnerRowEnd =
          numInputRows == 1 && lastInnerRowEnd.has_value() ? lastInnerRowEnd.value() : rawMaxSizes[startInputRow];
      func(startInputRow, firstInnerRowStart, firstInnerRowEnd - firstInnerRowStart);
      const auto lastInputRow = startInputRow + numInputRows - 1;
      for (auto inputRow = startInputRow + 1; inputRow < lastInputRow; ++inputRow) {
        func(inputRow, 0, rawMaxSizes[inputRow]);
      }
      if (numInputRows > 1) {
        func(lastInputRow, 0, lastInnerRowEnd.value_or(rawMaxSizes[lastInputRow]));
      }
    }
  };

  struct ChannelEncoding {
    facebook::velox::BufferPtr indices;
    facebook::velox::BufferPtr nulls;
    bool contiguousMapping{false};
    facebook::velox::vector_size_t sourceStart{0};
  };

  RowRange extractRowRange(facebook::velox::vector_size_t inputSize) const;
  void finishInput();
  facebook::velox::RowVectorPtr generateOutput(const RowRange& range);
  void generateRepeatedColumns(const RowRange& range, std::vector<facebook::velox::VectorPtr>& outputs);
  facebook::velox::VectorPtr generateOrdinalityVector(const RowRange& range);
  ChannelEncoding generateEncodingForChannel(facebook::velox::column_index_t channel, const RowRange& range);
  facebook::velox::VectorPtr wrapSingleColumn(
      const ChannelEncoding& encoding,
      const facebook::velox::VectorPtr& base,
      facebook::velox::vector_size_t wrapSize) const;
  void appendInlineStructColumns(
      const ChannelEncoding& encoding,
      const facebook::velox::VectorPtr& elementsVector,
      facebook::velox::vector_size_t wrapSize,
      facebook::velox::column_index_t& outputColumnIndex,
      std::vector<facebook::velox::VectorPtr>& outputs);

  const bool withOrdinality_;
  const bool isOuter_;
  const bool inlineStruct_;
  const facebook::velox::vector_size_t maxOutputSize_;

  std::vector<facebook::velox::column_index_t> unnestChannels_;
  std::vector<facebook::velox::DecodedVector> unnestDecoded_;

  facebook::velox::BufferPtr maxSizes_;
  facebook::velox::vector_size_t* rawMaxSizes_{nullptr};

  std::vector<const facebook::velox::vector_size_t*> rawSizes_;
  std::vector<const facebook::velox::vector_size_t*> rawOffsets_;
  std::vector<const facebook::velox::vector_size_t*> rawIndices_;

  bool singleContiguousInput_{false};
  facebook::velox::vector_size_t contiguousOutputOffset_{0};
  int64_t contiguousOutputEnd_{0};

  facebook::velox::vector_size_t nextInputRow_{0};
  facebook::velox::vector_size_t firstInnerRowStart_{0};
};

class SparkUnnestTranslator final : public facebook::velox::exec::Operator::PlanNodeTranslator {
 public:
  std::unique_ptr<facebook::velox::exec::Operator> toOperator(
      facebook::velox::exec::DriverCtx* ctx,
      int32_t id,
      const facebook::velox::core::PlanNodePtr& node) override;
};

} // namespace gluten
```

- [ ] **Step 2: 创建 `cpp/velox/operators/plannodes/SparkUnnest.cc`（实现单列 `Array / Map` 的 `inner` 展开与保留列生成）**

1. **`addInput(RowVectorPtr input)`**：
   - 调用 `child->loadedVector()` 并用 `unnestDecoded_[channel].decode(*child)` 解码；
   - 对单列 `!isOuter_ && !inlineStruct_` 且底层 `elements`（或 `mapKeys`/`mapValues`）支持紧凑 `slice` 的输入，使用 `currentDecoded.forEachNotNull(0, size, ...)` 扫描每行长度 `rawMaxSizes_[row] = length`，同时校验非零长行的 `offsets[index]` 是否首尾相接（注意：`length == 0` 或 `NULL` 的行在 `inner` 模式下输出 0 行，不破坏底层元素连续性）；
   - 记录 `singleContiguousInput_`、`contiguousOutputOffset_` 与 `contiguousOutputEnd_`。
2. **`extractRowRange(vector_size_t inputSize)`**：
   - 若 `singleContiguousInput_` 且 `contiguousOutputEnd_ - contiguousOutputOffset_ <= maxOutputSize_`，直接 O(1) 返回 `{nextInputRow_, inputSize - nextInputRow_, std::nullopt, remaining, false}`；
   - 否则按 `maxOutputSize_` 累加 `rawMaxSizes_[row]` 切分批次（支持单行超大数组跨多个输出 batch 切分，用 `firstInnerRowStart_` 与 `lastInnerRowEnd` 记录行内偏移）。
3. **`generateRepeatedColumns(range, outputs)`**：
   - 若 `identityProjections_.empty()`，直接返回；
   - 若 `range.numInputRows == 1`，直接对每个保留列调 `BaseVector::wrapInConstant(range.numInnerRows, range.startInputRow, input_->childAt(proj.inputChannel))`；
   - 若 `range.numInputRows > 1`，分配 `repeatedIndices` Buffer 并填充每行重复索引，各保留列共享该 `repeatedIndices` 调 `BaseVector::wrapInDictionary(nullptr, repeatedIndices, range.numInnerRows, ...)`。
4. **`generateEncodingForChannel` 与 `wrapSingleColumn`（单列 `inner`）**：
   - 当 `singleContiguousInput_`（或当前 `RowRange` 内 `contiguousMapping == true`）时，直接调 `base->slice(sourceStart, wrapSize)` 零拷贝返回；
   - 非连续映射时，若 `base->isFlatEncoding()`，直接创建目标 `FlatVector` 并调用 `flat->copy(base.get(), rows, rawIndices)`；若 `base->size() <= wrapSize && supportsCompactSlice(base)`，保留 `BaseVector::wrapInDictionary`。

- [ ] **Step 3: 注册到 `cpp/velox/CMakeLists.txt` 与 `cpp/velox/compute/VeloxBackend.cc`**

在 `cpp/velox/CMakeLists.txt` 的 `operators/plannodes/RowVectorStream.cc` 下新增：
```cmake
    operators/plannodes/SparkUnnest.cc
```
在 `cpp/velox/compute/VeloxBackend.cc` 的 `VeloxBackend::init` 中注册：
```cpp
  velox::exec::Operator::registerOperator(std::make_unique<SparkUnnestTranslator>());
```

- [ ] **Step 4: 创建 C++ 单测 `cpp/velox/tests/SparkUnnestTest.cc` 并注册到 `cpp/velox/tests/CMakeLists.txt`**

在 `cpp/velox/tests/CMakeLists.txt` 中新增：
```cmake
add_velox_test(spark_unnest_test SOURCES SparkUnnestTest.cc)
```
在 `cpp/velox/tests/SparkUnnestTest.cc` 中编写首批测试：
- `TEST_F(SparkUnnestTest, innerArrayContiguousAndEmptyGaps)`：验证跳过空数组 `[]` 与 `NULL` 数组后仍走连续 `slice` 快路径，且在 `preferredOutputBatchRows = 2` 下跨 batch 切分正确；
- `TEST_F(SparkUnnestTest, innerMapAndSingleRowConstantReplication)`：验证 `MAP<INTEGER, VARCHAR>` 展开为 `(key, value)` 两列，以及单行长数组跨批切分时保留列走 `ConstantVector` 快路径。

- [ ] **Step 5: 编译并运行 `spark_unnest_test`**

```bash
export VCPKG_ROOT=/home/chang/SourceCode/gluten2/dev/vcpkg/.vcpkg
export VCPKG_TRIPLET=x64-linux-avx
export VCPKG_MANIFEST_DIR=/home/chang/SourceCode/gluten2/dev/vcpkg
cmake -B cpp/build -DBUILD_TESTS=ON
cmake --build cpp/build -j 4 --target velox spark_unnest_test
./cpp/build/velox/tests/spark_unnest_test
```
Expected: `spark_unnest_test` 编译成功且全部测试 `PASSED`。
