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
  facebook::velox::VectorPtr generateOrdinalityVector(
      const RowRange& range,
      const facebook::velox::BufferPtr& movablePaddingNulls);

  template <bool trackContiguity, bool deferIndices>
  ChannelEncoding generateEncodingForChannelImpl(facebook::velox::column_index_t channel, const RowRange& range);

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
  facebook::velox::column_index_t ordinalityOutputChannel_{0};

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
