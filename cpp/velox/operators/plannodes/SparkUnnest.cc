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
#include <numeric>
#include "velox/common/base/Nulls.h"
#include "velox/common/base/Portability.h"
#include "velox/exec/OperatorType.h"
#include "velox/vector/ComplexVector.h"
#include "velox/vector/FlatVector.h"

namespace gluten {
namespace {
using namespace facebook::velox;

bool supportsCompactSlice(const VectorPtr& vector) {
  switch (vector->encoding()) {
    case VectorEncoding::Simple::FLAT:
    case VectorEncoding::Simple::CONSTANT:
    case VectorEncoding::Simple::BIASED:
    case VectorEncoding::Simple::ARRAY:
    case VectorEncoding::Simple::MAP:
      return true;
    case VectorEncoding::Simple::ROW:
      for (const auto& child : vector->as<RowVector>()->children()) {
        if (child && !supportsCompactSlice(child)) {
          return false;
        }
      }
      return true;
    default:
      return false;
  }
}

BufferPtr cloneNulls(const BufferPtr& nulls, vector_size_t size, memory::MemoryPool* pool) {
  if (!nulls) {
    return nullptr;
  }
  auto copy = allocateNulls(size, pool);
  std::memcpy(copy->asMutable<uint8_t>(), nulls->as<uint8_t>(), bits::nbytes(size));
  return copy;
}
} // namespace

SparkUnnestNode::SparkUnnestNode(
    const core::PlanNodeId& id,
    std::vector<core::FieldAccessTypedExprPtr> replicateVariables,
    std::vector<core::FieldAccessTypedExprPtr> unnestVariables,
    bool withOrdinality,
    bool isOuter,
    bool inlineStruct,
    RowTypePtr outputType,
    core::PlanNodePtr source)
    : core::PlanNode(id),
      replicateVariables_(std::move(replicateVariables)),
      unnestVariables_(std::move(unnestVariables)),
      withOrdinality_(withOrdinality),
      isOuter_(isOuter),
      inlineStruct_(inlineStruct),
      outputType_(std::move(outputType)),
      sources_{std::move(source)} {
  VELOX_USER_CHECK(!unnestVariables_.empty(), "SparkUnnestNode requires at least one unnest variable.");
  if (inlineStruct_) {
    VELOX_USER_CHECK_EQ(unnestVariables_.size(), 1, "SparkUnnestNode inlineStruct requires a single unnest variable.");
    VELOX_USER_CHECK(unnestVariables_[0]->type()->isArray(), "SparkUnnestNode inlineStruct expects an ARRAY input.");
    VELOX_USER_CHECK(
        unnestVariables_[0]->type()->asArray().elementType()->isRow(),
        "SparkUnnestNode inlineStruct expects ARRAY<ROW> input.");
  }
}

void SparkUnnestNode::addDetails(std::stringstream& stream) const {
  for (size_t i = 0; i < unnestVariables_.size(); ++i) {
    if (i > 0) {
      stream << ", ";
    }
    stream << unnestVariables_[i]->name();
  }
  if (withOrdinality_) {
    stream << " withOrdinality";
  }
  if (isOuter_) {
    stream << " outer";
  }
  if (inlineStruct_) {
    stream << " inline";
  }
}

SparkUnnest::SparkUnnest(
    int32_t operatorId,
    exec::DriverCtx* driverCtx,
    const std::shared_ptr<const SparkUnnestNode>& unnestNode)
    : exec::Operator(driverCtx, unnestNode->outputType(), operatorId, unnestNode->id(), exec::OperatorType::kUnnest),
      withOrdinality_(unnestNode->withOrdinality()),
      isOuter_(unnestNode->isOuter()),
      inlineStruct_(unnestNode->inlineStruct()),
      maxOutputSize_(
          driverCtx->queryConfig().unnestSplitOutput() ? driverCtx->queryConfig().preferredOutputBatchRows()
                                                       : std::numeric_limits<vector_size_t>::max()) {
  VELOX_USER_CHECK_GT(maxOutputSize_, 0, "SparkUnnest requires positive output batch size.");
  const auto& inputType = unnestNode->sources()[0]->outputType();
  column_index_t outputChannel = 0;
  for (const auto& variable : unnestNode->replicateVariables()) {
    identityProjections_.emplace_back(inputType->getChildIdx(variable->name()), outputChannel++);
  }
  if (withOrdinality_) {
    ordinalityOutputChannel_ = outputChannel++;
    VELOX_CHECK_EQ(
        outputType_->childAt(ordinalityOutputChannel_), INTEGER(), "SparkUnnest ordinality column must be INTEGER.");
  }

  unnestChannels_.reserve(unnestNode->unnestVariables().size());
  for (const auto& variable : unnestNode->unnestVariables()) {
    unnestChannels_.push_back(inputType->getChildIdx(variable->name()));
    if (inlineStruct_) {
      const auto& rowType = variable->type()->asArray().elementType()->asRow();
      for (uint32_t i = 0; i < rowType.size(); ++i) {
        VELOX_CHECK_EQ(outputType_->childAt(outputChannel++), rowType.childAt(i));
      }
    } else if (variable->type()->isArray()) {
      VELOX_CHECK_EQ(outputType_->childAt(outputChannel++), variable->type()->asArray().elementType());
    } else if (variable->type()->isMap()) {
      const auto& mapType = variable->type()->asMap();
      VELOX_CHECK_EQ(outputType_->childAt(outputChannel++), mapType.keyType());
      VELOX_CHECK_EQ(outputType_->childAt(outputChannel++), mapType.valueType());
    } else {
      VELOX_FAIL("Unexpected unnest channel type: {}", variable->type()->toString());
    }
  }
  VELOX_CHECK_EQ(outputChannel, outputType_->size(), "SparkUnnest output schema mismatch.");

  unnestDecoded_.resize(unnestChannels_.size());
  rawSizes_.resize(unnestChannels_.size());
  rawOffsets_.resize(unnestChannels_.size());
  rawIndices_.resize(unnestChannels_.size());
}

void SparkUnnest::addInput(RowVectorPtr input) {
  input_ = std::move(input);
  singleContiguousInput_ = false;
  contiguousOutputOffset_ = 0;
  contiguousOutputEnd_ = 0;

  for (auto& child : input_->children()) {
    child->loadedVector();
  }

  const auto size = input_->size();
  if (!maxSizes_ || !maxSizes_->unique() || maxSizes_->capacity() < size * sizeof(vector_size_t)) {
    maxSizes_ = allocateSizes(size, pool());
  }
  rawMaxSizes_ = maxSizes_->asMutable<vector_size_t>();
  std::fill(rawMaxSizes_, rawMaxSizes_ + size, 0);

  for (size_t channel = 0; channel < unnestChannels_.size(); ++channel) {
    const auto& unnestVector = input_->childAt(unnestChannels_[channel]);
    auto& currentDecoded = unnestDecoded_[channel];
    currentDecoded.decode(*unnestVector);
    rawIndices_[channel] = currentDecoded.indices();

    const auto* base = currentDecoded.base();
    if (base->typeKind() == TypeKind::ARRAY) {
      const auto* unnestBaseArray = base->as<ArrayVector>();
      rawSizes_[channel] = unnestBaseArray->rawSizes();
      rawOffsets_[channel] = unnestBaseArray->rawOffsets();
    } else {
      VELOX_CHECK_EQ(base->typeKind(), TypeKind::MAP);
      const auto* unnestBaseMap = base->as<MapVector>();
      rawSizes_[channel] = unnestBaseMap->rawSizes();
      rawOffsets_[channel] = unnestBaseMap->rawOffsets();
    }

    bool canUseContiguousInput = unnestChannels_.size() == 1 && !isOuter_;
    if (canUseContiguousInput) {
      if (base->typeKind() == TypeKind::ARRAY) {
        const auto& elements = base->as<ArrayVector>()->elements();
        if (inlineStruct_) {
          canUseContiguousInput = elements->encoding() == VectorEncoding::Simple::FLAT && !elements->mayHaveNulls() &&
              supportsCompactSlice(elements);
        } else {
          canUseContiguousInput = supportsCompactSlice(elements);
        }
      } else {
        const auto* map = base->as<MapVector>();
        canUseContiguousInput = supportsCompactSlice(map->mapKeys()) && supportsCompactSlice(map->mapValues());
      }
    }

    if (canUseContiguousInput) {
      singleContiguousInput_ = true;
      bool foundSource = false;
      int64_t expectedOffset = 0;
      const auto* sizes = rawSizes_[channel];
      const auto* offsets = rawOffsets_[channel];
      const auto* indices = rawIndices_[channel];
      currentDecoded.forEachNotNull(0, size, [&](auto row) INLINE_LAMBDA {
        const auto index = indices[row];
        const auto length = sizes[index];
        rawMaxSizes_[row] = length;
        if (singleContiguousInput_ && length > 0) {
          const auto offset = offsets[index];
          if (!foundSource) {
            contiguousOutputOffset_ = offset;
            expectedOffset = offset;
            foundSource = true;
          }
          if (offset != expectedOffset) {
            singleContiguousInput_ = false;
          }
          expectedOffset += length;
        }
      });
      contiguousOutputEnd_ = expectedOffset;
    } else {
      const auto* currentSizes = rawSizes_[channel];
      const auto* currentIndices = rawIndices_[channel];
      currentDecoded.forEachNotNull(0, size, [&](auto row) INLINE_LAMBDA {
        const auto unnestSize = currentSizes[currentIndices[row]];
        rawMaxSizes_[row] = std::max(rawMaxSizes_[row], unnestSize);
      });
    }
  }
}

RowVectorPtr SparkUnnest::getOutput() {
  if (!input_) {
    return nullptr;
  }

  const auto size = input_->size();
  const auto rowRange = extractRowRange(size);
  if (rowRange.numInnerRows == 0) {
    finishInput();
    return nullptr;
  }

  auto output = generateOutput(rowRange);
  VELOX_CHECK_NOT_NULL(output);
  if (singleContiguousInput_) {
    contiguousOutputOffset_ += rowRange.numInnerRows;
  }
  if (rowRange.lastInnerRowEnd.has_value()) {
    firstInnerRowStart_ = rowRange.lastInnerRowEnd.value();
    nextInputRow_ += rowRange.numInputRows - 1;
  } else {
    firstInnerRowStart_ = 0;
    nextInputRow_ += rowRange.numInputRows;
    if (nextInputRow_ >= size) {
      finishInput();
    }
  }
  return output;
}

void SparkUnnest::finishInput() {
  input_ = nullptr;
  nextInputRow_ = 0;
  firstInnerRowStart_ = 0;
}

SparkUnnest::RowRange SparkUnnest::extractRowRange(vector_size_t inputSize) const {
  if (singleContiguousInput_) {
    const auto remaining = contiguousOutputEnd_ - contiguousOutputOffset_;
    if (remaining <= maxOutputSize_) {
      return {nextInputRow_, inputSize - nextInputRow_, std::nullopt, static_cast<vector_size_t>(remaining), false};
    }
  }

  vector_size_t numInputRows{0};
  vector_size_t numInnerRows{0};
  std::optional<vector_size_t> lastInnerRowEnd;
  bool hasEmptyUnnestValue{false};

  auto inputRow = nextInputRow_;
  auto rowRemaining = rawMaxSizes_[inputRow] - firstInnerRowStart_;
  while (inputRow < inputSize) {
    const bool isEmptySubs = (rowRemaining == 0);
    const vector_size_t innerRows = isEmptySubs && isOuter_ ? 1 : rowRemaining;
    if (innerRows > 0) {
      hasEmptyUnnestValue |= isEmptySubs;
      if (numInnerRows + innerRows >= maxOutputSize_) {
        if (!isEmptySubs) {
          const auto rowsToTake = maxOutputSize_ - numInnerRows;
          if (rowsToTake < innerRows) {
            lastInnerRowEnd = rawMaxSizes_[inputRow] - innerRows + rowsToTake;
          }
          numInnerRows += rowsToTake;
        } else {
          ++numInnerRows;
        }
        ++numInputRows;
        break;
      }
      numInnerRows += innerRows;
    }
    ++numInputRows;
    ++inputRow;
    if (inputRow < inputSize) {
      rowRemaining = rawMaxSizes_[inputRow];
    }
  }
  return {nextInputRow_, numInputRows, lastInnerRowEnd, numInnerRows, hasEmptyUnnestValue};
}

void SparkUnnest::generateRepeatedColumns(const RowRange& range, std::vector<VectorPtr>& outputs) {
  if (identityProjections_.empty()) {
    return;
  }
  if (range.numInputRows == 1) {
    for (const auto& projection : identityProjections_) {
      outputs[projection.outputChannel] =
          BaseVector::wrapInConstant(range.numInnerRows, range.startInputRow, input_->childAt(projection.inputChannel));
    }
    return;
  }

  auto repeatedIndices = allocateIndices(range.numInnerRows, pool());
  auto* rawRepeatedIndices = repeatedIndices->asMutable<vector_size_t>();
  vector_size_t index = 0;
  range.forEachRow(
      [&](vector_size_t row, vector_size_t /*start*/, vector_size_t size) {
        if (size == 0 && isOuter_) {
          rawRepeatedIndices[index++] = row;
        } else if (size > 0) {
          std::fill(rawRepeatedIndices + index, rawRepeatedIndices + index + size, row);
          index += size;
        }
      },
      rawMaxSizes_,
      firstInnerRowStart_);

  for (const auto& projection : identityProjections_) {
    outputs[projection.outputChannel] = BaseVector::wrapInDictionary(
        nullptr, repeatedIndices, range.numInnerRows, input_->childAt(projection.inputChannel));
  }
}

template <bool trackContiguity, bool deferIndices>
SparkUnnest::ChannelEncoding SparkUnnest::generateEncodingForChannelImpl(
    column_index_t channel,
    const RowRange& range) {
  BufferPtr innerRowIndices;
  if constexpr (!deferIndices) {
    innerRowIndices = allocateIndices(range.numInnerRows, pool());
  }
  auto* rawInnerRowIndices = innerRowIndices ? innerRowIndices->asMutable<vector_size_t>() : nullptr;

  auto nulls = allocateNulls(range.numInnerRows, pool());
  auto* rawNulls = nulls->asMutable<uint64_t>();
  bool hasAnyNull = false;

  auto& currentDecoded = unnestDecoded_[channel];
  const auto* currentSizes = rawSizes_[channel];
  const auto* currentOffsets = rawOffsets_[channel];
  const auto* currentIndices = rawIndices_[channel];

  vector_size_t index = 0;
  bool contiguousMapping = trackContiguity;
  vector_size_t firstSourceIndex = 0;
  auto ensureIndices = [&]() {
    if constexpr (deferIndices) {
      if (!innerRowIndices) {
        innerRowIndices = allocateIndices(range.numInnerRows, pool());
        rawInnerRowIndices = innerRowIndices->asMutable<vector_size_t>();
        std::iota(rawInnerRowIndices, rawInnerRowIndices + index, firstSourceIndex);
      }
    }
  };

  range.forEachRow(
      [&](vector_size_t row, vector_size_t start, vector_size_t size) INLINE_LAMBDA {
        const auto end = start + size;
        if (size == 0 && isOuter_) {
          ensureIndices();
          contiguousMapping = false;
          hasAnyNull = true;
          bits::setNull(rawNulls, index++, true);
        } else if (!currentDecoded.isNullAt(row)) {
          const auto offset = currentOffsets[currentIndices[row]];
          const auto unnestSize = currentSizes[currentIndices[row]];
          const auto currentUnnestSize = std::min(end, unnestSize);
          if constexpr (trackContiguity) {
            if (contiguousMapping && currentUnnestSize > start) {
              const auto sourceStart = offset + start;
              if (index == 0) {
                firstSourceIndex = sourceStart;
              }
              if (static_cast<int64_t>(sourceStart) != static_cast<int64_t>(firstSourceIndex) + index) {
                ensureIndices();
                contiguousMapping = false;
              }
            }
          }
          if constexpr (deferIndices) {
            if (!innerRowIndices) {
              index += std::max<vector_size_t>(0, currentUnnestSize - start);
            } else {
              for (auto i = start; i < currentUnnestSize; ++i) {
                rawInnerRowIndices[index++] = offset + i;
              }
            }
          } else {
            for (auto i = start; i < currentUnnestSize; ++i) {
              rawInnerRowIndices[index++] = offset + i;
            }
          }
          if (std::max(start, currentUnnestSize) < end) {
            ensureIndices();
            contiguousMapping = false;
            hasAnyNull = true;
          }
          for (auto i = std::max(start, currentUnnestSize); i < end; ++i) {
            bits::setNull(rawNulls, index++, true);
          }
        } else if (size > 0) {
          ensureIndices();
          contiguousMapping = false;
          hasAnyNull = true;
          for (auto i = start; i < end; ++i) {
            bits::setNull(rawNulls, index++, true);
          }
        }
      },
      rawMaxSizes_,
      firstInnerRowStart_);

  return {innerRowIndices, hasAnyNull ? nulls : nullptr, contiguousMapping, firstSourceIndex};
}

SparkUnnest::ChannelEncoding SparkUnnest::generateEncodingForChannel(column_index_t channel, const RowRange& range) {
  if (singleContiguousInput_) {
    return {nullptr, nullptr, true, contiguousOutputOffset_};
  }
  if (isOuter_ && range.hasEmptyUnnestValue && unnestChannels_.size() == 1) {
    auto indices = allocateIndices(range.numInnerRows, pool());
    auto nulls = allocateNulls(range.numInnerRows, pool());
    auto* rawIndices = indices->asMutable<vector_size_t>();
    auto* rawNulls = nulls->asMutable<uint64_t>();
    const auto* offsets = rawOffsets_[channel];
    const auto* inputIndices = rawIndices_[channel];
    vector_size_t index = 0;
    range.forEachRow(
        [&](vector_size_t row, vector_size_t start, vector_size_t size) {
          if (size == 0) {
            bits::setNull(rawNulls, index++, true);
          } else {
            std::iota(rawIndices + index, rawIndices + index + size, offsets[inputIndices[row]] + start);
            index += size;
          }
        },
        rawMaxSizes_,
        firstInnerRowStart_);
    VELOX_DCHECK_EQ(index, range.numInnerRows);
    return {std::move(indices), std::move(nulls), false, 0};
  }

  const auto* base = unnestDecoded_[channel].base();
  bool canSlice = false;
  if (base->typeKind() == TypeKind::ARRAY) {
    canSlice = supportsCompactSlice(base->as<ArrayVector>()->elements());
  } else {
    const auto* map = base->as<MapVector>();
    canSlice = supportsCompactSlice(map->mapKeys()) && supportsCompactSlice(map->mapValues());
  }
  if (canSlice && !(isOuter_ && range.hasEmptyUnnestValue)) {
    return generateEncodingForChannelImpl<true, true>(channel, range);
  }
  return generateEncodingForChannelImpl<true, false>(channel, range);
}

VectorPtr SparkUnnest::wrapSingleColumn(const ChannelEncoding& encoding, const VectorPtr& base, vector_size_t wrapSize)
    const {
  if (encoding.contiguousMapping && supportsCompactSlice(base)) {
    if (encoding.sourceStart == 0 && wrapSize == base->size()) {
      return base;
    }
    return base->slice(encoding.sourceStart, wrapSize);
  }

  if (base->size() <= wrapSize && supportsCompactSlice(base)) {
    auto dictNulls =
        encoding.nulls && !encoding.nulls->unique() ? cloneNulls(encoding.nulls, wrapSize, pool()) : encoding.nulls;
    return BaseVector::wrapInDictionary(std::move(dictNulls), encoding.indices, wrapSize, base);
  }

  auto flat = BaseVector::create(base->type(), wrapSize, base->pool());
  SelectivityVector rows(wrapSize);
  if (base->isFlatEncoding()) {
    const auto* paddingNulls = encoding.nulls ? encoding.nulls->as<uint64_t>() : nullptr;
    if (paddingNulls) {
      rows.deselectNulls(paddingNulls, 0, wrapSize);
    }
    flat->copy(base.get(), rows, encoding.indices->as<vector_size_t>());
    if (paddingNulls) {
      rows.setAll();
      flat->addNulls(paddingNulls, rows);
    }
  } else {
    auto dictNulls =
        encoding.nulls && !encoding.nulls->unique() ? cloneNulls(encoding.nulls, wrapSize, pool()) : encoding.nulls;
    const auto wrapped = BaseVector::wrapInDictionary(std::move(dictNulls), encoding.indices, wrapSize, base);
    flat->copy(wrapped.get(), rows, nullptr);
  }
  return flat;
}

void SparkUnnest::appendInlineStructColumns(
    const ChannelEncoding& encoding,
    const VectorPtr& elementsVector,
    vector_size_t wrapSize,
    column_index_t& outputColumnIndex,
    std::vector<VectorPtr>& outputs) {
  const auto& rowType = elementsVector->type()->asRow();
  const auto numFields = rowType.size();
  if (numFields == 0) {
    return;
  }

  if (encoding.contiguousMapping && elementsVector->encoding() == VectorEncoding::Simple::FLAT) {
    auto sliced = (encoding.sourceStart == 0 && wrapSize == elementsVector->size())
        ? elementsVector
        : elementsVector->slice(encoding.sourceStart, wrapSize);
    const auto* slicedRow = sliced->as<RowVector>();
    if (!slicedRow->mayHaveNulls()) {
      for (uint32_t i = 0; i < numFields; ++i) {
        outputs[outputColumnIndex++] = slicedRow->childAt(i);
      }
      return;
    }
  }

  DecodedVector decodedElements(*elementsVector);
  const auto* baseRow = decodedElements.base()->as<RowVector>();
  VELOX_CHECK_NOT_NULL(baseRow);

  auto combinedIndices = allocateIndices(wrapSize, pool());
  auto* rawCombinedIndices = combinedIndices->asMutable<vector_size_t>();
  auto combinedNulls = allocateNulls(wrapSize, pool());
  auto* rawCombinedNulls = combinedNulls->asMutable<uint64_t>();
  const auto* rawPaddingNulls = encoding.nulls ? encoding.nulls->as<uint64_t>() : nullptr;
  const auto* rawEncodingIndices = encoding.indices ? encoding.indices->as<vector_size_t>() : nullptr;

  bool hasAnyNull = false;
  for (vector_size_t row = 0; row < wrapSize; ++row) {
    if (!encoding.contiguousMapping && rawPaddingNulls && bits::isBitNull(rawPaddingNulls, row)) {
      bits::setNull(rawCombinedNulls, row, true);
      hasAnyNull = true;
      continue;
    }
    const vector_size_t elementIdx =
        encoding.contiguousMapping ? (encoding.sourceStart + row) : rawEncodingIndices[row];
    if (decodedElements.isNullAt(elementIdx)) {
      bits::setNull(rawCombinedNulls, row, true);
      hasAnyNull = true;
    } else {
      rawCombinedIndices[row] = decodedElements.index(elementIdx);
    }
  }

  ChannelEncoding fieldEncoding{std::move(combinedIndices), hasAnyNull ? std::move(combinedNulls) : nullptr, false, 0};
  for (uint32_t i = 0; i < numFields; ++i) {
    outputs[outputColumnIndex++] = wrapSingleColumn(fieldEncoding, baseRow->childAt(i), wrapSize);
  }
}

VectorPtr SparkUnnest::generateOrdinalityVector(const RowRange& range, const BufferPtr& movablePaddingNulls) {
  auto ordinalityVector = BaseVector::create<FlatVector<int32_t>>(INTEGER(), range.numInnerRows, pool());
  auto* const rawValues = ordinalityVector->mutableRawValues();
  auto* rawOrdinality = rawValues;
  const bool hasEmpty = isOuter_ && range.hasEmptyUnnestValue;

  if (!hasEmpty) {
    range.forEachRow(
        [&](vector_size_t /*row*/, vector_size_t start, vector_size_t size) {
          std::iota(rawOrdinality, rawOrdinality + size, static_cast<int32_t>(start));
          rawOrdinality += size;
        },
        rawMaxSizes_,
        firstInnerRowStart_);
    return ordinalityVector;
  }

  if (movablePaddingNulls && movablePaddingNulls->unique()) {
    ordinalityVector->setNulls(movablePaddingNulls);
    range.forEachRow(
        [&](vector_size_t /*row*/, vector_size_t start, vector_size_t size) {
          if (FOLLY_LIKELY(size > 0)) {
            std::iota(rawOrdinality, rawOrdinality + size, static_cast<int32_t>(start));
            rawOrdinality += size;
          } else {
            *rawOrdinality++ = 0;
          }
        },
        rawMaxSizes_,
        firstInnerRowStart_);
    return ordinalityVector;
  }

  auto* rawNulls = ordinalityVector->mutableRawNulls();
  range.forEachRow(
      [&](vector_size_t /*row*/, vector_size_t start, vector_size_t size) {
        if (FOLLY_LIKELY(size > 0)) {
          std::iota(rawOrdinality, rawOrdinality + size, static_cast<int32_t>(start));
          rawOrdinality += size;
        } else {
          bits::setNull(rawNulls, rawOrdinality - rawValues, true);
          *rawOrdinality++ = 0;
        }
      },
      rawMaxSizes_,
      firstInnerRowStart_);
  return ordinalityVector;
}

RowVectorPtr SparkUnnest::generateOutput(const RowRange& range) {
  std::vector<VectorPtr> outputs(outputType_->size());
  generateRepeatedColumns(range, outputs);

  column_index_t outputColumnIndex = identityProjections_.size() + (withOrdinality_ ? 1 : 0);
  BufferPtr movablePaddingNulls;

  for (size_t channel = 0; channel < unnestChannels_.size(); ++channel) {
    auto encoding = generateEncodingForChannel(channel, range);
    const auto* base = unnestDecoded_[channel].base();
    if (base->typeKind() == TypeKind::ARRAY) {
      const auto* unnestBaseArray = base->as<ArrayVector>();
      if (inlineStruct_) {
        appendInlineStructColumns(
            encoding, unnestBaseArray->elements(), range.numInnerRows, outputColumnIndex, outputs);
      } else {
        outputs[outputColumnIndex++] = wrapSingleColumn(encoding, unnestBaseArray->elements(), range.numInnerRows);
      }
    } else {
      const auto* unnestBaseMap = base->as<MapVector>();
      outputs[outputColumnIndex++] = wrapSingleColumn(encoding, unnestBaseMap->mapKeys(), range.numInnerRows);
      outputs[outputColumnIndex++] = wrapSingleColumn(encoding, unnestBaseMap->mapValues(), range.numInnerRows);
    }
    if (unnestChannels_.size() == 1 && encoding.nulls && encoding.nulls->unique()) {
      movablePaddingNulls = std::move(encoding.nulls);
    }
  }

  if (withOrdinality_) {
    outputs[ordinalityOutputChannel_] = generateOrdinalityVector(range, movablePaddingNulls);
  }

  return std::make_shared<RowVector>(pool(), outputType_, BufferPtr(nullptr), range.numInnerRows, std::move(outputs));
}

bool SparkUnnest::isFinished() {
  return noMoreInput_ && input_ == nullptr;
}

std::unique_ptr<exec::Operator>
SparkUnnestTranslator::toOperator(exec::DriverCtx* ctx, int32_t id, const core::PlanNodePtr& node) {
  if (auto sparkUnnestNode = std::dynamic_pointer_cast<const SparkUnnestNode>(node)) {
    return std::make_unique<SparkUnnest>(id, ctx, sparkUnnestNode);
  }
  return nullptr;
}

} // namespace gluten
