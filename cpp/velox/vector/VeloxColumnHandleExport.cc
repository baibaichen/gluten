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
#include "vector/VeloxColumnHandleExport.h"

#include <cstring>
#include <memory>

#include "memory/ColumnarBatch.h"
#include "memory/VeloxColumnarBatch.h"
#include "utils/ObjectStore.h"
#include "velox/buffer/Buffer.h"
#include "velox/common/base/BitUtil.h"
#include "velox/common/base/Exceptions.h"
#include "velox/common/base/Nulls.h"
#include "velox/common/memory/Memory.h"
#include "velox/type/StringView.h"
#include "velox/type/Timestamp.h"
#include "velox/vector/ComplexVector.h"
#include "velox/vector/FlatVector.h"

namespace gluten {

using namespace facebook::velox;

/**
 * Returns the raw values pointer for a FLAT-encoded primitive vector cast to
 * const void*, dispatching over TypeKind. UNKNOWN has only nulls and no values
 * buffer; unsupported kinds throw VELOX_UNSUPPORTED.
 */
static const void* rawValuesAsVoid(const BaseVector* v) {
  // DECIMAL is a logical type whose TypeKind is BIGINT (short, precision <= 18) or HUGEINT (long,
  // precision > 18). It must be dispatched by the logical type BEFORE the TypeKind switch: a short
  // decimal's int64 unscaled buffer is read via FlatVector<int64_t>, a long decimal's int128
  // unscaled buffer (16B/row, little-endian two's complement) via FlatVector<int128_t>.
  if (v->type()->isShortDecimal()) {
    return v->asUnchecked<FlatVector<int64_t>>()->rawValues();
  }
  if (v->type()->isLongDecimal()) {
    return v->asUnchecked<FlatVector<int128_t>>()->rawValues();
  }
  if (v->type()->isCalendarInterval()) {
    // CalendarInterval::pack(): months (int32), days (int32), microseconds (int64).
    return v->asUnchecked<FlatVector<int128_t>>()->rawValues();
  }
  switch (v->typeKind()) {
    case TypeKind::UNKNOWN:
      VELOX_CHECK_EQ(
          BaseVector::countNulls(v->nulls(), v->size()), v->size(), "exportVector: UNKNOWN must contain only nulls");
      return nullptr;
    case TypeKind::BOOLEAN:
      // Velox FlatVector<bool> stores values bit-packed (1 bit/row, LSB-first) in the
      // values buffer, using the same layout as the validity/nulls bitmap. rawValues<uint64_t>()
      // returns the base of that bit-packed buffer. On little-endian this base is byte-addressable:
      // bit for row r lives at byte (r>>3), bit position (r&7), which is exactly what the Java
      // FlatAccessor.getBoolean reader expects. Exposing this pointer keeps BOOLEAN read zero-copy.
      return reinterpret_cast<const void*>(v->asUnchecked<FlatVector<bool>>()->rawValues<uint64_t>());
    case TypeKind::TINYINT:
      return v->asUnchecked<FlatVector<int8_t>>()->rawValues();
    case TypeKind::SMALLINT:
      return v->asUnchecked<FlatVector<int16_t>>()->rawValues();
    case TypeKind::INTEGER:
      return v->asUnchecked<FlatVector<int32_t>>()->rawValues();
    case TypeKind::BIGINT:
      return v->asUnchecked<FlatVector<int64_t>>()->rawValues();
    case TypeKind::REAL:
      return v->asUnchecked<FlatVector<float>>()->rawValues();
    case TypeKind::DOUBLE:
      return v->asUnchecked<FlatVector<double>>()->rawValues();
    case TypeKind::VARCHAR:
    case TypeKind::VARBINARY:
      // StringView is a 16-byte struct: [4B size][4B prefix][8B inline-or-pointer].
      // We expose the raw StringView array base; inline/pointer resolution is a
      // Java-accessor concern (Task 8). Do NOT copy or dereference the string bytes.
      return reinterpret_cast<const void*>(v->asUnchecked<FlatVector<StringView>>()->rawValues());
    case TypeKind::TIMESTAMP:
      // Velox Timestamp is a 16-byte struct: {int64 seconds; uint64 nanos}.
      // The Java TimestampAccessor reads 16 bytes per row and converts to micros.
      return v->asUnchecked<FlatVector<Timestamp>>()->rawValues();
    default:
      VELOX_UNSUPPORTED(
          "exportVector: unsupported TypeKind for FLAT primitive export: {}", static_cast<int>(v->typeKind()));
  }
}

/**
 * Exports a FLAT-encoded primitive vector into a VeloxColumnHandle descriptor.
 * nBuffers=2: buffers[0]=nulls bitmap (or nullptr), buffers[1]=raw values.
 * nChildren=0.
 */
static VeloxColumnHandle* exportFlat(const BaseVector* v) {
  // Resolve the raw values pointer first: rawValuesAsVoid throws VELOX_UNSUPPORTED
  // for unsupported TypeKinds or invalid UNKNOWN values. Doing it before allocColumnHandle keeps
  // exportFlat exception-safe (no descriptor node leaks on the throw path);
  // reachable via the JNI exportDescriptor endpoint which does not pre-check.
  const void* rawValues = rawValuesAsVoid(v);
  auto* h = allocColumnHandle(/*nBuffers=*/2, /*nChildren=*/0);
  h->length = static_cast<int64_t>(v->size());
  h->nullCount = v->typeKind() == TypeKind::UNKNOWN ? h->length : static_cast<int64_t>(v->getNullCount().value_or(-1));
  // rawNulls() returns nullptr when the vector has no null entries.
  h->buffers[0] = v->rawNulls();
  h->buffers[1] = rawValues;
  return h;
}

VeloxColumnHandle* exportVector(const VectorPtr& vec) {
  const BaseVector* v = vec.get();
  switch (v->encoding()) {
    case VectorEncoding::Simple::FLAT:
      return exportFlat(v);
    case VectorEncoding::Simple::ARRAY: {
      auto* a = v->asUnchecked<ArrayVector>();
      std::unique_ptr<VeloxColumnHandle, decltype(&freeColumnHandleTree)> h(
          allocColumnHandle(/*nBuffers=*/3, /*nChildren=*/1), freeColumnHandleTree);
      h->length = static_cast<int64_t>(a->size());
      h->nullCount = static_cast<int64_t>(a->getNullCount().value_or(-1));
      // buffers[0]: nulls bitmap (may be nullptr if no nulls)
      h->buffers[0] = a->rawNulls();
      // buffers[1]: offsets (const vector_size_t* = int32) -- one per row
      h->buffers[1] = a->rawOffsets();
      // buffers[2]: sizes (const vector_size_t* = int32) -- one per row; must be
      //   read directly, not recomputed via offsets[i+1]-offsets[i] since Velox
      //   allows out-of-order / overlapping offsets.
      h->buffers[2] = a->rawSizes();
      h->children[0] = exportVector(a->elements());
      return h.release();
    }
    case VectorEncoding::Simple::ROW: {
      auto* r = v->asUnchecked<RowVector>();
      std::unique_ptr<VeloxColumnHandle, decltype(&freeColumnHandleTree)> h(
          allocColumnHandle(/*nBuffers=*/1, /*nChildren=*/r->childrenSize()), freeColumnHandleTree);
      h->length = static_cast<int64_t>(r->size());
      h->nullCount = static_cast<int64_t>(r->getNullCount().value_or(-1));
      // buffers[0]: nulls bitmap (may be nullptr if no nulls).
      // ROW has no offsets/sizes; children are 1:1 aligned to parent rows.
      h->buffers[0] = r->rawNulls();
      for (size_t i = 0; i < r->childrenSize(); ++i) {
        h->children[i] = exportVector(r->childAt(static_cast<column_index_t>(i)));
      }
      return h.release();
    }
    case VectorEncoding::Simple::MAP: {
      auto* m = v->asUnchecked<MapVector>();
      std::unique_ptr<VeloxColumnHandle, decltype(&freeColumnHandleTree)> h(
          allocColumnHandle(/*nBuffers=*/3, /*nChildren=*/2), freeColumnHandleTree);
      h->length = static_cast<int64_t>(m->size());
      h->nullCount = static_cast<int64_t>(m->getNullCount().value_or(-1));
      // buffers[0]: nulls bitmap; buffers[1]: offsets; buffers[2]: sizes.
      // sizes[i] must be read directly -- Velox allows out-of-order offsets.
      h->buffers[0] = m->rawNulls();
      h->buffers[1] = m->rawOffsets();
      h->buffers[2] = m->rawSizes();
      h->children[0] = exportVector(m->mapKeys());
      h->children[1] = exportVector(m->mapValues());
      return h.release();
    }
    default:
      VELOX_UNSUPPORTED("exportVector: encoding not yet supported: {}", static_cast<int>(v->encoding()));
  }
}

/**
 * Returns true iff the flat leaf's TypeKind is supported by exportVector.
 * BOOLEAN is included (bit-packed values buffer, exported as-is). Conservative:
 * unsupported kinds -> false.
 */
static bool isSupportedFlatKind(TypeKind k) {
  switch (k) {
    case TypeKind::UNKNOWN:
    case TypeKind::BOOLEAN:
    case TypeKind::TINYINT:
    case TypeKind::SMALLINT:
    case TypeKind::INTEGER:
    case TypeKind::BIGINT:
    case TypeKind::REAL:
    case TypeKind::DOUBLE:
    case TypeKind::VARCHAR:
    case TypeKind::VARBINARY:
    case TypeKind::TIMESTAMP:
      return true;
    default:
      return false;
  }
}

bool classifyColumn(const BaseVector* v) {
  switch (v->encoding()) {
    case VectorEncoding::Simple::FLAT:
      // DECIMAL and CALENDAR_INTERVAL dispatch by logical type, not their shared integer kind.
      return isSupportedFlatKind(v->typeKind()) || v->type()->isDecimal() || v->type()->isCalendarInterval();
    case VectorEncoding::Simple::ARRAY: {
      const auto* a = v->asUnchecked<ArrayVector>();
      return classifyColumn(a->elements().get());
    }
    case VectorEncoding::Simple::ROW: {
      const auto* r = v->asUnchecked<RowVector>();
      for (column_index_t i = 0; i < static_cast<column_index_t>(r->childrenSize()); ++i) {
        if (!classifyColumn(r->childAt(i).get())) {
          return false;
        }
      }
      return true;
    }
    case VectorEncoding::Simple::MAP: {
      const auto* m = v->asUnchecked<MapVector>();
      return classifyColumn(m->mapKeys().get()) && classifyColumn(m->mapValues().get());
    }
    default:
      return false;
  }
}

// ---- Write path: shared allocation helpers ----

/**
 * Returns a reference to the module-level ObjectStore that owns output vectors.
 * Initialized on first call; lives for the process lifetime.
 */
static ObjectStore& outputStore() {
  static std::unique_ptr<ObjectStore> store = ObjectStore::create();
  return *store;
}

/**
 * Returns a reference to the module-level Velox leaf memory pool used for
 * allocating output vector buffers. Initialized on first call; lives for the
 * process lifetime.
 *
 * Requires the Velox MemoryManager to be initialized before the first call
 * (guaranteed by VeloxBackend startup before any JNI invocation, and by
 * ExportTest::SetUpTestCase in unit tests).
 */
static memory::MemoryPool& outputPool() {
  static auto rootPool = memory::memoryManager()->addRootPool("gluten_output_root");
  static auto leafPool = rootPool->addLeafChild("gluten_output_leaf");
  return *leafPool;
}

/**
 * Returns the mutable raw-values pointer for a FLAT-encoded fixed-width vector,
 * dispatching over TypeKind. Returns void* so the caller can store it in the
 * descriptor without dragging type parameters into the call site.
 */
static void* mutableValuesAsVoid(VectorPtr& vec) {
  switch (vec->typeKind()) {
    case TypeKind::TINYINT:
      return vec->asUnchecked<FlatVector<int8_t>>()->mutableRawValues();
    case TypeKind::SMALLINT:
      return vec->asUnchecked<FlatVector<int16_t>>()->mutableRawValues();
    case TypeKind::INTEGER:
      return vec->asUnchecked<FlatVector<int32_t>>()->mutableRawValues();
    case TypeKind::BIGINT:
      return vec->asUnchecked<FlatVector<int64_t>>()->mutableRawValues();
    case TypeKind::HUGEINT:
      // HUGEINT backs long DECIMAL (precision > 18): int128 unscaled, 16B/row, little-endian
      // two's complement. Java writes via putDecimal128 (lo @ +0, hi @ +8).
      return vec->asUnchecked<FlatVector<int128_t>>()->mutableRawValues();
    case TypeKind::REAL:
      return vec->asUnchecked<FlatVector<float>>()->mutableRawValues();
    case TypeKind::DOUBLE:
      return vec->asUnchecked<FlatVector<double>>()->mutableRawValues();
    case TypeKind::BOOLEAN:
      // BOOLEAN is bit-packed (1 bit/row, LSB-first). mutableRawValues<uint64_t>()
      // forces allocation of and returns the bit-packed values buffer base. Java
      // writes/reads individual bits via VeloxWritableColumnVector.putBoolean/getBoolean.
      return static_cast<void*>(vec->asUnchecked<FlatVector<bool>>()->mutableRawValues<uint64_t>());
    case TypeKind::VARCHAR:
    case TypeKind::VARBINARY:
      // FlatVector<StringView>::mutableRawValues() returns a StringView* (16 bytes/slot).
      // Java writes inline strings directly into the slot; long strings land in
      // string-chunk buffers allocated via allocateStringChunk.
      return static_cast<void*>(vec->asUnchecked<FlatVector<StringView>>()->mutableRawValues());
    case TypeKind::TIMESTAMP:
      // Velox Timestamp is a 16-byte struct: {int64 seconds; uint64 nanos}.
      // Java writes via putTimestampMicros (floorDiv/floorMod conversion) at 16B/slot stride.
      return static_cast<void*>(vec->asUnchecked<FlatVector<Timestamp>>()->mutableRawValues());
    default:
      VELOX_UNSUPPORTED("mutableValuesAsVoid: unsupported TypeKind: {}", static_cast<int>(vec->typeKind()));
  }
}

std::vector<VeloxColumnHandle*> exportBatchColumns(int64_t batchHandle) {
  // Retrieve the ColumnarBatch from the ObjectStore.  The handle encodes its
  // resident store, so retrieve is a direct lookup -- no runtime wrapper needed.
  auto cb = ObjectStore::retrieveChecked<ColumnarBatch>(static_cast<ObjectHandle>(batchHandle));

  // The batch must be a VeloxColumnarBatch to expose getRowVector().
  auto vcb = std::dynamic_pointer_cast<VeloxColumnarBatch>(cb);
  VELOX_CHECK_NOT_NULL(vcb, "exportBatchColumns: not a VeloxColumnarBatch for handle: {}", batchHandle);

  auto rowVector = vcb->getRowVector();
  VELOX_CHECK_NOT_NULL(rowVector, "exportBatchColumns: null RowVector for handle: {}", batchHandle);

  const size_t n = rowVector->childrenSize();
  std::vector<VeloxColumnHandle*> result;
  result.reserve(n);

  // Export each column as a read-side view descriptor.
  // Exception safety: if any exportVector() throws, free already-exported
  // descriptors before rethrowing so no descriptor node leaks.
  for (size_t i = 0; i < n; ++i) {
    try {
      auto column = rowVector->childAt(static_cast<column_index_t>(i));
      if (!classifyColumn(column.get())) {
        // The owning batch retains this view for the descriptor's lifetime.
        column = vcb->getFlattenedRowVector()->childAt(static_cast<column_index_t>(i));
      }
      result.push_back(exportVector(column));
    } catch (...) {
      for (auto* d : result) {
        freeColumnHandleTree(d);
      }
      throw;
    }
  }

  return result;
}

int64_t makeVeloxBatch(
    const std::vector<int64_t>& ownerHandles,
    const std::vector<std::string>& names,
    const std::function<int64_t(std::shared_ptr<ColumnarBatch>)>& saveFn) {
  VELOX_CHECK_EQ(ownerHandles.size(), names.size(), "makeVeloxBatch: handles/names size mismatch");

  std::vector<VectorPtr> children;
  std::vector<std::string> childNames;
  std::vector<TypePtr> childTypes;
  children.reserve(ownerHandles.size());
  childNames.reserve(ownerHandles.size());
  childTypes.reserve(ownerHandles.size());

  vector_size_t numRows = 0;
  for (size_t i = 0; i < ownerHandles.size(); ++i) {
    // C1 unwrap: the store holds shared_ptr<VectorPtr> (double-wrapped).
    // retrieve<VectorPtr> returns shared_ptr<VectorPtr>; dereference to get VectorPtr.
    auto owner = ObjectStore::retrieveChecked<facebook::velox::VectorPtr>(static_cast<ObjectHandle>(ownerHandles[i]));
    facebook::velox::VectorPtr v = *owner;
    if (i == 0) {
      numRows = v->size();
    } else {
      VELOX_CHECK_EQ(
          v->size(), numRows, "makeVeloxBatch: column size mismatch at index {} ({} vs {})", i, v->size(), numRows);
    }
    childTypes.push_back(v->type());
    childNames.push_back(names[i]);
    children.push_back(std::move(v));
  }

  auto rowType = ROW(std::move(childNames), std::move(childTypes));
  // RowVector ctor: pool, rowType, nulls, numRows, children.
  // Each child VectorPtr is copied (shared_ptr ref-count incremented) into the
  // RowVector, so releasing the ownerHandles afterwards does not free the data.
  auto rowVector = std::make_shared<RowVector>(
      children.empty() ? nullptr : children[0]->pool(), rowType, /*nulls=*/nullptr, numRows, children);

  auto batch = std::make_shared<VeloxColumnarBatch>(rowVector);
  return saveFn(std::static_pointer_cast<ColumnarBatch>(batch));
}

std::pair<int64_t, int64_t> allocateStringChunk(int64_t ownerHandle, int64_t minBytes) {
  return allocateStringChunkAt(ownerHandle, {}, minBytes);
}

void finalizeStringColumn(int64_t ownerHandle, const std::vector<int64_t>& chunkUsedBytes) {
  finalizeStringColumnAt(ownerHandle, {}, chunkUsedBytes);
}

// Navigates from a root vector down @p childPath to the target child vector.
// Mirrors growChild's traversal exactly: ARRAY->elements(), MAP->keys/values,
// ROW->childAt(idx).  An empty childPath returns @p root unchanged (top-level).
static facebook::velox::VectorPtr navigateChildPath(
    const facebook::velox::VectorPtr& root,
    const std::vector<int32_t>& childPath) {
  VectorPtr current = root;
  for (int32_t idx : childPath) {
    switch (current->encoding()) {
      case VectorEncoding::Simple::ARRAY: {
        VELOX_CHECK_EQ(idx, 0, "navigateChildPath: ARRAY child index must be 0, got {}", idx);
        current = current->asUnchecked<ArrayVector>()->elements();
        break;
      }
      case VectorEncoding::Simple::MAP: {
        VELOX_CHECK(
            idx == 0 || idx == 1, "navigateChildPath: MAP child index must be 0 (keys) or 1 (values), got {}", idx);
        auto* m = current->asUnchecked<MapVector>();
        current = (idx == 0) ? m->mapKeys() : m->mapValues();
        break;
      }
      case VectorEncoding::Simple::ROW: {
        auto* r = current->asUnchecked<RowVector>();
        VELOX_CHECK(
            idx >= 0 && static_cast<size_t>(idx) < r->childrenSize(),
            "navigateChildPath: ROW child index {} out of range (childrenSize={})",
            idx,
            r->childrenSize());
        current = r->childAt(static_cast<column_index_t>(idx));
        break;
      }
      default:
        VELOX_UNSUPPORTED(
            "navigateChildPath: cannot navigate into encoding {} -- only ARRAY, MAP, ROW are traversable",
            static_cast<int>(current->encoding()));
    }
  }
  return current;
}

std::pair<int64_t, int64_t>
allocateStringChunkAt(int64_t rootOwnerHandle, const std::vector<int32_t>& childPath, int64_t minBytes) {
  // C1 unwrap: retrieve<VectorPtr> returns shared_ptr<VectorPtr>; dereference once.
  auto owner = ObjectStore::retrieveChecked<facebook::velox::VectorPtr>(static_cast<ObjectHandle>(rootOwnerHandle));
  VectorPtr target = navigateChildPath(*owner, childPath);
  auto* flat = target->asFlatVector<StringView>();
  VELOX_CHECK_NOT_NULL(flat, "allocateStringChunkAt: target child vector is not FlatVector<StringView>");

  const size_t chunkCapacity =
      std::max<size_t>(static_cast<size_t>(FlatVector<StringView>::kInitialStringSize), static_cast<size_t>(minBytes));

  auto buf = AlignedBuffer::allocate<char>(chunkCapacity, flat->pool());
  buf->setSize(0);
  flat->addStringBuffer(buf);
  return {reinterpret_cast<int64_t>(buf->asMutable<char>()), static_cast<int64_t>(buf->capacity())};
}

void finalizeStringColumnAt(
    int64_t rootOwnerHandle,
    const std::vector<int32_t>& childPath,
    const std::vector<int64_t>& chunkUsedBytes) {
  // C1 unwrap: retrieve<VectorPtr> returns shared_ptr<VectorPtr>; dereference once.
  auto owner = ObjectStore::retrieveChecked<facebook::velox::VectorPtr>(static_cast<ObjectHandle>(rootOwnerHandle));
  VectorPtr target = navigateChildPath(*owner, childPath);
  auto* flat = target->asFlatVector<StringView>();
  VELOX_CHECK_NOT_NULL(flat, "finalizeStringColumnAt: target child vector is not FlatVector<StringView>");

  const auto& bufs = flat->stringBuffers();
  VELOX_CHECK_EQ(
      bufs.size(),
      chunkUsedBytes.size(),
      "finalizeStringColumnAt: registered chunk count ({}) != chunkUsedBytes count ({})",
      bufs.size(),
      chunkUsedBytes.size());

  for (size_t i = 0; i < bufs.size(); ++i) {
    bufs[i]->setSize(static_cast<size_t>(chunkUsedBytes[i]));
  }
}

// ---- Write path: allocateNestedOutput ----

/**
 * Recursively creates a writable Velox output vector tree and its matching
 * VeloxColumnHandle descriptor tree, with all nulls bitmaps pre-allocated and
 * initialized to all-valid.  The two outputs are returned together so that the
 * caller can save the vector in the ObjectStore and return the descriptor to Java.
 *
 * Encoding / buffer layout is consistent with exportVector's read-side layout
 * so that a batch assembled from these output vectors can be read back via
 * exportVector without any conversion.
 *
 * Exception safety: if any child build or descriptor allocation fails, all
 * already-allocated child descriptors are freed before the exception propagates.
 */
static std::pair<VectorPtr, VeloxColumnHandle*>
buildOutputTree(const TypePtr& type, int32_t capacity, memory::MemoryPool& pool) {
  switch (type->kind()) {
    // ---- Leaf: fixed-width primitive or VARCHAR/VARBINARY ----
    case TypeKind::TINYINT:
    case TypeKind::SMALLINT:
    case TypeKind::INTEGER:
    case TypeKind::BIGINT:
    case TypeKind::HUGEINT:
    case TypeKind::REAL:
    case TypeKind::DOUBLE:
    case TypeKind::BOOLEAN:
    case TypeKind::VARCHAR:
    case TypeKind::VARBINARY:
    case TypeKind::TIMESTAMP: {
      VectorPtr vec = BaseVector::create(type, static_cast<vector_size_t>(capacity), &pool);
      // Force nulls buffer allocation; initialize all rows to valid (bit = 1).
      // Must be done before mutableValuesAsVoid: some Velox impls reallocate internal
      // buffers on first nulls allocation; fetching values last avoids stale pointers.
      uint64_t* nulls = vec->mutableRawNulls();
      VELOX_CHECK_NOT_NULL(nulls, "buildOutputTree leaf: mutableRawNulls null for capacity={}", capacity);
      void* valPtr = mutableValuesAsVoid(vec);
      // Re-fetch the nulls pointer after all buffer mutations are complete.
      const void* nullsPtr = static_cast<const void*>(vec->mutableRawNulls());

      auto* h = allocColumnHandle(/*nBuffers=*/2, /*nChildren=*/0);
      h->length = static_cast<int64_t>(capacity);
      h->nullCount = 0;
      h->buffers[0] = nullsPtr;
      h->buffers[1] = static_cast<const void*>(valPtr);
      return {std::move(vec), h};
    }

    // ---- ARRAY: 3 buffers (nulls, offsets, sizes), 1 child (elements) ----
    case TypeKind::ARRAY: {
      // Recursively build the elements child first.
      auto [elemVec, elemDesc] = buildOutputTree(type->childAt(0), capacity, pool);

      try {
        // Allocate and zero-initialise offsets/sizes buffers; capture raw pointers
        // before moving the BufferPtrs into the ArrayVector constructor.
        auto offsetsBuf = AlignedBuffer::allocate<vector_size_t>(capacity, &pool);
        auto sizesBuf = AlignedBuffer::allocate<vector_size_t>(capacity, &pool);
        memset(offsetsBuf->asMutable<vector_size_t>(), 0, static_cast<size_t>(capacity) * sizeof(vector_size_t));
        memset(sizesBuf->asMutable<vector_size_t>(), 0, static_cast<size_t>(capacity) * sizeof(vector_size_t));
        void* offsetsRaw = static_cast<void*>(offsetsBuf->asMutable<vector_size_t>());
        void* sizesRaw = static_cast<void*>(sizesBuf->asMutable<vector_size_t>());

        // Assemble the ArrayVector.  elemVec is copied (not moved) so it remains alive
        // as the structured binding local; ArrayVector increments the shared_ptr refcount.
        auto arrayVec = std::make_shared<ArrayVector>(
            &pool,
            type,
            BufferPtr(nullptr), // nulls -- allocated on demand via mutableRawNulls()
            static_cast<vector_size_t>(capacity),
            std::move(offsetsBuf),
            std::move(sizesBuf),
            elemVec);

        uint64_t* nulls = arrayVec->mutableRawNulls();
        VELOX_CHECK_NOT_NULL(nulls, "buildOutputTree ARRAY: mutableRawNulls null");
        const void* nullsPtr = static_cast<const void*>(arrayVec->mutableRawNulls());

        auto* h = allocColumnHandle(/*nBuffers=*/3, /*nChildren=*/1);
        h->length = static_cast<int64_t>(capacity);
        h->nullCount = 0;
        h->buffers[0] = nullsPtr;
        h->buffers[1] = static_cast<const void*>(offsetsRaw);
        h->buffers[2] = static_cast<const void*>(sizesRaw);
        h->children[0] = elemDesc;

        VectorPtr vec = std::move(arrayVec);
        return {std::move(vec), h};
      } catch (...) {
        freeColumnHandleTree(elemDesc);
        throw;
      }
    }

    // ---- MAP: 3 buffers (nulls, offsets, sizes), 2 children (keys, values) ----
    case TypeKind::MAP: {
      // Build keys child first.
      auto [keysVec, keysDesc] = buildOutputTree(type->childAt(0), capacity, pool);

      // Build values child; free keysDesc on failure.
      VectorPtr valsVec;
      VeloxColumnHandle* valsDesc = nullptr;
      try {
        auto [vv, vd] = buildOutputTree(type->childAt(1), capacity, pool);
        valsVec = std::move(vv);
        valsDesc = vd;
      } catch (...) {
        freeColumnHandleTree(keysDesc);
        throw;
      }

      try {
        auto offsetsBuf = AlignedBuffer::allocate<vector_size_t>(capacity, &pool);
        auto sizesBuf = AlignedBuffer::allocate<vector_size_t>(capacity, &pool);
        memset(offsetsBuf->asMutable<vector_size_t>(), 0, static_cast<size_t>(capacity) * sizeof(vector_size_t));
        memset(sizesBuf->asMutable<vector_size_t>(), 0, static_cast<size_t>(capacity) * sizeof(vector_size_t));
        void* offsetsRaw = static_cast<void*>(offsetsBuf->asMutable<vector_size_t>());
        void* sizesRaw = static_cast<void*>(sizesBuf->asMutable<vector_size_t>());

        auto mapVec = std::make_shared<MapVector>(
            &pool,
            type,
            BufferPtr(nullptr),
            static_cast<vector_size_t>(capacity),
            std::move(offsetsBuf),
            std::move(sizesBuf),
            keysVec,
            valsVec);

        uint64_t* nulls = mapVec->mutableRawNulls();
        VELOX_CHECK_NOT_NULL(nulls, "buildOutputTree MAP: mutableRawNulls null");
        const void* nullsPtr = static_cast<const void*>(mapVec->mutableRawNulls());

        auto* h = allocColumnHandle(/*nBuffers=*/3, /*nChildren=*/2);
        h->length = static_cast<int64_t>(capacity);
        h->nullCount = 0;
        h->buffers[0] = nullsPtr;
        h->buffers[1] = static_cast<const void*>(offsetsRaw);
        h->buffers[2] = static_cast<const void*>(sizesRaw);
        h->children[0] = keysDesc;
        h->children[1] = valsDesc;

        VectorPtr vec = std::move(mapVec);
        return {std::move(vec), h};
      } catch (...) {
        freeColumnHandleTree(keysDesc);
        freeColumnHandleTree(valsDesc);
        throw;
      }
    }

    // ---- ROW: 1 buffer (nulls), N children (one per field) ----
    case TypeKind::ROW: {
      const size_t n = type->size();

      std::vector<VectorPtr> childVecs;
      std::vector<VeloxColumnHandle*> childDescs;
      childVecs.reserve(n);
      childDescs.reserve(n);

      // Build each field child; on any failure free all already-built child descriptors.
      for (uint32_t i = 0; i < static_cast<uint32_t>(n); ++i) {
        try {
          auto [cv, cd] = buildOutputTree(type->childAt(i), capacity, pool);
          childVecs.push_back(std::move(cv));
          childDescs.push_back(cd);
        } catch (...) {
          for (auto* d : childDescs) {
            freeColumnHandleTree(d);
          }
          throw;
        }
      }

      try {
        auto rowVec = std::make_shared<RowVector>(
            &pool, type, BufferPtr(nullptr), static_cast<vector_size_t>(capacity), childVecs);

        uint64_t* nulls = rowVec->mutableRawNulls();
        VELOX_CHECK_NOT_NULL(nulls, "buildOutputTree ROW: mutableRawNulls null");
        const void* nullsPtr = static_cast<const void*>(rowVec->mutableRawNulls());

        auto* h = allocColumnHandle(/*nBuffers=*/1, /*nChildren=*/static_cast<int32_t>(n));
        h->length = static_cast<int64_t>(capacity);
        h->nullCount = 0;
        h->buffers[0] = nullsPtr;
        for (size_t i = 0; i < n; ++i) {
          h->children[i] = childDescs[i];
        }

        VectorPtr vec = std::move(rowVec);
        return {std::move(vec), h};
      } catch (...) {
        for (auto* d : childDescs) {
          freeColumnHandleTree(d);
        }
        throw;
      }
    }

    default:
      VELOX_UNSUPPORTED(
          "allocateNestedOutput: unsupported TypeKind {}; "
          "supported: fixed-width primitives, VARCHAR, VARBINARY, ARRAY, MAP, ROW",
          static_cast<int>(type->kind()));
  }
}

std::pair<VeloxColumnHandle*, int64_t> allocateNestedOutput(const TypePtr& type, int32_t rowCapacity) {
  auto& pool = outputPool();
  auto [vec, desc] = buildOutputTree(type, rowCapacity, pool);
  // C1 double-wrap: store the VectorPtr inside a shared_ptr<VectorPtr> so that
  // retrieve<VectorPtr> returns shared_ptr<VectorPtr> which the caller dereferences once.
  auto vecOwner = std::make_shared<VectorPtr>(std::move(vec));
  int64_t handle = outputStore().save(std::move(vecOwner));
  return {desc, handle};
}

} // namespace gluten

// ---- Write path: growChild ----

namespace gluten {

/**
 * Recursively appends a vector node's refreshed buffer addresses to `addrs`, in the SAME
 * per-node layout the Java side expects:
 *   FLAT leaf:  [nulls, values]
 *   ARRAY/MAP:  [nulls, offsets, sizes]
 *   ROW:        [nulls, then each field's recursive block in field order]
 *
 * Used to refresh the field children of a ROW target after RowVector::resize has
 * recursively reallocated their buffers (see growChild).  `v` must already be sized to the
 * post-resize capacity; this function only re-fetches (and, for ARRAY/MAP/ROW, forces the
 * buffer allocation via mutable accessors) -- it does NOT change logical sizes.  Nulls for
 * newly grown slots are initialized to not-null by the enclosing RowVector::resize path;
 * here we only expose the pointers.
 */
static void appendNodeAddrs(const VectorPtr& vp, std::vector<int64_t>& addrs) {
  BaseVector* v = vp.get();
  uint64_t* nulls = v->mutableRawNulls();
  addrs.push_back(reinterpret_cast<int64_t>(nulls));
  switch (v->encoding()) {
    case VectorEncoding::Simple::FLAT: {
      VectorPtr tmp = vp;
      addrs.push_back(reinterpret_cast<int64_t>(mutableValuesAsVoid(tmp)));
      break;
    }
    case VectorEncoding::Simple::ARRAY: {
      auto* a = v->asUnchecked<ArrayVector>();
      BufferPtr offsBuf = a->mutableOffsets(a->size());
      BufferPtr sizesBuf = a->mutableSizes(a->size());
      addrs.push_back(reinterpret_cast<int64_t>(offsBuf->asMutable<vector_size_t>()));
      addrs.push_back(reinterpret_cast<int64_t>(sizesBuf->asMutable<vector_size_t>()));
      break;
    }
    case VectorEncoding::Simple::MAP: {
      auto* m = v->asUnchecked<MapVector>();
      BufferPtr offsBuf = m->mutableOffsets(m->size());
      BufferPtr sizesBuf = m->mutableSizes(m->size());
      addrs.push_back(reinterpret_cast<int64_t>(offsBuf->asMutable<vector_size_t>()));
      addrs.push_back(reinterpret_cast<int64_t>(sizesBuf->asMutable<vector_size_t>()));
      break;
    }
    case VectorEncoding::Simple::ROW: {
      auto* r = v->asUnchecked<RowVector>();
      for (size_t i = 0; i < r->childrenSize(); ++i) {
        appendNodeAddrs(r->childAt(static_cast<column_index_t>(i)), addrs);
      }
      break;
    }
    default:
      VELOX_UNSUPPORTED("growChild: unsupported ROW field child encoding {}", static_cast<int>(v->encoding()));
  }
}

std::vector<int64_t> growChild(int64_t ownerHandle, const std::vector<int32_t>& childPath, int32_t newCapacity) {
  // C1 unwrap: the store holds shared_ptr<VectorPtr> (double-wrapped).
  // retrieve<VectorPtr> returns shared_ptr<VectorPtr>; dereference once to get VectorPtr.
  auto owner = ObjectStore::retrieveChecked<VectorPtr>(static_cast<ObjectHandle>(ownerHandle));
  VectorPtr root = *owner;

  // Navigate the childPath from root to the target child vector.  Single source of truth:
  // navigateChildPath performs the exact ARRAY->elements() / MAP->keys|values / ROW->childAt(idx)
  // descent shared with allocateStringChunkAt / finalizeStringColumnAt (prevents divergence).
  VectorPtr current = navigateChildPath(root, childPath);

  // Resize the target child to at least newCapacity.  Velox resize is geometric: the
  // actual new size() equals newCapacity; backing buffer capacity may exceed it.
  // After resize, raw buffer pointers (values/nulls/offsets/sizes) may be stale due to
  // reallocation -- all addresses are re-fetched below.
  // Capture the pre-resize logical size: resize() PRESERVES the null bits for existing
  // rows [0, oldSize), so we must NOT clobber them below.
  const vector_size_t oldSize = current->size();
  current->resize(static_cast<vector_size_t>(newCapacity));

  // Re-fetch the nulls pointer; force allocation if not yet present, and initialize
  // ONLY the new slots [oldSize, newCapacity) to valid (not-null) so Java can write
  // without clearing nulls first -- preserving any null bits already set for existing
  // rows [0, oldSize).  A word-granular memset would corrupt the partial word straddling
  // oldSize, so use bits::fillBits for exact-range, bit-granular init.
  std::vector<int64_t> addrs;

  uint64_t* nulls = current->mutableRawNulls();
  VELOX_CHECK_NOT_NULL(nulls, "growChild: mutableRawNulls null after resize; newCapacity={}", newCapacity);
  bits::fillBits(nulls, oldSize, static_cast<vector_size_t>(newCapacity), bits::kNotNull);
  // Index 0 in returned array: nullsAddr (always present, matching descriptor buffers[0]).
  addrs.push_back(reinterpret_cast<int64_t>(nulls));

  // Re-fetch type-specific buffer addresses.  Layout mirrors VeloxColumnHandle descriptor:
  //   FLAT leaf:  [nullsAddr, valuesAddr]             (2 longs)
  //   ARRAY/MAP:  [nullsAddr, offsetsAddr, sizesAddr]  (3 longs)
  //   ROW:        [nullsAddr]                          (1 long)
  switch (current->encoding()) {
    case VectorEncoding::Simple::FLAT: {
      // Re-fetch mutable values pointer after resize (may have reallocated).  For a VARCHAR
      // (StringView) leaf this returns the fresh 16-byte slot base; FlatVector<StringView>::resize
      // preserves the existing slots (inline bytes AND absolute char* pointers into the pinned,
      // append-only string chunks -- which never move) and value-initializes the new slots to an
      // empty StringView, so Java's refreshFrom can safely re-point valuesAddr without touching the
      // string-chunk write cursor.  Guards against the nested-VARCHAR growth use-after-free.
      void* valPtr = mutableValuesAsVoid(current);
      const TypeKind kind = current->typeKind();
      if (kind == TypeKind::BOOLEAN) {
        // BOOLEAN values are bit-packed (1 bit/row, LSB-first).  Unlike the StringView leaf,
        // FlatVector<bool>::resize does NOT value-initialize the grown region: it forwards
        // std::nullopt to resizeValues -> AlignedBuffer::reallocate<bool>, so the new bits
        // [oldSize, newCapacity) hold uninitialized/garbage bits that can read back as `true`
        // (and, when the buffer had spare capacity, resize keeps stale bits from the prior
        // allocation).  Explicitly clear the new bits to FALSE so any grown index not yet
        // written by Java reads back as false, mirroring the new-row init done for nulls above.
        // Existing bits [0, oldSize) are preserved (resize copies them) and must not be touched.
        bits::fillBits(reinterpret_cast<uint64_t*>(valPtr), oldSize, static_cast<vector_size_t>(newCapacity), false);
      } else if (kind == TypeKind::VARCHAR || kind == TypeKind::VARBINARY) {
        // StringView-backed leaves (both VARCHAR and VARBINARY use FlatVector<StringView>).
        // FlatVector<StringView>::resize DOES value-initialize the grown slots to an empty
        // StringView (see the comment above), so we must NOT memset them -- doing so would
        // corrupt the StringView size/prefix/pointer invariant.  Leave them untouched.
      } else {
        // Remaining FLAT scalar leaves are fixed-width (TIMESTAMP=16, DECIMAL short=BIGINT/8 and
        // long=HUGEINT/16, plus INTEGER/BIGINT/REAL/DOUBLE/etc.).  FlatVector<T>::resize forwards
        // std::nullopt to resizeValues and does NOT value-initialize the grown range, so the new
        // slots [oldSize, newCapacity) hold garbage while marked VALID above -- a positional
        // consumer (unnest/explode/full-child copy) would read that garbage as real data.
        // Zero-init the grown byte range only (0 is a safe representable value: 0 micros = epoch,
        // 0 int128 = decimal 0), mirroring the BOOLEAN bit-clear.  Existing slots [0, oldSize) are
        // preserved by resize and untouched.  The stride is the type's cpp element width; the
        // post-resize buffer capacity is >= newCapacity*W (AlignedBuffer rounds up), so this
        // memset is in-bounds.
        const size_t w = current->type()->cppSizeInBytes();
        std::memset(
            static_cast<uint8_t*>(valPtr) + static_cast<size_t>(oldSize) * w,
            0,
            static_cast<size_t>(static_cast<vector_size_t>(newCapacity) - oldSize) * w);
      }
      addrs.push_back(reinterpret_cast<int64_t>(valPtr));
      break;
    }
    case VectorEncoding::Simple::ARRAY: {
      // mutableOffsets/mutableSizes resize the buffer to at least `size` and return the
      // BufferPtr; call asMutable<> to get the raw writable pointer.
      auto* a = current->asUnchecked<ArrayVector>();
      BufferPtr offsBuf = a->mutableOffsets(static_cast<vector_size_t>(newCapacity));
      BufferPtr sizesBuf = a->mutableSizes(static_cast<vector_size_t>(newCapacity));
      addrs.push_back(reinterpret_cast<int64_t>(offsBuf->asMutable<vector_size_t>()));
      addrs.push_back(reinterpret_cast<int64_t>(sizesBuf->asMutable<vector_size_t>()));
      break;
    }
    case VectorEncoding::Simple::MAP: {
      auto* m = current->asUnchecked<MapVector>();
      BufferPtr offsBuf = m->mutableOffsets(static_cast<vector_size_t>(newCapacity));
      BufferPtr sizesBuf = m->mutableSizes(static_cast<vector_size_t>(newCapacity));
      addrs.push_back(reinterpret_cast<int64_t>(offsBuf->asMutable<vector_size_t>()));
      addrs.push_back(reinterpret_cast<int64_t>(sizesBuf->asMutable<vector_size_t>()));
      break;
    }
    case VectorEncoding::Simple::ROW: {
      // ROW child: the earlier incomplete version returned only the ROW nulls, leaving the
      // Java field-child objects with STALE valuesAddr after RowVector::resize reallocated
      // their buffers -> heap UAF on the next putX.  Fix: append each field child's refreshed
      // address block (recursively), so Java can refresh every field child's pointers AND
      // bump its capacity.  Layout after this: [rowNulls, <field0 block>, <field1 block>, ...]
      // where a leaf block is [nulls, values], array/map is [nulls, offsets, sizes], and a
      // nested row is [nulls, <its field blocks>].
      auto* r = current->asUnchecked<RowVector>();
      for (size_t i = 0; i < r->childrenSize(); ++i) {
        appendNodeAddrs(r->childAt(static_cast<column_index_t>(i)), addrs);
      }
      break;
    }
    default:
      VELOX_UNSUPPORTED("growChild: unsupported target child encoding {}", static_cast<int>(current->encoding()));
  }

  return addrs;
}

} // namespace gluten
