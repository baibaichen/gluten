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
#include <jni.h>
#include <cstdint>
#include <functional>
#include <memory>
#include <utility>
#include <vector>
#include "memory/ColumnarBatch.h"
#include "vector/VeloxColumnHandle.h"
#include "velox/vector/BaseVector.h"

namespace gluten {

/**
 * Exports a Velox vector as a VeloxColumnHandle pure-view descriptor.
 *
 * Currently supports only FLAT-encoded primitive vectors (BOOLEAN, TINYINT,
 * SMALLINT, INTEGER, BIGINT, REAL, DOUBLE) plus VARCHAR / VARBINARY, and the
 * nested ARRAY / ROW / MAP encodings. Other encodings throw VELOX_UNSUPPORTED.
 * BOOLEAN is stored bit-packed (bit i lives in word i/64, bit i%64; equivalently
 * byte i/8, bit i%8 on little-endian). Its exported buffers[1] points at that
 * bit-packed values buffer and MUST be read bit-by-bit (LSB-first), matching the
 * Java FlatAccessor.getBoolean reader -- never byte-per-element like other kinds.
 *
 * The returned handle is a shallow view: it does NOT own the underlying Velox
 * buffer data. The caller is responsible for keeping the source VectorPtr alive
 * for the lifetime of the handle, and for releasing the descriptor memory via
 * freeColumnHandleShallow() when done.
 *
 * @param vec  The Velox vector to export. Must be FLAT-encoded and of a
 *             fixed-width primitive TypeKind.
 * @return     A heap-allocated VeloxColumnHandle with:
 *               nBuffers = 2
 *               buffers[0] = rawNulls() bitmap (nullptr when no nulls exist)
 *               buffers[1] = rawValues() pointer cast to const void*
 *               nChildren = 0
 */
VeloxColumnHandle* exportVector(const facebook::velox::VectorPtr& vec);

/**
 * Returns true iff the entire (possibly nested) column is exportable by the
 * current exportVector implementation. Post-order recursive; reads only
 * encoding metadata; allocates nothing.
 *
 * Returns false for:
 *  - Any encoding other than FLAT, ARRAY, ROW, MAP
 *  - Any descendant that is unclassifiable (whole-column fallback)
 *
 * @param v  Non-null pointer to a Velox BaseVector.
 * @return   true if exportVector can handle this vector and all its descendants.
 */
bool classifyColumn(const facebook::velox::BaseVector* v);

/**
 * Exports each column of a Velox batch as a read-side VeloxColumnHandle descriptor.
 *
 * Retrieves the ColumnarBatch identified by @p batchHandle from the ObjectStore,
 * casts it to VeloxColumnarBatch, then calls exportVector() on each child.
 * Already exportable children retain their original buffers. Encoded children
 * use getFlattenedRowVector(), whose storage is retained by the owning batch
 * without replacing its original RowVector. The descriptors do NOT own
 * the underlying Velox buffer data.  The caller must keep the source batch alive
 * for the lifetime of the returned descriptors, and must call freeColumnHandleTree()
 * on each descriptor when done.
 *
 * Exception safety: if exportVector() throws for any column, all already-exported
 * descriptors are freed before the exception propagates (no descriptor node leaks).
 *
 * @param batchHandle  An ObjectStore handle for a VeloxColumnarBatch.
 * @return             One VeloxColumnHandle* per column, in column order.
 * @throws VeloxException  If the handle is not found, is not a VeloxColumnarBatch,
 *                         or if any column cannot be exported.
 */
std::vector<VeloxColumnHandle*> exportBatchColumns(int64_t batchHandle);

/**
 * Assembles N owned output column vectors (created by allocateNestedOutput) into a
 * native Velox RowVector, wraps it in a VeloxColumnarBatch, persists it via
 * @p saveFn, and returns the handle produced by saveFn.
 *
 * The caller supplies @p saveFn so that the batch can be stored in any
 * ObjectStore -- typically the runtime's store (ctx->saveObject) so that the
 * resulting handle is compatible with ColumnarBatches / JVM ColumnarBatch.
 *
 * Ownership contract: makeVeloxBatch does NOT release the ownerHandles -- the
 * caller is responsible for that.  Each child VectorPtr is copied (shared_ptr
 * reference count incremented) into the RowVector; releasing an ownerHandle
 * afterwards drops only the ObjectStore's outer shared_ptr<VectorPtr> wrapper.
 * The inner vector stays alive as long as the batch (RowVector) is alive.
 *
 * The C1 double-wrap invariant: allocateNestedOutput stores shared_ptr<VectorPtr>
 * (i.e. shared_ptr<shared_ptr<BaseVector>>) in the ObjectStore.  This function
 * uses retrieve<facebook::velox::VectorPtr> and dereferences the result to
 * obtain the actual VectorPtr before passing it to the RowVector constructor.
 *
 * Cross-column size check: all columns must have the same size (derived from
 * the first column).  Mismatched sizes throw VELOX_CHECK_EQ immediately.
 *
 * @param ownerHandles  ObjectStore handles returned by allocateNestedOutput.
 * @param names         Column names corresponding to each handle, in order.
 * @param saveFn        Callback that persists the assembled VeloxColumnarBatch
 *                      and returns an int64_t handle for it.
 * @return              The int64_t handle returned by saveFn.
 * @throws VeloxException  If sizes differ, any handle is not found, or column
 *                         sizes are mismatched across columns.
 */
int64_t makeVeloxBatch(
    const std::vector<int64_t>& ownerHandles,
    const std::vector<std::string>& names,
    const std::function<int64_t(std::shared_ptr<gluten::ColumnarBatch>)>& saveFn);

/**
 * Allocates a new string-data chunk (native Velox Buffer) attached to the
 * FlatVector<StringView> identified by @p ownerHandle, and returns its base
 * address and usable capacity in bytes.
 *
 * Java uses the returned address to write long (>12-byte) string data directly
 * via Unsafe, advancing an offset within the chunk.  When the chunk is full,
 * Java calls this function again to get a fresh one.  All chunks live in the
 * vector's stringBuffers_ and are serialized correctly by Velox once
 * finalizeStringColumn has trimmed each buffer's size to the bytes actually
 * used.
 *
 * The chunk's initial size is set to 0; Java tracks the used offset and
 * reports it to finalizeStringColumn.
 *
 * @param ownerHandle  ObjectStore handle returned by allocateNestedOutput(VARCHAR(), n).
 * @param minBytes     Minimum number of bytes the caller needs; the actual
 *                     chunk may be larger (capped at FlatVector::kInitialStringSize
 *                     or minBytes, whichever is greater).
 * @return {rawAddr, capacity}  where rawAddr is the int64_t cast of the
 *         mutable char* base of the new chunk, and capacity is the usable byte
 *         count (== buf->capacity()).
 */
std::pair<int64_t, int64_t> allocateStringChunk(int64_t ownerHandle, int64_t minBytes);

/**
 * Finalizes a VARCHAR output column by setting each registered string-data
 * chunk buffer's size to the number of bytes actually written by Java.
 *
 * Must be called once, after Java has finished writing all string values,
 * and before makeVeloxBatch assembles the column into a RowVector.  Velox's
 * serializer uses Buffer::size() (not capacity()) to determine how many bytes
 * to include; without this call the trailing unwritten bytes would be
 * included in any serialization (e.g. shuffle).
 *
 * @param ownerHandle    ObjectStore handle returned by allocateNestedOutput(VARCHAR(), n).
 * @param chunkUsedBytes One entry per chunk registered via allocateStringChunk,
 *                       in registration order.  Each value is the number of
 *                       bytes written into that chunk.  The count must match
 *                       the number of chunks (VELOX_CHECK enforced).
 */
void finalizeStringColumn(int64_t ownerHandle, const std::vector<int64_t>& chunkUsedBytes);

/**
 * childPath-aware variant of allocateStringChunk for NESTED VARCHAR children.
 *
 * Navigates from the root vector (identified by @p rootOwnerHandle) down
 * @p childPath (ARRAY->elements, MAP->keys/values, ROW->childAt) to the target
 * FlatVector<StringView> child, then allocates a new string-data chunk attached
 * to that child.  An empty childPath addresses the root itself, so this is a
 * strict superset of allocateStringChunk.
 *
 * @param rootOwnerHandle ObjectStore handle of the ROOT vector (from
 *                        allocateNestedOutput).
 * @param childPath       Steps from root to the target FlatVector<StringView>.
 * @param minBytes        Minimum number of bytes the caller needs.
 * @return {rawAddr, capacity} of the newly allocated chunk (see allocateStringChunk).
 */
std::pair<int64_t, int64_t>
allocateStringChunkAt(int64_t rootOwnerHandle, const std::vector<int32_t>& childPath, int64_t minBytes);

/**
 * childPath-aware variant of finalizeStringColumn for NESTED VARCHAR children.
 *
 * Navigates from the root vector to the target FlatVector<StringView> child (as
 * in allocateStringChunkAt) and trims each registered string-data chunk of that
 * child to the bytes actually written.
 *
 * @param rootOwnerHandle ObjectStore handle of the ROOT vector.
 * @param childPath       Steps from root to the target FlatVector<StringView>.
 * @param chunkUsedBytes  One entry per chunk registered via allocateStringChunkAt.
 */
void finalizeStringColumnAt(
    int64_t rootOwnerHandle,
    const std::vector<int32_t>& childPath,
    const std::vector<int64_t>& chunkUsedBytes);

/**
 * Allocates an empty writable Velox output vector tree for any supported flat or nested type
 * and exports a writable descriptor tree mirroring the structure produced by exportVector.
 *
 * Recursively creates Velox vectors bottom-up:
 *   - Leaf (fixed-width primitive or VARCHAR/VARBINARY): FlatVector, nBuffers=2, nChildren=0.
 *   - ARRAY: ArrayVector with one elements child, nBuffers=3 (nulls/offsets/sizes), nChildren=1.
 *   - MAP:   MapVector with keys+values children, nBuffers=3 (nulls/offsets/sizes), nChildren=2.
 *   - ROW:   RowVector with N field children, nBuffers=1 (nulls), nChildren=N.
 *
 * All nulls bitmaps are pre-allocated and initialized to all-valid (0xFF bytes).
 * Offsets and sizes for ARRAY/MAP are zero-initialized.
 *
 * The buffers in the returned descriptor tree point to mutable addresses inside the
 * Velox vectors, consistent with the read-side layout produced by exportVector so that
 * the assembled batch can be read back without conversion.
 *
 * C1 double-wrap invariant: the root vector is stored in the ObjectStore as
 * shared_ptr<VectorPtr> (i.e. shared_ptr<shared_ptr<BaseVector>>).  To retrieve it:
 *   auto owner = ObjectStore::retrieve<VectorPtr>(static_cast<ObjectHandle>(handle));
 *   VectorPtr v = *owner;
 *
 * Child capacity: all child vectors are pre-allocated with rowCapacity slots.
 * Use growChild to grow beyond this initial capacity and refresh buffer addresses.
 *
 * @param type         The Velox TypePtr describing the output column's type.
 * @param rowCapacity  Number of rows (and initial element slots for children).
 * @return {descriptor, ownerHandle}
 *           descriptor  -- root of a heap-allocated writable VeloxColumnHandle tree.
 *                         Free with freeColumnHandleTree() when done.
 *           ownerHandle -- ObjectStore handle keeping the vector tree alive.
 *                         Release with ObjectStore::release(ownerHandle) when done.
 * @throws VeloxException  If any TypeKind in the tree is unsupported.
 */
std::pair<VeloxColumnHandle*, int64_t> allocateNestedOutput(const facebook::velox::TypePtr& type, int32_t rowCapacity);

/**
 * Navigates from the root vector (identified by ownerHandle) along childPath to
 * the target child vector, resizes it to at least newCapacity, and returns the
 * child's REFRESHED mutable buffer addresses so Java can re-fetch write pointers
 * after potential reallocation.
 *
 * Velox's BaseVector::resize() grows geometrically but may reallocate the child's
 * backing buffers, invalidating any raw pointers held by the Java descriptor.
 * This function performs the resize and re-exports only the target child's fresh
 * buffer addresses so the Java side can update its write-pointer cache.
 *
 * childPath navigation (each step descends one level):
 *   - At an ARRAY vector: index must be 0 -> elements()
 *   - At a MAP vector:    index 0 -> mapKeys(), index 1 -> mapValues()
 *   - At a ROW vector:    index i -> childAt(i)
 *
 * Returned long[] layout (mirrors VeloxColumnHandle descriptor buffer layout):
 *   FLAT leaf:  [nullsAddr, valuesAddr]              -- 2 longs
 *   ARRAY/MAP:  [nullsAddr, offsetsAddr, sizesAddr]  -- 3 longs
 *   ROW:        [nullsAddr]                          -- 1 long
 *
 * All returned addresses are non-null mutable pointers into the Velox
 * pool-backed buffers. The nulls bitmap is initialized to all-valid (0xFF)
 * for the new capacity after growth.
 *
 * C1 invariant: ownerHandle must be an ObjectStore handle holding
 * shared_ptr<VectorPtr> (double-wrapped), as produced by allocateNestedOutput.
 *
 * @param ownerHandle  ObjectStore handle returned by allocateNestedOutput.
 * @param childPath    Sequence of integer indices navigating from root to target child.
 * @param newCapacity  Minimum new capacity for the target child vector.
 * @return Refreshed mutable buffer addresses for the child (layout described above).
 * @throws VeloxException  If the handle is not found, any childPath step is invalid,
 *                         or the target child encoding is unsupported.
 */
std::vector<int64_t> growChild(int64_t ownerHandle, const std::vector<int32_t>& childPath, int32_t newCapacity);

/**
 * Parses a DFS pre-order type-code encoding into a Velox TypePtr, advancing @p pos
 * past the consumed codes. @p len is the number of valid int codes in @p enc; each
 * read and recursion is bounds-checked against it so a truncated/malformed encoding
 * throws (VELOX_USER_FAIL) instead of reading past the buffer.
 *
 * Exposed (non-static) so unit tests can exercise the bounds checking directly.
 */
facebook::velox::TypePtr parseTypeEncoding(const jint* enc, size_t& pos, size_t len);

} // namespace gluten
