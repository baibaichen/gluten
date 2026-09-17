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
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <cstdlib>

namespace gluten {

/**
 * Returns a reference to the process-wide live-node counter.
 * Every successful allocColumnHandle increments it; every freeColumnHandleShallow
 * decrements it. This seam is used by tests to verify that exception paths in
 * exportVector leave no leaked descriptor nodes. The function-local static ensures
 * a single counter is shared across all translation units.
 */
inline std::atomic<int64_t>& veloxColumnHandleLiveCount() {
  static std::atomic<int64_t> count{0};
  return count;
}

/**
 * VeloxColumnHandle is a fixed-layout, pure-view descriptor for a Velox column.
 * It is a 48-byte struct with 6 fields, each 8 bytes, at fixed offsets:
 *   [0]  length
 *   [8]  nullCount
 *   [16] nBuffers
 *   [24] nChildren
 *   [32] buffers
 *   [40] children
 *
 * This is a PURE VIEW: it does NOT own the underlying vector data buffers.
 * Vector data ownership on the read path belongs to the Velox operator;
 * on the write path it belongs to the ObjectStore owner handle (Task 11).
 * Only the descriptor memory itself (the handle node plus the buffers/children
 * pointer arrays) is managed by allocColumnHandle / freeColumnHandle*.
 */
struct VeloxColumnHandle {
  int64_t length;
  int64_t nullCount;
  int64_t nBuffers;
  int64_t nChildren;
  const void** buffers; // [nBuffers] — pointers to Velox buffer data (not owned)
  VeloxColumnHandle** children; // [nChildren] — child descriptor nodes
};

static_assert(sizeof(VeloxColumnHandle) == 48, "VeloxColumnHandle must be 48 bytes (pure view)");

/**
 * Allocates a VeloxColumnHandle node plus its buffers pointer array
 * (nBuffers × 8 bytes) and children pointer array (nChildren × 8 bytes).
 * All memory is zero-initialised via calloc.
 * Returns a non-null pointer on success; aborts on allocation failure
 * (consistent with the rest of Gluten's native code which does not handle
 * out-of-memory gracefully in this layer).
 */
inline VeloxColumnHandle* allocColumnHandle(int64_t nBuffers, int64_t nChildren) {
  auto* h = static_cast<VeloxColumnHandle*>(std::calloc(1, sizeof(VeloxColumnHandle)));
  if (!h) {
    std::abort();
  }
  h->nBuffers = nBuffers;
  h->nChildren = nChildren;
  if (nBuffers > 0) {
    h->buffers = static_cast<const void**>(std::calloc(nBuffers, sizeof(void*)));
    if (!h->buffers) {
      std::abort();
    }
  } else {
    h->buffers = nullptr;
  }
  if (nChildren > 0) {
    h->children = static_cast<VeloxColumnHandle**>(std::calloc(nChildren, sizeof(VeloxColumnHandle*)));
    if (!h->children) {
      std::abort();
    }
  } else {
    h->children = nullptr;
  }
  veloxColumnHandleLiveCount().fetch_add(1, std::memory_order_relaxed);
  return h;
}

/**
 * Frees the descriptor memory for a single VeloxColumnHandle node:
 * the buffers pointer array, the children pointer array, and the handle itself.
 * Does NOT recurse into child nodes and does NOT touch any vector data buffers.
 */
inline void freeColumnHandleShallow(VeloxColumnHandle* h) {
  if (!h) {
    return;
  }
  veloxColumnHandleLiveCount().fetch_sub(1, std::memory_order_relaxed);
  std::free(h->buffers);
  std::free(h->children);
  std::free(h);
}

/**
 * Recursively frees the entire descriptor memory tree rooted at h.
 * Calls freeColumnHandleShallow on every node in post-order.
 * Does NOT release any underlying Velox vector data buffers — those are
 * owned externally (see struct doc above).
 */
inline void freeColumnHandleTree(VeloxColumnHandle* h) {
  if (!h) {
    return;
  }
  for (int64_t i = 0; i < h->nChildren; ++i) {
    freeColumnHandleTree(h->children[i]);
  }
  freeColumnHandleShallow(h);
}

} // namespace gluten
