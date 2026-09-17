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

/**
 * VeloxColumnHandleJni.cc -- JNI endpoints for the read-side zero-copy descriptor.
 *
 * exportDescriptor: takes a raw pointer to a Velox VectorPtr and returns the
 * address of a freshly allocated VeloxColumnHandle descriptor tree.
 *
 * freeDescriptor: recursively frees the descriptor tree produced by
 * exportDescriptor.  Does NOT touch the underlying Velox vector data -- vector
 * lifetime is managed by the owning VectorPtr on the C++ side.
 */

#include <jni.h>
#include <stdexcept>

#include "jni/JniError.h"
#include "utils/ObjectStore.h"
#include "vector/VeloxColumnHandle.h"
#include "vector/VeloxColumnHandleExport.h"
#include "velox/vector/BaseVector.h"

extern "C" {

/**
 * exportDescriptor -- export a Velox vector as a read-side view descriptor.
 *
 * @param veloxVectorPtr  Raw address of a facebook::velox::VectorPtr held by
 *                        the caller (the vector must remain alive for the full
 *                        lifetime of the returned descriptor).
 * @return Address of the root VeloxColumnHandle descriptor, or 0 on failure.
 *         Caller must release it with freeDescriptor when done.
 */
JNIEXPORT jlong JNICALL Java_org_apache_gluten_velox_vector_VeloxColumnHandleJniWrapper_exportDescriptor(
    JNIEnv* env,
    jclass /*clazz*/,
    jlong veloxVectorPtr) {
  JNI_METHOD_START
  auto* vp = reinterpret_cast<facebook::velox::VectorPtr*>(veloxVectorPtr);
  gluten::VeloxColumnHandle* handle = gluten::exportVector(*vp);
  return reinterpret_cast<jlong>(handle);
  JNI_METHOD_END(0)
}

/**
 * freeDescriptor -- recursively free the descriptor tree produced by exportDescriptor.
 *
 * Frees only the VeloxColumnHandle descriptor nodes and their pointer arrays.
 * Does NOT release any underlying Velox buffer data.
 *
 * @param rootDescAddr  Address returned by exportDescriptor (may be 0/null).
 */
JNIEXPORT void JNICALL Java_org_apache_gluten_velox_vector_VeloxColumnHandleJniWrapper_freeDescriptor(
    JNIEnv* env,
    jclass /*clazz*/,
    jlong rootDescAddr) {
  JNI_METHOD_START
  gluten::freeColumnHandleTree(reinterpret_cast<gluten::VeloxColumnHandle*>(rootDescAddr));
  JNI_METHOD_END()
}

/**
 * releaseOutput -- release a write-side Velox vector from the ObjectStore.
 *
 * Drops the reference to the VectorPtr saved by allocateNestedOutput, allowing the
 * Velox vector and its backing buffers to be freed. The caller must stop
 * accessing the native buffers after this call.
 *
 * @param ownerHandle  The ownerHandle returned by allocateNestedOutput[1].
 */
JNIEXPORT void JNICALL Java_org_apache_gluten_velox_vector_VeloxColumnHandleJniWrapper_releaseOutput(
    JNIEnv* env,
    jclass /*clazz*/,
    jlong ownerHandle) {
  JNI_METHOD_START
  gluten::ObjectStore::release(static_cast<gluten::ObjectHandle>(ownerHandle));
  JNI_METHOD_END()
}

/**
 * exportBatchColumnDescriptors -- export each column of a Velox batch as a read-side descriptor.
 *
 * Retrieves the ColumnarBatch identified by batchHandle from the ObjectStore,
 * then calls exportBatchColumns() to produce one VeloxColumnHandle* per column.
 * The returned jlongArray contains the descriptor addresses in column order.
 * Each address must be imported via VeloxColumnVector#importFromNative and
 * released via that vector's close() -> freeDescriptor.
 *
 * @param batchHandle  An ObjectStore handle for a VeloxColumnarBatch.
 * @return             jlongArray of descriptor addresses, one per column, in column order.
 */
JNIEXPORT jlongArray JNICALL
Java_org_apache_gluten_velox_vector_VeloxColumnHandleJniWrapper_exportBatchColumnDescriptors(
    JNIEnv* env,
    jclass,
    jlong batchHandle) {
  JNI_METHOD_START
  auto descs = gluten::exportBatchColumns(static_cast<int64_t>(batchHandle));

  // Helper: free all descriptors (used on every failure path).
  auto freeAll = [&descs]() {
    for (auto* d : descs) {
      gluten::freeColumnHandleTree(d);
    }
  };

  // Build the address array inside a try/catch so that any C++ exception
  // (e.g. std::bad_alloc from the std::vector constructor) frees the
  // heap-allocated descriptors before the exception propagates.
  jlongArray result = nullptr;
  try {
    result = env->NewLongArray(static_cast<jsize>(descs.size()));
    if (result == nullptr) {
      // NewLongArray failed (OOM in JVM); ownership of descs stays with us.
      freeAll();
      return nullptr;
    }

    std::vector<jlong> addrs(descs.size());
    for (size_t i = 0; i < descs.size(); ++i) {
      addrs[i] = reinterpret_cast<jlong>(descs[i]);
    }
    env->SetLongArrayRegion(result, 0, static_cast<jsize>(addrs.size()), addrs.data());
  } catch (...) {
    freeAll();
    throw;
  }

  // SUCCESS: descriptor ownership is transferred to the Java caller via the
  // returned addresses.  Do NOT free descs here.
  return result;
  JNI_METHOD_END(nullptr)
}

/**
 * allocateStringChunk -- allocate a new string-data chunk for a VARCHAR output column.
 *
 * Delegates to gluten::allocateStringChunk which allocates a Velox Buffer
 * attached to the FlatVector<StringView> identified by ownerHandle.  Java
 * writes long (>12-byte) string data directly into the chunk via Unsafe and
 * calls this function again when the chunk is full.  All chunks are finalized
 * by finalizeStringColumn once writing is complete.
 *
 * @param ownerHandle  ObjectStore handle returned by allocateNestedOutput(VARCHAR(), n).
 * @param minBytes     Minimum usable capacity requested by the caller.
 * @return jlong[2] = {addr, capacity} where addr is the mutable char* base cast
 *         to int64_t and capacity is the usable byte count of the new chunk.
 */
JNIEXPORT jlongArray JNICALL Java_org_apache_gluten_velox_vector_VeloxColumnHandleJniWrapper_allocateStringChunk(
    JNIEnv* env,
    jclass,
    jlong ownerHandle,
    jlong minBytes) {
  JNI_METHOD_START
  auto [addr, cap] = gluten::allocateStringChunk(static_cast<int64_t>(ownerHandle), static_cast<int64_t>(minBytes));
  jlongArray out = env->NewLongArray(2);
  if (out == nullptr) {
    // NewLongArray failed (JVM OOM); exception already pending. The chunk buffer
    // is attached to the vector owned by ownerHandle (still owned by the caller),
    // so there is no separate native tree to free here -- just return safely to
    // avoid SetLongArrayRegion on a null array (UB).
    return nullptr;
  }
  jlong buf[2] = {static_cast<jlong>(addr), static_cast<jlong>(cap)};
  env->SetLongArrayRegion(out, 0, 2, buf);
  return out;
  JNI_METHOD_END(nullptr)
}

/**
 * finalizeStringColumn -- set each string-data chunk's size to the bytes actually written.
 *
 * Must be called once after Java has finished writing all string values and
 * before makeVeloxBatch assembles the column into a RowVector.  Delegates to
 * gluten::finalizeStringColumn.
 *
 * @param ownerHandle    ObjectStore handle returned by allocateNestedOutput(VARCHAR(), n).
 * @param chunkUsedBytes One entry per chunk allocated via allocateStringChunk, in
 *                       registration order; each value is the number of bytes written.
 */
JNIEXPORT void JNICALL Java_org_apache_gluten_velox_vector_VeloxColumnHandleJniWrapper_finalizeStringColumn(
    JNIEnv* env,
    jclass,
    jlong ownerHandle,
    jlongArray chunkUsedBytes) {
  JNI_METHOD_START
  jsize n = env->GetArrayLength(chunkUsedBytes);
  std::vector<int64_t> used(n);
  env->GetLongArrayRegion(chunkUsedBytes, 0, n, reinterpret_cast<jlong*>(used.data()));
  gluten::finalizeStringColumn(static_cast<int64_t>(ownerHandle), used);
  JNI_METHOD_END()
}

/**
 * allocateStringChunkAt -- childPath-aware chunk allocation for NESTED VARCHAR children.
 *
 * Navigates from the root vector (rootOwnerHandle) down childPath to the target
 * FlatVector<StringView> child and allocates a new string-data chunk on it.
 * Delegates to gluten::allocateStringChunkAt.
 */
JNIEXPORT jlongArray JNICALL Java_org_apache_gluten_velox_vector_VeloxColumnHandleJniWrapper_allocateStringChunkAt(
    JNIEnv* env,
    jclass,
    jlong rootOwnerHandle,
    jintArray childPath,
    jlong minBytes) {
  JNI_METHOD_START
  jsize pathLen = env->GetArrayLength(childPath);
  std::vector<int32_t> path(static_cast<size_t>(pathLen));
  env->GetIntArrayRegion(childPath, 0, pathLen, reinterpret_cast<jint*>(path.data()));
  auto [addr, cap] =
      gluten::allocateStringChunkAt(static_cast<int64_t>(rootOwnerHandle), path, static_cast<int64_t>(minBytes));
  jlongArray out = env->NewLongArray(2);
  if (out == nullptr) {
    return nullptr;
  }
  jlong buf[2] = {static_cast<jlong>(addr), static_cast<jlong>(cap)};
  env->SetLongArrayRegion(out, 0, 2, buf);
  return out;
  JNI_METHOD_END(nullptr)
}

/**
 * finalizeStringColumnAt -- childPath-aware finalize for NESTED VARCHAR children.
 *
 * Navigates from the root vector (rootOwnerHandle) down childPath to the target
 * FlatVector<StringView> child and trims each registered chunk to bytes written.
 * Delegates to gluten::finalizeStringColumnAt.
 */
JNIEXPORT void JNICALL Java_org_apache_gluten_velox_vector_VeloxColumnHandleJniWrapper_finalizeStringColumnAt(
    JNIEnv* env,
    jclass,
    jlong rootOwnerHandle,
    jintArray childPath,
    jlongArray chunkUsedBytes) {
  JNI_METHOD_START
  jsize pathLen = env->GetArrayLength(childPath);
  std::vector<int32_t> path(static_cast<size_t>(pathLen));
  env->GetIntArrayRegion(childPath, 0, pathLen, reinterpret_cast<jint*>(path.data()));
  jsize n = env->GetArrayLength(chunkUsedBytes);
  std::vector<int64_t> used(n);
  env->GetLongArrayRegion(chunkUsedBytes, 0, n, reinterpret_cast<jlong*>(used.data()));
  gluten::finalizeStringColumnAt(static_cast<int64_t>(rootOwnerHandle), path, used);
  JNI_METHOD_END()
}

/**
 * growChild -- resize a nested child vector and return its refreshed mutable buffer
 * addresses so the Java write path can re-fetch write pointers after potential
 * reallocation.
 *
 * Navigates from the root vector (identified by ownerHandle) along childPath to the
 * target child, calls BaseVector::resize(newCapacity), and returns the child's fresh
 * buffer addresses.
 *
 * childPath navigation: ARRAY->elements() (index 0); MAP->keys/values (0/1); ROW->childAt(i).
 *
 * Returned long[] layout (mirrors VeloxColumnHandle descriptor buffer layout):
 *   FLAT leaf:  [nullsAddr, valuesAddr]              -- 2 longs
 *   ARRAY/MAP:  [nullsAddr, offsetsAddr, sizesAddr]  -- 3 longs
 *   ROW:        [nullsAddr]                          -- 1 long
 *
 * @param ownerHandle  ObjectStore handle returned by allocateNestedOutput.
 * @param childPath    Steps from root to target child.
 * @param newCapacity  Minimum new capacity for the target child.
 * @return jlong[] of refreshed mutable buffer addresses.
 */
JNIEXPORT jlongArray JNICALL Java_org_apache_gluten_velox_vector_VeloxColumnHandleJniWrapper_growChild(
    JNIEnv* env,
    jclass,
    jlong ownerHandle,
    jintArray childPath,
    jint newCapacity) {
  JNI_METHOD_START
  jsize pathLen = env->GetArrayLength(childPath);
  std::vector<int32_t> path(static_cast<size_t>(pathLen));
  env->GetIntArrayRegion(childPath, 0, pathLen, reinterpret_cast<jint*>(path.data()));

  auto addrs = gluten::growChild(static_cast<int64_t>(ownerHandle), path, static_cast<int32_t>(newCapacity));

  jlongArray result = env->NewLongArray(static_cast<jsize>(addrs.size()));
  if (result == nullptr) {
    return nullptr; // JVM OOM; exception already set by NewLongArray
  }
  env->SetLongArrayRegion(result, 0, static_cast<jsize>(addrs.size()), reinterpret_cast<const jlong*>(addrs.data()));
  return result;
  JNI_METHOD_END(nullptr)
}

// Helper: recursively parse the DFS-encoded int[] type encoding into a
// facebook::velox::TypePtr. Returns the parsed TypePtr and advances pos past consumed codes.
// Exposed with external linkage (declared in VeloxColumnHandleExport.h) so unit tests can
// exercise the bounds-checking behavior directly. Defined with C++ linkage (extern "C++")
// because it lives inside this extern "C" translation-unit block but is a namespaced C++
// symbol matching the header declaration.
extern "C++" facebook::velox::TypePtr gluten::parseTypeEncoding(const jint* enc, size_t& pos, size_t len) {
  if (pos >= len) {
    throw std::runtime_error("parseTypeEncoding: truncated encoding (missing type code)");
  }
  int code = static_cast<int>(enc[pos++]);
  switch (code) {
    case 0:
      return facebook::velox::INTEGER();
    case 1:
      return facebook::velox::BIGINT();
    case 2:
      return facebook::velox::SMALLINT();
    case 3:
      return facebook::velox::TINYINT();
    case 4:
      return facebook::velox::REAL();
    case 5:
      return facebook::velox::DOUBLE();
    case 6:
      return facebook::velox::VARCHAR();
    case 10:
      return facebook::velox::DATE();
    case 11:
      return facebook::velox::VARBINARY();
    case 12:
      return facebook::velox::BOOLEAN();
    case 13:
      return facebook::velox::TIMESTAMP();
    case 14: { // DECIMAL(precision, scale)
      if (pos + 1 >= len) {
        throw std::runtime_error("parseTypeEncoding: truncated encoding (missing DECIMAL precision/scale)");
      }
      int precision = static_cast<int>(enc[pos++]);
      int scale = static_cast<int>(enc[pos++]);
      // Velox DECIMAL is BIGINT-backed for precision <= 18 (short) and HUGEINT-backed otherwise.
      return facebook::velox::DECIMAL(precision, scale);
    }
    case 7: { // ARRAY
      auto elemType = gluten::parseTypeEncoding(enc, pos, len);
      return facebook::velox::ARRAY(elemType);
    }
    case 8: { // MAP
      auto keyType = gluten::parseTypeEncoding(enc, pos, len);
      auto valType = gluten::parseTypeEncoding(enc, pos, len);
      return facebook::velox::MAP(keyType, valType);
    }
    case 9: { // ROW (STRUCT)
      if (pos >= len) {
        throw std::runtime_error("parseTypeEncoding: truncated encoding (missing ROW field count)");
      }
      int nFields = static_cast<int>(enc[pos++]);
      std::vector<facebook::velox::TypePtr> fields;
      fields.reserve(static_cast<size_t>(nFields));
      for (int i = 0; i < nFields; ++i) {
        fields.push_back(gluten::parseTypeEncoding(enc, pos, len));
      }
      return facebook::velox::ROW(std::move(fields));
    }
    default:
      throw std::runtime_error(std::string("allocateNestedOutput: unknown type code: ") + std::to_string(code));
  }
}

/**
 * allocateNestedOutput -- allocate a writable flat or nested Velox output vector for an arbitrary
 * supported type encoded as a DFS pre-order int array.
 *
 * Type encoding (Java side: VeloxWritableColumnVector.encodeType):
 *   0=INTEGER, 1=BIGINT, 2=SMALLINT, 3=TINYINT, 4=REAL, 5=DOUBLE, 6=VARCHAR, 10=DATE
 *   7=ARRAY<T>: [7, ...T...]
 *   8=MAP<K,V>: [8, ...K..., ...V...]
 *   9=ROW<n,T1,...Tn>: [9, n, ...T1..., ..., ...Tn...]
 *   11=VARBINARY, 12=BOOLEAN, 13=TIMESTAMP, 14=DECIMAL: [14, precision, scale]
 *
 * @param typeEncoding DFS pre-order type code array.
 * @param rowCapacity  Number of rows (and initial element slots for children).
 * @return long[2] = {rootDescAddr, ownerHandle}.
 */
JNIEXPORT jlongArray JNICALL Java_org_apache_gluten_velox_vector_VeloxColumnHandleJniWrapper_allocateNestedOutput(
    JNIEnv* env,
    jclass /*clazz*/,
    jintArray typeEncoding,
    jint rowCapacity) {
  JNI_METHOD_START
  jsize len = env->GetArrayLength(typeEncoding);
  std::vector<jint> enc(static_cast<size_t>(len));
  env->GetIntArrayRegion(typeEncoding, 0, len, enc.data());
  size_t pos = 0;
  auto type = gluten::parseTypeEncoding(enc.data(), pos, static_cast<size_t>(len));
  auto [desc, ownerHandle] = gluten::allocateNestedOutput(type, static_cast<int32_t>(rowCapacity));
  jlongArray result = env->NewLongArray(2);
  if (result == nullptr) {
    // NewLongArray failed (JVM OOM); exception already pending. Free the saved
    // native tree and release the ObjectStore vector so neither leaks.
    gluten::freeColumnHandleTree(desc);
    gluten::ObjectStore::release(static_cast<gluten::ObjectHandle>(ownerHandle));
    return nullptr;
  }
  jlong buf[2] = {reinterpret_cast<jlong>(desc), static_cast<jlong>(ownerHandle)};
  env->SetLongArrayRegion(result, 0, 2, buf);
  return result;
  JNI_METHOD_END(nullptr)
}

} // extern "C"
