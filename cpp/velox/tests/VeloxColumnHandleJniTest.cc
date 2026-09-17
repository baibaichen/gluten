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
 * VeloxColumnHandleJniTest
 *
 * Native gtest for the JNI endpoints exportDescriptor / freeDescriptor.
 * We call the JNIEXPORT functions directly (with nullptr, nullptr for JNIEnv*
 * and jclass) because those parameters are unused in our implementation.
 * This avoids the Java-classpath / embedded-JVM complexity while still
 * exercising the full code path through exportVector.
 */

#include <gtest/gtest.h>
#include <jni.h>

#include <cstring>

#include "jni/JniError.h"
#include "vector/VeloxColumnHandle.h"
#include "velox/buffer/Buffer.h"
#include "velox/vector/tests/utils/VectorTestBase.h"

// Forward-declare the JNIEXPORT symbols so this TU can call them
// without including VeloxColumnHandleJni.cc directly.
extern "C" {
JNIEXPORT jlong JNICALL
Java_org_apache_gluten_velox_vector_VeloxColumnHandleJniWrapper_exportDescriptor(JNIEnv*, jclass, jlong veloxVectorPtr);

JNIEXPORT void JNICALL
Java_org_apache_gluten_velox_vector_VeloxColumnHandleJniWrapper_freeDescriptor(JNIEnv*, jclass, jlong rootDescAddr);

JNIEXPORT jlongArray JNICALL Java_org_apache_gluten_velox_vector_VeloxColumnHandleJniWrapper_allocateNestedOutput(
    JNIEnv*,
    jclass,
    jintArray typeEncoding,
    jint rowCapacity);
}

// Forward-declare the (non-static) type-encoding parser so we can bounds-test it directly.
namespace gluten {
facebook::velox::TypePtr parseTypeEncoding(const jint* enc, size_t& pos, size_t len);
}

using namespace facebook::velox;

class VeloxColumnHandleJniTest : public testing::Test, public test::VectorTestBase {
 protected:
  static void SetUpTestCase() {
    memory::MemoryManager::testingSetInstance(memory::MemoryManager::Options{});
  }
};

// exportDescriptor on a flat INT vector should return a descriptor whose
// length field matches the number of rows.
TEST_F(VeloxColumnHandleJniTest, FlatIntExportLength) {
  auto vec = makeFlatVector<int32_t>({10, 20, 30});
  // Pass the address of the VectorPtr (which is what the JNI endpoint expects).
  jlong descAddr = Java_org_apache_gluten_velox_vector_VeloxColumnHandleJniWrapper_exportDescriptor(
      nullptr, nullptr, reinterpret_cast<jlong>(&vec));
  ASSERT_NE(descAddr, 0);
  auto* h = reinterpret_cast<gluten::VeloxColumnHandle*>(descAddr);
  EXPECT_EQ(h->length, 3);
  EXPECT_EQ(h->nBuffers, 2);
  EXPECT_EQ(h->nChildren, 0);
  // freeDescriptor must free descriptor memory and NOT touch the live vector.
  Java_org_apache_gluten_velox_vector_VeloxColumnHandleJniWrapper_freeDescriptor(nullptr, nullptr, descAddr);
  // vec still alive — vector not touched by descriptor free.
  EXPECT_EQ(vec->size(), 3u);
}

// exportDescriptor on an ARRAY<INT> vector (nChildren==1) and verify child
// descriptor is reachable through the returned address.
TEST_F(VeloxColumnHandleJniTest, ArrayOfIntExportChildren) {
  auto elements = makeFlatVector<int32_t>({10, 20, 30, 40, 50});
  // row0=[10,20], row1=NULL, row2=[30,40,50]
  auto arr = makeArrayVector({0, 2, 2}, elements, /*nullRows=*/{1});
  jlong descAddr = Java_org_apache_gluten_velox_vector_VeloxColumnHandleJniWrapper_exportDescriptor(
      nullptr, nullptr, reinterpret_cast<jlong>(&arr));
  ASSERT_NE(descAddr, 0);
  auto* h = reinterpret_cast<gluten::VeloxColumnHandle*>(descAddr);
  EXPECT_EQ(h->length, 3);
  EXPECT_EQ(h->nChildren, 1);
  ASSERT_NE(h->children, nullptr);
  auto* child = h->children[0];
  ASSERT_NE(child, nullptr);
  EXPECT_EQ(child->length, 5);
  Java_org_apache_gluten_velox_vector_VeloxColumnHandleJniWrapper_freeDescriptor(nullptr, nullptr, descAddr);
  // Array vector still valid after descriptor free.
  EXPECT_EQ(arr->size(), 3u);
}

// freeDescriptor with a null-equivalent address should be a no-op (the tree
// free calls freeColumnHandleTree which guards on nullptr).
TEST_F(VeloxColumnHandleJniTest, FreeDescriptorZeroAddrIsNoOp) {
  // Pass addr=0 — freeColumnHandleTree guards on nullptr, so this must not crash.
  Java_org_apache_gluten_velox_vector_VeloxColumnHandleJniWrapper_freeDescriptor(nullptr, nullptr, 0LL);
}

// ============================================================
// Fix #7 regression tests: JNI_METHOD_START / JNI_METHOD_END guard
// ============================================================
//
// Verify that the JNI exception guards convert C++ exceptions thrown by
// native callees into a PENDING JAVA EXCEPTION rather than letting them
// escape the extern "C" JNI boundary (which is UB and aborts the JVM).
//
// A lightweight mock JNIEnv is used: no embedded JVM is required, so the
// tests are fast and hermetic.  JniErrorState is pre-seeded via the mock
// env so that glutenExceptionClass() resolves correctly when JNI_METHOD_END
// calls env->ThrowNew(...) inside its catch block.

namespace {

// ---- Per-thread mock JNI state ----
struct MockJniEnvState {
  bool exceptionPending{false};
  // When true, NewLongArray returns nullptr to simulate a JVM OOM allocation failure.
  bool forceNewLongArrayNull{false};
};
thread_local MockJniEnvState tl_mockJniState;

// Stable fake objects used by the mock functions below.
// The pointers are never dereferenced by our mock implementations.
static jclass g_fakeClass = reinterpret_cast<jclass>(static_cast<uintptr_t>(0xCAFEBABE));
static JavaVM* g_fakeVm = nullptr; // set into JniErrorState::vm_; never called in tests

// ---- Mock JNI function table ----

static jclass JNICALL mkFindClass(JNIEnv*, const char*) {
  return g_fakeClass; // All class lookups during JniErrorState::initialize return fakeClass
}

static jobject JNICALL mkNewGlobalRef(JNIEnv*, jobject obj) {
  return obj; // Return the object itself (not a copy; good enough for tests)
}

static void JNICALL mkDeleteGlobalRef(JNIEnv*, jobject) {} // no-op
static void JNICALL mkDeleteLocalRef(JNIEnv*, jobject) {} // no-op

static jint JNICALL mkThrowNew(JNIEnv*, jclass, const char*) {
  // Record that a Java exception was "thrown".
  tl_mockJniState.exceptionPending = true;
  return JNI_OK;
}

static jboolean JNICALL mkExceptionCheck(JNIEnv*) {
  return tl_mockJniState.exceptionPending ? JNI_TRUE : JNI_FALSE;
}

static void JNICALL mkExceptionClear(JNIEnv*) {
  tl_mockJniState.exceptionPending = false;
}

static jint JNICALL mkGetJavaVM(JNIEnv*, JavaVM** vm) {
  *vm = g_fakeVm; // JniErrorState saves this; it is only used if close() is called
  return JNI_OK;
}

// Backing store for a fake long array so SetLongArrayRegion has somewhere to write.
static jlong g_fakeLongArrayStore[8];

static jsize JNICALL mkGetArrayLength(JNIEnv*, jarray) {
  return 1;
}

static void JNICALL mkGetIntArrayRegion(JNIEnv*, jintArray arr, jsize start, jsize len, jint* buf) {
  std::memcpy(buf, reinterpret_cast<const jint*>(arr) + start, len * sizeof(jint));
}

static jlongArray JNICALL mkNewLongArray(JNIEnv*, jsize len) {
  if (tl_mockJniState.forceNewLongArrayNull) {
    return nullptr; // Simulate JVM OOM: allocation fails.
  }
  (void)len;
  return reinterpret_cast<jlongArray>(&g_fakeLongArrayStore[0]);
}

static void JNICALL mkSetLongArrayRegion(JNIEnv*, jlongArray arr, jsize start, jsize len, const jlong* buf) {
  // If this is ever called with a null array, that is the UB we are guarding against.
  ASSERT_NE(arr, nullptr) << "SetLongArrayRegion must never be called on a null array";
  for (jsize i = 0; i < len && (start + i) < 8; ++i) {
    reinterpret_cast<jlong*>(arr)[start + i] = buf[i];
  }
}

// ---- Mock JNIEnv construction ----
static JNINativeInterface_ g_mockFuncs;
static JNIEnv_ g_mockEnvObj;
static bool g_mockEnvReady = false;

JNIEnv* getMockEnv() {
  if (!g_mockEnvReady) {
    std::memset(&g_mockFuncs, 0, sizeof(g_mockFuncs));
    g_mockFuncs.FindClass = mkFindClass;
    g_mockFuncs.NewGlobalRef = mkNewGlobalRef;
    g_mockFuncs.DeleteGlobalRef = mkDeleteGlobalRef;
    g_mockFuncs.DeleteLocalRef = mkDeleteLocalRef;
    g_mockFuncs.ThrowNew = mkThrowNew;
    g_mockFuncs.ExceptionCheck = mkExceptionCheck;
    g_mockFuncs.ExceptionClear = mkExceptionClear;
    g_mockFuncs.GetJavaVM = mkGetJavaVM;
    g_mockFuncs.GetArrayLength = mkGetArrayLength;
    g_mockFuncs.GetIntArrayRegion = mkGetIntArrayRegion;
    g_mockFuncs.NewLongArray = mkNewLongArray;
    g_mockFuncs.SetLongArrayRegion = mkSetLongArrayRegion;
    g_mockEnvObj.functions = &g_mockFuncs;
    g_mockEnvReady = true;
  }
  return &g_mockEnvObj;
}

} // anonymous namespace

// ---- Test fixture ----
class VeloxColumnHandleJniGuardTest : public testing::Test, public test::VectorTestBase {
 protected:
  static void SetUpTestCase() {
    // Velox memory manager needed for vector allocation inside tests and for
    // allocateNestedOutput's internal pool.
    memory::MemoryManager::testingSetInstance(memory::MemoryManager::Options{});
    // Pre-initialise JniErrorState with the mock env so that the guard's
    // glutenExceptionClass() call resolves without requiring a real JVM.
    // ensureInitialized() is idempotent (mutex-guarded).
    gluten::getJniErrorState()->ensureInitialized(getMockEnv());
  }

  void SetUp() override {
    // Isolate per-test mock exception state.
    tl_mockJniState.exceptionPending = false;
    tl_mockJniState.forceNewLongArrayNull = false;
  }
};

// ---- Guard test 1: exportDescriptor on a DICTIONARY-encoded vector ----
//
// exportVector() hits the `default:` case in its encoding switch and throws
// VELOX_UNSUPPORTED.
//
// GREEN (current, with guards):
//   JNI_METHOD_END catches the C++ exception, calls env->ThrowNew() (which sets
//   tl_mockJniState.exceptionPending), and returns 0.  EXPECT_NO_THROW passes;
//   ExceptionCheck() == JNI_TRUE.
//
// RED (pre-fix, without guards):
//   The VeloxException propagates out of the extern "C" JNI function.
//   EXPECT_NO_THROW detects the escaping exception → test failure.
TEST_F(VeloxColumnHandleJniGuardTest, ExportDescriptorDictionaryVectorGuard) {
  JNIEnv* env = getMockEnv();

  // Build a DICTIONARY-encoded vector.  exportVector() has no case for DICTIONARY
  // and throws VELOX_UNSUPPORTED — no memory is allocated before the throw,
  // so there is nothing to leak.
  auto innerVec = makeFlatVector<int32_t>({10, 20, 30});
  // Allocate an indices buffer (int32 = vector_size_t on most platforms).
  auto indicesBuf = AlignedBuffer::allocate<vector_size_t>(3, pool_.get());
  auto* idxData = indicesBuf->asMutable<vector_size_t>();
  idxData[0] = 0;
  idxData[1] = 1;
  idxData[2] = 2;
  facebook::velox::VectorPtr vp =
      BaseVector::wrapInDictionary(/*nulls=*/BufferPtr(nullptr), indicesBuf, /*size=*/3, innerVec);
  ASSERT_EQ(vp->encoding(), VectorEncoding::Simple::DICTIONARY)
      << "Precondition: vector must be DICTIONARY-encoded to trigger the throw";

  jlong addr = reinterpret_cast<jlong>(&vp);
  jlong ret = 0;

  // With the guard: no C++ exception must escape the JNI boundary.
  EXPECT_NO_THROW({
    ret =
        Java_org_apache_gluten_velox_vector_VeloxColumnHandleJniWrapper_exportDescriptor(env, /*clazz=*/nullptr, addr);
  });

  // Guard converts the exception to a safe default return value.
  EXPECT_EQ(ret, 0);

  // Guard sets a pending Java exception (tl_mockJniState.exceptionPending == true).
  EXPECT_EQ(env->ExceptionCheck(), JNI_TRUE)
      << "A pending Java exception must be set after the guard catches the C++ throw";

  // Clear so subsequent mock JNI calls in this suite are unaffected.
  env->ExceptionClear();
  EXPECT_EQ(env->ExceptionCheck(), JNI_FALSE) << "ExceptionClear must remove the pending exception";
}

// ---- Guard test 2: allocateNestedOutput with an unsupported type code ----
//
// Type code 99 is unsupported. The implementation signals
// this via a pending Java exception and returns nullptr.  With the JNI guard in
// place, no C++ exception can escape the function even if internal logic throws.
//
// GREEN (current, with guards):
//   EXPECT_NO_THROW passes; ExceptionCheck() == JNI_TRUE; ret == nullptr.
//
// RED (pre-fix, without guards):
//   The parser's C++ exception escapes → EXPECT_NO_THROW fails.
TEST_F(VeloxColumnHandleJniGuardTest, AllocateOutputUnsupportedTypeCodeGuard) {
  JNIEnv* env = getMockEnv();

  // Use a non-null sentinel so we can verify the function sets it to nullptr.
  jlongArray ret = reinterpret_cast<jlongArray>(static_cast<uintptr_t>(0xDEAD));
  jint typeCode = 99;

  EXPECT_NO_THROW({
    ret = Java_org_apache_gluten_velox_vector_VeloxColumnHandleJniWrapper_allocateNestedOutput(
        env, /*clazz=*/nullptr, reinterpret_cast<jintArray>(&typeCode), /*rowCapacity=*/10);
  });

  // On failure the function must return the safe default (nullptr).
  EXPECT_EQ(ret, nullptr);

  // A pending Java exception must be set (either via env->ThrowNew in the default
  // branch, or via the guard catching a C++ exception from the callee).
  EXPECT_EQ(env->ExceptionCheck(), JNI_TRUE) << "A pending Java exception must be set for unsupported typeCode";

  env->ExceptionClear();
  EXPECT_EQ(env->ExceptionCheck(), JNI_FALSE) << "ExceptionClear must remove the pending exception";
}

// ============================================================
// Bug 2 regression: parseTypeEncoding must bounds-check every read/recursion against
// the encoding length instead of walking past the buffer on a truncated encoding.
// ============================================================
//
// RED (pre-fix, 2-arg unbounded parser): an ARRAY tag (7) with no following child
// code reads enc[pos] past the end of the 1-element buffer -> OOB read (garbage /
// ASAN fault), and no exception is thrown.
// GREEN (post-fix): the bounds check throws std::runtime_error on the truncation.
TEST_F(VeloxColumnHandleJniGuardTest, ParseTypeEncodingTruncatedArrayThrows) {
  // Buffer sized EXACTLY to the truncation point: one code, the ARRAY tag, and
  // nothing after it. Reading a child code would step past index 0.
  std::vector<jint> enc{7 /*ARRAY*/};
  size_t pos = 0;
  EXPECT_THROW({ gluten::parseTypeEncoding(enc.data(), pos, enc.size()); }, std::runtime_error)
      << "truncated ARRAY encoding must throw, not read past the buffer";
}

// Empty encoding: no type code available at all -> must throw, not read enc[0].
TEST_F(VeloxColumnHandleJniGuardTest, ParseTypeEncodingEmptyThrows) {
  std::vector<jint> enc{}; // length 0
  size_t pos = 0;
  EXPECT_THROW({ gluten::parseTypeEncoding(enc.data(), pos, enc.size()); }, std::runtime_error);
}

// Truncated ROW: tag 9 present but the field-count code is missing -> must throw.
TEST_F(VeloxColumnHandleJniGuardTest, ParseTypeEncodingTruncatedRowThrows) {
  std::vector<jint> enc{9 /*ROW*/}; // missing nFields
  size_t pos = 0;
  EXPECT_THROW({ gluten::parseTypeEncoding(enc.data(), pos, enc.size()); }, std::runtime_error);
}

// Well-formed input must still parse correctly (semantics unchanged): ARRAY<INT>.
TEST_F(VeloxColumnHandleJniGuardTest, ParseTypeEncodingWellFormedArrayInt) {
  std::vector<jint> enc{7 /*ARRAY*/, 0 /*INTEGER*/};
  size_t pos = 0;
  facebook::velox::TypePtr t;
  EXPECT_NO_THROW({ t = gluten::parseTypeEncoding(enc.data(), pos, enc.size()); });
  ASSERT_NE(t, nullptr);
  EXPECT_EQ(t->kind(), facebook::velox::TypeKind::ARRAY);
  EXPECT_EQ(pos, 2u) << "parser must consume exactly the two codes";
}
