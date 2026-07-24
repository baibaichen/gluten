# Gluten vcpkg Arrow Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make Gluten's vcpkg build provide Arrow 18.0.0 and its testing library to Velox, deprecate duplicate vcpkg-mode Arrow installation, and test whether the enabled native dependency graph can reject host third-party packages.

**Architecture:** A Gluten Arrow overlay port extends the official vcpkg Arrow 18.0.0 port with a testing-library feature and the same Arrow testing patch used by Velox. vcpkg mode selects SYSTEM dependencies and uses vcpkg-installed Arrow; host-path isolation is implemented and validated as a separate candidate change so it can be withheld if an unrelated dependency still legitimately requires `/usr/local`.

**Tech Stack:** Bash, CMake, vcpkg manifests/overlay ports/registries, Arrow 18.0.0, Velox CMake dependency resolution.

---

## File Map

- Create `dev/vcpkg/ports/arrow/`: Gluten's Arrow 18.0.0 overlay port, copied from official vcpkg port tree `f62b9ba3d5fcf264637a1acc6edc72557b0f1461`.
- Create `dev/vcpkg/ports/arrow/arrow-testing-boost.patch`: Arrow testing-library-only dependency fix already used by the pinned Velox source.
- Modify `dev/vcpkg/ports/arrow/portfile.cmake`: apply the testing patch and map a `testing` feature to `ARROW_TESTING`.
- Modify `dev/vcpkg/ports/arrow/vcpkg.json`: declare the `testing` feature and its package dependencies.
- Modify `dev/vcpkg/vcpkg.json`: install `arrow[testing]` for the Velox feature.
- Modify `dev/vcpkg/vcpkg-configuration.json`: route all modular Boost ports to the Boost 1.84 vcpkg baseline.
- Modify `dev/builddeps-veloxbe.sh`: make vcpkg mode skip and deprecate the independent Arrow installer.
- Modify `ep/build-velox/src/build-velox.sh`: select vcpkg-managed SYSTEM dependencies in vcpkg mode.
- Candidate modify `dev/vcpkg/toolchain.cmake`: prefer and restrict package discovery to vcpkg while ignoring non-OS host prefixes.
- Candidate modify `dev/vcpkg/env.sh`: make pkg-config search only vcpkg directories.
- Candidate modify `dev/builddep-veloxbe-inc.sh`: preserve the same pkg-config isolation in incremental builds.

### Task 1: Prove the Official Arrow Port Gap

**Files:**
- Inspect: `dev/vcpkg/.vcpkg/ports/arrow/portfile.cmake`
- Inspect: `dev/vcpkg/.vcpkg/ports/arrow/vcpkg.json`

- [x] **Step 1: Ensure the vcpkg checkout exists**

Run:

```bash
bash -c '
  set -e
  export VCPKG_ROOT="$PWD/dev/vcpkg/.vcpkg"
  export VCPKG="$VCPKG_ROOT/vcpkg"
  export VCPKG_TRIPLET=x64-linux-avx
  export VCPKG_TRIPLET_INSTALL_DIR="$PWD/dev/vcpkg/vcpkg_installed/$VCPKG_TRIPLET"
  source dev/vcpkg/env.sh \
    --build_tests=OFF \
    --enable_s3=OFF \
    --enable_gcs=OFF \
    --enable_hdfs=OFF \
    --enable_abfs=OFF
'
```

Expected: vcpkg initialization succeeds. Existing packages may be reused.

- [x] **Step 2: Run the failing feature probe**

Run:

```bash
dev/vcpkg/.vcpkg/vcpkg install 'arrow[testing]:x64-linux-avx' \
  --overlay-triplets="$PWD/dev/vcpkg/triplets" \
  --overlay-ports="$PWD/dev/vcpkg/ports" \
  --x-install-root="$PWD/dev/vcpkg/vcpkg_installed"
```

Expected: FAIL because the official Arrow port has no `testing` feature.

- [x] **Step 3: Confirm why the feature is required**

Run:

```bash
gh api \
  repos/IBM/velox/contents/CMake/FindArrow.cmake \
  --method GET \
  -f ref=dft-2026_07_16 \
  --jq .content | base64 -d |
  grep -E 'ARROW_TESTING_LIB|libarrow_testing'
```

Expected: `FindArrow.cmake` requires `libarrow_testing.a` through
`ARROW_TESTING_LIB`.

### Task 2: Add the Arrow 18 Overlay Port

**Files:**
- Create: `dev/vcpkg/ports/arrow/*`
- Create: `dev/vcpkg/ports/arrow/arrow-testing-boost.patch`
- Modify: `dev/vcpkg/ports/arrow/portfile.cmake`
- Modify: `dev/vcpkg/ports/arrow/vcpkg.json`

- [x] **Step 1: Copy the official Arrow 18.0.0 port**

Run:

```bash
mkdir -p dev/vcpkg/ports/arrow
git -C dev/vcpkg/.vcpkg archive \
  f62b9ba3d5fcf264637a1acc6edc72557b0f1461 |
  tar -x -C dev/vcpkg/ports/arrow
```

Expected: the directory contains `portfile.cmake`, `vcpkg.json`, the official
patches, and usage files. `dev/vcpkg/ports/arrow/vcpkg.json` reports version
`18.0.0`.

- [x] **Step 2: Add the Arrow testing patch**

Create `dev/vcpkg/ports/arrow/arrow-testing-boost.patch` with:

```diff
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

diff --git a/cpp/cmake_modules/ThirdpartyToolchain.cmake b/cpp/cmake_modules/ThirdpartyToolchain.cmake
index db151b4..1da99f1 100644
--- a/cpp/cmake_modules/ThirdpartyToolchain.cmake
+++ b/cpp/cmake_modules/ThirdpartyToolchain.cmake
@@ -1266,13 +1266,19 @@ endif()
 # - Gandiva has a compile-time (header-only) dependency on Boost, not runtime.
 # - Tests need Boost at runtime.
 # - S3FS and Flight benchmarks need Boost at runtime.
+# - arrow_testing uses boost::filesystem. So arrow_testing requires
+#   Boost library. (boost::filesystem isn't header-only.) But if we
+#   use arrow_testing as a static library without
+#   using arrow::util::Process, we don't need boost::filesystem.
 if(ARROW_BUILD_INTEGRATION
    OR ARROW_BUILD_TESTS
    OR (ARROW_FLIGHT AND (ARROW_TESTING OR ARROW_BUILD_BENCHMARKS))
-   OR (ARROW_S3 AND ARROW_BUILD_BENCHMARKS))
+   OR (ARROW_S3 AND ARROW_BUILD_BENCHMARKS)
+   OR (ARROW_TESTING AND ARROW_BUILD_SHARED))
   set(ARROW_USE_BOOST TRUE)
   set(ARROW_BOOST_REQUIRE_LIBRARY TRUE)
 elseif(ARROW_GANDIVA
+       OR ARROW_TESTING
        OR ARROW_WITH_THRIFT
        OR (NOT ARROW_USE_NATIVE_INT128))
   set(ARROW_USE_BOOST TRUE)
diff --git a/cpp/src/arrow/CMakeLists.txt b/cpp/src/arrow/CMakeLists.txt
index c911f0f..84673d4 100644
--- a/cpp/src/arrow/CMakeLists.txt
+++ b/cpp/src/arrow/CMakeLists.txt
@@ -647,8 +647,8 @@
 set(ARROW_TESTING_SHARED_LINK_LIBS arrow_shared ${ARROW_GTEST_GTEST})
-set(ARROW_TESTING_SHARED_PRIVATE_LINK_LIBS arrow::flatbuffers RapidJSON Boost::process)
+set(ARROW_TESTING_SHARED_PRIVATE_LINK_LIBS arrow::flatbuffers RapidJSON Boost::filesystem Boost::system)
 set(ARROW_TESTING_STATIC_LINK_LIBS
     arrow::flatbuffers
     RapidJSON
-    Boost::process
+    Boost::filesystem Boost::system
     arrow_static
     ${ARROW_GTEST_GTEST})
 set(ARROW_TESTING_SHARED_INSTALL_INTERFACE_LIBS Arrow::arrow_shared)
```

- [x] **Step 3: Apply the patch and map the testing feature**

In `dev/vcpkg/ports/arrow/portfile.cmake`, make the patch list and feature map:

```cmake
vcpkg_extract_source_archive(
    SOURCE_PATH
    ARCHIVE ${ARCHIVE_PATH}
    PATCHES
        android.patch
        msvc-static-name.patch
        utf8proc.patch
        thrift.patch
        arrow-testing-boost.patch
)

vcpkg_check_features(OUT_FEATURE_OPTIONS FEATURE_OPTIONS
    FEATURES
        acero       ARROW_ACERO
        compute     ARROW_COMPUTE
        csv         ARROW_CSV
        cuda        ARROW_CUDA
        dataset     ARROW_DATASET
        filesystem  ARROW_FILESYSTEM
        flight      ARROW_FLIGHT
        flightsql   ARROW_FLIGHT_SQL
        gcs         ARROW_GCS
        jemalloc    ARROW_JEMALLOC
        json        ARROW_JSON
        mimalloc    ARROW_MIMALLOC
        orc         ARROW_ORC
        parquet     ARROW_PARQUET
        parquet     PARQUET_REQUIRE_ENCRYPTION
        s3          ARROW_S3
        testing     ARROW_TESTING
)
```

Keep the existing port options:

```cmake
-DARROW_BUILD_TESTS=OFF
-DARROW_DEPENDENCY_SOURCE=SYSTEM
-DARROW_DEPENDENCY_USE_SHARED=${ARROW_DEPENDENCY_USE_SHARED}
```

- [x] **Step 4: Declare testing feature dependencies**

Add this feature to `dev/vcpkg/ports/arrow/vcpkg.json`:

```json
"testing": {
  "description": "Build the Arrow testing support library",
  "dependencies": [
    "boost-process",
    "gtest",
    "rapidjson"
  ]
}
```

Ensure the preceding feature has a trailing comma and the file remains valid
JSON.

- [x] **Step 5: Validate the overlay metadata**

Run:

```bash
python3 -m json.tool dev/vcpkg/ports/arrow/vcpkg.json >/dev/null
grep -n 'testing.*ARROW_TESTING' dev/vcpkg/ports/arrow/portfile.cmake
grep -n 'arrow-testing-boost.patch' dev/vcpkg/ports/arrow/portfile.cmake
```

Expected: JSON validation succeeds and both grep commands find one line.

### Task 3: Add Arrow and Boost 1.84 to the Gluten vcpkg Graph

**Files:**
- Modify: `dev/vcpkg/vcpkg.json`
- Modify: `dev/vcpkg/vcpkg-configuration.json`

- [x] **Step 1: Add Arrow to the Velox feature**

Add this entry to the `velox` dependencies in `dev/vcpkg/vcpkg.json`:

```json
{
  "name": "arrow",
  "default-features": false,
  "features": ["testing"]
}
```

Place it near the compression and Boost dependencies so the native dependency
group remains readable.

- [x] **Step 2: Route all Boost modules to the 1.84 baseline**

Add this registry to `dev/vcpkg/vcpkg-configuration.json`:

```json
{
  "kind": "git",
  "repository": "https://github.com/Microsoft/vcpkg",
  "baseline": "943c5ef1c8f6b5e6ced092b242c8299caae2ff01",
  "packages": ["boost", "boost-*"]
}
```

Do not change the root `builtin-baseline`; unrelated packages must stay on the
current baseline.

- [x] **Step 3: Validate both manifests**

Run:

```bash
python3 -m json.tool dev/vcpkg/vcpkg.json >/dev/null
python3 -m json.tool dev/vcpkg/vcpkg-configuration.json >/dev/null
```

Expected: both commands exit zero.

- [x] **Step 4: Re-run the formerly failing feature probe**

Run:

```bash
bash -c '
  set -e
  source dev/vcpkg/env.sh \
    --build_tests=ON \
    --enable_s3=OFF \
    --enable_gcs=OFF \
    --enable_hdfs=OFF \
    --enable_abfs=OFF
'
```

Expected: manifest resolution uses the Arrow overlay and scoped Boost registry,
and the `testing` feature is accepted. If configuration reports a missing
SYSTEM dependency, add that package to
`dev/vcpkg/ports/arrow/vcpkg.json`, rerun the command, and record the exact
dependency in the commit message.

- [x] **Step 5: Verify Arrow artifacts and configuration**

Run:

```bash
triplet=dev/vcpkg/vcpkg_installed/x64-linux-avx
test -f "$triplet/lib/libarrow.a"
test -f "$triplet/lib/libarrow_testing.a"
test -f "$triplet/include/arrow/api.h"
grep -R '^ARROW_BUILD_TESTS:BOOL=OFF$' \
  dev/vcpkg/.vcpkg/buildtrees/arrow/*/CMakeCache.txt
grep -R '^ARROW_TESTING:BOOL=ON$' \
  dev/vcpkg/.vcpkg/buildtrees/arrow/*/CMakeCache.txt
```

Expected: all files exist and both cache values are found.

- [x] **Step 6: Verify the complete Boost family version**

Run:

```bash
dev/vcpkg/.vcpkg/vcpkg list \
  --x-install-root=dev/vcpkg/vcpkg_installed |
  awk -F: '/^boost-/{print $1, $2}' |
  grep -v ' 1.84.0'
```

Expected: no output. Any output is a mixed Boost graph and blocks progress.

- [x] **Step 7: Commit the vcpkg package**

Run the repository header check:

```bash
./dev/check.py header main --fix
```

If the checkout still lacks `dev/license-header.py`, run the tracked checker
directly for changed supported files. Its configured exclusions intentionally
skip `dev/*`; check changed non-`dev` CMake modules explicitly because the
tracked checker does not recognize the `.cmake` extension:

```bash
git diff --name-only origin/main...HEAD |
  grep -E '(^|/)CMakeLists\.txt$|\.sh$' |
  PYTHONPATH=.github/workflows/util \
    .github/workflows/util/license-header.py -k -

for file in $(git diff --name-only origin/main...HEAD | grep -E '^(cpp|ep)/.*\.cmake$'); do
  head -n 20 "$file" | grep -q 'Licensed to the Apache Software Foundation'
done
```

Then commit:

```bash
git add \
  dev/vcpkg/ports/arrow \
  dev/vcpkg/vcpkg.json \
  dev/vcpkg/vcpkg-configuration.json
git commit -m "[VL] Manage Arrow through Gluten vcpkg" \
  -m "Co-authored-by: Copilot <223556219+Copilot@users.noreply.github.com>" \
  -m "Copilot-Session: 81cd27bb-c736-4cf5-b65f-2aeb9f954996"
```

### Task 4: Deprecate the Duplicate vcpkg Arrow Installer

**Files:**
- Modify: `dev/builddeps-veloxbe.sh:47,142-145,160-205`

- [x] **Step 1: Add the explicit-option state**

Immediately after `BUILD_ARROW=ON`, add:

```bash
BUILD_ARROW_EXPLICIT=OFF
```

In the `--build_arrow=*` case, use:

```bash
--build_arrow=*)
BUILD_ARROW="${arg#*=}"
BUILD_ARROW_EXPLICIT=ON
shift
;;
```

- [x] **Step 2: Add the failing vcpkg-mode guard**

After argument parsing and before sourcing `dev/vcpkg/env.sh`, add:

```bash
if [ "$ENABLE_VCPKG" = "ON" ]; then
    if [ "$BUILD_ARROW_EXPLICIT" = "ON" ] && [ "$BUILD_ARROW" = "ON" ]; then
        echo "ERROR: --build_arrow=ON is deprecated with --enable_vcpkg=ON; Arrow is managed by Gluten vcpkg." >&2
        exit 1
    fi
    BUILD_ARROW=OFF
fi
```

- [x] **Step 3: Verify explicit ON fails before vcpkg starts**

Run:

```bash
set +e
output=$(./dev/builddeps-veloxbe.sh \
  --enable_vcpkg=ON \
  --build_arrow=ON \
  true 2>&1)
status=$?
set -e
test "$status" -ne 0
grep -F -- '--build_arrow=ON is deprecated' <<<"$output"
! grep -F 'Installing' <<<"$output"
```

Expected: nonzero status, deprecation message present, no dependency
installation starts.

- [x] **Step 4: Verify non-vcpkg behavior is unchanged**

Run:

```bash
bash -x ./dev/builddeps-veloxbe.sh \
  --run_setup_script=OFF \
  true 2>&1 |
  grep -F 'BUILD_ARROW=ON'
```

Expected: trace contains the original default `BUILD_ARROW=ON` and the command
exits zero.

- [x] **Step 5: Verify omitted vcpkg `build_arrow` resolves to OFF**

Run:

```bash
bash -x ./dev/builddeps-veloxbe.sh \
  --enable_vcpkg=ON \
  true 2>&1 |
  tee /tmp/gluten-vcpkg-build-arrow-default.log
grep -F 'BUILD_ARROW=OFF' /tmp/gluten-vcpkg-build-arrow-default.log
! grep -F 'source ./build-arrow.sh' /tmp/gluten-vcpkg-build-arrow-default.log
```

Expected: vcpkg dependency initialization may run, the resolved value is OFF,
and the independent Arrow script is not sourced.

- [x] **Step 6: Commit the argument behavior**

Run:

```bash
git add dev/builddeps-veloxbe.sh
git commit -m "[VL] Deprecate vcpkg build-arrow path" \
  -m "Co-authored-by: Copilot <223556219+Copilot@users.noreply.github.com>" \
  -m "Copilot-Session: 81cd27bb-c736-4cf5-b65f-2aeb9f954996"
```

### Task 5: Select vcpkg SYSTEM Dependencies in Velox

**Files:**
- Modify: `ep/build-velox/src/build-velox.sh:179-180`

- [x] **Step 1: Replace unconditional AUTO exports**

Replace:

```bash
export simdjson_SOURCE=AUTO
export Arrow_SOURCE=AUTO
```

with:

```bash
if [ -n "${GLUTEN_VCPKG_ENABLED:-}" ]; then
  export VELOX_DEPENDENCY_SOURCE=SYSTEM
  export simdjson_SOURCE=SYSTEM
  export Arrow_SOURCE=SYSTEM
else
  export simdjson_SOURCE=AUTO
  export Arrow_SOURCE=AUTO
fi
```

- [x] **Step 2: Configure and build the Velox target**

Run:

```bash
rm -rf ep/build-velox/build/velox_ep/_build/debug
set -o pipefail
./dev/builddeps-veloxbe.sh \
 --enable_vcpkg=ON \
 --build_arrow=OFF \
 --build_tests=ON \
  --build_benchmarks=OFF \
  --enable_s3=OFF \
  --enable_gcs=OFF \
  --enable_hdfs=OFF \
  --enable_abfs=OFF \
  --spark_version=4.1 \
  --build_type=Debug \
  --num_threads="$(nproc --ignore=2)" 2>&1 |
  tee /tmp/gluten-vcpkg-system-arrow.log
```

Expected: Velox configuration reports SYSTEM Arrow from
`dev/vcpkg/vcpkg_installed`, and no `arrow_ep` build is started.

- [x] **Step 3: Verify the selected dependency sources**

Run:

```bash
grep -F 'Setting Arrow source to SYSTEM' /tmp/gluten-vcpkg-system-arrow.log
grep -F 'Setting simdjson source to SYSTEM' /tmp/gluten-vcpkg-system-arrow.log
grep -F 'Using SYSTEM Arrow' /tmp/gluten-vcpkg-system-arrow.log
find ep/build-velox/build/velox_ep/_build/debug \
  -path '*arrow_ep*' -print -quit |
  grep -q . && {
    echo "Unexpected Arrow ExternalProject" >&2
    exit 1
  } || true
```

Expected: both source values are SYSTEM and no Arrow ExternalProject path is
printed.

- [x] **Step 4: Commit SYSTEM source selection**

Run:

```bash
git add ep/build-velox/src/build-velox.sh
git commit -m "[VL] Use vcpkg Arrow in Velox builds" \
  -m "Co-authored-by: Copilot <223556219+Copilot@users.noreply.github.com>" \
  -m "Copilot-Session: 81cd27bb-c736-4cf5-b65f-2aeb9f954996"
```

### Task 6: Implement Candidate Host-Path Isolation

**Files:**
- Candidate modify: `dev/vcpkg/toolchain.cmake:31-45`
- Candidate modify: `dev/vcpkg/env.sh:49-53`
- Candidate modify: `dev/builddep-veloxbe-inc.sh:142-145`
- Candidate modify: `dev/builddeps-veloxbe.sh:275-278`
- Candidate modify: `ep/build-velox/src/build-velox.sh:127-129`

- [x] **Step 1: Stop importing arbitrary CMake prefixes**

Remove from `dev/vcpkg/toolchain.cmake`:

```cmake
# Force read CMAKE_PREFIX_PATH from env
set(CMAKE_PREFIX_PATH $ENV{CMAKE_PREFIX_PATH})
```

- [x] **Step 2: Add vcpkg package isolation after the standard toolchain**

Immediately after:

```cmake
include($ENV{VCPKG_ROOT}/scripts/buildsystems/vcpkg.cmake)
```

add:

```cmake
set(CMAKE_FIND_PACKAGE_PREFER_CONFIG ON CACHE BOOL
    "Prefer vcpkg package configuration files" FORCE)
set(CMAKE_FIND_ROOT_PATH_MODE_PACKAGE ONLY CACHE STRING
    "Resolve packages only from the vcpkg root path" FORCE)
set(CMAKE_FIND_USE_PACKAGE_REGISTRY OFF CACHE BOOL
    "Disable the CMake user package registry" FORCE)
set(CMAKE_FIND_USE_SYSTEM_PACKAGE_REGISTRY OFF CACHE BOOL
    "Disable the CMake system package registry" FORCE)

set(GLUTEN_VCPKG_IGNORED_PREFIXES /usr/local)
if(APPLE)
  list(APPEND GLUTEN_VCPKG_IGNORED_PREFIXES /opt/homebrew /opt/local)
endif()
if(DEFINED ENV{CONDA_PREFIX} AND NOT "$ENV{CONDA_PREFIX}" STREQUAL "")
  list(APPEND GLUTEN_VCPKG_IGNORED_PREFIXES "$ENV{CONDA_PREFIX}")
endif()
set(CMAKE_IGNORE_PREFIX_PATH "${GLUTEN_VCPKG_IGNORED_PREFIXES}" CACHE STRING
    "Non-OS host prefixes excluded from vcpkg builds" FORCE)
unset(GLUTEN_VCPKG_IGNORED_PREFIXES)
```

Do not set `CMAKE_SYSTEM_IGNORE_PREFIX_PATH` or
`CMAKE_SYSTEM_IGNORE_PATH`.

- [x] **Step 3: Lock pkg-config to vcpkg**

Replace `dev/vcpkg/env.sh`'s `PKG_CONFIG_PATH` export with:

```bash
unset PKG_CONFIG_PATH
export PKG_CONFIG_LIBDIR=${VCPKG_TRIPLET_INSTALL_DIR}/lib/pkgconfig:${VCPKG_TRIPLET_INSTALL_DIR}/share/pkgconfig
```

In `dev/builddep-veloxbe-inc.sh`, replace the manual `PKG_CONFIG_PATH` export
with the same two lines.

- [x] **Step 4: Keep explicit install prefixes out of Gluten dependency search**

In `dev/builddeps-veloxbe.sh`, change the `INSTALL_PREFIX` handling to:

```bash
if [ -n "${INSTALL_PREFIX:-}" ]; then
  GLUTEN_CMAKE_OPTIONS+=("-DCMAKE_INSTALL_PREFIX=$INSTALL_PREFIX")
  if [ -z "${GLUTEN_VCPKG_ENABLED:-}" ]; then
    GLUTEN_CMAKE_OPTIONS+=("-DCMAKE_PREFIX_PATH=$INSTALL_PREFIX")
  fi
fi
```

In `ep/build-velox/src/build-velox.sh`, make the equivalent change:

```bash
if [ -n "${INSTALL_PREFIX:-}" ]; then
  COMPILE_OPTION="$COMPILE_OPTION -DCMAKE_INSTALL_PREFIX=${INSTALL_PREFIX}"
  if [ -z "${GLUTEN_VCPKG_ENABLED:-}" ]; then
    COMPILE_OPTION="$COMPILE_OPTION -DCMAKE_PREFIX_PATH=${INSTALL_PREFIX}"
  fi
fi
```

- [x] **Step 5: Verify environment poisoning is removed**

Run:

```bash
env \
  CMAKE_PREFIX_PATH=/tmp/gluten-poison-prefix \
  PKG_CONFIG_PATH=/tmp/gluten-poison-pkgconfig \
  bash -c '
    set -e
    source dev/vcpkg/env.sh \
      --build_tests=OFF \
      --enable_s3=OFF \
      --enable_gcs=OFF \
      --enable_hdfs=OFF \
      --enable_abfs=OFF
    test -z "${PKG_CONFIG_PATH+x}"
    case "$PKG_CONFIG_LIBDIR" in
      "$PWD"/dev/vcpkg/vcpkg_installed/*) ;;
      *) echo "Unexpected PKG_CONFIG_LIBDIR=$PKG_CONFIG_LIBDIR" >&2; exit 1 ;;
    esac
  '
```

Expected: the inherited pkg-config path is unset and only vcpkg directories
remain.

- [x] **Step 6: Reconfigure the requested Velox matrix**

Remove only the generated Velox CMake build directory, not the source tree:

```bash
rm -rf ep/build-velox/build/velox_ep/_build/debug
```

Then run:

```bash
env \
  CMAKE_PREFIX_PATH=/tmp/gluten-poison-prefix \
  PKG_CONFIG_PATH=/tmp/gluten-poison-pkgconfig \
  ./dev/builddeps-veloxbe.sh \
    --enable_vcpkg=ON \
    --build_arrow=OFF \
    --build_tests=ON \
    --build_benchmarks=ON \
    --enable_s3=ON \
    --enable_gcs=ON \
    --enable_hdfs=ON \
    --enable_abfs=OFF \
    --spark_version=4.1 \
    --build_type=Debug \
    --num_threads="$(nproc --ignore=2)"
```

Expected: configuration and build succeed without using the poison prefixes.

- [x] **Step 7: Apply the case-by-case decision gate**

If Step 6 fails on a non-Arrow dependency:

1. Save the complete failing `find_package` or `find_library` message.
2. Identify whether the dependency is already declared in
   `dev/vcpkg/vcpkg.json`.
3. If declared, inspect why its vcpkg Config file or library was not selected.
4. If undeclared, determine whether it is an ordinary compile-time package or
   an approved OS/runtime/platform exception from the design.
5. Stop and present that concrete dependency to the user before adding an
   exception or removing the candidate isolation.

Do not commit Task 6 changes until this decision gate passes.

- [x] **Step 8: Audit resolved managed dependency paths**

Run:

```bash
for file in \
  ep/build-velox/build/velox_ep/_build/debug/CMakeCache.txt \
  cpp/build/CMakeCache.txt \
  cpp/build/compile_commands.json; do
  test -f "$file"
  grep -nE '/usr/local|/opt/homebrew|/opt/local|gluten-poison' "$file" || true
done
```

Review every hit. Compiler/runtime SDK hits allowed by the design must be
identified explicitly. Any Folly, Arrow, Boost, zstd, gflags, glog, protobuf,
or other vcpkg-managed include/library hit blocks the commit.

- [x] **Step 9: Commit isolation only if the matrix passes**

Run:

```bash
git add \
  dev/vcpkg/toolchain.cmake \
  dev/vcpkg/env.sh \
  dev/builddep-veloxbe-inc.sh \
  dev/builddeps-veloxbe.sh \
  ep/build-velox/src/build-velox.sh
git commit -m "[VL] Isolate vcpkg dependency discovery" \
  -m "Co-authored-by: Copilot <223556219+Copilot@users.noreply.github.com>" \
  -m "Copilot-Session: 81cd27bb-c736-4cf5-b65f-2aeb9f954996"
```

### Task 7: Final Verification

**Files:**
- Verify: all files changed in Tasks 2-6

- [x] **Step 1: Validate JSON and shell syntax**

Run:

```bash
python3 -m json.tool dev/vcpkg/vcpkg.json >/dev/null
python3 -m json.tool dev/vcpkg/vcpkg-configuration.json >/dev/null
python3 -m json.tool dev/vcpkg/ports/arrow/vcpkg.json >/dev/null
bash -n dev/builddeps-veloxbe.sh
bash -n dev/vcpkg/env.sh
bash -n dev/builddep-veloxbe-inc.sh
bash -n ep/build-velox/src/build-velox.sh
```

Expected: every command exits zero.

- [x] **Step 2: Reconfirm Arrow and Boost artifacts**

Run:

```bash
triplet=dev/vcpkg/vcpkg_installed/x64-linux-avx
test -f "$triplet/lib/libarrow.a"
test -f "$triplet/lib/libarrow_testing.a"
test -f "$triplet/include/arrow/api.h"
boost_versions=$(
  dev/vcpkg/.vcpkg/vcpkg list \
    --x-install-root=dev/vcpkg/vcpkg_installed |
    awk '$1 ~ /^boost-/ && $2 ~ /^[0-9]/ {print $2}'
)
test -n "$boost_versions"
if grep -vE '^1\.84\.0(#[0-9]+)?$' <<<"$boost_versions"; then
  exit 1
fi
```

Expected: all Arrow files exist and the Boost check prints nothing.

- [x] **Step 3: Verify the original vcpkg build command**

Run:

```bash
./dev/buildbundle-veloxbe.sh \
  --enable_vcpkg=ON \
  --build_tests=ON \
  --build_arrow=OFF \
  --build_benchmarks=ON \
  --enable_s3=ON \
  --enable_gcs=ON \
  --enable_hdfs=ON \
  --spark_version=4.1 \
  --build_type=Debug
```

Expected: the build uses SYSTEM Arrow from `vcpkg_installed`, does not create
an Arrow ExternalProject, and does not reproduce the shared-zstd or
Boost.Process compilation errors.

- [x] **Step 4: Check the final diff**

Run:

```bash
git --no-pager diff --check
git --no-pager status --short
git --no-pager log -5 --oneline
```

Expected: no whitespace errors; only the user's pre-existing
`tools/gluten-it/spark-home/` remains unrelated and untracked.
