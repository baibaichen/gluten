# Gluten vcpkg Arrow Design

## Goal

Make vcpkg the only source of supported compile-time third-party dependencies
when Gluten builds Velox with `--enable_vcpkg=ON`. In particular:

- vcpkg builds and installs Arrow 18.0.0, including `libarrow_testing.a`;
- Velox and Gluten consume that Arrow installation without using a bundled or
  host Arrow;
- vcpkg builds do not run Gluten's independent `dev/build-arrow.sh`;
- package discovery cannot silently select dependencies from `/usr/local`,
  inherited CMake prefixes, package registries, or pkg-config defaults; and
- non-vcpkg builds retain their existing dependency and Arrow behavior.

The system compiler, libc, pthread, dl, rt, and explicitly supported platform
SDKs remain outside vcpkg ownership.

## Pre-fix Behavior

- `dev/vcpkg/init.sh` ran `vcpkg install` under `set -e`, so an installation
  failure stopped the build before CMake. A successful installation did not,
  however, prevent CMake from finding packages in host prefixes.
- Velox used `Arrow_SOURCE=AUTO` and `simdjson_SOURCE=AUTO`. A clean machine
  without a system Arrow fell back to Velox's Arrow ExternalProject, while a
  machine with `/usr/local` Arrow could silently use that installation.
- Gluten's independent `--build_arrow` path installed another native Arrow
  before Velox configuration.
- The official vcpkg Arrow 18 port did not build `libarrow_testing.a`. The
  pinned Velox `CMake/FindArrow.cmake` requires both `libarrow.a` and
  `libarrow_testing.a`, even when Velox tests are disabled.
- vcpkg prepended its CMake and pkg-config paths but did not prevent fallback
  to host paths, CMake package registries, or inherited environment prefixes.

## Non-goals

- Do not modify Velox source or its dependency resolver.
- Do not change Gluten's Java Arrow dependency versions.
- Do not build or run Arrow's complete unit-test suite.
- Do not change non-vcpkg or ppc64le `build-arrow.sh` behavior.
- Do not provide compiler/sysroot hermeticity.
- Do not permit broad `/usr/local` exceptions for ordinary compile-time
  dependencies.

## Arrow vcpkg Package

### Arrow overlay port

Gluten provides `dev/vcpkg/ports/arrow`, based on the official Microsoft vcpkg
Arrow 18.0.0 port.

The port adds a `testing` feature that:

- sets `ARROW_TESTING=ON`;
- keeps `ARROW_BUILD_TESTS=OFF`;
- declares Boost.Process, GTest, and RapidJSON dependencies; and
- installs `libarrow_testing.a` without compiling Arrow's full test suite.

The port retains:

```text
ARROW_DEPENDENCY_SOURCE=SYSTEM
ARROW_DEPENDENCY_USE_SHARED=<derived from VCPKG_LIBRARY_LINKAGE>
```

All Arrow dependencies must therefore be declared in the port manifest. A
missing package fails the port build instead of triggering a bundled fallback.

The port installs:

```text
include/arrow/api.h
lib/libarrow.a
lib/libarrow_testing.a
```

### Arrow testing patches

The overlay applies two testing patches in order:

1. `arrow-testing-boost.patch` carries the same hunks as the patch in the
   pinned Velox source, with line endings and trailing whitespace normalized.
   It replaces the `Boost::process` link dependency with Boost.Filesystem and
   Boost.System while preserving Velox's intended static link behavior.
2. `arrow-testing-static-boost.patch` fixes the remaining static-only case.
   When `ARROW_TESTING=ON`, it makes Arrow request the Boost filesystem/system
   components so their CMake targets exist.

The second patch is required because `ARROW_TESTING=ON` with
`ARROW_BUILD_TESTS=OFF` otherwise selects Arrow's header-only Boost path while
the first patch links compiled Boost targets.

### Toolchain processor handling

The copied Arrow 18 port intentionally omits the old
`CMAKE_SYSTEM_PROCESSOR=${VCPKG_TARGET_ARCHITECTURE}` override. On x86_64 the
vcpkg architecture name `x64` differs from CMake's `x86_64`, which falsely
triggered cross-compilation and searches for nonexistent
`x64-linux-gnu-*` tools.

### Boost version alignment

vcpkg models Boost as many independent `boost-*` ports. Gluten routes `boost`
and `boost-*` to the Microsoft vcpkg `2024.04.26` baseline:

```text
943c5ef1c8f6b5e6ced092b242c8299caae2ff01
```

This keeps the complete Boost family at 1.84.0 without downgrading unrelated
packages or maintaining dozens of individual overrides. Boost 1.84 is
compatible with the fallback code selected by the pinned Arrow testing patch.

### Gluten manifest

The `velox` feature in `dev/vcpkg/vcpkg.json` depends on:

```json
{
  "name": "arrow",
  "default-features": false,
  "features": ["testing"]
}
```

Arrow CSV, Dataset, Filesystem, and Parquet are not enabled. Gluten's only
accidental dependency on an Arrow Filesystem header was unused and was
removed, so the smaller feature set is sufficient.

## vcpkg Build Activation

vcpkg mode is active when either:

- the caller passes `--enable_vcpkg=ON`; or
- `GLUTEN_VCPKG_ENABLED` is already present in the environment.

This definition is used consistently for Arrow suppression, dependency-prefix
handling, and positional command dispatch. A pre-sourced vcpkg environment
does not rerun vcpkg initialization solely because the CLI option is absent.

### Deprecated independent Arrow installer

In active vcpkg mode:

- omitted `--build_arrow` resolves to `OFF`;
- explicit `--build_arrow=OFF` continues normally;
- explicit `--build_arrow=ON` fails before dependency installation; and
- positional `build_arrow` also fails before invoking `dev/build-arrow.sh`.

Non-vcpkg builds retain the original default and explicit behavior.

Production callers were migrated accordingly:

- static-build Docker dependency-cache layers use positional `true`, which
  initializes vcpkg without building the full backend;
- the openEuler weekly workflow no longer forwards `--build_arrow=ON`; and
- the benchmark notebook no longer rewrites `--build_arrow=OFF` to `ON`.

## Dependency Source Policy

When vcpkg is active, `ep/build-velox/src/build-velox.sh` sets:

```text
VELOX_DEPENDENCY_SOURCE=SYSTEM
Arrow_SOURCE=SYSTEM
simdjson_SOURCE=SYSTEM
```

An enabled compile-time dependency that is unavailable from vcpkg must fail
configuration and be analyzed individually. The build must not silently fall
back to bundled or host packages.

Non-vcpkg builds keep `Arrow_SOURCE=AUTO` and `simdjson_SOURCE=AUTO`.

## CMake Package Discovery

### Root-path isolation

`dev/vcpkg/toolchain.cmake` sets
`CMAKE_FIND_ROOT_PATH_MODE_PACKAGE=ONLY` before loading the standard vcpkg
toolchain. This allows vcpkg to perform its required root-path adjustment. The
value is then stored with `CACHE ... FORCE` after the include so nested
projects cannot weaken it.

The toolchain also:

- disables CMake user and system package registries;
- stops copying the shell's `CMAKE_PREFIX_PATH`;
- ignores `/usr/local`, Homebrew prefixes, and an active Conda prefix; and
- leaves `/usr` available for the system compiler and OS libraries.

The vcpkg toolchain does not introduce `CMAKE_SYSTEM_IGNORE_PREFIX_PATH` or
`CMAKE_SYSTEM_IGNORE_PATH`; it uses `CMAKE_IGNORE_PREFIX_PATH` for dependency
isolation. Existing macOS build-script arguments still pass
`CMAKE_SYSTEM_IGNORE_PATH` as part of their SDK and `/usr/local` header-search
isolation behavior.

### Config-mode default and Velox exception

New vcpkg CMake build trees default to:

```text
CMAKE_FIND_PACKAGE_PREFER_CONFIG=ON
```

This makes Gluten and other vcpkg consumers use package-exported CMake targets
and their usage requirements.

Velox is a deliberate exception. The pinned Velox source expects its
`CMake/FindArrow.cmake` module to create plain `arrow` and `arrow_testing`
targets with a specific static link order. vcpkg's Arrow Config package only
exports namespaced targets. Therefore the Velox `make` invocation is scoped
with:

```text
GLUTEN_VCPKG_PREFER_CONFIG=OFF
```

The override applies only to Velox. Subsequent Gluten configuration returns to
the Config-first default.

The toolchain uses this precedence:

1. a nonempty `GLUTEN_VCPKG_PREFER_CONFIG` environment override;
2. the build tree's existing cached value; or
3. `ON` for a new build tree.

Preserving the cached value is required so an incremental CMake regeneration
does not switch Velox back to Config mode and generate bare `-larrow` links.

### pkg-config isolation

Active vcpkg environments unset `PKG_CONFIG_PATH` and set:

```text
PKG_CONFIG_LIBDIR=<vcpkg lib/pkgconfig>:<vcpkg share/pkgconfig>
```

`PKG_CONFIG_LIBDIR` replaces pkg-config's compiled-in defaults rather than
merely prepending vcpkg paths.

### Install-prefix separation

In vcpkg mode, `INSTALL_PREFIX` remains an install destination but is not added
to `CMAKE_PREFIX_PATH` or injected as an explicit include directory. This rule
also applies to macOS, where non-vcpkg builds may still add a custom prefix
include path.

Non-vcpkg builds retain the original dependency-prefix behavior.

## Gluten Arrow Integration

Gluten uses the Arrow Config package in vcpkg mode:

```cmake
find_package(Arrow CONFIG REQUIRED)
```

`cpp/CMake/ConfigArrow.cmake` provides Gluten's existing `Arrow::arrow`
consumer target as an interface over `Arrow::arrow_static`. This retains
Arrow's exported transitive dependencies, including Brotli, BZip2, LZ4,
Snappy, zlib, zstd, Threads, dl, and rt.

Gluten does not search for or link `Arrow::arrow_bundled_dependencies` in
vcpkg mode because a SYSTEM-dependency Arrow build does not produce that
archive. Non-vcpkg bundled Arrow behavior is unchanged.

## Static Dependency Propagation

Velox is built as a mono static archive and imported into Gluten targets.
Imported archive paths do not carry the usage requirements from Velox's
original CMake targets, so Gluten restores the required edges explicitly:

- `xxHash::xxhash` is linked to the Gluten Velox backend in vcpkg mode;
- imported `velox_exec_test_lib` links the imported VectorFuzzer archives in
  the same dependency direction as the Velox build;
- vcpkg FBThrift archives are PUBLIC dependencies because the public imported
  parquet thrift archive requires their symbols;
- `glog::glog` receives a `google::glog` compatibility alias for existing
  Gluten consumers; and
- combined test-and-benchmark builds link GTest through the benchmark common
  target when imported Velox test utilities require it.

These are target usage requirements, not host-library exceptions.

## Incremental Builds

The no-update path in `dev/builddep-veloxbe-inc.sh` restores the same vcpkg
environment used by a full build:

- `VCPKG_ROOT`;
- target triplet and installed directory;
- `VCPKG_MANIFEST_DIR`;
- `CMAKE_TOOLCHAIN_FILE`;
- isolated `PKG_CONFIG_LIBDIR`; and
- `GLUTEN_VCPKG_ENABLED`.

The incremental Velox `cmake --build` command additionally scopes:

```text
VELOX_DEPENDENCY_SOURCE=SYSTEM
Arrow_SOURCE=SYSTEM
simdjson_SOURCE=SYSTEM
GLUTEN_VCPKG_PREFER_CONFIG=OFF
```

These values apply only to the Velox command. If CMake regenerates, it retains
SYSTEM dependency resolution and Velox's Module-first Arrow behavior. The
subsequent Gluten incremental build does not inherit the preference override
and continues to use the Config-first default.

## Explicit Exceptions

The following are outside ordinary vcpkg compile-time package ownership:

- the system compiler and its runtime libraries;
- libc, pthread, dl, rt, and equivalent OS facilities;
- HDFS runtime-loaded `libhdfs` from `HADOOP_HOME`; and
- CUDA and QAT platform SDKs when enabled.

No exception permits Folly, Boost, Arrow, zstd, gflags, glog, protobuf,
xxHash, or FBThrift to resolve from `/usr/local`.

## Ancillary Buildability Fixes

- Copying Gluten's Arrow compatibility patch into the user-owned Velox
  checkout no longer invokes `sudo`. Dependency installation privileges are
  unchanged.
- An unused `arrow/filesystem/filesystem.h` include was removed from
  `VeloxParquetDataSource.h`. It had silently selected an Arrow 15 header from
  `/usr/local/include` because Arrow Filesystem is not part of the selected
  vcpkg feature set.

## Build Flow

```text
Gluten vcpkg init
  -> install Arrow 18.0.0 and all enabled compile-time dependencies
  -> stop immediately on any vcpkg failure
  -> configure Velox with SYSTEM dependency sources and Module-first Arrow
  -> create plain arrow/arrow_testing targets with absolute vcpkg paths
  -> build Velox mono archive
  -> configure Gluten Config-first with exported vcpkg targets
  -> restore imported static archive usage requirements
  -> build Gluten native libraries, tests, benchmarks, and Maven packages
```

No Arrow ExternalProject or independent `build-arrow.sh` is used in this
flow.

## Verified Results

### Package and source selection

- `libarrow.a`, `libarrow_testing.a`, and `arrow/api.h` are installed under
  `dev/vcpkg/vcpkg_installed`.
- All 78 active modular Boost package records resolve to 1.84.0.
- Velox Debug and Release build graphs contain no bare `-larrow` or
  `-larrow_testing`; Arrow libraries use absolute vcpkg paths.
- No Arrow ExternalProject is generated.

### Poisoned-prefix matrix

The Debug matrix was run with deliberately poisoned `CMAKE_PREFIX_PATH` and
`PKG_CONFIG_PATH`:

```text
--enable_vcpkg=ON
--build_arrow=OFF
--build_tests=ON
--build_benchmarks=ON
--enable_s3=ON
--enable_gcs=ON
--enable_hdfs=ON
--enable_abfs=OFF
--spark_version=4.1
--build_type=Debug
```

Results:

```text
Velox:  976/976 build steps
Gluten: 234/234 build steps
```

Resolved managed dependencies came from vcpkg. Poison prefixes and managed
`/usr/local` include/library paths were absent.

### End-to-end build

After all integration and caller fixes, a final rerun of the exact
`buildbundle-veloxbe.sh` invocation with tests, benchmarks, S3, GCS, HDFS,
Spark 4.1, and Debug exited successfully. All ten Maven reactor modules,
including `Gluten Package`, reported `SUCCESS`.

### Compiler and caller audits

- Ninja's compiler dependency database contains zero
  `/usr/local/include` dependencies.
- Production Docker, workflow, shell, and notebook callers contain no active
  vcpkg `build_arrow`, explicit `--build_arrow=ON`, or OFF-to-ON rewrite.
- Explicit vcpkg `--build_arrow=ON` fails before vcpkg initialization.
  Positional `build_arrow` fails at function dispatch before
  `dev/build-arrow.sh`; omitted vcpkg `--build_arrow` resolves to OFF.

## Maintenance

- Keep the Arrow overlay version and both testing patches aligned with the
  Arrow version used by the pinned Velox source.
- Remove the overlay testing workaround when Velox no longer requires
  `libarrow_testing.a` or Arrow fixes the static testing dependency model.
- Revisit the scoped Boost 1.84 registry when the pinned Velox and Arrow
  configuration can consume newer Boost.Process APIs.
- Preserve the Velox Module-first exception until Velox consumes namespaced
  Arrow Config targets directly.
- Re-run Debug and Release bare-link checks, compiler dependency audits, and
  production caller audits whenever dependency discovery changes.
