# Gluten vcpkg Arrow Design

## Goal

Make Arrow a vcpkg-managed dependency when Gluten builds Velox with
`--enable_vcpkg=ON`. The build must not preinstall Arrow through
`dev/build-arrow.sh`, and Velox must use the Arrow package installed under
Gluten's `dev/vcpkg/vcpkg_installed` tree.

This task primarily fixes Arrow. It will also probe whether all currently
enabled Velox compile-time dependencies can be restricted to vcpkg without
using `/usr/local`. Any unrelated dependency that fails this probe requires
case-by-case analysis before the global restriction is retained.

## Confirmed Current Behavior

- `dev/vcpkg/init.sh` runs `vcpkg install` under `set -e`. A failed vcpkg
  installation stops the build before CMake runs.
- A successful vcpkg installation does not make CMake dependency discovery
  hermetic. vcpkg prepends its paths, but CMake may still search
  `/usr/local`, environment prefixes, package registries, and pkg-config
  defaults.
- Velox currently exports `Arrow_SOURCE=AUTO` and
  `simdjson_SOURCE=AUTO`. AUTO may select a host installation or fall back to
  a bundled build.
- The official vcpkg Arrow 18 port does not build
  `libarrow_testing.a`. Current Velox `CMake/FindArrow.cmake` requires both
  `libarrow.a` and `libarrow_testing.a`, even when Velox tests are disabled.
- Velox's Arrow Parquet writer tests link `arrow_testing` when
  `VELOX_BUILD_TESTING=ON`.

## Non-goals

- Do not modify Velox source or its dependency resolver.
- Do not change Gluten's Java Arrow dependency versions.
- Do not build or run Arrow's complete unit-test suite.
- Do not change non-vcpkg or ppc64le `build-arrow.sh` behavior.
- Do not attempt compiler/sysroot hermeticity. The system compiler, libc,
  pthread, dl, and other OS toolchain libraries remain allowed.
- Do not automatically add `/usr/local` exceptions for unrelated
  dependencies discovered during validation.

## Arrow vcpkg Package

### Arrow overlay port

Add `dev/vcpkg/ports/arrow` based on the official Microsoft vcpkg Arrow
18.0.0 port.

Add a `testing` feature that:

- sets `ARROW_TESTING=ON`;
- keeps `ARROW_BUILD_TESTS=OFF`;
- declares the GTest, Boost.Process, and RapidJSON dependencies needed by
  Arrow's testing library; and
- applies the same `arrow-testing-boost.patch` used by the pinned Velox
  source.

The patch is required for Arrow 18's testing-library-only configuration. It
makes `ARROW_TESTING=ON` resolve Boost correctly without building Arrow's full
test suite.

The port must retain:

```text
ARROW_DEPENDENCY_SOURCE=SYSTEM
ARROW_DEPENDENCY_USE_SHARED=<derived from VCPKG_LIBRARY_LINKAGE>
```

It must install:

```text
include/arrow/api.h
lib/libarrow.a
lib/libarrow_testing.a
```

### Boost version alignment

vcpkg models Boost as many independent `boost-*` ports. Add a scoped registry
to `dev/vcpkg/vcpkg-configuration.json`:

- repository: `https://github.com/Microsoft/vcpkg`;
- baseline: vcpkg `2024.04.26`
  (`943c5ef1c8f6b5e6ced092b242c8299caae2ff01`);
- package patterns: `boost` and `boost-*`.

This keeps the complete Boost family at 1.84.0 without downgrading unrelated
vcpkg packages or maintaining dozens of individual overrides. Boost 1.84 is
compatible with the fallback selected after Velox's Arrow testing patch.

### Gluten manifest

Add `arrow[testing]` to the `velox` feature in `dev/vcpkg/vcpkg.json`, with
Arrow default features disabled. Arrow CSV, Dataset, and Parquet are not
needed for Velox's system Arrow package.

The Arrow overlay port must explicitly declare every compile-time dependency.
Because it uses `ARROW_DEPENDENCY_SOURCE=SYSTEM`, a missing dependency must
fail the vcpkg port build rather than download a bundled fallback.

## vcpkg Build Behavior

### Arrow source selection

In `ep/build-velox/src/build-velox.sh`, vcpkg mode must set:

```text
VELOX_DEPENDENCY_SOURCE=SYSTEM
Arrow_SOURCE=SYSTEM
simdjson_SOURCE=SYSTEM
```

`Arrow_SOURCE` and `simdjson_SOURCE` are explicit because the script currently
exports AUTO for both, which would override the global Velox source policy.
Non-vcpkg builds retain their current AUTO behavior.

The global `VELOX_DEPENDENCY_SOURCE=SYSTEM` setting is a candidate enforcement
change. It is retained only if the enabled dependency graph is fully provided
by vcpkg or uses an approved OS/runtime exception.

### Deprecated vcpkg `build_arrow` path

Update `dev/builddeps-veloxbe.sh` so vcpkg mode never invokes
`dev/build-arrow.sh`:

- omitted `--build_arrow` resolves to `OFF` in vcpkg mode;
- explicit `--build_arrow=OFF` continues normally;
- explicit `--build_arrow=ON` fails before dependency compilation with a
  deprecation message;
- non-vcpkg builds retain the current default and explicit behavior.

The script must distinguish an omitted option from an explicit `ON`; changing
the global default to `OFF` would break non-vcpkg builds.

## Candidate Host-Path Isolation

The following changes are tested together with the Arrow implementation. They
are retained only after the enabled Velox dependency graph passes.

### CMake discovery

Update `dev/vcpkg/toolchain.cmake` to:

- stop copying the shell's `CMAKE_PREFIX_PATH` into CMake;
- include the standard vcpkg toolchain first;
- prefer Config-mode packages with
  `CMAKE_FIND_PACKAGE_PREFER_CONFIG=TRUE`;
- restrict Config-mode package resolution to the vcpkg root with
  `CMAKE_FIND_ROOT_PATH_MODE_PACKAGE=ONLY`;
- disable CMake user and system package registries;
- set `CMAKE_IGNORE_PREFIX_PATH` for non-OS host prefixes, including
  `/usr/local`, Homebrew prefixes, and an active Conda prefix.

Do not set `CMAKE_SYSTEM_IGNORE_PREFIX_PATH` or
`CMAKE_SYSTEM_IGNORE_PATH`; CMake documents those as platform/toolchain
implementation variables, not project controls.

`/usr` remains searchable for the compiler and OS libraries. The selected
vcpkg package Config files should prevent third-party Module-mode lookup; the
validation phase checks that resolved third-party paths still point into
`vcpkg_installed`.

### pkg-config discovery

Update `dev/vcpkg/env.sh` to replace inherited `PKG_CONFIG_PATH` behavior with:

```text
PKG_CONFIG_LIBDIR=<vcpkg lib/pkgconfig>:<vcpkg share/pkgconfig>
```

Unset `PKG_CONFIG_PATH` in vcpkg mode. `PKG_CONFIG_LIBDIR` replaces
pkg-config's compiled-in defaults instead of merely prepending vcpkg paths.

## Explicit Exceptions

These are not ordinary vcpkg-managed compile-time libraries:

- the system compiler and its runtime libraries;
- libc, pthread, dl, rt, and equivalent OS toolchain facilities;
- HDFS runtime-loaded `libhdfs` from `HADOOP_HOME`;
- CUDA and QAT platform SDKs when their optional features are enabled.

An exception does not permit an unrelated library such as Folly, Boost,
Arrow, zstd, gflags, or glog to resolve from `/usr/local`.

## Build Flow

```text
Gluten vcpkg init
  -> vcpkg installs Arrow 18.0.0 and all enabled compile-time dependencies
  -> any vcpkg install failure stops the shell
  -> Velox configures with SYSTEM dependency sources
  -> CMake finds Arrow/Folly/Boost/etc. under vcpkg_installed
  -> missing or mispackaged dependencies fail instead of using /usr/local
  -> Gluten native build uses the same vcpkg toolchain
```

The supported vcpkg invocation uses `--build_arrow=OFF` explicitly or omits
the option and accepts the vcpkg-mode implicit OFF.

## Staged Validation and Decision Gate

### Stage 1: Arrow package

1. Resolve the vcpkg manifest and confirm all `boost-*` packages are 1.84.0.
2. Build `arrow[testing]`.
3. Confirm `libarrow.a`, `libarrow_testing.a`, and `arrow/api.h` are installed
   under `vcpkg_installed`.
4. Confirm Arrow's complete test executables are not built or run.

### Stage 2: Velox integration

1. Configure Velox with Arrow and simdjson forced to SYSTEM.
2. Confirm Velox reports `Using SYSTEM Arrow` with vcpkg paths.
3. Build the affected Velox Arrow/Parquet targets and testing utilities.
4. Confirm no Arrow ExternalProject is generated.

### Stage 3: Global vcpkg enforcement

1. Enable `VELOX_DEPENDENCY_SOURCE=SYSTEM` and the candidate host-path
   isolation.
2. Configure the requested connector/test matrix.
3. Inspect CMake caches, configure logs, compile commands, and link commands
   for Folly, Arrow, Boost, zstd, gflags, glog, and other managed packages.
4. Confirm managed paths are under `vcpkg_installed`, not `/usr/local` or
   another user prefix.
5. Use a deliberately polluted `CMAKE_PREFIX_PATH` and `PKG_CONFIG_PATH` to
   confirm those inputs cannot override vcpkg.

If a non-Arrow dependency fails in Stage 3, stop and analyze that dependency
individually:

- add it to vcpkg if it is an ordinary compile-time dependency;
- classify it as an approved OS/runtime/platform exception if appropriate; or
- treat it as a blocker to global enforcement.

Do not silently add a `/usr/local` exception. The final implementation may
retain only the Arrow-specific changes if the broader enforcement is blocked;
the decision is made from the concrete failure.

### Stage 4: Argument behavior

1. Confirm omitted `--build_arrow` skips `dev/build-arrow.sh` in vcpkg mode.
2. Confirm explicit vcpkg `--build_arrow=ON` fails with the deprecation
   message.
3. Confirm non-vcpkg argument handling still defaults to
   `--build_arrow=ON`.

## Maintenance

The Arrow overlay follows the Arrow version and testing patch used by the
pinned Velox source. Update or remove the overlay when Velox changes its Arrow
version or no longer requires `libarrow_testing.a`.

Revisit the scoped Boost registry when the pinned Velox and Arrow
configuration supports newer Boost.Process APIs without relying on the
1.84-compatible fallback.
