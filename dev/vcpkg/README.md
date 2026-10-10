# Build Gluten + Velox in Vcpkg Environment

## Overview

Currently, the `builtin-baseline` set in `vcpkg.json` is the commit hash for the `2026.03.18` tag of vcpkg.
The versions of all dependency libraries are determined by their respective ports at this vcpkg version,
except for those overridden in `vcpkg.json`, `vcpkg-configuration.json`, and overlay ports.

## Build in docker

For main branch code, you can follow the commands below.

- Pull the docker image: `docker pull apache/gluten:vcpkg-almalinux-8-gcc13`
- Build native code: `bash dev/ci-velox-buildstatic-centos-8.sh`
- Build JVM code: `./build/mvn clean install -Pbackends-velox -Pspark-3.5 -DskipTests`

The gluten packages will be placed in `$GLUTEN_REPO/package/target/gluten-velox-bundle-*.jar`.

## Setup build environment manually

### Install native dependencies only

From the Gluten checkout, install dependencies without compiling Gluten or
Velox and without running Maven:

```sh
source dev/vcpkg/env.sh --build_tests=ON --enable_s3=ON --enable_gcs=ON --enable_hdfs=ON
```

Gluten's toolchain also works without sourcing this environment: it defaults to
`dev/vcpkg/.vcpkg` and `dev/vcpkg/vcpkg_installed`. Explicit CMake
`VCPKG_TARGET_TRIPLET`, `VCPKG_HOST_TRIPLET`, and `VCPKG_INSTALLED_DIR` values
take precedence over defaults. Dependency installation remains a separate step.
Roaring 4.3.11 is managed by vcpkg; missing Roaring packages fail configuration
instead of downloading sources during the native build.

Run the dependency integration checks after installing dependencies:

```sh
python3 dev/vcpkg/tests/test_cmake.py
```

Native tests link implementation objects rather than production JNI shared
libraries. Check the ELF dependencies and GoogleTest discovery after building:

```sh
python3 cpp/CMake/tests/check_native_runtime.py \
  cpp/build/velox/tests/velox_plan_conversion_test \
  cpp/build/velox/compute/delta/tests/velox_roaring_bitmap_array_test
ctest --test-dir cpp/build --output-on-failure
```

### Unified native CMake graph

Open `cpp/CMakeLists.txt` as the native project. Velox is added from
`VELOX_HOME`; its CMake files are not modified. The real `velox` target builds
the static Velox library, while `gluten_velox_backend` produces `libvelox.so`.
`gluten` continues to produce `libgluten.so`. Native tests and benchmarks link
the implementation object targets instead of these JNI shared libraries.
Gluten's memory test target is named `gluten_velox_memory_test` to distinguish
it from the upstream `velox_memory_test`.

Gluten tests and benchmarks are enabled by default; Velox's own tests and
benchmarks are disabled by default. Configure with `BUILD_TESTS` /
`BUILD_BENCHMARKS` for Gluten and `VELOX_BUILD_TESTING` /
`VELOX_ENABLE_BENCHMARKS` for Velox to override these defaults. These options
are independent; the native library and required test utilities are built in
the same graph. `package-vcpkg.sh` initializes vcpkg, builds only the
`gluten_velox_backend` target (which also produces both Java JNI libraries),
then runs Maven packaging. To check IDE target visibility, request the CMake
File API:

```sh
mkdir -p cpp/build/.cmake/api/v1/query
touch cpp/build/.cmake/api/v1/query/codemodel-v2
# Re-run your CMake configure command with all four test/benchmark options ON.
python3 cpp/CMake/tests/check_unified_graph.py cpp/build
```

Velox tests/benchmarks enable its GEO support because upstream fuzzer utilities
reference S2 operations even outside geospatial tests. GEOS and S2 remain
vcpkg-managed dependencies; library-only builds retain GEO disabled.

### Three independent build stages

The following commands run on Linux from the Gluten checkout. Dependency
installation is explicit; CMake does not install packages.

1. Install dependencies:

   ```sh
   source dev/vcpkg/env.sh --build_tests=ON --enable_s3=ON --enable_gcs=ON --enable_hdfs=ON
   ```

   `--build_tests=ON` installs DuckDB, also required by Velox test utilities and
   benchmarks. Use it when enabling either project's tests or benchmarks.

2. Configure and build native targets:

   ```sh
   cmake -S cpp -B "$PWD/.clion-build/debug" -G Ninja \
     -DCMAKE_BUILD_TYPE=Debug \
     -DENABLE_S3=ON -DENABLE_GCS=ON -DENABLE_HDFS=ON
   jobs=$(( $(nproc) > 2 ? $(nproc) - 2 : 1 ))
   cmake --build "$PWD/.clion-build/debug" --parallel "$jobs" \
     --target gluten_velox_backend
   ```

   For Release use `.clion-build/release` and `-DCMAKE_BUILD_TYPE=Release`.
   Ninja limits native linking to six concurrent jobs to bound memory use;
   override this with `-DMAX_LINK_JOBS=N` if needed.
   Large Debug test executables can exhaust memory at higher concurrency.
   Lower this value on smaller machines; existing caches retain their setting.
   `VELOX_HOME` defaults to `ep/build-velox/build/velox_ep`. Missing or empty
   directories are cloned using the repo/ref defaults in `get-velox.sh`.
   `-DVELOX_REPO` and `-DVELOX_BRANCH` override those defaults for new clones.
   Existing nonempty directories are never fetched, reset, checked out, or
   updated; missing files/submodules must be prepared manually.
   A custom `-DVELOX_HOME=/path/to/velox` must contain a checkout compatible
   with the current Gluten revision. An arbitrary upstream Meta checkout
   may lack APIs supplied by Gluten's default IBM Velox branch.

3. Build Java and package existing native libraries:

   ```sh
   export JAVA_HOME=/usr/lib/jvm/msopenjdk-17
   export PATH="$JAVA_HOME/bin:$PATH"
   ./dev/buildbundle-veloxbe.sh --skip_native=ON \
     --cpp_build_dir="$PWD/.clion-build/debug" \
     --spark_version=4.1
   ```

   This stage validates the two existing JNI libraries and skips native
   compilation and vcpkg installation. Maven uses the selected native output
   directory. If stage 2 used a custom `VELOX_HOME`, pass the matching
   `--velox_home=/path/to/velox` here for consistent build information.
   Configure Maven repository access through `MAVEN_ARGS` if required by
   your environment. For the original all-in-one pipeline, continue to run
   `./dev/package-vcpkg.sh`; CI invocation and default `cpp/build/releases`
   packaging paths are unchanged.

### CLion without presets

Open the Gluten `cpp` directory as a CMake project. No overlay, copied Velox
CMake files, or `CMakePresets.json` is needed. The CMakeLists selects Gluten's
vcpkg toolchain before `project()`. Install dependencies first as above.

In Settings > Build, Execution, Deployment > CMake, create ordinary profiles:

| Profile | Build type | Build directory |
| --- | --- | --- |
| Debug | Debug | `$GLUTEN_HOME/.clion-build/debug` |
| Release | Release | `$GLUTEN_HOME/.clion-build/release` |

Use absolute paths in CLion and select Ninja as the generator. No extra CMake
options are needed for Gluten tests and benchmarks; Velox tests and benchmarks
remain off unless explicitly enabled. Build the `gluten_velox_backend` target
to produce both Java JNI libraries. Since Velox is a separate checkout outside
`cpp`, CLion may show it under **External Sources**; this is expected and code
navigation works. Copying or symlinking Velox into the project would add
complexity without improving the CMake build graph.

Use the container's C/C++ compiler, CMake 3.28 or newer, Ninja, and installed
JDK. Library/header navigation follows CMake usage requirements; vcpkg binary
packages do not necessarily include third-party implementation source files.
Do not reuse a standalone Velox cache for this unified project.

### Runtime validation notes

Run Velox tests through CTest, which preserves upstream per-test process
isolation. Running an entire test executable can expose shared-state conflicts
between otherwise independent cases.
Use `ctest` from the same CMake installation that configured the build.
After sourcing `dev/vcpkg/env.sh`, keep that environment for test execution;
an older system CTest may not understand the generated GoogleTest policies.

On WSL, a piped kernel crash collector can hang GoogleTest death-test children.
If this occurs, suppress the collector for the test process only, without
changing the host's kernel configuration:

```sh
prlimit --core=1:1 ctest --test-dir .clion-build/debug --output-on-failure
```

Spark 4.1 enables ANSI mode by default. For native integration suites expecting
non-ANSI execution, pass `-DargLine=-Dspark.sql.ansi.enabled=false` to Maven.
This avoids validating Spark fallback instead of the native backend.

### Setup build toolkits

Please install build depends on your system to compile all libraries:

``` sh
sudo $GLUTEN_REPO/dev/vcpkg/setup-build-depends.sh
```

GCC 12 is the minimum required compiler. It needs to be enabled beforehand.

For unsupported linux distro, you can install the following packages from package manager.

* zip
* tar
* wget
* curl
* git >= 2.7.4
* gcc >= 12
* pkg-config
* autotools
* flex >= 2.6.0
* bison
* openjdk 8
* maven

### Build gluten + velox with vcpkg installed dependencies

With `--enable_vcpkg=ON`, the below script will install all static libraries into `./vcpkg_installed/`. And it will
also set `$PATH` and `$CMAKE_TOOLCHAIN_FILE` to make CMake to locate the binary tools and libraries.
You can configure [binary cache](https://learn.microsoft.com/en-us/vcpkg/users/binarycaching) to accelerate the build.

``` sh
$GLUTEN_REPO/dev/buildbundle-veloxbe.sh --enable_vcpkg=ON --build_tests=ON --build_benchmarks=ON --enable_s3=ON  --enable_hdfs=ON
```
