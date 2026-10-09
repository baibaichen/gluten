# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import os
from pathlib import Path
import re
import subprocess
import tempfile
import unittest


ROOT = Path(__file__).resolve().parents[3]


class NativeDependencyTest(unittest.TestCase):
    def run_command(self, *args):
        result = subprocess.run(args, text=True, stdout=subprocess.PIPE,
                                stderr=subprocess.STDOUT)
        self.assertEqual(result.returncode, 0, result.stdout)
        return result.stdout

    def test_core_logging_links_after_velox_dependency_resolution(self):
        velox = Path(os.environ.get(
            "VELOX_HOME", ROOT / "ep/build-velox/build/velox_ep"
        )).resolve()
        core = (ROOT / "cpp/core/CMakeLists.txt").read_text()
        logging_link = re.search(
            r"target_link_libraries\(gluten_core_impl PRIVATE glog::glog[^)]*\)",
            core,
        ).group()
        with tempfile.TemporaryDirectory() as directory:
            source = Path(directory)
            (source / "velox").mkdir()
            (source / "velox/CMakeLists.txt").write_text(
                "set(CMAKE_FIND_PACKAGE_PREFER_CONFIG OFF)\n"
                "set(CMAKE_FIND_PACKAGE_TARGETS_GLOBAL ON)\n"
                f'list(PREPEND CMAKE_MODULE_PATH "{velox}/CMake")\n'
                "include(ResolveDependency)\n"
                "set(VELOX_DEPENDENCY_SOURCE SYSTEM)\n"
                "if(ENABLE_GLUTEN_VCPKG)\n"
                "  set(VELOX_GFLAGS_TYPE static)\n"
                "else()\n"
                "  set(VELOX_GFLAGS_TYPE shared)\n"
                "endif()\n"
                "velox_set_source(gflags)\n"
                "velox_resolve_dependency(gflags COMPONENTS ${VELOX_GFLAGS_TYPE})\n"
                "if(NOT TARGET gflags::gflags)\n"
                "  add_library(gflags::gflags ALIAS gflags)\n"
                "endif()\n"
                "velox_set_source(glog)\n"
                "velox_resolve_dependency(glog)\n"
            )
            (source / "CMakeLists.txt").write_text(
                "cmake_minimum_required(VERSION 3.28)\n"
                "project(core_logging LANGUAGES CXX)\n"
                "set(CMAKE_CXX_STANDARD 20)\n"
                "set(CMAKE_POSITION_INDEPENDENT_CODE ON)\n"
                "add_subdirectory(velox)\n"
                f'list(PREPEND CMAKE_MODULE_PATH "{ROOT}/cpp/CMake")\n'
                "find_package(glog REQUIRED)\n"
                "add_library(gluten_core_impl OBJECT logging.cc)\n"
                f"{logging_link}\n"
                "add_library(gluten SHARED)\n"
                "target_link_libraries(gluten PUBLIC gluten_core_impl)\n"
                "if(NOT APPLE)\n"
                "  target_link_options(gluten PRIVATE LINKER:--no-undefined)\n"
                "endif()\n"
                "add_executable(consumer main.cc)\n"
                "target_link_libraries(consumer PRIVATE gluten)\n"
            )
            (source / "logging.cc").write_text(
                '#include <glog/logging.h>\n#include <gflags/gflags.h>\n'
                'void log_message() { google::InitGoogleLogging("core_logging"); '
                'google::SetCommandLineOption("logtostderr", "1"); '
                'LOG(INFO) << "core logging works"; google::ShutdownGoogleLogging(); }\n'
            )
            (source / "main.cc").write_text(
                "void log_message(); int main() { log_message(); }\n"
            )
            build = source / "build"
            self.run_command("cmake", "-S", str(source), "-B", str(build), "-G", "Ninja")
            self.run_command("cmake", "--build", str(build), "--parallel", "2")
            self.assertIn("core logging works", self.run_command(str(build / "consumer")))

    def test_missing_benchmark_uses_fallback_in_both_dependency_modes(self):
        cmake = (ROOT / "cpp/CMakeLists.txt").read_text()
        benchmark = cmake[cmake.index("if(BUILD_TESTS OR BUILD_BENCHMARKS)"):
                          cmake.index("if(ENABLE_QAT)")]
        with tempfile.TemporaryDirectory() as directory:
            source = Path(directory)
            (source / "BuildGoogleBenchmark.cmake").write_text(
                'message(STATUS "benchmark fallback selected")\n'
            )
            for mode in ("ON", "OFF"):
                with self.subTest(vcpkg=mode):
                    (source / "CMakeLists.txt").write_text(
                        "cmake_minimum_required(VERSION 3.28)\n"
                        "project(benchmark_fallback LANGUAGES NONE)\n"
                        f"set(ENABLE_GLUTEN_VCPKG {mode})\n"
                        "set(BUILD_BENCHMARKS ON)\n"
                        "set(CMAKE_DISABLE_FIND_PACKAGE_benchmark ON)\n"
                        'list(PREPEND CMAKE_MODULE_PATH "${CMAKE_CURRENT_SOURCE_DIR}")\n'
                        f"{benchmark}\n"
                    )
                    output = self.run_command(
                        "cmake", "-S", str(source), "-B", str(source / mode), "-G", "Ninja"
                    )
                    self.assertIn("benchmark fallback selected", output)


if __name__ == "__main__":
    unittest.main()
