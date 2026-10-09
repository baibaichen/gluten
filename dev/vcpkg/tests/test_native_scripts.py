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
import subprocess
import tempfile
import unittest


ROOT = Path(__file__).resolve().parents[3]


class NativeScriptTest(unittest.TestCase):
    def test_native_build_calls_one_cmake_graph_and_preserves_build_directory(self):
        with tempfile.TemporaryDirectory() as directory:
            temp = Path(directory)
            build = temp / "native build"
            build.mkdir()
            sentinel = build / "keep"
            sentinel.write_text("incremental\n")
            log = temp / "commands"
            cmake = temp / "cmake"
            cmake.write_text('#!/bin/bash\nprintf "%s\\n" "$*" >> "$COMMAND_LOG"\n')
            cmake.chmod(0o755)
            env = dict(os.environ, PATH=f"{temp}:{os.environ['PATH']}",
                       GLUTEN_VCPKG_ENABLED="installed", COMMAND_LOG=str(log))
            result = subprocess.run(
                ["bash", str(ROOT / "dev/builddeps-veloxbe.sh"),
                 "--enable_vcpkg=OFF", f"--cpp_build_dir={build}",
                 "--build_type=Debug", "--build_tests=ON",
                 "--build_velox_tests=ON", "--build_velox_benchmarks=ON",
                 "--spark_version=4.1", "--num_threads=30", "build_gluten_cpp"],
                cwd=ROOT, env=env, text=True,
                stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
            )
            self.assertEqual(result.returncode, 0, result.stdout)
            commands = log.read_text().splitlines()
            self.assertEqual(len(commands), 2, commands)
            self.assertIn("-DVELOX_BUILD_TESTING=ON", commands[0])
            self.assertIn("-DVELOX_ENABLE_BENCHMARKS=ON", commands[0])
            self.assertIn("--build", commands[1])
            self.assertIn("--parallel 30", commands[1])
            self.assertEqual(sentinel.read_text(), "incremental\n")

    def test_packaging_only_rejects_missing_native_libraries(self):
        with tempfile.TemporaryDirectory() as directory:
            result = subprocess.run(
                ["bash", str(ROOT / "dev/builddeps-veloxbe.sh"),
                 "--skip_native=ON", f"--cpp_build_dir={directory}",
                 "--spark_version=4.1"],
                cwd=ROOT, text=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
            )
            self.assertNotEqual(result.returncode, 0)
            self.assertIn("Missing native library", result.stdout)

    def test_relative_build_directory_is_exported_as_an_absolute_path(self):
        with tempfile.TemporaryDirectory(dir=ROOT) as directory:
            temp = Path(directory)
            log = temp / "arguments"
            cmake = temp / "cmake"
            cmake.write_text('#!/bin/bash\nprintf "%s\\n" "$@" >> "$COMMAND_LOG"\n')
            cmake.chmod(0o755)
            build = temp / "native build"
            env = dict(os.environ, PATH=f"{temp}:{os.environ['PATH']}",
                       GLUTEN_VCPKG_ENABLED="installed", COMMAND_LOG=str(log))
            result = subprocess.run(
                ["bash", str(ROOT / "dev/builddeps-veloxbe.sh"),
                 "--enable_vcpkg=OFF",
                 f"--cpp_build_dir={build.relative_to(ROOT)}",
                 "--spark_version=4.1", "build_gluten_cpp"],
                cwd=ROOT, env=env, text=True,
                stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
            )
            self.assertEqual(result.returncode, 0, result.stdout)
            arguments = log.read_text().splitlines()
            configured = Path(arguments[arguments.index("-B") + 1])
            self.assertTrue(configured.is_absolute(), configured)
            self.assertEqual(configured.resolve(), build)

    def test_packaging_only_skips_dependency_installation_and_native_build(self):
        with tempfile.TemporaryDirectory() as directory:
            build = Path(directory)
            (build / "releases").mkdir()
            for name in ("libgluten.so", "libvelox.so"):
                (build / "releases" / name).touch()
            result = subprocess.run(
                ["bash", str(ROOT / "dev/builddeps-veloxbe.sh"),
                 "--skip_native=ON", "--enable_vcpkg=ON",
                 f"--cpp_build_dir={build}", "--spark_version=4.1"],
                cwd=ROOT, text=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
            )
            self.assertEqual(result.returncode, 0, result.stdout)
            self.assertNotIn("+ source ", result.stdout)
            self.assertNotIn("+ cmake ", result.stdout)


if __name__ == "__main__":
    unittest.main()
