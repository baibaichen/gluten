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
            self.assertIn("--target gluten_velox_backend", commands[1])
            self.assertIn("--parallel 30", commands[1])
            self.assertEqual(sentinel.read_text(), "incremental\n")

    def test_vcpkg_init_installs_duckdb_by_default_and_respects_overrides(self):
        for arguments, expected in (([], True), (["--build_tests=ON"], True),
                                    (["--build_tests=OFF"], False)):
            with self.subTest(arguments=arguments), tempfile.TemporaryDirectory() as directory:
                temp = Path(directory)
                log = temp / "arguments"
                vcpkg = temp / "vcpkg"
                vcpkg.write_text('#!/bin/bash\nprintf "%s\\n" "$@" > "$COMMAND_LOG"\n')
                vcpkg.chmod(0o755)
                lib = temp / "installed/lib"
                lib.mkdir(parents=True)
                for name in ("z", "ssl", "crypto", "lzma", "dwarf"):
                    (lib / f"lib{name}.a").touch()
                env = dict(os.environ, VCPKG_ROOT=str(temp), VCPKG=str(vcpkg),
                           VCPKG_TRIPLET="x64-linux-avx",
                           VCPKG_TRIPLET_INSTALL_DIR=str(lib.parent),
                           COMMAND_LOG=str(log))
                result = subprocess.run(
                    ["bash", str(ROOT / "dev/vcpkg/init.sh"), *arguments],
                    cwd=ROOT, env=env, text=True,
                    stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                )
                self.assertEqual(result.returncode, 0, result.stdout)
                self.assertEqual("--x-feature=duckdb" in log.read_text().splitlines(), expected)

    def test_vcpkg_build_uses_effective_cmake_options_for_test_dependencies(self):
        options = {
            "BUILD_TESTS": "build_tests",
            "BUILD_BENCHMARKS": "build_benchmarks",
            "VELOX_BUILD_TESTING": "build_velox_tests",
            "VELOX_ENABLE_BENCHMARKS": "build_velox_benchmarks",
        }
        disabled = dict.fromkeys(options, "OFF")
        gluten_off = ["--build_tests=OFF", "--build_benchmarks=OFF"]
        all_off = [f"--{option}=OFF" for option in options.values()]
        cases = [
            ({}, [], "ON"),
            ({}, all_off, "OFF"),
            (disabled, [], "OFF"),
            (dict.fromkeys(options, "ON"), all_off, "OFF"),
            ({**disabled, "BUILD_TESTS": ""}, [], "OFF"),
        ]
        for option in (*options, "BUILD_TEST_UTILS", "VELOX_BUILD_TEST_UTILS"):
            cases.append(({**disabled, option: "ON"}, [], "ON"))
        for option in ("VELOX_BUILD_TESTING", "VELOX_ENABLE_BENCHMARKS",
                       "BUILD_TEST_UTILS", "VELOX_BUILD_TEST_UTILS"):
            cases.append(({**disabled, option: "ON"}, gluten_off, "ON"))
        for option in options.values():
            cases.append((disabled, [f"--{option}=ON"], "ON"))
        for value, expected in (("1", "ON"), ("true", "ON"), ("0", "OFF"),
                                ("false", "OFF")):
            cases.append(({**disabled, "BUILD_TEST_UTILS": value}, gluten_off, expected))
        with tempfile.TemporaryDirectory() as directory:
            temp = Path(directory)
            repo = temp / "repo"
            dev = repo / "dev"
            vcpkg = dev / "vcpkg"
            vcpkg.mkdir(parents=True)
            script = dev / "builddeps-veloxbe.sh"
            script.write_text((ROOT / "dev/builddeps-veloxbe.sh").read_text())
            env_log = temp / "env-args"
            (vcpkg / "env.sh").write_text(
                f'printf "%s\\n" "$@" > "{env_log}"\n'
                'export GLUTEN_VCPKG_ENABLED=1\n'
            )
            cmake_log = temp / "cmake-args"
            cmake = temp / "cmake"
            cmake.write_text(
                '#!/bin/bash\nprintf "%s\\n" "$*" >> "$COMMAND_LOG"\n'
            )
            cmake.chmod(0o755)
            env = dict(os.environ, PATH=f"{temp}:{os.environ['PATH']}",
                       COMMAND_LOG=str(cmake_log))
            for index, (cached, arguments, expected) in enumerate(cases):
                with self.subTest(cached=cached, arguments=arguments):
                    build = repo / f"build {index}"
                    build.mkdir()
                    if cached:
                        (build / "CMakeCache.txt").write_text("".join(
                            f"{key}:BOOL={value}\n" for key, value in cached.items()
                        ))
                    cmake_log.write_text("")
                    result = subprocess.run(
                        ["bash", str(script), "--enable_vcpkg=ON",
                         "--spark_version=4.1", f"--cpp_build_dir={build}",
                         *arguments, "build_gluten_cpp"],
                        cwd=repo, env=env, text=True,
                        stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                    )
                    self.assertEqual(result.returncode, 0, result.stdout)
                    self.assertIn(f"--build_tests={expected}", env_log.read_text())
                    commands = cmake_log.read_text().splitlines()
                    self.assertEqual(len(commands), 2, commands)
                    for option, argument in options.items():
                        explicit = next((arg.split("=", 1)[1] for arg in arguments
                                         if arg.startswith(f"--{argument}=")), None)
                        if explicit is None:
                            self.assertNotIn(f"-D{option}=", commands[0])
                        else:
                            self.assertIn(f"-D{option}={explicit}", commands[0])
                    self.assertIn("--target gluten_velox_backend", commands[1])

    def test_incremental_dependency_update_includes_test_utils(self):
        disabled = dict.fromkeys(("BUILD_TESTS", "BUILD_BENCHMARKS",
                                  "VELOX_BUILD_TESTING", "VELOX_ENABLE_BENCHMARKS"), "OFF")
        cases = [(disabled, "--update_vcpkg", "--build_tests=OFF")]
        for option in (*disabled, "BUILD_TEST_UTILS", "VELOX_BUILD_TEST_UTILS"):
            cases.append(({**disabled, option: "ON"}, "--update_vcpkg", "--build_tests=ON"))
        cases.append(({**disabled, "BUILD_TEST_UTILS": "ON"}, None, "--skip-install"))
        for value, expected in (("1", "ON"), ("true", "ON"), ("0", "OFF"),
                                ("false", "OFF")):
            cases.append(({**disabled, "BUILD_TEST_UTILS": value}, "--update_vcpkg",
                          f"--build_tests={expected}"))
        with tempfile.TemporaryDirectory() as directory:
            repo = Path(directory)
            dev = repo / "dev"
            vcpkg = dev / "vcpkg"
            vcpkg.mkdir(parents=True)
            script = dev / "builddep-veloxbe-inc.sh"
            script.write_text((ROOT / "dev/builddep-veloxbe-inc.sh").read_text())
            log = repo / "env-args"
            (vcpkg / "env.sh").write_text(f'printf "%s\\n" "$@" > "{log}"\n')
            cmake = repo / "cmake"
            cmake.write_text("#!/bin/bash\nexit 0\n")
            cmake.chmod(0o755)
            build = repo / "native build"
            releases = build / "releases"
            releases.mkdir(parents=True)
            (releases / "libgluten.so").touch()
            (releases / "libvelox.so").touch()
            env = dict(os.environ, PATH=f"{repo}:{os.environ['PATH']}")
            for cached, argument, expected in cases:
                with self.subTest(cached=cached, argument=argument):
                    (build / "CMakeCache.txt").write_text("".join(
                        f"{key}:BOOL={value}\n" for key, value in cached.items()
                    ))
                    result = subprocess.run(
                        ["bash", str(script), f"--cpp_build_dir={build}",
                         *([argument] if argument else [])],
                        cwd=repo, env=env, text=True,
                        stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                    )
                    self.assertEqual(result.returncode, 0, result.stdout)
                    self.assertIn(expected, log.read_text().splitlines())

    def test_package_script_leaves_test_and_benchmark_options_to_cmake(self):
        package_script = (ROOT / "dev/package-vcpkg.sh").read_text()
        self.assertNotIn("--build_tests=ON", package_script)
        self.assertNotIn("--build_benchmarks=ON", package_script)

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
