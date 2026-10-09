import os
from pathlib import Path
import subprocess
import tempfile
import unittest


ROOT = Path(__file__).resolve().parents[3]
TOOLCHAIN = ROOT / "dev/vcpkg/toolchain.cmake"


class VcpkgCMakeTest(unittest.TestCase):
    def configure(self, source, build, *options):
        env = os.environ.copy()
        for name in (
            "VCPKG_ROOT", "VCPKG_MANIFEST_DIR", "VCPKG_TRIPLET",
            "GLUTEN_VCPKG_ENABLED", "CMAKE_TOOLCHAIN_FILE",
            "CMAKE_PREFIX_PATH", "PKG_CONFIG_PATH", "PKG_CONFIG_LIBDIR",
        ):
            env.pop(name, None)
        return subprocess.run(
            ["cmake", "-S", str(source), "-B", str(build), "-G", "Ninja",
             f"-DCMAKE_TOOLCHAIN_FILE={TOOLCHAIN}", *options],
            env=env, text=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
        )

    def test_roaring_consumer_without_shell_environment(self):
        with tempfile.TemporaryDirectory() as directory:
            source = Path(directory)
            (source / "CMakeLists.txt").write_text(
                "cmake_minimum_required(VERSION 3.28)\n"
                "project(roaring_consumer LANGUAGES C CXX)\n"
                f'list(PREPEND CMAKE_MODULE_PATH "{ROOT}/cpp/CMake")\n'
                "find_package(Roaring MODULE REQUIRED)\n"
                "add_executable(consumer main.cc)\n"
                "target_link_libraries(consumer PRIVATE roaring)\n"
            )
            (source / "main.cc").write_text(
                "#include <roaring/roaring64map.hh>\n"
                "int main() { roaring::Roaring64Map bitmap; bitmap.add(uint64_t(42)); "
                "return bitmap.contains(uint64_t(42)) ? 0 : 1; }\n"
            )
            build = source / "build"
            result = self.configure(source, build)
            self.assertEqual(result.returncode, 0, result.stdout)
            subprocess.run(["cmake", "--build", str(build)], check=True)
            subprocess.run([str(build / "consumer")], check=True)
            cache = (build / "CMakeCache.txt").read_text()
            self.assertIn(str(ROOT / "dev/vcpkg/vcpkg_installed"), cache)
            self.assertFalse((build / "_deps").exists())

    def test_missing_roaring_fails_without_fetchcontent(self):
        with tempfile.TemporaryDirectory() as directory:
            source = Path(directory)
            (source / "CMakeLists.txt").write_text(
                "cmake_minimum_required(VERSION 3.28)\n"
                "project(missing_roaring LANGUAGES C CXX)\n"
                f'list(PREPEND CMAKE_MODULE_PATH "{ROOT}/cpp/CMake")\n'
                "set(CMAKE_DISABLE_FIND_PACKAGE_roaring ON)\n"
                "find_package(Roaring MODULE REQUIRED)\n"
            )
            result = self.configure(source, source / "build")
            self.assertNotEqual(result.returncode, 0, result.stdout)
            self.assertIn("roaring", result.stdout.lower())
            self.assertFalse((source / "build/_deps").exists())


if __name__ == "__main__":
    unittest.main()
