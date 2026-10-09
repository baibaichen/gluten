import argparse
from pathlib import Path
import subprocess


def check(binary):
    dependencies = subprocess.check_output(
        ["readelf", "-d", str(binary)], text=True
    )
    for library in ("libgluten.so", "libvelox.so"):
        if f"[{library}]" in dependencies:
            raise RuntimeError(f"{binary} loads production JNI library {library}")
    subprocess.run([str(binary), "--gtest_list_tests"], check=True,
                   stdout=subprocess.DEVNULL, timeout=60)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("binaries", type=Path, nargs="+")
    args = parser.parse_args()
    for binary in args.binaries:
        check(binary.resolve())
    print(f"Verified {len(args.binaries)} native test binaries")
