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

import argparse
import json
from pathlib import Path


def check(build):
    reply = build / ".cmake/api/v1/reply"
    index = json.loads(max(reply.glob("index-*.json")).read_text())
    model = json.loads(
        (reply / index["reply"]["codemodel-v2"]["jsonFile"]).read_text()
    )
    targets = {
        entry["name"]: json.loads((reply / entry["jsonFile"]).read_text())
        for entry in model["configurations"][0]["targets"]
    }
    options = {}
    for name in (
        "BUILD_TESTS",
        "BUILD_BENCHMARKS",
        "VELOX_BUILD_TESTING",
        "VELOX_ENABLE_BENCHMARKS",
    ):
        value = next(
            line.split("=", 1)[1]
            for line in (build / "CMakeCache.txt").read_text().splitlines()
            if line.startswith(f"{name}:")
        )
        options[name] = value.upper() == "ON"
    for name, kind in (
        ("velox", "STATIC_LIBRARY"),
        ("gluten", "SHARED_LIBRARY"),
        ("gluten_velox_backend", "SHARED_LIBRARY"),
        ("gluten_core_impl", "OBJECT_LIBRARY"),
        ("gluten_velox_impl", "OBJECT_LIBRARY"),
    ):
        assert targets[name]["type"] == kind, (name, targets[name]["type"])
    for name, option in (
        ("velox_plan_conversion_test", "BUILD_TESTS"),
        ("gluten_velox_memory_test", "BUILD_TESTS"),
        ("delta_bitmap_benchmark", "BUILD_BENCHMARKS"),
        ("velox_memory_test", "VELOX_BUILD_TESTING"),
        (
            "velox_common_indexed_priority_queue_benchmark",
            "VELOX_ENABLE_BENCHMARKS",
        ),
    ):
        assert (name in targets) == options[option], (name, options[option])
    core_link = " ".join(
        fragment["fragment"]
        for fragment in targets["gluten"]["link"]["commandFragments"]
    )
    assert "libgflags" in core_link, "Core JNI library loses glog's gflags dependency"
    backend = targets["gluten_velox_backend"]
    core = targets["gluten_core_impl"]["paths"]["build"]
    objects = [
        source["path"]
        for source in backend["sources"]
        if source["path"].endswith((".o", ".obj"))
    ]
    assert not any(
        source.startswith(f"{core}/CMakeFiles/gluten_core_impl.dir/")
        for source in objects
    ), "Backend JNI library duplicates core implementation objects"
    print(f"Unified graph verified: {len(targets)} targets; options={options}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("build", type=Path)
    check(parser.parse_args().build.resolve())
