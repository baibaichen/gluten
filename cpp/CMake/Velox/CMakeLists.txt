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

set(CMAKE_FIND_PACKAGE_PREFER_CONFIG OFF)
set(CMAKE_FIND_PACKAGE_TARGETS_GLOBAL ON)
set(VELOX_MONO_LIBRARY ON)
set(VELOX_BUILD_SHARED OFF)
set(VELOX_BUILD_RUNNER OFF)
set(VELOX_ENABLE_PARQUET ON)
set(VELOX_ENABLE_GEO OFF)
set(VELOX_SIMDJSON_SKIPUTF8VALIDATION ON)
set(VELOX_ENABLE_S3 "${ENABLE_S3}")
set(VELOX_ENABLE_GCS "${ENABLE_GCS}")
set(VELOX_ENABLE_HDFS "${ENABLE_HDFS}")
set(VELOX_ENABLE_ABFS "${ENABLE_ABFS}")
set(VELOX_ENABLE_CUDF "${ENABLE_GPU}")
set(VELOX_ENABLE_LTO "${ENABLE_LTO}")
if(BUILD_TESTS OR BUILD_BENCHMARKS OR BUILD_TEST_UTILS
   OR VELOX_BUILD_TESTING OR VELOX_ENABLE_BENCHMARKS)
  set(VELOX_BUILD_TEST_UTILS ON)
endif()
if(ENABLE_GLUTEN_VCPKG)
  set(VELOX_DEPENDENCY_SOURCE SYSTEM)
  set(ENV{VELOX_DEPENDENCY_SOURCE} SYSTEM)
  set(ENV{Arrow_SOURCE} SYSTEM)
  set(ENV{simdjson_SOURCE} SYSTEM)
  set(VELOX_GFLAGS_TYPE static)
endif()
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wno-error=stringop-overflow -Wno-error=cpp")
add_subdirectory("${VELOX_HOME}" "${VELOX_BUILD_PATH}")
