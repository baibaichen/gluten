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

find_package(Roaring MODULE REQUIRED)
set(GLUTEN_ROARING_LINK_LIBRARY roaring)
find_package(xxHash CONFIG REQUIRED)

set(VELOX_PROTO_SRC_DIR
    "${GLUTEN_HOME}/backends-velox/src/main/resources/org/apache/gluten/proto")
set(PROTO_OUTPUT_DIR "${CMAKE_CURRENT_BINARY_DIR}/proto")
file(MAKE_DIRECTORY "${PROTO_OUTPUT_DIR}")
file(GLOB VELOX_PROTO_FILES CONFIGURE_DEPENDS "${VELOX_PROTO_SRC_DIR}/*.proto")
foreach(PROTO IN LISTS VELOX_PROTO_FILES)
  get_filename_component(PROTO_NAME "${PROTO}" NAME_WE)
  list(APPEND VELOX_PROTO_SRCS "${PROTO_OUTPUT_DIR}/${PROTO_NAME}.pb.cc")
  list(APPEND VELOX_PROTO_HDRS "${PROTO_OUTPUT_DIR}/${PROTO_NAME}.pb.h")
endforeach()
find_protobuf()
add_custom_command(
  OUTPUT ${VELOX_PROTO_SRCS} ${VELOX_PROTO_HDRS}
  COMMAND "${PROTOC_BIN}" --proto_path "${VELOX_PROTO_SRC_DIR}"
          --cpp_out "${PROTO_OUTPUT_DIR}" ${VELOX_PROTO_FILES}
  DEPENDS ${VELOX_PROTO_FILES}
  VERBATIM)
add_custom_target(velox_jni_proto DEPENDS ${VELOX_PROTO_SRCS} ${VELOX_PROTO_HDRS})

set(VELOX_SRCS
    ${VELOX_PROTO_SRCS}
    compute/VeloxBackend.cc
    compute/VeloxRuntime.cc
    compute/VeloxPlanConverter.cc
    compute/WholeStageResultIterator.cc
    compute/delta/DeltaConnector.cpp
    compute/delta/DeltaDataSource.cpp
    compute/delta/DeltaDeletionVectorReader.cpp
    compute/delta/DeltaSplit.cpp
    compute/delta/DeltaSplitReader.cpp
    compute/delta/RoaringBitmapArray.cpp
    compute/iceberg/IcebergPlanConverter.cc
    compute/iceberg/IcebergFormat.cc
    compute/iceberg/IcebergWriter.cc
    jni/JniFileSystem.cc
    jni/JniUdf.cc
    jni/JniHashTable.cc
    memory/BufferOutputStream.cc
    memory/VeloxColumnarBatch.cc
    memory/VeloxMemoryManager.cc
    operators/functions/RegistrationAllFunctions.cc
    operators/functions/delta/DeltaBitmapAggregator.cc
    operators/functions/overlay/GetMapValue.cc
    operators/functions/overlay/RegisterFunctionOverlay.cc
    operators/functions/RowConstructorWithNull.cc
    operators/functions/SparkExprToSubfieldFilterParser.cc
    operators/plannodes/RowVectorStream.cc
    operators/hashjoin/HashTableBuilder.cc
    operators/hashjoin/HashTableSerializer.cc
    operators/reader/FileReaderIterator.cc
    operators/reader/ParquetReaderIterator.cc
    operators/serializer/VeloxColumnarBatchSerializer.cc
    operators/serializer/VeloxColumnarToRowConverter.cc
    operators/serializer/VeloxRowToColumnarConverter.cc
    operators/writer/VeloxColumnarBatchWriter.cc
    operators/writer/VeloxParquetDataSource.cc
    shuffle/ArrowShuffleDictionaryWriter.cc
    shuffle/ReaderThreadPool.cc
    shuffle/VeloxHashShuffleWriter.cc
    shuffle/VeloxRssSortShuffleWriter.cc
    shuffle/VeloxShuffleReader.cc
    shuffle/VeloxShuffleWriter.cc
    shuffle/VeloxSortShuffleWriter.cc
    substrait/SubstraitExtensionCollector.cc
    substrait/SubstraitParser.cc
    substrait/SubstraitToVeloxExpr.cc
    substrait/SubstraitToVeloxPlan.cc
    substrait/SubstraitToVeloxPlanValidator.cc
    substrait/VariantToVectorConverter.cc
    substrait/VeloxSubstraitSignature.cc
    substrait/VeloxToSubstraitExpr.cc
    substrait/VeloxToSubstraitPlan.cc
    substrait/VeloxToSubstraitType.cc
    udf/UdfLoader.cc
    utils/Common.cc
    utils/ConfigExtractor.cc
    utils/VeloxArrowUtils.cc
    utils/VeloxBatchResizer.cc
    utils/VeloxWholeStageDumper.cc
    utils/VeloxWriterUtils.cc)

if(ENABLE_S3)
  list(APPEND VELOX_SRCS filesystem/GlutenS3FileSystem.cc)
endif()
if(BUILD_TESTS OR BUILD_BENCHMARKS OR BUILD_TEST_UTILS)
  list(APPEND VELOX_SRCS tests/utils/LocalRssClient.cc
       tests/utils/TestAllocationListener.cc)
endif()
if(ENABLE_GPU)
  list(APPEND VELOX_SRCS
       cudf/CudfPlanValidator.cc cudf/GpuLock.cc
       operators/plannodes/CudfVectorStream.cc
       shuffle/VeloxGpuAsyncShuffleReader.cc shuffle/VeloxGpuShuffleReader.cc
       operators/serializer/VeloxGpuColumnarBatchSerializer.cc
       utils/GpuBufferBatchResizer.cc memory/GpuBufferColumnarBatch.cc)
endif()

add_library(gluten_velox_impl OBJECT ${VELOX_SRCS})
add_dependencies(gluten_velox_impl jni_proto velox_jni_proto)
target_include_directories(
  gluten_velox_impl PUBLIC "${CMAKE_CURRENT_SOURCE_DIR}" "${VELOX_HOME}"
  "${VELOX_BUILD_PATH}" "${PROTO_OUTPUT_DIR}" ${JNI_INCLUDE_DIRS})
target_link_libraries(
  gluten_velox_impl PUBLIC gluten_core_impl velox roaring xxHash::xxhash)
target_compile_definitions(gluten_velox_impl PUBLIC VELOX_ENABLE_PARQUET)
foreach(FEATURE S3 GCS HDFS ABFS)
  if(ENABLE_${FEATURE})
    target_compile_definitions(gluten_velox_impl PUBLIC ENABLE_${FEATURE})
  endif()
endforeach()
if(ENABLE_JEMALLOC_STATS)
  find_package(jemalloc REQUIRED)
  target_compile_definitions(gluten_velox_impl PUBLIC ENABLE_JEMALLOC_STATS)
  target_link_libraries(gluten_velox_impl PUBLIC jemalloc::jemalloc)
endif()

add_library(gluten_velox_backend SHARED jni/VeloxJniWrapper.cc)
target_link_libraries(gluten_velox_backend PUBLIC gluten_velox_impl gluten)
set_target_properties(
  gluten_velox_backend PROPERTIES OUTPUT_NAME velox
  LIBRARY_OUTPUT_DIRECTORY "${root_directory}/releases")
if(ENABLE_GLUTEN_VCPKG AND NOT CMAKE_SYSTEM_NAME MATCHES "Darwin")
  target_link_options(gluten_velox_backend PRIVATE
    "-Wl,--version-script=${CMAKE_CURRENT_SOURCE_DIR}/symbols.map"
    "LINKER:--no-undefined")
endif()
if(CMAKE_SYSTEM_NAME MATCHES "Darwin")
  target_link_options(gluten_velox_backend PRIVATE
    "LINKER:-force_load,$<TARGET_FILE:velox>")
endif()

if(BUILD_TESTS OR BUILD_BENCHMARKS)
  set(GLUTEN_VELOX_TEST_LIBRARIES velox_exec_test_lib)
endif()
if(ENABLE_GPU)
  target_compile_definitions(gluten_velox_impl PUBLIC
    LIBCUDACXX_ENABLE_EXPERIMENTAL_MEMORY_RESOURCE)
  target_link_libraries(gluten_velox_impl PUBLIC
    velox_cudf_exec velox_cudf_vector velox_cudf_iceberg_connector
    velox_cudf_hive_connector velox_cudf_expression
    velox_cudf_expression_registry cudf::cudf)
  add_library(velox_curl ALIAS CURL::libcurl)
endif()
if(BUILD_TESTS)
  add_subdirectory(compute/delta/tests)
  add_subdirectory(tests)
endif()
if(BUILD_BENCHMARKS)
  add_subdirectory(benchmarks)
endif()
if(BUILD_EXAMPLES)
  add_subdirectory(udf/examples)
endif()
