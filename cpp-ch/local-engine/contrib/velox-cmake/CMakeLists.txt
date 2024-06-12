set(VELOX_ROOT_DIR "/data0/hongbin/code/gluten-velox/velox-chenchang" CACHE PATH "Velox root directory")

# Sets new behavior for CMP0135, which controls how timestamps are extracted
# when using ExternalProject_Add():
# https://cmake.org/cmake/help/latest/policy/CMP0135.html
if(POLICY CMP0135)
    cmake_policy(SET CMP0135 NEW)
    set(CMAKE_POLICY_DEFAULT_CMP0135 NEW)
endif()

set(WRAP_VELOX_CMAKE "${CMAKE_CURRENT_SOURCE_DIR}/cmake")

list(PREPEND CMAKE_MODULE_PATH "${VELOX_ROOT_DIR}/CMake")
include(ResolveDependency)

set(gflags_SOURCE BUNDLED)
set(glog_SOURCE BUNDLED)
set(folly_SOURCE BUNDLED)
set(WITH_GTEST OFF)
set(WITH_PKGCONFIG OFF)
set(BUILD_TESTING OFF)
resolve_dependency(gflags COMPONENTS shared)
if(NOT TARGET gflags::gflags)
    # This is a bit convoluted, but we want to be able to use gflags::gflags as a
    # target even when velox is built as a subproject which uses
    # `find_package(gflags)` which does not create a globally imported target that
    # we can ALIAS.
    add_library(gflags_gflags INTERFACE)
    target_link_libraries(gflags_gflags INTERFACE gflags)
    add_library(gflags::gflags ALIAS gflags_gflags)
endif()
resolve_dependency(glog)

# fix
target_include_directories (glogbase BEFORE PUBLIC
        $<TARGET_PROPERTY:glog,INCLUDE_DIRECTORIES>)

target_compile_options(gflags_nothreads_static PRIVATE
        -Wno-zero-as-null-pointer-constant
        -Wno-format-nonliteral
        -Wno-extra-semi-stmt
        -Wno-implicit-fallthrough
        -Wno-shadow
        -Wno-covered-switch-default
        -Wno-unreachable-code
        -Wno-unused-member-function
)
target_compile_options(gflags_static PRIVATE
        -Wno-zero-as-null-pointer-constant
        -Wno-format-nonliteral
        -Wno-extra-semi-stmt
        -Wno-implicit-fallthrough
        -Wno-shadow
        -Wno-covered-switch-default
        -Wno-unreachable-code
        -Wno-unused-member-function
)
target_compile_options(glogbase PRIVATE
        -Wno-format-nonliteral
        -Wno-zero-as-null-pointer-constant
        -Wno-reserved-macro-identifier
        -Wno-header-hygiene
        -Wno-reserved-identifier
        -Wno-reserved-macro-identifier
        -Wno-suggest-override
        -Wno-suggest-destructor-override
        -Wno-deprecated-dynamic-exception-spec
        -Wno-unused-macros
        -Wno-atomic-implicit-seq-cst
        -Wno-unreachable-code-return
        -Wno-unused-but-set-variable
        -Wno-shorten-64-to-32
        -Wno-unused-member-function
        -Wno-extra-semi-stmt
        -Wno-cast-function-type-strict
        -Wno-missing-noreturn
        -Wno-old-style-cast
)


add_library(_glog_remove_warning INTERFACE)  # only for remove waring
target_compile_options(_glog_remove_warning INTERFACE
        -Wno-reserved-macro-identifier
        -Wno-reserved-identifier
        -Wno-deprecated-dynamic-exception-spec
        -Wno-suggest-override                   # glog
        -Wno-zero-as-null-pointer-constant      # glog
        -Wno-suggest-destructor-override        # glog
        -Wno-sign-compare                       # glog
)
# target_link_libraries(_glog_remove_warning INTERFACE glog::glog)
add_library(ch_contrib::glog_compile_option ALIAS _glog_remove_warning)

add_compile_options(-Wno-unused-exception-parameter) 

# folly
list(PREPEND CMAKE_MODULE_PATH ${WRAP_VELOX_CMAKE})
set(FOLLY_BINARY_DIR "${GLUTEN_CONTRIB_BINARY_DIR}/folly")

## TODO: why check failed?
set(FOLLY_HAVE_OPENSSL_ASN1_TIME_DIFF ON)
set(FOLLY_CPP_ATOMIC_BUILTIN ON)
set(FOLLY_HAVE_WEAK_SYMBOLS 1)
add_subdirectory("${VELOX_ROOT_DIR}/CMake/resolve_dependency_modules/folly" "${FOLLY_BINARY_DIR}")

add_library(_folly_workaround INTERFACE)
target_include_directories(_folly_workaround INTERFACE "${GLUTEN_CONTRIB_DIR}/folly/workaround")
target_link_libraries( folly_deps INTERFACE _folly_workaround)

add_library(_folly_compile_options INTERFACE)
target_compile_options(_folly_compile_options INTERFACE
        -Wno-unknown-warning-option             # folly/Utility.h           => FOLLY_GNU_DISABLE_WARNING("-Wmaybe-uninitialized")
        -Wno-comma                              # folly/Traits.h
        -Wno-deprecated                         # folly/lang/StaticConst.h
        -Wno-old-style-cast                     # folly/lang/SafeAssert.h
        -Wno-undef                              # folly                     => __cpp_lib_atomic_ref
        -Wno-documentation                      # folly
        -Wno-cast-qual                          # folly/FBVector.h
        -Wno-thread-safety-analysis             # folly
        -Wno-covered-switch-default             # folly
        -Wno-extra-semi-stmt                    # folly
)
target_link_libraries(_folly_compile_options INTERFACE Folly::folly ch_contrib::glog_compile_option)
add_library(ch_contrib::folly ALIAS _folly_compile_options)

#TODO: Why and How to transfer to the other target?
include_directories(SYSTEM "${VELOX_ROOT_DIR}/velox")
include_directories(${VELOX_ROOT_DIR})

include_directories(${VELOX_ROOT_DIR}/velox/external/xxhash)

# velox warning
add_library(_velox_remove_warning INTERFACE)  # only for remove waring
target_compile_options(_velox_remove_warning INTERFACE
        -Wno-gnu-zero-variadic-macro-arguments  # velox                     => __VA_ARGS__
        -Wno-shadow-uncaptured-local            # velox
        -Wno-shorten-64-to-32                   # velox
        -Wno-gnu-statement-expression-from-macro-expansion # velox
        -Wno-sign-compare                       # velox
        -Wno-shadow-field-in-constructor        # velox
        -Wno-gnu-statement-expression           # velox
        -Wno-undefined-reinterpret-cast         # velox
        -Wno-inconsistent-missing-destructor-override   # velox
        -Wno-shadow-field-in-constructor        # velox
        -Wno-unused-parameter                   # velox
        -Wno-missing-noreturn                   # velox
        -Wno-shadow                             # velox
        -Wno-nullability-completeness           # velox
        -Wno-extra-semi                         # velox
        -Wno-float-conversion                   # velox
        -Wno-nullable-to-nonnull-conversion     # velox
        -Wno-shadow-field                       # velox
        -Wno-implicit-float-conversion          # velox
        -Wno-unused-member-function             # velox
        # dwio_common
        -Wno-unreachable-code-return            # velox
        -Wno-unused-variable                    # velox
        -Wno-format-nonliteral                  # velox
        -Wno-ignored-qualifiers                 # velox
        -Wno-mismatched-tags                    # velox
        # native_parquet_reader
        -Wno-unused-but-set-variable            # velox
        -Wno-unused-private-field               # velox
        -Wno-macro-redefined                    # conflict with glog
)
target_link_libraries(_velox_remove_warning INTERFACE
        ch_contrib::folly
        ch_contrib::xsimd
)
add_library(ch_contrib::velox_compile_option ALIAS _velox_remove_warning)

################################
#    velox targets             #
################################

# gluten_velox_test_util
set(VELOX_TEST_UTIL_DIR "${VELOX_ROOT_DIR}/velox/common/testutil")
add_library(gluten_velox_test_util ${VELOX_TEST_UTIL_DIR}/TestValue.cpp)
target_link_libraries(gluten_velox_test_util ch_contrib::velox_compile_option)
# gluten_velox_flag_definitions
set(VELOX_FLAG_DIR "${VELOX_ROOT_DIR}/velox/flag_definitions")
add_library(gluten_velox_flag_definitions OBJECT ${VELOX_FLAG_DIR}/flags.cpp)
target_link_libraries(gluten_velox_flag_definitions gflags::gflags)

# gluten_velox_common_compression
set(VELOX_COMMON_COMPRESSION_DIR "${VELOX_ROOT_DIR}/velox/common/compression")
add_library(gluten_velox_common_compression
        ${VELOX_COMMON_COMPRESSION_DIR}/Compression.cpp
        ${VELOX_COMMON_COMPRESSION_DIR}/LzoDecompressor.cpp
)
target_link_libraries(gluten_velox_common_compression ch_contrib::velox_compile_option)
# gluten_velox_process
set(VELOX_PROCESS_DIR "${VELOX_ROOT_DIR}/velox/common/process")
add_library(gluten_velox_process
        ${VELOX_PROCESS_DIR}/ProcessBase.cpp
        ${VELOX_PROCESS_DIR}/StackTrace.cpp
        ${VELOX_PROCESS_DIR}/ThreadDebugInfo.cpp
        ${VELOX_PROCESS_DIR}/TraceContext.cpp
)
target_link_libraries(gluten_velox_process ch_contrib::folly)
# gluten_velox_exception
set(VELOX_BASE_DIR "${VELOX_ROOT_DIR}/velox/common/base")
add_library(gluten_velox_exception
        ${VELOX_BASE_DIR}/Exceptions.cpp
        ${VELOX_BASE_DIR}/VeloxException.cpp
        ${VELOX_BASE_DIR}/Exceptions.h
)
target_compile_options(gluten_velox_exception PRIVATE
        -Wno-unused-exception-parameter
)
target_link_libraries(gluten_velox_exception ch_contrib::folly)

# gluten_velox_serialization
set(VELOX_SERIALIZATION_DIR "${VELOX_ROOT_DIR}/velox/common/serialization")
add_library(gluten_velox_serialization ${VELOX_SERIALIZATION_DIR}/DeserializationRegistry.cpp)
target_link_libraries(gluten_velox_serialization ch_contrib::velox_compile_option)
# gluten_velox_common_base
add_library(gluten_velox_common_base
        ${VELOX_BASE_DIR}/BitUtil.cpp
        ${VELOX_BASE_DIR}/Counters.cpp
        ${VELOX_BASE_DIR}/Fs.cpp
        ${VELOX_BASE_DIR}/RandomUtil.cpp
        ${VELOX_BASE_DIR}/RawVector.cpp
        ${VELOX_BASE_DIR}/RuntimeMetrics.cpp
        ${VELOX_BASE_DIR}/SimdUtil.cpp
        ${VELOX_BASE_DIR}/StatsReporter.cpp
        ${VELOX_BASE_DIR}/SuccinctPrinter.cpp
)
target_compile_options(gluten_velox_common_base PRIVATE
        -Wno-unused-function
)
target_link_libraries(gluten_velox_common_base ch_contrib::velox_compile_option)
# gluten_velox_file
set(VELOX_FILE_DIR "${VELOX_ROOT_DIR}/velox/common/file")
add_library(gluten_velox_file
        ${VELOX_FILE_DIR}/File.cpp
        ${VELOX_FILE_DIR}/FileSystems.cpp
        ${VELOX_FILE_DIR}/Utils.cpp
)
target_link_libraries(gluten_velox_file ch_contrib::velox_compile_option)
# gluten_velox_time +1
set(VELOX_TIME_DIR "${VELOX_ROOT_DIR}/velox/common/time")
add_library(gluten_velox_time
        ${VELOX_TIME_DIR}/Timer.cpp
        ${VELOX_TIME_DIR}/CpuWallTimer.cpp
)
target_link_libraries(gluten_velox_time ch_contrib::folly)

# gluten_velox_encode -1
set(VELOX_ENCODER_DIR "${VELOX_ROOT_DIR}/velox/common/encode")
add_library(gluten_velox_encode ${VELOX_ENCODER_DIR}/Base64.cpp)
target_link_libraries(gluten_velox_encode ch_contrib::folly)
################################################################

# gluten_velox_memory
set(VELOX_MEMORY_DIR "${VELOX_ROOT_DIR}/velox/common/memory")
add_library(
        gluten_velox_memory
        ${VELOX_MEMORY_DIR}/Allocation.cpp
        ${VELOX_MEMORY_DIR}/AllocationPool.cpp
        ${VELOX_MEMORY_DIR}/ByteStream.cpp
        ${VELOX_MEMORY_DIR}/HashStringAllocator.cpp
        ${VELOX_MEMORY_DIR}/MallocAllocator.cpp
        ${VELOX_MEMORY_DIR}/Memory.cpp
        ${VELOX_MEMORY_DIR}/MemoryAllocator.cpp
        ${VELOX_MEMORY_DIR}/MemoryArbitrator.cpp
        ${VELOX_MEMORY_DIR}/MemoryPool.cpp
        ${VELOX_MEMORY_DIR}/MmapAllocator.cpp
        ${VELOX_MEMORY_DIR}/MmapArena.cpp
        ${VELOX_MEMORY_DIR}/SharedArbitrator.cpp
        ${VELOX_MEMORY_DIR}/StreamArena.cpp
)
target_compile_options(gluten_velox_memory PRIVATE
        -Wno-unused-exception-parameter
        -Wno-conditional-uninitialized
        -Wno-return-type
)
target_link_libraries(gluten_velox_memory
        ch_contrib::velox_compile_option
        ch_contrib::re2
)

#date

set(VELOX_EXTERNAL_DIR "${VELOX_ROOT_DIR}/velox/external")
include_directories(SYSTEM ${VELOX_EXTERNAL_DIR})

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -DUSE_OS_TZDB")

add_library(gluten_velox_external_date ${VELOX_EXTERNAL_DIR}/date/tz.cpp)

#md5

add_library(gluten_md5 ${VELOX_EXTERNAL_DIR}/md5/md5.cpp)

add_compile_definitions(FOLLY_HAVE_INT128_T=1)
target_link_libraries(
        gluten_md5
        ch_contrib::folly)

 target_compile_options(gluten_md5 PRIVATE
        -Wno-cast-align
        -Wno-shorten-64-to-32
)


# gluten_velox_type
set(DWIO_TYPE_DIR "${VELOX_ROOT_DIR}/velox/type")
add_library(
        gluten_velox_type_tz
        ${DWIO_TYPE_DIR}/tz/TimeZoneMap.h 
        ${DWIO_TYPE_DIR}/tz/TimeZoneDatabase.cpp 
        ${DWIO_TYPE_DIR}/tz/TimeZoneMap.cpp)
target_link_libraries(gluten_velox_type_tz 
        #Boost::regex
        ch_contrib::fmt ch_contrib::folly)

add_library(
        gluten_velox_type
        ${DWIO_TYPE_DIR}/Conversions.cpp
        ${DWIO_TYPE_DIR}/DecimalUtil.cpp
        ${DWIO_TYPE_DIR}/DoubleUtil.cpp
        ${DWIO_TYPE_DIR}/Filter.cpp
        ${DWIO_TYPE_DIR}/HugeInt.cpp
        ${DWIO_TYPE_DIR}/StringView.h
        ${DWIO_TYPE_DIR}/Subfield.cpp
        ${DWIO_TYPE_DIR}/Timestamp.cpp
        ${DWIO_TYPE_DIR}/TimestampConversion.cpp
        ${DWIO_TYPE_DIR}/Tokenizer.cpp
        ${DWIO_TYPE_DIR}/Type.cpp
        ${DWIO_TYPE_DIR}/Variant.cpp
)
target_link_libraries(gluten_velox_type PRIVATE
        gluten_velox_type_tz
        gluten_velox_external_date
        gluten_velox_encode
        boost::headers_only
        ch_contrib::velox_compile_option
        ch_contrib::re2
)

# gluten_velox_vector
set(VELOX_VECTOR_DIR "${VELOX_ROOT_DIR}/velox/vector")
add_library(gluten_velox_vector
        ${VELOX_VECTOR_DIR}/BaseVector.cpp
        ${VELOX_VECTOR_DIR}/ComplexVector.cpp
        ${VELOX_VECTOR_DIR}/ConstantVector.cpp
        ${VELOX_VECTOR_DIR}/DecodedVector.cpp
        ${VELOX_VECTOR_DIR}/FlatVector.cpp
        ${VELOX_VECTOR_DIR}/LazyVector.cpp
        ${VELOX_VECTOR_DIR}/SelectivityVector.cpp
        ${VELOX_VECTOR_DIR}/SequenceVector.cpp
        ${VELOX_VECTOR_DIR}/VectorSaver.cpp
        ${VELOX_VECTOR_DIR}/VectorEncoding.cpp
        ${VELOX_VECTOR_DIR}/VectorPool.cpp
        ${VELOX_VECTOR_DIR}/VectorPrinter.cpp
        ${VELOX_VECTOR_DIR}/VectorStream.cpp
        ${VELOX_VECTOR_DIR}/VariantToVector.cpp
)
target_compile_options(gluten_velox_vector PRIVATE
        -Wno-conditional-uninitialized
)
target_link_libraries(gluten_velox_vector
        boost::headers_only
        ch_contrib::velox_compile_option
)



# gluten_velox_type_calculation
set(VELOX_TYPE_CALC_DIR "${VELOX_ROOT_DIR}/velox/expression/type_calculation")
find_package(BISON 3.0.4 REQUIRED)
find_package(FLEX 2.5.13 REQUIRED)

bison_target(
  TypeCalculationParser ${VELOX_TYPE_CALC_DIR}/TypeCalculation.yy
  ${CMAKE_CURRENT_BINARY_DIR}/TypeCalculation.yy.cc
  DEFINES_FILE ${CMAKE_CURRENT_BINARY_DIR}/velox/expression/type_calculation/TypeCalculation.yy.h)

flex_target(
  TypeCalculationScanner ${VELOX_TYPE_CALC_DIR}/TypeCalculation.ll
  ${CMAKE_CURRENT_BINARY_DIR}/Scanner.cpp COMPILE_FLAGS "-Cf --prefix=veloxtc")

message(STATUS "CMAKE_CURRENT_BINARY_DIR: ${CMAKE_CURRENT_BINARY_DIR}, PROJECT_BINARY_DIR: ${PROJECT_BINARY_DIR}") 
message(STATUS "FLEX_INCLUDE_DIRS: ${FLEX_INCLUDE_DIRS}")
message(STATUS "BISON_TypeCalculationParser_OUTPUTS: ${BISON_TypeCalculationParser_OUTPUTS}")
message(STATUS "FLEX_TypeCalculationScanner_OUTPUTS: ${FLEX_TypeCalculationScanner_OUTPUTS}")

add_flex_bison_dependency(TypeCalculationScanner TypeCalculationParser)

include_directories(${PROJECT_BINARY_DIR})
include_directories(${CMAKE_CURRENT_BINARY_DIR}/velox/expression/type_calculation)
include_directories(${FLEX_INCLUDE_DIRS})
include_directories("/tmp/bison")


add_library(
  gluten_velox_type_calculation
  ${BISON_TypeCalculationParser_OUTPUTS} ${FLEX_TypeCalculationScanner_OUTPUTS}
  ${VELOX_TYPE_CALC_DIR}/Scanner.h ${VELOX_TYPE_CALC_DIR}/TypeCalculation.h)

target_link_libraries(gluten_velox_type_calculation gluten_velox_common_base)
target_compile_options(gluten_velox_type_calculation PRIVATE
  -Wno-unused-macros
  -Wno-implicit-fallthrough
)

# get_target_property(_include_dirs gluten_velox_type_calculation INCLUDE_DIRECTORIES)
# message("Included files for YourLibrary:")
# foreach(_dir ${_include_dirs})
#     file(GLOB_RECURSE _files ${_dir}/*.h)
#     foreach(_file ${_files})
#         message("including:   ${_file}")
#     endforeach()
# endforeach()

# gluten_velox_expression_functions

set(VELOX_EXPR_DIR "${VELOX_ROOT_DIR}/velox/expression")
add_library(
  gluten_velox_expression_functions 
  ${VELOX_EXPR_DIR}/FunctionSignature.cpp 
  ${VELOX_EXPR_DIR}/SignatureBinder.cpp
  ${VELOX_EXPR_DIR}/ReverseSignatureBinder.cpp)

target_link_libraries(gluten_velox_expression_functions gluten_velox_common_base
                      gluten_velox_type_calculation)


# # caching
# set(VELOX_CACHING_DIR "${VELOX_ROOT_DIR}/velox/common/caching")
# add_library(
#   gluten_velox_caching
#   ${VELOX_CACHING_DIR}/FileIds.cpp
#   ${VELOX_CACHING_DIR}/StringIdMap.cpp
#   ${VELOX_CACHING_DIR}/AsyncDataCache.cpp
#   ${VELOX_CACHING_DIR}/ScanTracker.cpp
#   ${VELOX_CACHING_DIR}/SsdCache.cpp
#   ${VELOX_CACHING_DIR}/SsdFile.cpp
#   ${VELOX_CACHING_DIR}/SsdFileTracker.cpp)
# target_link_libraries(
#         gluten_velox_caching
#   PUBLIC gluten_velox_common_base
#          gluten_velox_exception
#          gluten_velox_file
#          gluten_velox_memory
#          ch_contrib::folly
#          ch_contrib::fmt
#          gflags::gflags
#   PRIVATE velox_time)


# gluten_velox_core

set(VELOX_CORE_DIR "${VELOX_ROOT_DIR}/velox/core")
# add_library(gluten_velox_core Expressions.cpp PlanFragment.cpp PlanNode.cpp
#                        QueryConfig.cpp QueryCtx.cpp SimpleFunctionMetadata.cpp)
add_library(gluten_velox_core ${VELOX_CORE_DIR}/Expressions.cpp
        ${VELOX_CORE_DIR}/QueryConfig.cpp
        ${VELOX_CORE_DIR}/QueryCtx.cpp
        ${VELOX_CORE_DIR}/Config.cpp
        ${VELOX_CORE_DIR}/SimpleFunctionMetadata.cpp
)

# target_link_libraries(
#   velox_core
#   velox_caching
#   velox_config
#   velox_expression_functions
#   velox_type
#   velox_connector
#   velox_serialization
#   velox_vector
#   Folly::folly)

target_link_libraries(
  gluten_velox_core
  gluten_velox_expression_functions
  gluten_velox_type
  gluten_velox_vector
  gluten_velox_exception
  ch_contrib::folly)


# function util
set(VELOX_FUNCTIONS_LIB_DIR "${VELOX_ROOT_DIR}/velox/functions/lib")


add_library(gluten_velox_is_null_functions ${VELOX_FUNCTIONS_LIB_DIR}/IsNull.cpp)

target_link_libraries(gluten_velox_is_null_functions gluten_velox_expression)

add_library(
        gluten_velox_functions_util 
        ${VELOX_FUNCTIONS_LIB_DIR}/LambdaFunctionUtil.cpp
        ${VELOX_FUNCTIONS_LIB_DIR}/RowsTranslationUtil.cpp)

target_link_libraries(gluten_velox_functions_util gluten_velox_vector gluten_velox_common_base)

add_library(
  gluten_velox_functions_lib
  ${VELOX_FUNCTIONS_LIB_DIR}/CheckDuplicateKeys.cpp
  ${VELOX_FUNCTIONS_LIB_DIR}/DateTimeFormatter.cpp
  ${VELOX_FUNCTIONS_LIB_DIR}/DateTimeFormatterBuilder.cpp
  ${VELOX_FUNCTIONS_LIB_DIR}/KllSketch.cpp
  ${VELOX_FUNCTIONS_LIB_DIR}/MapConcat.cpp
  ${VELOX_FUNCTIONS_LIB_DIR}/Re2Functions.cpp
  ${VELOX_FUNCTIONS_LIB_DIR}/StringEncodingUtils.cpp)

target_link_libraries(gluten_velox_functions_lib gluten_velox_functions_util gluten_velox_vector
        ch_contrib::re2 ch_contrib::folly)

target_compile_options(gluten_velox_functions_lib PRIVATE
        -Wno-unused-function
)


# gluten_velox_expression

add_library(
  gluten_velox_expression
  ${VELOX_EXPR_DIR}/BooleanMix.cpp
  ${VELOX_EXPR_DIR}/CastExpr.cpp
  ${VELOX_EXPR_DIR}/CoalesceExpr.cpp
  ${VELOX_EXPR_DIR}/ConjunctExpr.cpp
  ${VELOX_EXPR_DIR}/ConstantExpr.cpp
  ${VELOX_EXPR_DIR}/EvalCtx.cpp
  ${VELOX_EXPR_DIR}/Expr.cpp
  ${VELOX_EXPR_DIR}/ExprCompiler.cpp
  ${VELOX_EXPR_DIR}/ExprToSubfieldFilter.cpp
  ${VELOX_EXPR_DIR}/FieldReference.cpp
  ${VELOX_EXPR_DIR}/FunctionCallToSpecialForm.cpp
  ${VELOX_EXPR_DIR}/LambdaExpr.cpp
  ${VELOX_EXPR_DIR}/VectorFunction.cpp
  ${VELOX_EXPR_DIR}/RegisterSpecialForm.cpp
  ${VELOX_EXPR_DIR}/RowConstructor.cpp
  ${VELOX_EXPR_DIR}/SimpleFunctionRegistry.cpp
  ${VELOX_EXPR_DIR}/SpecialFormRegistry.cpp
  ${VELOX_EXPR_DIR}/SwitchExpr.cpp
  ${VELOX_EXPR_DIR}/TryExpr.cpp
  ${VELOX_EXPR_DIR}/GenericWriter.cpp
  ${VELOX_EXPR_DIR}/PeeledEncoding.cpp)

target_link_libraries(
  gluten_velox_expression
  gluten_velox_core 
  gluten_velox_vector 
  gluten_velox_common_base
  gluten_velox_expression_functions
  gluten_velox_functions_util
  )

target_compile_options(gluten_velox_expression PRIVATE
  -Wno-unused-exception-parameter
  -Wno-atomic-implicit-seq-cst
  -Wno-unused-function
)


# buffer
add_library(gluten_velox_buffer ${VELOX_ROOT_DIR}/velox/buffer/StringViewBufferHolder.cpp)

target_link_libraries(gluten_velox_buffer gluten_velox_memory gluten_velox_common_base ch_contrib::folly)


# function registry

##
set(VELOX_FUNCTIONS_DIR "${VELOX_ROOT_DIR}/velox/functions")

add_library(gluten_velox_function_registry ${VELOX_FUNCTIONS_DIR}/FunctionRegistry.cpp)

target_link_libraries(gluten_velox_function_registry gluten_velox_expression gluten_velox_type
                      gluten_velox_core gluten_velox_exception)

target_compile_options(gluten_velox_function_registry PRIVATE
        -Wno-unused-exception-parameter
)

set(VELOX_PRESTO_FUNCTIONS_DIR "${VELOX_ROOT_DIR}/velox/functions/prestosql")

## presto types
add_library(gluten_velox_presto_types 
        ${VELOX_PRESTO_FUNCTIONS_DIR}/types/HyperLogLogType.cpp
        ${VELOX_PRESTO_FUNCTIONS_DIR}/types/JsonType.cpp
        ${VELOX_PRESTO_FUNCTIONS_DIR}/types/TimestampWithTimeZoneType.cpp)

target_link_libraries(gluten_velox_presto_types gluten_velox_memory gluten_velox_expression
                      gluten_velox_functions_util)


## presto function impl

add_library(
  gluten_velox_functions_prestosql_impl
  ${VELOX_PRESTO_FUNCTIONS_DIR}/ArrayConstructor.cpp
  ${VELOX_PRESTO_FUNCTIONS_DIR}/ArrayContains.cpp
  ${VELOX_PRESTO_FUNCTIONS_DIR}/ArrayDistinct.cpp
  ${VELOX_PRESTO_FUNCTIONS_DIR}/ArrayDuplicates.cpp
  ${VELOX_PRESTO_FUNCTIONS_DIR}/ArrayIntersectExcept.cpp
  ${VELOX_PRESTO_FUNCTIONS_DIR}/ArrayMatch.cpp
  ${VELOX_PRESTO_FUNCTIONS_DIR}/ArrayPosition.cpp
  ${VELOX_PRESTO_FUNCTIONS_DIR}/ArrayShuffle.cpp
  ${VELOX_PRESTO_FUNCTIONS_DIR}/ArraySort.cpp
  ${VELOX_PRESTO_FUNCTIONS_DIR}/ArraySum.cpp
  ${VELOX_PRESTO_FUNCTIONS_DIR}/Comparisons.cpp
  ${VELOX_PRESTO_FUNCTIONS_DIR}/DecimalVectorFunctions.cpp
  ${VELOX_PRESTO_FUNCTIONS_DIR}/ElementAt.cpp
  ${VELOX_PRESTO_FUNCTIONS_DIR}/FilterFunctions.cpp
  ${VELOX_PRESTO_FUNCTIONS_DIR}/FromUnixTime.cpp
  ${VELOX_PRESTO_FUNCTIONS_DIR}/FromUtf8.cpp
  ${VELOX_PRESTO_FUNCTIONS_DIR}/GreatestLeast.cpp
  ${VELOX_PRESTO_FUNCTIONS_DIR}/InPredicate.cpp
  ${VELOX_PRESTO_FUNCTIONS_DIR}/JsonFunctions.cpp
  ${VELOX_PRESTO_FUNCTIONS_DIR}/Map.cpp
  ${VELOX_PRESTO_FUNCTIONS_DIR}/MapEntries.cpp
  ${VELOX_PRESTO_FUNCTIONS_DIR}/MapFromEntries.cpp
  ${VELOX_PRESTO_FUNCTIONS_DIR}/MapKeysAndValues.cpp
  ${VELOX_PRESTO_FUNCTIONS_DIR}/MapZipWith.cpp
  ${VELOX_PRESTO_FUNCTIONS_DIR}/Not.cpp
  ${VELOX_PRESTO_FUNCTIONS_DIR}/Reduce.cpp
  ${VELOX_PRESTO_FUNCTIONS_DIR}/Repeat.cpp
  ${VELOX_PRESTO_FUNCTIONS_DIR}/Reverse.cpp
  ${VELOX_PRESTO_FUNCTIONS_DIR}/RowFunction.cpp
  ${VELOX_PRESTO_FUNCTIONS_DIR}/Sequence.cpp
  ${VELOX_PRESTO_FUNCTIONS_DIR}/SimpleComparisonMatcher.cpp
  ${VELOX_PRESTO_FUNCTIONS_DIR}/Slice.cpp
  ${VELOX_PRESTO_FUNCTIONS_DIR}/Split.cpp
  ${VELOX_PRESTO_FUNCTIONS_DIR}/StringFunctions.cpp
  ${VELOX_PRESTO_FUNCTIONS_DIR}/Subscript.cpp
  ${VELOX_PRESTO_FUNCTIONS_DIR}/ToUtf8.cpp
  ${VELOX_PRESTO_FUNCTIONS_DIR}/Transform.cpp
  ${VELOX_PRESTO_FUNCTIONS_DIR}/TransformKeys.cpp
  ${VELOX_PRESTO_FUNCTIONS_DIR}/TransformValues.cpp
  ${VELOX_PRESTO_FUNCTIONS_DIR}/URLFunctions.cpp
  ${VELOX_PRESTO_FUNCTIONS_DIR}/Utf8Utils.cpp
  ${VELOX_PRESTO_FUNCTIONS_DIR}/VectorArithmetic.cpp
  ${VELOX_PRESTO_FUNCTIONS_DIR}/WidthBucketArray.cpp
  ${VELOX_PRESTO_FUNCTIONS_DIR}/Zip.cpp
  ${VELOX_PRESTO_FUNCTIONS_DIR}/ZipWith.cpp)

target_link_libraries(
  gluten_velox_functions_prestosql_impl
#   velox_common_hyperloglog
#   velox_functions_json
  gluten_velox_functions_lib
  gluten_velox_expression
  gluten_md5
  gluten_velox_type_tz
  gluten_velox_presto_types
  gluten_velox_functions_util
  ch_contrib::folly)

set_property(TARGET gluten_velox_functions_prestosql_impl PROPERTY JOB_POOL_COMPILE
                                                            high_memory_pool)
target_compile_options(gluten_velox_functions_prestosql_impl PRIVATE
        -Wno-unused-exception-parameter
)





## presto functions registration

add_library(
  gluten_velox_functions_prestosql
  ${VELOX_PRESTO_FUNCTIONS_DIR}/registration/ArithmeticFunctionsRegistration.cpp
  ${VELOX_PRESTO_FUNCTIONS_DIR}/registration/ArrayFunctionsRegistration.cpp
  ${VELOX_PRESTO_FUNCTIONS_DIR}/registration/BinaryFunctionsRegistration.cpp
  ${VELOX_PRESTO_FUNCTIONS_DIR}/registration/CheckedArithmeticRegistration.cpp
  ${VELOX_PRESTO_FUNCTIONS_DIR}/registration/ComparisonFunctionsRegistration.cpp
#   ${VELOX_PRESTO_FUNCTIONS_DIR}/registration/DateTimeFunctionsRegistration.cpp
  ${VELOX_PRESTO_FUNCTIONS_DIR}/registration/GeneralFunctionsRegistration.cpp
#   ${VELOX_PRESTO_FUNCTIONS_DIR}/registration/HyperLogFunctionsRegistration.cpp
#   ${VELOX_PRESTO_FUNCTIONS_DIR}/registration/JsonFunctionsRegistration.cpp
  ${VELOX_PRESTO_FUNCTIONS_DIR}/registration/MapFunctionsRegistration.cpp
#   ${VELOX_PRESTO_FUNCTIONS_DIR}/registration/StringFunctionsRegistration.cpp
  ${VELOX_PRESTO_FUNCTIONS_DIR}/registration/BitwiseFunctionsRegistration.cpp
  ${VELOX_PRESTO_FUNCTIONS_DIR}/registration/URLFunctionsRegistration.cpp
  ${VELOX_PRESTO_FUNCTIONS_DIR}/registration/RegistrationFunctions.cpp)

target_link_libraries(gluten_velox_functions_prestosql gluten_velox_functions_prestosql_impl
                      gluten_velox_is_null_functions 
                      #simdjson::simdjson
                      )

set_property(TARGET gluten_velox_functions_prestosql PROPERTY JOB_POOL_COMPILE
                                                       high_memory_pool)

target_compile_options(gluten_velox_functions_prestosql PRIVATE
        -Wno-used-but-marked-unused
)
                      
# duckdb
set_property(GLOBAL PROPERTY JOB_POOLS high_memory_pool=1000)

add_subdirectory(${VELOX_ROOT_DIR}/velox/external/duckdb ${CMAKE_CURRENT_BINARY_DIR}/duckdb)

set(VELOX_DUCKDB_DIR "${VELOX_ROOT_DIR}/velox/duckdb/conversion")
add_library(gluten_velox_duckdb_conversion ${VELOX_DUCKDB_DIR}/DuckConversion.cpp
        ${VELOX_DUCKDB_DIR}/DuckWrapper.cpp)

target_link_libraries(gluten_velox_duckdb_conversion gluten_velox_core gluten_velox_vector duckdb
tpch_extension)

add_library(gluten_velox_duckdb_parser ${VELOX_DUCKDB_DIR}/DuckParser.cpp)

target_link_libraries(gluten_velox_duckdb_parser gluten_velox_duckdb_conversion duckdb
                      gluten_velox_parse_expression)

target_compile_options(gluten_velox_duckdb_parser PRIVATE
        -Wno-unused-exception-parameter
)

# parser

add_library(gluten_velox_parse_utils ${VELOX_ROOT_DIR}/velox/parse/TypeResolver.cpp)
target_link_libraries(gluten_velox_parse_utils gluten_velox_buffer gluten_velox_type gluten_velox_vector
gluten_velox_function_registry)
target_compile_options(gluten_velox_parse_utils PRIVATE
        -Wno-unused-exception-parameter
)


add_library(gluten_velox_parse_expression ${VELOX_ROOT_DIR}/velox/parse/Expressions.cpp)
target_link_libraries(gluten_velox_parse_expression gluten_velox_type gluten_velox_parse_utils
                      gluten_velox_expression 
                      #velox_exec
                      )
target_compile_options(gluten_velox_parse_expression PRIVATE
        -Wno-unused-exception-parameter
        -Wno-unused-function
)

add_library(gluten_velox_parse_parser ${VELOX_ROOT_DIR}/velox/parse/ExpressionsParser.cpp 
#QueryPlanner.cpp
)
target_link_libraries(gluten_velox_parse_parser gluten_velox_parse_expression gluten_velox_type
                      gluten_velox_duckdb_parser)

target_compile_options(gluten_velox_parse_parser PRIVATE
        -Wno-unused-exception-parameter
)




################################
# DWIO                         #
################################
# gluten_dwio_common_compression
set(DWIO_COMMON_COMPRESSION_DIR "${VELOX_ROOT_DIR}/velox/dwio/common/compression")
add_library(gluten_dwio_common_compression
        ${DWIO_COMMON_COMPRESSION_DIR}/Compression.cpp
        ${DWIO_COMMON_COMPRESSION_DIR}/PagedInputStream.cpp
        ${DWIO_COMMON_COMPRESSION_DIR}/PagedOutputStream.cpp
)
target_link_libraries(gluten_dwio_common_compression
        ch_contrib::velox_compile_option
        ch_contrib::protobuf
        ch_contrib::lz4
        ch_contrib::zstd
        ch_contrib::snappy
)
# gluten_dwio_common_exception
set(DWIO_COMMON_EXCEPTION_DIR "${VELOX_ROOT_DIR}/velox/dwio/common/exception")
add_library(gluten_dwio_common_exception
        ${DWIO_COMMON_EXCEPTION_DIR}/Exception.cpp
        ${DWIO_COMMON_EXCEPTION_DIR}/Exceptions.cpp
)
target_link_libraries(gluten_dwio_common_exception ch_contrib::velox_compile_option)
# gluten_dwio_common
set(DWIO_COMMON_DIR "${VELOX_ROOT_DIR}/velox/dwio/common")
add_library(
        gluten_dwio_common
        ${DWIO_COMMON_DIR}/BitConcatenation.cpp
        ${DWIO_COMMON_DIR}/BitPackDecoder.cpp
        ${DWIO_COMMON_DIR}/BufferedInput.cpp
        ${DWIO_COMMON_DIR}/CachedBufferedInput.cpp
        ${DWIO_COMMON_DIR}/CacheInputStream.cpp
        ${DWIO_COMMON_DIR}/ColumnSelector.cpp
        ${DWIO_COMMON_DIR}/DataBufferHolder.cpp
        ${DWIO_COMMON_DIR}/DecoderUtil.cpp
        ${DWIO_COMMON_DIR}/DirectDecoder.cpp
        ${DWIO_COMMON_DIR}/DwioMetricsLog.cpp
        ${DWIO_COMMON_DIR}/FileSink.cpp
        ${DWIO_COMMON_DIR}/FlatMapHelper.cpp
        ${DWIO_COMMON_DIR}/InputStream.cpp
        ${DWIO_COMMON_DIR}/IntDecoder.cpp
        ${DWIO_COMMON_DIR}/IoStatistics.cpp
        # ${DWIO_COMMON_DIR}/MetadataFilter.cpp                        # Remove velox expression dependency
        ${DWIO_COMMON_DIR}/Options.cpp
        ${DWIO_COMMON_DIR}/OutputStream.cpp
        ${DWIO_COMMON_DIR}/Range.cpp
        ${DWIO_COMMON_DIR}/Reader.cpp
        ${DWIO_COMMON_DIR}/ReaderFactory.cpp
        ${DWIO_COMMON_DIR}/ScanSpec.cpp
        ${DWIO_COMMON_DIR}/ColumnLoader.cpp
        ${DWIO_COMMON_DIR}/SelectiveByteRleColumnReader.cpp
        ${DWIO_COMMON_DIR}/SelectiveColumnReader.cpp
        ${DWIO_COMMON_DIR}/SelectiveRepeatedColumnReader.cpp
        ${DWIO_COMMON_DIR}/SelectiveStructColumnReader.cpp
        ${DWIO_COMMON_DIR}/SeekableInputStream.cpp
        ${DWIO_COMMON_DIR}/TypeUtils.cpp
        ${DWIO_COMMON_DIR}/TypeWithId.cpp
        ${DWIO_COMMON_DIR}/WriterFactory.cpp
        ${DWIO_COMMON_DIR}/SortingWriter.cpp
        ${DWIO_COMMON_DIR}/SortingWriter.h
)
target_compile_options(gluten_dwio_common PRIVATE
        -Wno-unused-exception-parameter
)
target_link_libraries(
        gluten_dwio_common
        ch_contrib::velox_compile_option
        ch_contrib::fmt
        ch_contrib::protobuf
)

# gluten_dwio_parquet_thrift
set(PARQUET_THRIFT_DIR "${VELOX_ROOT_DIR}/velox/dwio/parquet/thrift")
add_library(gluten_dwio_parquet_thrift ${PARQUET_THRIFT_DIR}/ParquetThriftTypes.cpp)
target_link_libraries(gluten_dwio_parquet_thrift
        ch_contrib::thrift
        ch_contrib::velox_compile_option  # 简化警告处理，后续可以考虑移除
)

# gluten_dwio_native_parquet_reader
set(PARQUET_READER_DIR "${VELOX_ROOT_DIR}/velox/dwio/parquet/reader")
add_library(gluten_dwio_native_parquet_reader
        ${PARQUET_READER_DIR}/NestedStructureDecoder.cpp
        ${PARQUET_READER_DIR}/ParquetReader.cpp
        ${PARQUET_READER_DIR}/ParquetTypeWithId.cpp
        ${PARQUET_READER_DIR}/PageReader.cpp
        ${PARQUET_READER_DIR}/ParquetColumnReader.cpp
        ${PARQUET_READER_DIR}/ParquetData.cpp
        ${PARQUET_READER_DIR}/RepeatedColumnReader.cpp
        ${PARQUET_READER_DIR}/RleBpDecoder.cpp
        ${PARQUET_READER_DIR}/Statistics.cpp
        ${PARQUET_READER_DIR}/StructColumnReader.cpp
        ${PARQUET_READER_DIR}/StringColumnReader.cpp
)
target_link_libraries(gluten_dwio_native_parquet_reader
        ch_contrib::velox_compile_option
        ch_contrib::fmt
        ch_contrib::parquet
        ch_contrib::protobuf
        ch_contrib::lz4
        ch_contrib::zstd
        ch_contrib::snappy
)
