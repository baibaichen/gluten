set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -mavx2 -mfma -mavx -mf16c -mlzcnt -mbmi2")
clickhouse_add_executable (xsimd_check example/xsimd.cpp)
target_compile_options(xsimd_check PRIVATE
        -Wextra
        -Wreorder
        -Wno-undef
        -Wno-old-style-cast
        -Wno-reserved-identifier
        -Wno-zero-as-null-pointer-constant
        -Wno-documentation
        -Wno-deprecated
        -Wno-cast-align
        -Wno-shorten-64-to-32
)

target_link_libraries(xsimd_check PRIVATE ch_contrib::xsimd)

include_directories(${VELOX_ROOT_DIR})
clickhouse_add_executable(gluten_dwio_parquet_reader_example example/parquet_read.cpp)
target_compile_options(
        gluten_dwio_parquet_reader_example PRIVATE
        -Wno-unknown-warning-option             # folly/Utility.h           => FOLLY_GNU_DISABLE_WARNING("-Wmaybe-uninitialized")
        -Wno-reserved-macro-identifier          # folly/CPortability.h      => __CLANG_PREREQ
        -Wno-comma                              # folly/Traits.h
        -Wno-deprecated                         # folly/lang/StaticConst.h
        -Wno-reserved-identifier                # folly                     => folly_cpo__
        -Wno-old-style-cast                     # folly/lang/SafeAssert.h
        -Wno-undef                              # folly                     => __cpp_lib_atomic_ref
        -Wno-documentation                      # folly
        -Wno-cast-qual                          # folly/FBVector.h
        -Wno-thread-safety-analysis             # folly
        -Wno-covered-switch-default             # folly
        -Wno-gnu-zero-variadic-macro-arguments  # velox                     => __VA_ARGS__
        -Wno-extra-semi-stmt                    # velox
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
        -Wno-suggest-override                   # glog
        -Wno-zero-as-null-pointer-constant      # glog
        -Wno-suggest-destructor-override        # glog
        -Wno-cast-align                         # xsimd
        -Wno-unused-exception-parameter
)
target_link_libraries(gluten_dwio_parquet_reader_example PRIVATE
        gluten_velox_flag_definitions
        gluten_velox_test_util

        gluten_velox_exception
        gluten_velox_process
        gluten_velox_common_base
        gluten_velox_file
        gluten_velox_serialization
        gluten_velox_common_compression

        gluten_velox_vector
        gluten_velox_type
        gluten_velox_time
        gluten_velox_memory
        gluten_velox_expression
        gluten_velox_parse_parser
        gluten_velox_functions_prestosql

        gluten_dwio_common_exception
        gluten_dwio_common
        gluten_dwio_common_compression
        gluten_dwio_native_parquet_reader
        gluten_dwio_parquet_thrift


)