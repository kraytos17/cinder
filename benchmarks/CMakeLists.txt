set(CINDER_BENCH_SOURCES
    throughput_bench.cpp
    ring_lookup_bench.cpp
    allocator_bench.cpp
)

add_executable(cinder_throughput_bench ${CINDER_BENCH_SOURCES})
target_link_libraries(cinder_throughput_bench PRIVATE
    cinder::core
    benchmark::benchmark_main
)
set_target_properties(cinder_throughput_bench PROPERTIES
    RUNTIME_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}/bin"
    CXX_STANDARD 23
    CXX_STANDARD_REQUIRED ON
    CXX_EXTENSIONS OFF
    UNITY_BUILD ON
)
target_compile_options(cinder_throughput_bench PRIVATE
    $<$<NOT:$<CONFIG:Release>>:-O3 -DNDEBUG>
)

list(TRANSFORM CINDER_BENCH_SOURCES PREPEND "${CMAKE_CURRENT_SOURCE_DIR}/")
source_group(TREE ${CMAKE_CURRENT_SOURCE_DIR} PREFIX benchmarks FILES ${CINDER_BENCH_SOURCES})
