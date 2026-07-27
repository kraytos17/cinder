add_executable(cinder_throughput_bench
    throughput_bench.cpp
    ring_lookup_bench.cpp
    allocator_bench.cpp
)
target_link_libraries(cinder_throughput_bench PRIVATE
    cinder::core
    benchmark::benchmark
    benchmark::benchmark_main
)

set_target_properties(cinder_throughput_bench PROPERTIES
    RUNTIME_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}/bin"
)

target_compile_options(cinder_throughput_bench PRIVATE
    $<$<NOT:$<CONFIG:Release>>:-O3 -DNDEBUG>
)
