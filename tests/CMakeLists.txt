include(GoogleTest)

function(cinder_add_test name)
    add_executable(${name} ${ARGN})
    target_link_libraries(${name} PRIVATE cinder::core GTest::gtest_main)
    target_include_directories(${name} PRIVATE ${CMAKE_CURRENT_SOURCE_DIR})
    _cinder_apply_debug_options(${name})
    cinder_reuse_pch(${name})
    list(TRANSFORM ARGN PREPEND "${CMAKE_CURRENT_SOURCE_DIR}/")
    source_group(TREE ${CMAKE_CURRENT_SOURCE_DIR} PREFIX tests FILES ${ARGN})
endfunction()

# ---------------------------------------------------------------------------
# Unit tests are split by area rather than built as one monolith. The single
# 22-TU target meant any edit to a core header rebuilt all 357 tests, and
# sanitizer runs (which recompile everything they run) paid that cost for a
# one-file change. Every target keeps the `unit` label, so `just test-unit`
# and CI's `ctest -L unit` are unaffected.
# ---------------------------------------------------------------------------
cinder_add_test(cinder_unit_store_tests
    unit/lru_store_test.cpp
    unit/lfu_store_test.cpp
    unit/ttl_wheel_test.cpp
    unit/versioned_store_test.cpp
    unit/persistence_test.cpp
    unit/lfu_concurrent_test.cpp
    unit/store_concurrency_test.cpp
    sim/sim_clock.cpp
)
gtest_discover_tests(cinder_unit_store_tests PROPERTIES LABELS "unit")

cinder_add_test(cinder_unit_ring_tests
    unit/consistent_hash_ring_test.cpp
    unit/membership_test.cpp
    unit/gossip_test.cpp
    unit/anti_entropy_test.cpp
    unit/cluster_components_test.cpp
)
gtest_discover_tests(cinder_unit_ring_tests PROPERTIES LABELS "unit")

cinder_add_test(cinder_unit_net_tests
    unit/protocol_framing_test.cpp
    unit/tcp_server_stress_test.cpp
    unit/rpc_timeout_test.cpp
    unit/client_routing_test.cpp
    unit/connection_pool_concurrency_test.cpp
)
gtest_discover_tests(cinder_unit_net_tests PROPERTIES LABELS "unit")

cinder_add_test(cinder_unit_node_tests
    unit/cache_operations_test.cpp
    unit/cache_node_server_test.cpp
    unit/fanout_test.cpp
    unit/metrics_test.cpp
    unit/tracing_test.cpp
    unit/status_test.cpp
    unit/config_test.cpp
    unit/auth_test.cpp
    unit/client_auth_test.cpp
    unit/error_provenance_test.cpp
)
gtest_discover_tests(cinder_unit_node_tests PROPERTIES LABELS "unit")

cinder_add_test(cinder_integration_tests
    integration/cluster_smoke_test.cpp
    integration/replica_failover_test.cpp
    integration/rebalance_on_join_test.cpp
    integration/rebalance_rf2_test.cpp
    integration/read_repair_test.cpp
    integration/multi_get_test.cpp
    integration/client_surface_test.cpp
    integration/redirect_test.cpp
    integration/tls_test.cpp
    integration/admin_test.cpp
)
add_dependencies(cinder_integration_tests cinderd)
target_compile_definitions(cinder_integration_tests PRIVATE
    CINDER_TEST_CINDERD_PATH="${CMAKE_BINARY_DIR}/bin/cinderd"
    CMAKE_CURRENT_SOURCE_DIR="${CMAKE_CURRENT_SOURCE_DIR}"
    CMAKE_CURRENT_BINARY_DIR="${CMAKE_CURRENT_BINARY_DIR}"
)

if(CINDER_ENABLE_GRPC)
    target_sources(cinder_integration_tests PRIVATE integration/grpc_test.cpp)
    target_link_libraries(cinder_integration_tests PRIVATE cinder_proto grpc++)
    # The test TU is guarded by #ifdef CINDER_ENABLE_GRPC like the gateway
    # sources (see the cinder_grpc compile definition in the root CMakeLists).
    target_compile_definitions(cinder_integration_tests PRIVATE CINDER_ENABLE_GRPC)
endif()
gtest_discover_tests(cinder_integration_tests
    PROPERTIES LABELS "integration" TIMEOUT "300"
    DISCOVERY_TIMEOUT 60
)

cinder_add_test(cinder_sim_tests
    sim/sim_clock.cpp
    sim/sim_transport.cpp
    sim/replication_sim_test.cpp
    sim/gossip_partition_sim_test.cpp
    sim/rebalance_sim_test.cpp
    sim/concurrent_rebalance_sim_test.cpp
)
gtest_discover_tests(cinder_sim_tests PROPERTIES LABELS "sim")

cinder_add_test(cinder_cli_tests
    integration/cli_test.cpp
)
add_dependencies(cinder_cli_tests cinderd cinder-cli)
target_compile_definitions(cinder_cli_tests PRIVATE
    CINDER_TEST_CINDERD_PATH="${CMAKE_BINARY_DIR}/bin/cinderd"
    CINDER_TEST_CLI_PATH="${CMAKE_BINARY_DIR}/bin/cinder-cli"
    CMAKE_CURRENT_SOURCE_DIR="${CMAKE_CURRENT_SOURCE_DIR}"
    CMAKE_CURRENT_BINARY_DIR="${CMAKE_CURRENT_BINARY_DIR}"
)
gtest_discover_tests(cinder_cli_tests
    PROPERTIES LABELS "cli"
    DISCOVERY_TIMEOUT 60
)

# ---------------------------------------------------------------------------
# Fuzz targets (opt-in via -DCINDER_ENABLE_FUZZER=ON, implying ASan) and
# coverage-instrumented variants (opt-in via -DCINDER_ENABLE_COVERAGE=ON).
# One helper: the coverage flavor only adds -fprofile-instr-generate and
# -fcoverage-mapping. The libFuzzer note below applies to both — never put
# -fsanitize=fuzzer on a global flag, or every target (incl. cinderd) grows
# a second main().
# ---------------------------------------------------------------------------
if(CINDER_ENABLE_FUZZER OR CINDER_ENABLE_COVERAGE)
    function(cinder_add_fuzz name)
        cmake_parse_arguments(_FUZZ "COVERAGE" "" "" ${ARGN})
        add_executable(${name} ${_FUZZ_UNPARSED_ARGUMENTS})
        target_link_libraries(${name} PRIVATE cinder::core)
        target_compile_options(${name} PRIVATE
            -fsanitize=fuzzer,address,undefined -fno-omit-frame-pointer)
        target_link_options(${name} PRIVATE
            -fsanitize=fuzzer,address,undefined)
        if(_FUZZ_COVERAGE)
            # Links against cinder_core with coverage flags; use a wrapper
            # script to run them under libFuzzer with -runs=1 for per-seed
            # coverage collection.
            target_compile_options(${name} PRIVATE
                -fprofile-instr-generate -fcoverage-mapping)
            target_link_options(${name} PRIVATE
                -fprofile-instr-generate -fcoverage-mapping)
        endif()
    endfunction()

    function(cinder_add_fuzz_suite suffix use_coverage)
        foreach(_fuzz_name IN ITEMS
                protocol_decode gossip_parse store_put snapshot wal
                anti_entropy http_parse redirect_parse byte_reader
                ttl_wheel hint_queue ring config)
            if(use_coverage)
                cinder_add_fuzz(${_fuzz_name}${suffix} COVERAGE fuzz/${_fuzz_name}_fuzz.cpp)
            else()
                cinder_add_fuzz(${_fuzz_name}${suffix} fuzz/${_fuzz_name}_fuzz.cpp)
            endif()
        endforeach()
    endfunction()

    if(CINDER_ENABLE_FUZZER)
        # -fsanitize=fuzzer is applied ONLY to these fuzz targets,
        # not globally, to avoid duplicate main() in non-fuzz targets like cinderd.
        cinder_add_fuzz_suite("_fuzz" FALSE)
    endif()

    if(CINDER_ENABLE_COVERAGE)
        cinder_add_fuzz_suite("_cov" TRUE)
    endif()
endif()
