include(GoogleTest)

function(cinder_add_test name)
    add_executable(${name} ${ARGN})
    target_link_libraries(${name} PRIVATE cinder::core GTest::gtest_main)
    target_include_directories(${name} PRIVATE ${CMAKE_CURRENT_SOURCE_DIR})
    _cinder_apply_debug_options(${name})
    list(TRANSFORM ARGN PREPEND "${CMAKE_CURRENT_SOURCE_DIR}/")
    source_group(TREE ${CMAKE_CURRENT_SOURCE_DIR} PREFIX tests FILES ${ARGN})
endfunction()

cinder_add_test(cinder_unit_tests
    unit/lru_store_test.cpp
    unit/lfu_store_test.cpp
    unit/ttl_wheel_test.cpp
    unit/consistent_hash_ring_test.cpp
    unit/protocol_framing_test.cpp
    unit/status_test.cpp
    unit/client_routing_test.cpp
    unit/versioned_store_test.cpp
    unit/cache_node_server_test.cpp
    unit/membership_test.cpp
    unit/config_test.cpp
    unit/persistence_test.cpp
    unit/tcp_server_stress_test.cpp
    unit/rpc_timeout_test.cpp
    unit/gossip_test.cpp
    unit/error_provenance_test.cpp
    unit/lfu_concurrent_test.cpp
    unit/metrics_test.cpp
    sim/sim_clock.cpp
)
gtest_discover_tests(cinder_unit_tests PROPERTIES LABELS "unit")

cinder_add_test(cinder_integration_tests
    integration/cluster_smoke_test.cpp
    integration/replica_failover_test.cpp
    integration/rebalance_on_join_test.cpp
    integration/rebalance_rf2_test.cpp
    integration/read_repair_test.cpp
    integration/multi_get_test.cpp
    integration/tls_test.cpp
)
add_dependencies(cinder_integration_tests cinderd)
target_compile_definitions(cinder_integration_tests PRIVATE
    CINDER_TEST_CINDERD_PATH="${CMAKE_BINARY_DIR}/bin/cinderd"
    CMAKE_CURRENT_SOURCE_DIR="${CMAKE_CURRENT_SOURCE_DIR}"
    CMAKE_CURRENT_BINARY_DIR="${CMAKE_CURRENT_BINARY_DIR}"
)
gtest_discover_tests(cinder_integration_tests
    PROPERTIES LABELS "integration"
    RUN_SERIAL TRUE
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
)
gtest_discover_tests(cinder_cli_tests
    PROPERTIES LABELS "cli"
    DISCOVERY_TIMEOUT 60
)

# ---------------------------------------------------------------------------
# Fuzz targets (opt-in via -DCINDER_ENABLE_FUZZER=ON)
# ---------------------------------------------------------------------------
if(CINDER_ENABLE_FUZZER)
    function(cinder_add_fuzz name)
        add_executable(${name} ${ARGN})
        target_link_libraries(${name} PRIVATE cinder::core)
        target_compile_options(${name} PRIVATE -fsanitize=fuzzer,address,undefined -fno-omit-frame-pointer)
        target_link_options(${name} PRIVATE -fsanitize=fuzzer,address,undefined)
    endfunction()

    cinder_add_fuzz(protocol_decode_fuzz fuzz/protocol_decode_fuzz.cpp)
    cinder_add_fuzz(gossip_parse_fuzz fuzz/gossip_parse_fuzz.cpp)
    cinder_add_fuzz(store_put_fuzz fuzz/store_put_fuzz.cpp)
    cinder_add_fuzz(snapshot_fuzz fuzz/snapshot_fuzz.cpp)
endif()
