include(GoogleTest)

function(cinder_add_test name)
    add_executable(${name} ${ARGN})
    target_link_libraries(${name} PRIVATE cinder::core GTest::gtest_main)
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
)
set_target_properties(cinder_unit_tests PROPERTIES UNITY_BUILD ON)
gtest_discover_tests(cinder_unit_tests PROPERTIES LABELS "unit")

cinder_add_test(cinder_integration_tests
    integration/cluster_smoke_test.cpp
    integration/replica_failover_test.cpp
    integration/rebalance_on_join_test.cpp
)
add_dependencies(cinder_integration_tests cinderd)
target_compile_definitions(cinder_integration_tests PRIVATE
    CINDER_TEST_CINDERD_PATH="${CMAKE_BINARY_DIR}/bin/cinderd"
)
gtest_discover_tests(cinder_integration_tests
    PROPERTIES LABELS "integration"
    DISCOVERY_TIMEOUT 60
)

cinder_add_test(cinder_sim_tests
    sim/sim_clock.cpp
    sim/sim_transport.cpp
    sim/replication_sim_test.cpp
    sim/gossip_partition_sim_test.cpp
    sim/rebalance_sim_test.cpp
)
set_target_properties(cinder_sim_tests PROPERTIES UNITY_BUILD ON)
gtest_discover_tests(cinder_sim_tests PROPERTIES LABELS "sim")
