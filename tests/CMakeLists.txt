include(GoogleTest)

add_executable(cinder_unit_tests
    unit/lru_store_test.cpp
    unit/lfu_store_test.cpp
    unit/ttl_wheel_test.cpp
    unit/consistent_hash_ring_test.cpp
    unit/protocol_framing_test.cpp
)
target_link_libraries(cinder_unit_tests PRIVATE
    cinder::core
    GTest::gtest_main
)
gtest_discover_tests(cinder_unit_tests
    PROPERTIES LABELS "unit"
)

add_executable(cinder_integration_tests
    integration/cluster_smoke_test.cpp
    integration/replica_failover_test.cpp
    integration/rebalance_on_join_test.cpp
)
target_link_libraries(cinder_integration_tests PRIVATE
    cinder::core
    GTest::gtest_main
)
add_dependencies(cinder_integration_tests cinderd)
target_compile_definitions(cinder_integration_tests PRIVATE
    CINDER_TEST_CINDERD_PATH="${CMAKE_BINARY_DIR}/bin/cinderd"
)
gtest_discover_tests(cinder_integration_tests
    PROPERTIES LABELS "integration"
    DISCOVERY_TIMEOUT 60
)

add_executable(cinder_sim_tests
    sim/sim_clock.cpp
    sim/sim_transport.cpp
    sim/replication_sim_test.cpp
    sim/gossip_partition_sim_test.cpp
    sim/rebalance_sim_test.cpp
)
target_link_libraries(cinder_sim_tests PRIVATE
    cinder::core
    GTest::gtest_main
)
gtest_discover_tests(cinder_sim_tests
    PROPERTIES LABELS "sim"
)
