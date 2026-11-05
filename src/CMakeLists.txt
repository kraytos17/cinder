set(CINDER_CORE_SOURCES
    common/tracing.cpp
    common/config.cpp
    common/hmac.cpp
    common/metrics.cpp
    store/lru_store.cpp
    store/lfu_store.cpp
    store/wal.cpp
    store/snapshot.cpp
    store/persistence.cpp
    hashing/consistent_hash_ring.cpp
    node/cache_node_server.cpp
    node/shard_manager.cpp
    node/replication_manager.cpp
    node/hint_journal.cpp
    node/anti_entropy.cpp
    cluster/membership.cpp
    cluster/gossip.cpp
    cluster/failure_detector.cpp
    net/protocol.cpp
    net/http_parser.cpp
    net/tcp_server.cpp
    net/tcp_connection.cpp
    net/tcp_transport.cpp
    client/cache_client.cpp
    client/connection_pool.cpp
)

add_library(cinder_core OBJECT ${CINDER_CORE_SOURCES})

target_include_directories(cinder_core PUBLIC
    # PROJECT_SOURCE_DIR (not CMAKE_CURRENT_SOURCE_DIR/..) keeps -I paths
    # canonical and is correct even if cinder is ever a subproject.
    $<BUILD_INTERFACE:${PROJECT_SOURCE_DIR}/include>
    $<INSTALL_INTERFACE:include>
)

target_include_directories(cinder_core SYSTEM PUBLIC
    $<BUILD_INTERFACE:${PROJECT_SOURCE_DIR}/third_party>
)

target_link_libraries(cinder_core PUBLIC
    Threads::Threads
    asio
    xxhash
    spdlog::spdlog
    yaml-cpp::yaml-cpp
)

# mimalloc must come before other libs on the link line so its malloc/free
# symbols are the ones the linker resolves against. PUBLIC so every
# executable linking cinder::core (cinderd, cinder-cli, tests, benchmarks)
# picks up the override automatically.
if(CINDER_USE_MIMALLOC)
    target_link_libraries(cinder_core PUBLIC mimalloc-static)
    target_compile_definitions(cinder_core PUBLIC CINDER_USE_MIMALLOC)
endif()

if(CINDER_ENABLE_TLS)
    target_link_libraries(cinder_core PUBLIC OpenSSL::SSL OpenSSL::Crypto)
    target_compile_definitions(cinder_core PUBLIC CINDER_ENABLE_TLS CINDER_ENABLE_CRYPTO)
elseif(CINDER_ENABLE_CRYPTO)
    target_link_libraries(cinder_core PUBLIC OpenSSL::Crypto)
    target_compile_definitions(cinder_core PUBLIC CINDER_ENABLE_CRYPTO)
endif()

target_compile_options(cinder_core PRIVATE
    -Wall -Wextra -Wpedantic -Wshadow -Wconversion
    $<$<CONFIG:Release>:-O3>
    $<$<BOOL:${CINDER_ENABLE_COVERAGE}>:-fprofile-instr-generate;-fcoverage-mapping>
)
if(CINDER_ENABLE_COVERAGE)
    target_link_options(cinder_core PUBLIC -fprofile-instr-generate -fcoverage-mapping)
endif()
_cinder_apply_debug_options(cinder_core)

target_precompile_headers(cinder_core PRIVATE
    <asio.hpp>
    <spdlog/spdlog.h>
    <xxhash.h>
)

list(TRANSFORM CINDER_CORE_SOURCES PREPEND "${CMAKE_CURRENT_SOURCE_DIR}/")
source_group(TREE ${CMAKE_CURRENT_SOURCE_DIR}
    PREFIX src
    FILES ${CINDER_CORE_SOURCES}
)
