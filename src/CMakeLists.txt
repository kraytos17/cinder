set(CINDER_CORE_SOURCES
    common/logger.cpp
    store/lru_store.cpp
    store/lfu_store.cpp
    store/ttl_wheel.cpp
    hashing/consistent_hash_ring.cpp
    node/cache_node_server.cpp
    node/shard_manager.cpp
    node/replication_manager.cpp
    cluster/membership.cpp
    cluster/gossip.cpp
    cluster/failure_detector.cpp
    net/protocol.cpp
    net/tcp_server.cpp
    net/tcp_connection.cpp
    client/cache_client.cpp
    client/connection_pool.cpp
)

add_library(cinder_core OBJECT ${CINDER_CORE_SOURCES})

list(TRANSFORM CINDER_CORE_SOURCES PREPEND "${CMAKE_CURRENT_SOURCE_DIR}/")
source_group(TREE ${CMAKE_CURRENT_SOURCE_DIR}
    PREFIX src
    FILES ${CINDER_CORE_SOURCES}
)
