#pragma once

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <string>
#include <vector>

using std::chrono::steady_clock;

namespace cinder {

using Bytes = std::vector<std::byte>;
using Key = std::string;
using NodeId = std::string;
using Version = uint64_t;

enum class EvictionPolicy : uint8_t {
    LRU,
    LFU,
    TTL
};

enum class ConsistencyMode : uint8_t {
    Async,
    Quorum
};

struct CacheEntry {
    std::string value;
    steady_clock::time_point expires_at;
    bool has_ttl = false;
};

struct VersionedEntry {
    bool has_ttl = false;
    // 7 bytes padding
    steady_clock::time_point expires_at;
    Version version = 0;
    uint64_t writer_node_hash = 0;
    std::string value;
};
} // namespace cinder
