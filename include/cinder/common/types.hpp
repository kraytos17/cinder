#pragma once

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <string>
#include <vector>

namespace cinder {

using Bytes = std::vector<std::byte>;
using Key = std::string;
using NodeId = std::string;
using Version = uint64_t;

enum class EvictionPolicy : uint8_t { LRU, LFU, TTL };
enum class ConsistencyMode : uint8_t{ Async, Quorum };

struct CacheEntry {
    std::string value;
    std::chrono::steady_clock::time_point expires_at;
    bool has_ttl = false;
};

struct VersionedEntry {
    std::string value;
    Version version = 0;
    uint64_t writer_node_hash = 0;
    std::chrono::steady_clock::time_point expires_at;
    bool has_ttl = false;
};
} // namespace cinder
