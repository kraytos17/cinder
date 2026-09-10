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
    // Bit-packed: bit 0 = has_ttl, bits 1-63 = version (actual_version << 1).
    Version version_and_ttl = 0;
    steady_clock::time_point expires_at;
    uint64_t writer_node_hash = 0;
    std::string value;

    [[nodiscard]] auto version() const -> Version { return version_and_ttl >> 1U; }

    [[nodiscard]] auto hasTtl() const -> bool { return version_and_ttl & 1U; }

    void setVersion(Version v) { version_and_ttl = (v << 1U) | (version_and_ttl & 1U); }

    void setHasTtl(bool ttl) {
        version_and_ttl = (version_and_ttl & ~uint64_t{1}) | (ttl ? 1U : 0);
    }

    void setVersionAndTtl(Version v, bool ttl) { version_and_ttl = (v << 1U) | (ttl ? 1U : 0); }
};
} // namespace cinder
