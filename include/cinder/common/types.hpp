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
        TTL,
    };

    enum class ConsistencyMode : uint8_t {
        Async,
        Quorum,
    };

    struct CacheEntry {
        std::string value;
        steady_clock::time_point expires_at;
        bool has_ttl = false;
    };

    struct VersionedEntry {
        // Bit-packed: bit 0 = has_ttl, bit 1 = tombstone (deleted marker),
        // bits 2-63 = version (actual_version << 2). The tombstone bit is part
        // of the entry identity: digests, IBLT sketches, and metadata rows all
        // cover it, so a delete is a first-class versioned write that wins or
        // loses LWW exactly like a put. Dotted version vectors were considered
        // for delete-vs-concurrent-write causality but deferred: LWW over
        // (version, writer) already orders delete-vs-write the same way it
        // orders write-vs-write, which is this system's consistency contract.
        Version version_and_ttl = 0;
        steady_clock::time_point expires_at;
        uint64_t writer_node_hash = 0;
        std::string value;

        [[nodiscard]]
        auto version() const -> Version {
            return version_and_ttl >> 2U;
        }

        [[nodiscard]]
        auto hasTtl() const -> bool {
            return version_and_ttl & 1U;
        }

        [[nodiscard]]
        auto isTombstone() const -> bool {
            return (version_and_ttl & 2U) != 0;
        }

        void setVersion(Version v) {
            version_and_ttl = (v << 2U) | (version_and_ttl & 3U);
        }

        void setHasTtl(bool ttl) {
            version_and_ttl = (version_and_ttl & ~uint64_t{1}) | (ttl ? 1U : 0);
        }

        void setVersionAndTtl(Version v, bool ttl) {
            version_and_ttl = (v << 2U) | (ttl ? 1U : 0);
        }

        void setTombstone(bool tombstone) {
            version_and_ttl = (version_and_ttl & ~uint64_t{2}) | (tombstone ? 2U : 0);
        }
    };

    static_assert(sizeof(VersionedEntry) == 56, "VersionedEntry should fit in one cache line");
} // namespace cinder
