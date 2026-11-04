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

    // Total LWW order shared by every convergence path (store applies,
    // quorum-read reduction, anti-entropy diff, hint coalescing, read repair).
    // Compare version, then writer hash; a contradictory tombstone bit at an
    // otherwise identical stamp resolves tombstone-wins so replicas converge
    // instead of disagreeing by arrival order. (Identical stamps can only
    // arise across standalone writers sharing writer 0; per-writer versions
    // are monotonic.)
    struct LwwStamp {
        Version version = 0;
        uint64_t writer = 0;
        bool tombstone = false;
    };

    enum class LwwOrder : uint8_t {
        Less,
        Equal,
        Greater,
    };

    [[nodiscard]]
    constexpr auto compareLww(const LwwStamp& a, const LwwStamp& b) -> LwwOrder {
        if (a.version != b.version) {
            return a.version < b.version ? LwwOrder::Less : LwwOrder::Greater;
        }
        if (a.writer != b.writer) {
            return a.writer < b.writer ? LwwOrder::Less : LwwOrder::Greater;
        }
        if (a.tombstone != b.tombstone) {
            return a.tombstone ? LwwOrder::Greater : LwwOrder::Less;
        }
        return LwwOrder::Equal;
    }

    // Strictly-greater check for "incoming wins over resident" decisions
    // (store apply, read reduction, need/fetch derivation).
    [[nodiscard]]
    constexpr auto lwwWinsOver(const LwwStamp& incoming, const LwwStamp& resident) -> bool {
        return compareLww(incoming, resident) == LwwOrder::Greater;
    }

    // Greater-or-equal check for idempotent refresh decisions (hint
    // coalescing): an identical stamp counts as newer so a retried write
    // refreshes TTL without changing the outcome.
    [[nodiscard]]
    constexpr auto lwwCovers(const LwwStamp& incoming, const LwwStamp& resident) -> bool {
        return compareLww(incoming, resident) != LwwOrder::Less;
    }

    [[nodiscard]]
    inline auto stampOf(const VersionedEntry& entry) -> LwwStamp {
        return {entry.version(), entry.writer_node_hash, entry.isTombstone()};
    }
} // namespace cinder
