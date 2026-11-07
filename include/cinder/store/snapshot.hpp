#pragma once

#include <cstdint>
#include <fstream>
#include <functional>
#include <optional>
#include <span>
#include <string>
#include <vector>

#include "cinder/common/status.hpp"
#include "cinder/common/types.hpp"

namespace cinder {

    constexpr uint32_t K_SNAPSHOT_MAGIC = 0x43534E50; // "CSNP"
    // v1: legacy entries (no tombstone byte; deletes absent from snapshots).
    // v2: trailing tombstone byte per entry; tombstones restore as versioned
    // delete markers so a restart cannot resurrect deleted keys.
    constexpr uint32_t K_SNAPSHOT_FORMAT_VERSION = 2;
    constexpr uint32_t K_SNAPSHOT_FORMAT_VERSION_V1 = 1;

    // On-disk format version, parsed once from the header. Writers always
    // emit V2; readers accept every enumerator. A v3 adds an enumerator
    // plus a capability query below — call sites ask what a format
    // supports rather than branching on version numbers.
    enum class SnapshotFormat : uint32_t {
        V1 = 1,
        V2 = 2,
    };

    static_assert(
        static_cast<uint32_t>(SnapshotFormat::V2) == K_SNAPSHOT_FORMAT_VERSION,
        "SnapshotFormat::V2 must match the version writers emit"
    );

    [[nodiscard]]
    inline auto parseSnapshotFormat(uint32_t v) -> std::optional<SnapshotFormat> {
        switch (v) {
            case static_cast<uint32_t>(SnapshotFormat::V1):
                return SnapshotFormat::V1;
            case static_cast<uint32_t>(SnapshotFormat::V2):
                return SnapshotFormat::V2;
            default:
                return std::nullopt;
        }
    }

    // v2 appends the trailing tombstone byte per entry.
    [[nodiscard]]
    constexpr auto snapshotHasTombstone(SnapshotFormat f) -> bool {
        return f >= SnapshotFormat::V2;
    }

    struct SnapshotEntry {
        std::string key;
        std::string value;
        Version version = 0;
        uint64_t writer_node_hash = 0;
        uint64_t expires_at_ms = 0; // wall-clock ms since epoch, 0 if no TTL
        bool has_ttl = false;
        bool tombstone = false;
        size_t freq = 0; // 0 for LRU
    };

    static_assert(sizeof(SnapshotEntry) == 104, "SnapshotEntry must stay 104 bytes");

    struct SnapshotData {
        Version next_version = 0;
        std::vector<SnapshotEntry> entries;
    };

    class SnapshotWriter {
    public:

        explicit SnapshotWriter(const std::string& path);
        ~SnapshotWriter() = default;

        SnapshotWriter(const SnapshotWriter&) = delete;
        auto operator=(const SnapshotWriter&) -> SnapshotWriter& = delete;
        SnapshotWriter(SnapshotWriter&&) = delete;
        auto operator=(SnapshotWriter&&) -> SnapshotWriter& = delete;

        auto write(Version next_version, std::span<const SnapshotEntry> entries) -> Result<void>;

    private:

        std::string path_;
        // Encode buffer for the header and for entries larger than the
        // 256 KiB write chunk (which bypass chunking). Grows to the
        // largest entry seen; never shrinks within a snapshot.
        std::vector<std::byte> scratch_;
    };

    class SnapshotReader {
    public:

        explicit SnapshotReader(const std::string& path);
        ~SnapshotReader() = default;

        SnapshotReader(const SnapshotReader&) = delete;
        auto operator=(const SnapshotReader&) -> SnapshotReader& = delete;
        SnapshotReader(SnapshotReader&&) = delete;
        auto operator=(SnapshotReader&&) -> SnapshotReader& = delete;

        auto readAll() -> Result<SnapshotData>;
        // Streaming read: invokes fn per entry without accumulating the full
        // vector, so large snapshots recover in O(1) memory. next_version is
        // delivered first via the out-param once the header validates.
        auto readStream(Version& next_version, std::move_only_function<void(SnapshotEntry&)> fn)
            -> Result<void>;

    private:

        std::ifstream in_;
    };
} // namespace cinder
