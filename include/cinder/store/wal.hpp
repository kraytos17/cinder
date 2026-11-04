#pragma once

#include <cstddef>
#include <cstdint>
#include <fstream>
#include <string>
#include <vector>

#include "cinder/common/status.hpp"
#include "cinder/common/types.hpp"

namespace cinder {

    constexpr uint32_t K_WAL_MAGIC = 0x57414C30; // "WAL0"
    // v1: legacy layout (no tombstone byte; Del replays as hard remove).
    // v2: trailing tombstone byte per record; Set+tombstone replays as a
    // versioned delete marker, Del keeps hard-remove semantics (migration
    // source cleanup must not resurrect markers).
    constexpr uint32_t K_WAL_FORMAT_VERSION = 2;
    constexpr uint32_t K_WAL_FORMAT_VERSION_V1 = 1;

    struct WalEntry {
        enum class Op : uint8_t {
            Set = 1,
            Del = 2,
        };

        Op op = Op::Set;
        std::string key;
        std::string value;
        Version version = 0;
        uint64_t writer_node_hash = 0;
        uint64_t expires_at_ms = 0; // wall-clock ms since epoch, 0 if no TTL
        bool has_ttl = false;
        bool is_tombstone = false;
    };

    class WalWriter {
    public:

        explicit WalWriter(const std::string& path);
        ~WalWriter();

        WalWriter(const WalWriter&) = delete;
        auto operator=(const WalWriter&) -> WalWriter& = delete;
        WalWriter(WalWriter&&) = delete;
        auto operator=(WalWriter&&) -> WalWriter& = delete;

        auto append(const WalEntry& entry) -> Result<void>;
        void flush();

        // Durability barrier: flushes userspace buffers, then fdatasyncs the
        // file so acknowledged writes survive an OS crash, not just a process
        // crash. No-op (ok) where fdatasync is unavailable.
        auto sync() -> Result<void>;

    private:

        std::ofstream out_;
        std::vector<std::byte> scratch_; // reused buffer for record encoding
#ifndef _WIN32
        // Companion fd for fdatasync: opened O_APPEND on the same path, so it
        // shares the file offset with the stream. ofstream exposes no portable
        // file descriptor, hence the second handle. -1 = open failed.
        int sync_fd_ = -1;
#endif
    };

    class WalReader {
    public:

        explicit WalReader(const std::string& path);
        ~WalReader() = default;

        WalReader(const WalReader&) = delete;
        auto operator=(const WalReader&) -> WalReader& = delete;
        WalReader(WalReader&&) = delete;
        auto operator=(WalReader&&) -> WalReader& = delete;

        auto next() -> std::optional<WalEntry>;
        auto hasMore() -> bool;

    private:

        std::ifstream in_;
        // File format version: 0 = pre-magic legacy (no checksums), 1 = v1
        // checksummed records, 2 = v2 records with trailing tombstone byte.
        uint32_t format_version_ = 0;
        bool header_checked_ = false;
    };
} // namespace cinder
