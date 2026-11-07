#pragma once

#include <cstddef>
#include <cstdint>
#include <optional>
#include <span>
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

    // On-disk format version, parsed once from the header. Legacy (0) is
    // the pre-magic format with no checksums. Writers always emit V2;
    // readers accept every enumerator. A v3 adds an enumerator plus a
    // capability query below — call sites ask what a format supports
    // rather than branching on version numbers.
    enum class WalFormat : uint32_t {
        Legacy = 0,
        V1 = 1,
        V2 = 2,
    };

    static_assert(
        static_cast<uint32_t>(WalFormat::V2) == K_WAL_FORMAT_VERSION,
        "WalFormat::V2 must match the version writers emit"
    );

    [[nodiscard]]
    inline auto parseWalFormat(uint32_t v) -> std::optional<WalFormat> {
        switch (v) {
            case static_cast<uint32_t>(WalFormat::V1):
                return WalFormat::V1;
            case static_cast<uint32_t>(WalFormat::V2):
                return WalFormat::V2;
            default:
                return std::nullopt;
        }
    }

    // v2 appends the trailing tombstone byte (covered by the checksum).
    [[nodiscard]]
    constexpr auto walHasTombstone(WalFormat f) -> bool {
        return f >= WalFormat::V2;
    }

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

    // How the writer opens its file. Truncate starts a fresh log (compact
    // rotation, tests); Append resumes the existing log across restarts so a
    // crash between replay and the next snapshot loses nothing. In Append
    // mode a missing/empty file gets a fresh header; a present-but-invalid
    // header truncates to a fresh log with a warning (the file is garbage).
    enum class WalOpenMode : uint8_t {
        Truncate,
        Append,
    };

    class WalWriter {
    public:

        explicit WalWriter(const std::string& path, WalOpenMode mode = WalOpenMode::Truncate);
        ~WalWriter();

        WalWriter(const WalWriter&) = delete;
        auto operator=(const WalWriter&) -> WalWriter& = delete;
        WalWriter(WalWriter&&) = delete;
        auto operator=(WalWriter&&) -> WalWriter& = delete;

        auto append(const WalEntry& entry) -> Result<void>;
        // Group commit: encode the whole batch into one buffer and land it
        // with a single stream write. On-disk bytes are identical to N
        // sequential append() calls (per-record checksums preserved), so
        // readers, checksums, and fuzz corpora are unaffected. Returns the
        // byte count appended. Fails atomically before writing anything when
        // any entry exceeds the uint32 length limit.
        auto appendBatch(std::span<const WalEntry> entries) -> Result<size_t>;
        void flush();

        // Durability barrier: flushes userspace buffers, then fdatasyncs the
        // file so acknowledged writes survive an OS crash, not just a process
        // crash. No-op (ok) where fdatasync is unavailable.
        auto sync() -> Result<void>;

    private:

        // Lands the scratch buffer with one fd write (POSIX) or stream write
        // (Windows legacy path).
        auto writeScratch() -> Result<void>;
        [[nodiscard]]
        auto isOpen() const -> bool;

        // Single handle for appends and fdatasync (O_APPEND keeps
        // positioning atomic; no userspace buffering, so every append is in
        // the kernel the moment it returns). -1 = open failed.
        std::vector<std::byte> scratch_; // reused buffer for record encoding
#ifndef _WIN32
        int fd_ = -1;
#else
        // Legacy buffered path (Windows): ofstream + flush-only durability.
        std::ofstream out_;
#endif
    };

    class WalReader {
    public:

        // Outcome of replaying one WAL file: physically decoded records plus
        // the end offset of the last fully decoded record (for torn-tail
        // truncation; see WalReader::validSize).
        struct ReplayStats {
            size_t records = 0;
            uint64_t valid_bytes = 0;
        };

        explicit WalReader(const std::string& path);
        ~WalReader() = default;

        WalReader(const WalReader&) = delete;
        auto operator=(const WalReader&) -> WalReader& = delete;
        WalReader(WalReader&&) = delete;
        auto operator=(WalReader&&) -> WalReader& = delete;

        auto next() -> std::optional<WalEntry>;
        auto hasMore() -> bool;

        // End offset of the last fully decoded record. A torn tail (crash
        // mid-append) leaves validSize() < file size; recovery truncates
        // the file there before appending again, otherwise the next replay
        // misframes past the partial record and reports corruption.
        [[nodiscard]]
        auto validSize() const -> uint64_t {
            return commit_pos_;
        }

        // True once next() has seen a structurally invalid record
        // (bad op, bad tombstone byte, checksum mismatch). Clean EOF and
        // torn-tail truncation do NOT set this — they are normal crash
        // artifacts and replay stops with Ok.
        [[nodiscard]]
        auto corrupted() const -> bool {
            return corrupted_;
        }

    private:

        // Whole-file image slurped once in the constructor: one read syscall
        // instead of per-field stream reads plus tellg/seek round-trips per
        // string. Segment rotation bounds the image by max_wal_segment_bytes.
        std::vector<char> data_;
        size_t pos_ = 0;
        // End offset of the last fully decoded record (see validSize()).
        uint64_t commit_pos_ = 0;
        // File format version: Legacy (0) = pre-magic format with no
        // checksums; parsed once from the header into a WalFormat.
        WalFormat format_version_ = WalFormat::Legacy;
        bool corrupted_ = false;
    };
} // namespace cinder
