#pragma once

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <deque>
#include <filesystem>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <vector>

#include "cinder/cluster/clock.hpp"
#include "cinder/common/persistence_tuning.hpp"
#include "cinder/common/status.hpp"
#include "cinder/common/types.hpp"
#include "cinder/store/snapshot.hpp"
#include "cinder/store/wal.hpp"

namespace cinder {

    class CacheStore;

    class PersistenceManager {
    public:

        struct Options {
            std::string data_dir;
            bool enabled = false;
            PersistenceTuning tuning;
        };

        PersistenceManager(Options opts, CacheStore& store, Clock* clock = nullptr);
        ~PersistenceManager() = default;

        PersistenceManager(const PersistenceManager&) = delete;
        auto operator=(const PersistenceManager&) -> PersistenceManager& = delete;
        PersistenceManager(PersistenceManager&&) = delete;
        auto operator=(PersistenceManager&&) -> PersistenceManager& = delete;

        // Load latest snapshot + replay WAL. Called once before serving.
        auto recover() -> Result<void>;

        // Append a write to the WAL. Suppressed during recovery.
        void onWrite(const WalEntry& entry);

        // Flush buffered WAL data to disk.
        void flush();

        // True when the WAL has grown past the auto-compact threshold and the
        // server should schedule a (debounced, async) compact. Lock-free read;
        // zero when disabled or when both limits are 0.
        [[nodiscard]]
        auto walNeedsCompact() const -> bool {
            const size_t total_entries = wal_entry_count_.load(std::memory_order_relaxed)
                + wal_sealed_entries_.load(std::memory_order_relaxed);
            const bool entries_hit =
                opts_.tuning.max_wal_entries > 0 && total_entries >= opts_.tuning.max_wal_entries;
            const size_t total_bytes = wal_bytes_.load(std::memory_order_relaxed)
                + wal_sealed_bytes_.load(std::memory_order_relaxed);
            const bool bytes_hit =
                opts_.tuning.max_wal_bytes > 0 && total_bytes >= opts_.tuning.max_wal_bytes;
            return entries_hit || bytes_hit;
        }

        // Create snapshot and truncate WAL.
        auto compact() -> Result<void>;

        // Final flush on graceful shutdown.
        void shutdown();

        [[nodiscard]]
        auto enabled() const -> bool {
            return opts_.enabled;
        }

        [[nodiscard]]
        auto snapshotInterval() const -> size_t {
            return opts_.tuning.snapshot_interval_s;
        }

    private:

        // Outcome of draining the pending WAL queue: Empty (nothing to write),
        // Complete (whole batch appended), or Partial (an append failed and
        // the batch was abandoned early).
        enum class DrainResult : uint8_t {
            Empty,
            Complete,
            Partial,
        };

        auto createSnapshot() -> Result<void>;
        // Store-entry conversions shared by snapshot write and both replay
        // paths. The remaining-lifetime computation lives in exactly one
        // place (rebaseExpiry) so a TTL edge-case fix cannot land in one
        // path and miss the other. Converters return nullopt for already
        // expired entries, which the caller skips instead of resurrecting.
        auto toSnapshotEntry(std::string key, VersionedEntry entry) -> SnapshotEntry;
        auto versionedFromSnapshot(SnapshotEntry& entry) -> std::optional<VersionedEntry>;
        auto versionedFromWal(WalEntry& entry) -> std::optional<VersionedEntry>;
        // Replays the WAL, returning the number of physically decoded records
        // (including expired-skipped ones still occupying the file) so the
        // caller can seed its byte/entry accounting.
        auto replayWal(const std::filesystem::path& path) -> Result<size_t>;
        auto findLatestSnapshot() const -> std::string;
        // Sealed segments in seq order plus the active wal.log last (if it
        // exists). A crash between seal-rename and fresh-file creation leaves
        // only sealed segments; recovery handles that.
        auto findWalFiles() const -> std::vector<std::filesystem::path>;
        // Delete all but the newest K_SNAPSHOTS_TO_KEEP snapshots so the
        // data dir does not grow without bound. Best-effort: failures are
        // logged, never fatal to compact().
        void pruneOldSnapshots() const;

        // Seal the active wal.log into wal_<seq>.log and open a fresh active
        // file. Caller must hold mutex_; wal_ must be non-null. Best-effort
        // rollback keeps wal_ non-null: on fresh-file failure the sealed
        // file is moved back and reopened in Append mode.
        auto sealActiveWalLocked() -> Result<void>;
        // Stage a fresh truncated WAL at <path>.tmp, fsync, rename over
        // <path>, fsync the dir, then adopt it as wal_ (resetting active
        // counters). Caller must hold mutex_.
        auto replaceWalLocked(const std::filesystem::path& path) -> Result<void>;
        // Delete sealed wal_<seq>.log segments; the latest snapshot covers
        // them. Caller must hold mutex_. Best-effort: failures are logged.
        void deleteSealedSegmentsLocked();

        // Append every queued entry to the current WAL writer. Caller must hold
        // mutex_; internally takes queue_mutex_ (lock order: mutex_ >
        // queue_mutex_). Store writers call onWrite() while holding their own
        // lock, which only ever takes queue_mutex_
        // Returns whether anything was appended and whether the batch
        // completed (an append error abandons the rest of the batch).
        auto drainQueueLocked() -> DrainResult;

        auto clock() const -> const Clock& {
            return clock_ != nullptr ? *clock_ : static_cast<const Clock&>(real_clock_fallback_);
        }

        Options opts_;
        CacheStore& store_;
        Clock* clock_ = nullptr;
        RealClock real_clock_fallback_;

        // Pending WAL entries enqueued by onWrite(); drained under mutex_.
        std::mutex queue_mutex_;
        std::deque<WalEntry> pending_wal_;

        std::unique_ptr<WalWriter> wal_;
        // Active-file entries/bytes, advanced under mutex_, read lock-free by
        // walNeedsCompact() on the housekeeping path. Reset by seal/rotate.
        std::atomic<size_t> wal_entry_count_{0};
        std::atomic<size_t> wal_bytes_{0};
        // Sealed-segment totals; added on seal, cleared when a compact
        // deletes the segments. walNeedsCompact() sums both sides.
        std::atomic<size_t> wal_sealed_entries_{0};
        std::atomic<uint64_t> wal_sealed_bytes_{0};
        // Next segment sequence number. Guarded by mutex_; set at recover
        // from the max sealed seq on disk.
        uint64_t wal_next_seq_ = 1;
        std::mutex mutex_;
        // Set during recover()'s snapshot/WAL load; atomic because onWrite checks
        // it from store call paths on other threads.
        std::atomic<bool> loading_{false};
    };
} // namespace cinder
