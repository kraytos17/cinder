#pragma once

#include <atomic>
#include <cstddef>
#include <deque>
#include <filesystem>
#include <memory>
#include <mutex>
#include <string>

#include "cinder/cluster/clock.hpp"
#include "cinder/common/status.hpp"
#include "cinder/store/wal.hpp"

namespace cinder {

    class CacheStore;

    class PersistenceManager {
    public:

        struct Options {
            std::string data_dir;
            bool enabled = false;
            size_t snapshot_interval_s = 60;
            size_t max_wal_entries = 10'000;
            // Rotate/compact once the WAL exceeds this many bytes (0 =
            // disabled). Entry counts alone under-trigger on large values;
            // bytes alone under-trigger on many tiny tombstones — hence both.
            size_t max_wal_bytes = 64U << 20U;
            // fdatasync on every flush: acknowledged writes survive an OS
            // crash, not just a process crash. Off by default (page-cache
            // durability); enable when the WAL is the durability story.
            bool wal_fsync = false;
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
            const bool entries_hit = opts_.max_wal_entries
                > 0
                && wal_entry_count_.load(std::memory_order_relaxed)
                >= opts_.max_wal_entries;
            const bool bytes_hit = opts_.max_wal_bytes
                > 0
                && wal_bytes_.load(std::memory_order_relaxed)
                >= opts_.max_wal_bytes;
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
            return opts_.snapshot_interval_s;
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
        // Replays the WAL, returning the number of physically decoded records
        // (including expired-skipped ones still occupying the file) so the
        // caller can seed its byte/entry accounting.
        auto replayWal(const std::filesystem::path& path) -> Result<size_t>;
        auto findLatestSnapshot() const -> std::string;
        auto findWalPath() const -> std::string;
        // Delete all but the newest K_SNAPSHOTS_TO_KEEP snapshots so the
        // data dir does not grow without bound. Best-effort: failures are
        // logged, never fatal to compact().
        void pruneOldSnapshots() const;

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
        // Entries/bytes appended since the last truncate; advanced under
        // mutex_, read lock-free by walNeedsCompact() on the housekeeping path.
        std::atomic<size_t> wal_entry_count_{0};
        std::atomic<size_t> wal_bytes_{0};
        std::mutex mutex_;
        // Set during recover()'s snapshot/WAL load; atomic because onWrite checks
        // it from store call paths on other threads.
        std::atomic<bool> loading_{false};
    };
} // namespace cinder
