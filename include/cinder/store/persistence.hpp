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

    // Create snapshot and truncate WAL.
    auto compact() -> Result<void>;

    // Final flush on graceful shutdown.
    void shutdown();

    [[nodiscard]] auto enabled() const -> bool { return opts_.enabled; }

    [[nodiscard]] auto snapshotInterval() const -> size_t { return opts_.snapshot_interval_s; }

  private:

    auto createSnapshot() -> Result<void>;
    auto replayWal(const std::filesystem::path& path) -> Result<void>;
    auto findLatestSnapshot() const -> std::string;
    auto findWalPath() const -> std::string;

    // Append every queued entry to the current WAL writer. Caller must hold
    // mutex_; internally takes queue_mutex_ (lock order: mutex_ >
    // queue_mutex_). Store writers call onWrite() while holding their own
    // lock, which only ever takes queue_mutex_
    void drainQueueLocked();

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
    size_t wal_entry_count_ = 0;
    std::mutex mutex_;
    // Set during recover()'s snapshot/WAL load; atomic because onWrite checks
    // it from store call paths on other threads.
    std::atomic<bool> loading_{false};
};
} // namespace cinder
