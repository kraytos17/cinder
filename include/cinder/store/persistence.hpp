#pragma once

#include <cstddef>
#include <filesystem>
#include <memory>
#include <mutex>
#include <string>

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

    PersistenceManager(Options opts, CacheStore& store);
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

    Options opts_;
    CacheStore& store_;
    std::unique_ptr<WalWriter> wal_;
    size_t wal_entry_count_ = 0;
    std::mutex mutex_;
    bool loading_ = false;
};
} // namespace cinder
