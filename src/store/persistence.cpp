#include "cinder/store/persistence.hpp"

#include <chrono>
#include <filesystem>
#include <utility>

#include "cinder/common/logger.hpp"
#include "cinder/store/cache_store.hpp"
#include "cinder/store/snapshot.hpp"
#include "cinder/store/wal.hpp"

namespace cinder {
using std::chrono::duration_cast;
using std::chrono::milliseconds;

namespace {
constexpr const char* K_WAL_FILENAME = "wal.log";
constexpr const char* K_SNAPSHOT_PREFIX = "snapshot_";
constexpr const char* K_SNAPSHOT_SUFFIX = ".dat";
} // namespace

PersistenceManager::PersistenceManager(Options opts, CacheStore& store)
    : opts_(std::move(opts)),
      store_(store) {}

auto
PersistenceManager::recover() -> Result<void> {
    if (!opts_.enabled) {
        return ok();
    }

    std::filesystem::create_directories(opts_.data_dir);
    loading_ = true;

    // Load snapshot if present
    auto snap_path = findLatestSnapshot();
    if (!snap_path.empty()) {
        SnapshotReader reader(snap_path);
        auto result = reader.readAll();
        if (!result.has_value()) {
            loading_ = false;
            return err(result.error());
        }

        auto& data = result.value();
        for (auto& entry : data.entries) {
            // Skip already-expired entries
            if (entry.has_ttl && entry.expires_at_ms > 0) {
                auto now_ms = static_cast<uint64_t>(
                    duration_cast<milliseconds>(system_clock::now().time_since_epoch()).count());
                if (entry.expires_at_ms <= now_ms) {
                    continue;
                }
            }

            VersionedEntry ve;
            ve.value = std::move(entry.value);
            ve.version = entry.version;
            ve.writer_node_hash = entry.writer_node_hash;
            ve.has_ttl = entry.has_ttl;
            if (entry.has_ttl) {
                ve.expires_at = std::chrono::steady_clock::now() + std::chrono::milliseconds(1);
            }
            [[maybe_unused]] auto res = store_.putVersioned(entry.key, std::move(ve));
        }

        Logger::info("recovered {} entries from snapshot", data.entries.size());
    }

    // Replay WAL if present
    auto wal_path = findWalPath();
    if (!wal_path.empty()) {
        auto wal_result = replayWal(std::filesystem::path(wal_path));
        if (!wal_result.has_value()) {
            loading_ = false;
            return err(wal_result.error());
        }
    }

    loading_ = false;

    // Open fresh WAL for writes
    auto new_wal_path = std::filesystem::path(opts_.data_dir) / K_WAL_FILENAME;
    wal_ = std::make_unique<WalWriter>(new_wal_path.string());
    wal_entry_count_ = 0;

    return ok();
}

void
PersistenceManager::onWrite(const WalEntry& entry) {
    if (loading_ || !wal_) {
        return;
    }

    std::scoped_lock lock(mutex_);
    if (auto result = wal_->append(entry); !result.has_value()) {
        Logger::error("WAL append failed: {}", result.error().message());
    }
    ++wal_entry_count_;
}

void
PersistenceManager::flush() {
    std::scoped_lock lock(mutex_);
    if (wal_) {
        wal_->flush();
    }
}

auto
PersistenceManager::compact() -> Result<void> {
    if (!opts_.enabled) {
        return ok();
    }

    std::scoped_lock lock(mutex_);

    auto result = createSnapshot();
    if (!result.has_value()) {
        return err(result.error());
    }

    // Truncate WAL
    wal_.reset();
    auto wal_path = std::filesystem::path(opts_.data_dir) / K_WAL_FILENAME;
    std::error_code ec;
    std::filesystem::remove(wal_path, ec);
    wal_ = std::make_unique<WalWriter>(wal_path.string());
    wal_entry_count_ = 0;

    return ok();
}

void
PersistenceManager::shutdown() {
    if (!opts_.enabled) {
        return;
    }

    std::scoped_lock lock(mutex_);
    if (wal_) {
        wal_->flush();
    }
    [[maybe_unused]] auto snap_result = createSnapshot();
}

auto
PersistenceManager::createSnapshot() -> Result<void> {
    auto filename = K_SNAPSHOT_PREFIX
                    + std::to_string(system_clock::now().time_since_epoch().count())
                    + K_SNAPSHOT_SUFFIX;
    auto snap_path = std::filesystem::path(opts_.data_dir) / filename;

    // Collect all entries from store
    std::vector<SnapshotEntry> all_entries;
    Version next_version = 0;

    store_.forEach([&](const std::string& key, const VersionedEntry& ve) {
        SnapshotEntry se;
        se.key = key;
        se.value = ve.value;
        se.version = ve.version;
        se.writer_node_hash = ve.writer_node_hash;
        se.has_ttl = ve.has_ttl;
        if (ve.has_ttl) {
            RealClock real_clock;
            auto sys = toSystemExpiry(real_clock, ve.expires_at);
            se.expires_at_ms = static_cast<uint64_t>(
                std::chrono::duration_cast<std::chrono::milliseconds>(sys.time_since_epoch())
                    .count());
        } else {
            se.expires_at_ms = 0;
        }

        se.freq = 0;
        if (ve.version >= next_version) {
            next_version = ve.version + 1;
        }
        all_entries.push_back(std::move(se));
    });

    SnapshotWriter writer(snap_path.string());
    return writer.write(next_version, all_entries);
}

auto
PersistenceManager::replayWal(const std::filesystem::path& wal_path) -> Result<void> {
    WalReader reader(wal_path.string());

    size_t replayed = 0;
    while (reader.hasMore()) {
        auto entry = reader.next();
        if (!entry.has_value()) {
            break;
        }

        VersionedEntry ve;
        ve.value = std::move(entry->value);
        ve.version = entry->version;
        ve.writer_node_hash = entry->writer_node_hash;
        ve.has_ttl = entry->has_ttl;
        if (entry->has_ttl && entry->expires_at_ms > 0) {
            auto now_ms =
                static_cast<uint64_t>(std::chrono::duration_cast<std::chrono::milliseconds>(
                    std::chrono::system_clock::now().time_since_epoch())
                        .count());
            if (entry->expires_at_ms <= now_ms) {
                continue; // skip expired
            }
            ve.expires_at = std::chrono::steady_clock::now() + std::chrono::milliseconds(1);
        }

        if (entry->op == WalEntry::Op::Set) {
            [[maybe_unused]] auto put_res = store_.putVersioned(entry->key, std::move(ve));
        } else if (entry->op == WalEntry::Op::Del) {
            [[maybe_unused]] auto del_res = store_.remove(entry->key);
        }
        ++replayed;
    }

    if (replayed > 0) {
        Logger::info("replayed {} WAL entries", replayed);
    }
    return ok();
}

auto
PersistenceManager::findLatestSnapshot() const -> std::string {
    std::string latest;
    if (!std::filesystem::exists(opts_.data_dir)) {
        return {};
    }

    std::string_view prefix(K_SNAPSHOT_PREFIX);
    std::string_view suffix(K_SNAPSHOT_SUFFIX);
    for (const auto& entry : std::filesystem::directory_iterator(opts_.data_dir)) {
        auto name = entry.path().filename().string();
        if (name.size() > prefix.size() + suffix.size() && name.starts_with(prefix)
            && name.substr(name.size() - suffix.size()) == suffix) {
            if (latest.empty() || name > latest) {
                latest = entry.path().string();
            }
        }
    }
    return latest;
}

auto
PersistenceManager::findWalPath() const -> std::string {
    auto wal_path = std::filesystem::path(opts_.data_dir) / K_WAL_FILENAME;
    if (std::filesystem::exists(wal_path)) {
        return wal_path.string();
    }
    return {};
}
} // namespace cinder
