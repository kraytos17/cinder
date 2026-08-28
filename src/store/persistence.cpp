#include "cinder/store/persistence.hpp"

#include <chrono>
#include <filesystem>
#include <utility>

#include "cinder/common/logger.hpp"
#include "cinder/store/cache_store.hpp"
#include "cinder/store/snapshot.hpp"
#include "cinder/store/wal.hpp"

namespace cinder {
using std::chrono::milliseconds;

namespace {
constexpr const char* K_WAL_FILENAME = "wal.log";
constexpr const char* K_SNAPSHOT_PREFIX = "snapshot_";
constexpr const char* K_SNAPSHOT_SUFFIX = ".dat";
} // namespace

PersistenceManager::PersistenceManager(Options opts, CacheStore& store, Clock* clock)
    : opts_(std::move(opts)),
      store_(store),
      clock_(clock) {}

auto
PersistenceManager::recover() -> Result<void> {
    if (!opts_.enabled) {
        return ok();
    }

    std::filesystem::create_directories(opts_.data_dir);
    Logger::debug("cinder persistence: data directory ready path={}", opts_.data_dir);
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
            VersionedEntry ve;
            ve.value = std::move(entry.value);
            ve.version = entry.version;
            ve.writer_node_hash = entry.writer_node_hash;
            ve.has_ttl = entry.has_ttl;

            if (entry.has_ttl && entry.expires_at_ms > 0) {
                // Preserve the entry's remaining wall-clock lifetime across
                // restart: convert the stored absolute expiry back onto the
                // local (possibly simulated) steady basis instead of expiring
                // it immediately.
                const uint64_t now_ms = nowSystemMs(clock_);
                if (entry.expires_at_ms <= now_ms) {
                    continue; // skip expired
                }

                auto remaining_ms =
                    milliseconds{static_cast<int64_t>(entry.expires_at_ms - now_ms)};
                if (remaining_ms.count() < 1) {
                    remaining_ms = milliseconds{1};
                }
                ve.expires_at = clock().now() + remaining_ms;
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
    auto new_wal_path = std::filesystem::path(opts_.data_dir) / K_WAL_FILENAME;
    wal_ = std::make_unique<WalWriter>(new_wal_path.string());
    wal_entry_count_ = 0;
    return ok();
}

void
PersistenceManager::onWrite(const WalEntry& entry) {
    // Enqueue-only: store writers invoke this while holding THEIR OWN lock,
    // so this path must never take mutex_ — that would
    // invert against compact()'s mutex_-then-forEach(store) ordering. Entries
    // land in pending_wal_ and are appended by drainQueueLocked() from
    // flush/compact/shutdown. loading_ is checked at drain time, so entries
    // pushed during recovery are simply discarded with the queue.
    std::scoped_lock lock(queue_mutex_);
    if (loading_.load(std::memory_order_acquire)) {
        return;
    }
    pending_wal_.push_back(entry);
}

void
PersistenceManager::drainQueueLocked() {
    // Caller holds mutex_. Swap the batch out under queue_mutex_, then append
    // without holding queue_mutex_
    std::deque<WalEntry> batch;
    {
        std::scoped_lock lock(queue_mutex_);
        if (pending_wal_.empty()) {
            return;
        }
        batch.swap(pending_wal_);
    }

    for (auto& entry : batch) {
        if (auto result = wal_->append(entry); !result.has_value()) {
            Logger::error("WAL append failed: {}", result.error().message());
        }
        ++wal_entry_count_;
    }
}

void
PersistenceManager::flush() {
    std::scoped_lock lock(mutex_);
    if (wal_) {
        drainQueueLocked();
        wal_->flush();
    }
}

auto
PersistenceManager::compact() -> Result<void> {
    if (!opts_.enabled) {
        return ok();
    }

    // Safe under pooling: onWrite only ever touches queue_mutex_, so holding
    // mutex_ across createSnapshot()'s store traversal cannot cycle with
    // store writers anymore. Drain ordering guarantees no loss:
    //   1. drain pending entries into the CURRENT WAL (durable pre-truncate)
    //   2. snapshot the store (captures every entry applied before step 2)
    //   3. final drain, then swap writers; anything enqueued between steps
    //      lands in the fresh WAL (duplicate application is idempotent).
    Logger::info("cinder persistence: compaction started");
    std::scoped_lock lock(mutex_);
    if (wal_) {
        drainQueueLocked();
    }

    size_t entry_count = 0;
    for ([[maybe_unused]] const auto& [k, v] : store_.liveEntries()) {
        ++entry_count;
    }

    auto result = createSnapshot();
    if (!result.has_value()) {
        return err(result.error());
    }
    if (wal_) {
        drainQueueLocked();
        wal_.reset();
    }

    auto wal_path = std::filesystem::path(opts_.data_dir) / K_WAL_FILENAME;
    std::error_code ec;
    std::filesystem::remove(wal_path, ec);
    wal_ = std::make_unique<WalWriter>(wal_path.string());
    wal_entry_count_ = 0;
    Logger::info("cinder persistence: compaction complete entries={}", entry_count);
    return ok();
}

void
PersistenceManager::shutdown() {
    if (!opts_.enabled) {
        return;
    }

    std::scoped_lock lock(mutex_);
    if (wal_) {
        drainQueueLocked();
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
    std::vector<SnapshotEntry> all_entries;
    Version next_version = 0;
    for (const auto& [key, ve] : store_.liveEntries()) {
        SnapshotEntry se;
        se.key = key;
        se.value = ve.value;
        se.version = ve.version;
        se.writer_node_hash = ve.writer_node_hash;
        se.has_ttl = ve.has_ttl;

        if (ve.has_ttl) {
            se.expires_at_ms = toSystemMsOrDefault(clock_, ve.expires_at);
        } else {
            se.expires_at_ms = 0;
        }

        se.freq = 0;
        if (ve.version >= next_version) {
            next_version = ve.version + 1;
        }
        all_entries.push_back(std::move(se));
    }

    SnapshotWriter writer(snap_path.string());
    auto write_result = writer.write(next_version, all_entries);
    if (write_result.has_value()) {
        Logger::debug("cinder persistence: snapshot written entries={}", all_entries.size());
    }
    return write_result;
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
            // Preserve remaining wall-clock lifetime across restart (see
            // recover()).
            const uint64_t now_ms = nowSystemMs(clock_);
            if (entry->expires_at_ms <= now_ms) {
                Logger::trace("cinder persistence: skipped expired WAL entry key={}", entry->key);
                continue; // skip expired
            }

            auto remaining_ms = milliseconds{static_cast<int64_t>(entry->expires_at_ms - now_ms)};
            if (remaining_ms.count() < 1) {
                remaining_ms = milliseconds{1};
            }
            ve.expires_at = clock().now() + remaining_ms;
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
