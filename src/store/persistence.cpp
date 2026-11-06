#include "cinder/store/persistence.hpp"

#include <algorithm>
#include <charconv>
#include <chrono>
#include <filesystem>
#include <format>
#include <fstream>
#include <utility>

#include "cinder/common/tracing.hpp"
#include "cinder/store/cache_store.hpp"
#include "cinder/store/detail/io_utils.hpp"
#include "cinder/store/snapshot.hpp"
#include "cinder/store/wal.hpp"

namespace cinder {
    using std::chrono::milliseconds;

    namespace {
        constexpr const char* K_WAL_FILENAME = "wal.log";
        constexpr const char* K_WAL_SEGMENT_PREFIX = "wal_";
        constexpr const char* K_WAL_SEGMENT_SUFFIX = ".log";
        constexpr size_t K_WAL_HEADER_SIZE = 8; // magic(4) + version(4)
        constexpr const char* K_SNAPSHOT_PREFIX = "snapshot_";
        constexpr const char* K_SNAPSHOT_SUFFIX = ".dat";
        constexpr const char* K_CURRENT_FILENAME = "CURRENT";
        // Retain the latest two snapshots: the newest for recovery, the
        // previous as a crash fallback while the newest is being written.
        constexpr size_t K_SNAPSHOTS_TO_KEEP = 2;

        // Publish the newest snapshot via a CURRENT pointer file
        // (RocksDB/LevelDB MANIFEST pattern): write tmp, fsync file, rename,
        // fsync dir. Recovery prefers CURRENT and falls back to a directory
        // scan, so a crash mid-publish never loses the previous snapshot.
        auto publishCurrent(const std::filesystem::path& dir, std::string_view snap_filename)
            -> Result<void> {
            const auto current_path = dir / K_CURRENT_FILENAME;
            const auto tmp_path =
                std::filesystem::path(std::string(current_path.string()) + ".tmp");
            {
                std::ofstream out(tmp_path, std::ios::binary | std::ios::trunc);
                if (!out.is_open()) {
                    return err(Error(Errc::InternalError, "CURRENT open failed"));
                }

                out.write(snap_filename.data(), static_cast<std::streamsize>(snap_filename.size()));
                out.put('\n');
                out.flush();
                if (!out.good()) {
                    return err(Error(Errc::InternalError, "CURRENT write failed"));
                }

                out.close();
                if (!out.good()) {
                    return err(Error(Errc::InternalError, "CURRENT close failed"));
                }
            }
            if (!detail::syncFileContents(tmp_path.string())) {
                std::error_code ec;
                std::filesystem::remove(tmp_path, ec);
                return err(Error(Errc::InternalError, "CURRENT fsync failed"));
            }

            std::error_code ec;
            std::filesystem::rename(tmp_path, current_path, ec);
            if (ec) {
                std::filesystem::remove(tmp_path, ec);
                return err(Error(Errc::InternalError, "CURRENT rename failed"));
            }
            if (!detail::syncDirectory(current_path.string())) {
                return err(Error(Errc::InternalError, "CURRENT dir fsync failed"));
            }
            return ok();
        }

        // Read the CURRENT pointer, returning the snapshot filename or empty.
        auto readCurrent(const std::filesystem::path& dir) -> std::string {
            const auto current_path = dir / K_CURRENT_FILENAME;
            if (!std::filesystem::exists(current_path)) {
                return {};
            }

            std::ifstream in(current_path);
            std::string name;
            if (in && std::getline(in, name)) {
                while (!name.empty() && (name.back() == '\n' || name.back() == '\r')) {
                    name.pop_back();
                }
                return name;
            }
            return {};
        }

        // Zero-padded so lexicographic order equals numeric order, even
        // beyond seq 9 (no fragile name > comparison).
        auto walSegmentName(uint64_t seq) -> std::string {
            return std::format("{}{:020}{}", K_WAL_SEGMENT_PREFIX, seq, K_WAL_SEGMENT_SUFFIX);
        }

        // Parse wal_<seq>.log filenames; nullopt for anything else.
        auto parseWalSegmentSeq(std::string_view name) -> std::optional<uint64_t> {
            std::string_view prefix(K_WAL_SEGMENT_PREFIX);
            std::string_view suffix(K_WAL_SEGMENT_SUFFIX);
            if (name.size()
                <= prefix.size()
                + suffix.size()
                || !name.starts_with(prefix)
                || !name.ends_with(suffix)) {
                return std::nullopt;
            }

            const auto digits =
                name.substr(prefix.size(), name.size() - prefix.size() - suffix.size());
            uint64_t seq = 0;
            const auto [ptr, ec] =
                std::from_chars(digits.data(), digits.data() + digits.size(), seq);
            if (ec != std::errc{} || ptr != digits.data() + digits.size()) {
                return std::nullopt;
            }
            return seq;
        }
    } // namespace

    PersistenceManager::PersistenceManager(Options opts, CacheStore& store, Clock* clock) :
        opts_(std::move(opts)),
        store_(store),
        clock_(clock) {}

    auto PersistenceManager::recover() -> Result<void> {
        Span span("persistence.recover", captureTraceContext());
        if (!opts_.enabled) {
            return ok();
        }

        std::filesystem::create_directories(opts_.data_dir);
        CINDER_DEBUG("data directory ready", {"path", opts_.data_dir});
        loading_ = true;
        // Load snapshot if present (streaming: O(1) memory, no full vector).
        auto snap_path = findLatestSnapshot();
        if (!snap_path.empty()) {
            SnapshotReader reader(snap_path);
            size_t snap_count = 0;
            Version unused_next = 0;
            auto result = reader.readStream(unused_next, [&](SnapshotEntry& entry) {
                VersionedEntry ve;
                ve.value = std::move(entry.value);
                ve.setVersion(entry.version);
                ve.writer_node_hash = entry.writer_node_hash;
                ve.setHasTtl(entry.has_ttl);
                ve.setTombstone(entry.tombstone);
                if (entry.has_ttl && entry.expires_at_ms > 0) {
                    // Preserve the entry's remaining wall-clock lifetime across
                    // restart: convert the stored absolute expiry back onto the
                    // local (possibly simulated) steady basis instead of expiring
                    // it immediately.
                    const uint64_t now_ms = nowSystemMs(clock_);
                    if (entry.expires_at_ms <= now_ms) {
                        return; // skip expired
                    }

                    auto remaining_ms =
                        milliseconds{static_cast<int64_t>(entry.expires_at_ms - now_ms)};
                    if (remaining_ms.count() < 1) {
                        remaining_ms = milliseconds{1};
                    }
                    ve.expires_at = clock().now() + remaining_ms;
                }

                [[maybe_unused]]
                auto res = store_.putVersioned(entry.key, std::move(ve));
                // WAL records carry no freq (frozen v2 framing): only the
                // snapshot restores policy state; WAL-only keys start at 1.
                if (entry.freq > 1) {
                    store_.restoreFreq(entry.key, entry.freq);
                }
                ++snap_count;
            });
            if (!result.has_value()) {
                loading_ = false;
                span.setStatus(SpanStatus::Error);
                return err(result.error());
            }
            CINDER_INFO("recovered entries from snapshot", {"count", snap_count});
        }

        // Replay sealed segments in seq order, then the active file. The log
        // is kept (append mode): entries already applied to the store stay on
        // disk, so a crash before the next snapshot loses nothing. Counters
        // seed from the replayed files.
        size_t active_records = 0;
        size_t sealed_records = 0;
        uint64_t active_bytes = 0;
        uint64_t sealed_bytes = 0;
        uint64_t max_sealed_seq = 0;
        bool have_sealed = false;
        for (const auto& wal_file : findWalFiles()) {
            const bool is_active = wal_file.filename() == K_WAL_FILENAME;
            std::error_code size_ec;
            const uint64_t file_bytes = std::filesystem::file_size(wal_file, size_ec);
            auto wal_result = replayWal(wal_file);
            if (!wal_result.has_value()) {
                loading_ = false;
                span.setStatus(SpanStatus::Error);
                return err(wal_result.error());
            }
            if (is_active) {
                active_records = wal_result.value();
                active_bytes = size_ec ? 0 : file_bytes;
            } else {
                sealed_records += wal_result.value();
                if (!size_ec) {
                    sealed_bytes += file_bytes;
                }
                if (auto seq = parseWalSegmentSeq(wal_file.filename().string())) {
                    have_sealed = true;
                    max_sealed_seq = std::max(max_sealed_seq, seq.value());
                }
            }
        }

        loading_ = false;
        auto new_wal_path = std::filesystem::path(opts_.data_dir) / K_WAL_FILENAME;
        wal_ = std::make_unique<WalWriter>(new_wal_path.string(), WalOpenMode::Append);
        wal_next_seq_ = have_sealed ? max_sealed_seq + 1 : 1;
        wal_entry_count_.store(active_records, std::memory_order_relaxed);
        wal_bytes_.store(static_cast<size_t>(active_bytes), std::memory_order_relaxed);
        wal_sealed_entries_.store(sealed_records, std::memory_order_relaxed);
        wal_sealed_bytes_.store(sealed_bytes, std::memory_order_relaxed);
        span.setStatus(SpanStatus::Ok);
        return ok();
    }

    void PersistenceManager::onWrite(const WalEntry& entry) {
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

    auto PersistenceManager::drainQueueLocked() -> DrainResult {
        // Caller holds mutex_. Swap the batch out under queue_mutex_, then
        // append as one group-commit write without holding queue_mutex_.
        std::vector<WalEntry> batch;
        {
            std::scoped_lock lock(queue_mutex_);
            if (pending_wal_.empty()) {
                return DrainResult::Empty;
            }

            batch.reserve(pending_wal_.size());
            for (auto& entry : pending_wal_) {
                batch.push_back(std::move(entry));
            }
            pending_wal_.clear();
        }

        auto appended = wal_->appendBatch(batch);
        if (!appended.has_value()) {
            CINDER_ERROR("WAL append failed", {"reason", appended.error().message()});
            // Requeue the whole batch so a transient error drops nothing;
            // a single group-commit write either lands or it does not, so
            // there is no partial prefix to account for.
            std::scoped_lock lock(queue_mutex_);
            for (size_t j = batch.size(); j > 0; --j) {
                pending_wal_.push_front(std::move(batch[j - 1]));
            }
            return DrainResult::Partial;
        }

        wal_entry_count_.fetch_add(batch.size(), std::memory_order_relaxed);
        wal_bytes_.fetch_add(appended.value(), std::memory_order_relaxed);
        return DrainResult::Complete;
    }

    void PersistenceManager::flush() {
        Span span("persistence.flush", captureTraceContext());
        std::scoped_lock lock(mutex_);
        if (!wal_) {
            span.setStatus(SpanStatus::Ok);
            return;
        }
        if (drainQueueLocked() == DrainResult::Empty) {
            span.setStatus(SpanStatus::Ok);
            return; // nothing pending: skip the flush syscall
        }
        if (opts_.wal_fsync) {
            // sync() flushes userspace buffers first, then fdatasyncs.
            if (auto synced = wal_->sync(); !synced.has_value()) {
                CINDER_ERROR("WAL fsync failed", {"reason", synced.error().message()});
                span.setStatus(SpanStatus::Error);
                return;
            }
        } else {
            wal_->flush();
        }
        // Size-based rotation: seal oversized active files so no single
        // recovery file grows without bound. Sealed segments are deleted by
        // the next successful compact.
        if (opts_.max_wal_segment_bytes > 0) {
            std::error_code size_ec;
            const auto active_size = std::filesystem::file_size(
                std::filesystem::path(opts_.data_dir) / K_WAL_FILENAME,
                size_ec
            );
            if (!size_ec
                && active_size
                >= opts_.max_wal_segment_bytes
                && active_size
                > K_WAL_HEADER_SIZE) {
                if (auto sealed = sealActiveWalLocked(); !sealed.has_value()) {
                    CINDER_ERROR("WAL seal failed", {"reason", sealed.error().message()});
                    span.setStatus(SpanStatus::Error);
                    return;
                }
            }
        }
        span.setStatus(SpanStatus::Ok);
    }

    auto PersistenceManager::compact() -> Result<void> {
        Span span("persistence.compact", captureTraceContext());
        if (!opts_.enabled) {
            return ok();
        }

        // Safe under pooling: onWrite only ever touches queue_mutex_, so holding
        // mutex_ across createSnapshot()'s store traversal cannot cycle with
        // store writers anymore. Drain ordering guarantees no loss:
        //   1. drain pending entries into the CURRENT WAL (durable pre-truncate)
        //   2. snapshot the store (captures every entry applied before step 2)
        //   3. rotate to a fresh WAL first, then drain anything enqueued
        //      during the snapshot into the NEW wal. The old code drained a
        //      second batch into the old WAL and then deleted the file, which
        //      lost exactly those second-batch entries when the snapshot had
        //      missed them.
        CINDER_INFO("compaction started");
        std::scoped_lock lock(mutex_);
        if (wal_) {
            drainQueueLocked();
        }

        size_t entry_count = store_.liveEntries().size();
        auto result = createSnapshot();
        if (!result.has_value()) {
            span.setStatus(SpanStatus::Error);
            return err(result.error());
        }

        pruneOldSnapshots();
        // The new snapshot covers every sealed segment: delete them, then
        // replace the active file with a fresh WAL and drain into it.
        deleteSealedSegmentsLocked();
        auto wal_path = std::filesystem::path(opts_.data_dir) / K_WAL_FILENAME;
        if (auto rotated = replaceWalLocked(wal_path); !rotated.has_value()) {
            span.setStatus(SpanStatus::Error);
            return err(rotated.error());
        }
        if (wal_) {
            drainQueueLocked();
        }

        CINDER_INFO("compaction complete", {"entries", entry_count});
        span.setAttribute({"entries", entry_count});
        span.setStatus(SpanStatus::Ok);
        return ok();
    }

    void PersistenceManager::shutdown() {
        Span span("persistence.shutdown", captureTraceContext());
        if (!opts_.enabled) {
            span.setStatus(SpanStatus::Ok);
            return;
        }

        std::scoped_lock lock(mutex_);
        if (wal_) {
            drainQueueLocked();
            wal_->flush();
        }
        if (auto snap_result = createSnapshot(); !snap_result.has_value()) {
            CINDER_ERROR("shutdown snapshot failed", {"reason", snap_result.error().message()});
            span.setStatus(SpanStatus::Error);
            return;
        }
        span.setStatus(SpanStatus::Ok);
    }

    auto PersistenceManager::createSnapshot() -> Result<void> {
        auto filename = K_SNAPSHOT_PREFIX
            + std::to_string(system_clock::now().time_since_epoch().count())
            + K_SNAPSHOT_SUFFIX;

        auto snap_path = std::filesystem::path(opts_.data_dir) / filename;
        // Single copy: liveEntries() already snapshots (skipping expired
        // entries, like forEach did); move its strings into SnapshotEntry
        // instead of copying them a second time via the forEach visitor.
        std::vector<SnapshotEntry> all_entries;
        Version next_version = 0;
        auto live = store_.liveEntries();
        all_entries.reserve(live.size());
        for (auto& [key, ve] : live) {
            SnapshotEntry se;
            // Query LFU policy state before moving the key: liveEntries()
            // drops it, and a restart would otherwise reset hot entries.
            se.freq = store_.entryFreq(key);
            se.key = std::move(key);
            se.value = std::move(ve.value);
            se.version = ve.version();
            se.writer_node_hash = ve.writer_node_hash;
            se.has_ttl = ve.hasTtl();
            se.tombstone = ve.isTombstone();
            if (ve.hasTtl()) {
                se.expires_at_ms = toSystemMsOrDefault(clock_, ve.expires_at);
            } else {
                se.expires_at_ms = 0;
            }

            if (ve.version() >= next_version) {
                next_version = ve.version() + 1;
            }
            all_entries.push_back(std::move(se));
        }

        SnapshotWriter writer(snap_path.string());
        auto write_result = writer.write(next_version, all_entries);
        if (!write_result.has_value()) {
            return write_result;
        }

        CINDER_DEBUG("snapshot written", {"entries", all_entries.size()});
        // Publish via CURRENT so recovery does not depend on timestamp
        // ordering or clock skew. Best-effort: a failed publish only means
        // recovery falls back to the lexicographic scan.
        if (auto pub = publishCurrent(opts_.data_dir, filename); !pub.has_value()) {
            CINDER_WARN("CURRENT publish failed", {"reason", pub.error().message()});
        }
        return ok();
    }

    auto PersistenceManager::replayWal(const std::filesystem::path& wal_path) -> Result<size_t> {
        Span span("persistence.replay", captureTraceContext());
        WalReader reader(wal_path.string());
        size_t replayed = 0;
        size_t decoded = 0;
        while (reader.hasMore()) {
            auto entry = reader.next();
            if (!entry.has_value()) {
                if (reader.corrupted()) {
                    span.setStatus(SpanStatus::Error);
                    return err<size_t>(Error(Errc::CorruptData, "WAL checksum mismatch"));
                }
                break; // torn tail: clean crash artifact, stop with Ok
            }

            ++decoded;
            VersionedEntry ve;

            ve.value = std::move(entry->value);
            ve.setVersion(entry->version);
            ve.writer_node_hash = entry->writer_node_hash;
            ve.setHasTtl(entry->has_ttl);
            ve.setTombstone(entry->is_tombstone);
            if (entry->has_ttl && entry->expires_at_ms > 0) {
                // Preserve remaining wall-clock lifetime across restart (see
                // recover()).
                const uint64_t now_ms = nowSystemMs(clock_);
                if (entry->expires_at_ms <= now_ms) {
                    CINDER_TRACE("skipped expired WAL entry", {"key", entry->key});
                    continue; // skip expired
                }

                auto remaining_ms =
                    milliseconds{static_cast<int64_t>(entry->expires_at_ms - now_ms)};
                if (remaining_ms.count() < 1) {
                    remaining_ms = milliseconds{1};
                }
                ve.expires_at = clock().now() + remaining_ms;
            }
            if (entry->op == WalEntry::Op::Set) {
                [[maybe_unused]]
                auto put_res = store_.putVersioned(entry->key, std::move(ve));
            } else if (entry->op == WalEntry::Op::Del) {
                [[maybe_unused]]
                auto del_res = store_.remove(entry->key);
            }
            ++replayed;
        }
        if (replayed > 0) {
            CINDER_INFO("replayed WAL entries", {"count", replayed});
        }

        span.setAttribute({"entries", replayed});
        span.setStatus(SpanStatus::Ok);
        return decoded;
    }

    auto PersistenceManager::findLatestSnapshot() const -> std::string {
        if (!std::filesystem::exists(opts_.data_dir)) {
            return {};
        }

        // Prefer the CURRENT pointer published by createSnapshot; it is
        // exact under clock skew, whereas lexicographic order assumes
        // monotonic timestamps.
        const auto current_name = readCurrent(opts_.data_dir);
        if (!current_name.empty()) {
            std::string_view prefix(K_SNAPSHOT_PREFIX);
            std::string_view suffix(K_SNAPSHOT_SUFFIX);
            if (current_name.starts_with(prefix)
                && current_name.size()
                >= suffix.size()
                && current_name.substr(current_name.size() - suffix.size())
                == suffix) {
                const auto candidate = std::filesystem::path(opts_.data_dir) / current_name;
                if (std::filesystem::exists(candidate)) {
                    return candidate.string();
                }
            }
        }

        std::string latest;
        std::string_view prefix(K_SNAPSHOT_PREFIX);
        std::string_view suffix(K_SNAPSHOT_SUFFIX);
        for (const auto& entry : std::filesystem::directory_iterator(opts_.data_dir)) {
            auto name = entry.path().filename().string();
            if (name.size()
                > prefix.size()
                + suffix.size()
                && name.starts_with(prefix)
                && name.substr(name.size() - suffix.size())
                == suffix) {
                if (latest.empty() || name > latest) {
                    latest = entry.path().string();
                }
            }
        }
        return latest;
    }

    void PersistenceManager::pruneOldSnapshots() const {
        if (!std::filesystem::exists(opts_.data_dir)) {
            return;
        }

        std::string_view prefix(K_SNAPSHOT_PREFIX);
        std::string_view suffix(K_SNAPSHOT_SUFFIX);
        std::vector<std::filesystem::path> snaps;
        for (const auto& entry : std::filesystem::directory_iterator(opts_.data_dir)) {
            auto name = entry.path().filename().string();
            if (name.size()
                > prefix.size()
                + suffix.size()
                && name.starts_with(prefix)
                && name.substr(name.size() - suffix.size())
                == suffix) {
                snaps.push_back(entry.path());
            }
        }
        if (snaps.size() <= K_SNAPSHOTS_TO_KEEP) {
            return;
        }

        std::ranges::sort(snaps);
        // Never delete the CURRENT target: a crash between snapshot write
        // and prune must not remove the only pointer recovery trusts.
        const auto protected_name = readCurrent(opts_.data_dir);
        const std::filesystem::path protected_path = protected_name.empty()
            ? std::filesystem::path{}
            : std::filesystem::path(opts_.data_dir) / protected_name;

        size_t removed = 0;
        for (const auto& snap : snaps) {
            if (snaps.size() - removed <= K_SNAPSHOTS_TO_KEEP) {
                break;
            }
            if (snap == protected_path) {
                continue;
            }

            std::error_code ec;
            std::filesystem::remove(snap, ec);
            if (ec) {
                CINDER_WARN("snapshot prune failed", {"path", snap.string()});
            } else {
                ++removed;
            }
        }
        if (!detail::syncDirectory(opts_.data_dir)) {
            CINDER_WARN("snapshot prune dir fsync failed", {"path", opts_.data_dir});
        }
    }

    auto PersistenceManager::findWalFiles() const -> std::vector<std::filesystem::path> {
        std::vector<std::pair<uint64_t, std::filesystem::path>> sealed;
        std::filesystem::path active;
        if (!std::filesystem::exists(opts_.data_dir)) {
            return {};
        }
        for (const auto& entry : std::filesystem::directory_iterator(opts_.data_dir)) {
            const auto name = entry.path().filename().string();
            if (name == K_WAL_FILENAME) {
                active = entry.path();
            } else if (auto seq = parseWalSegmentSeq(name)) {
                sealed.emplace_back(seq.value(), entry.path());
            }
        }

        std::ranges::sort(sealed, {}, &std::pair<uint64_t, std::filesystem::path>::first);
        std::vector<std::filesystem::path> files;
        files.reserve(sealed.size() + (active.empty() ? 0U : 1U));
        for (const auto& [_, path] : sealed) {
            files.push_back(path);
        }
        if (!active.empty()) {
            files.push_back(active);
        }
        return files;
    }

    auto PersistenceManager::replaceWalLocked(const std::filesystem::path& path) -> Result<void> {
        // Stage the fresh WAL at <path>.tmp, fsync it, rename over <path>,
        // fsync the directory — then adopt it. The old writer stays open
        // across the rename so wal_ is never null; resetting it afterwards
        // just closes the unlinked pre-rotation inode.
        auto tmp = std::filesystem::path(std::string(path.string()) + ".tmp");
        auto fresh = std::make_unique<WalWriter>(tmp.string());
        fresh->flush();
        if (!detail::syncFileContents(tmp.string())) {
            std::error_code ec;
            std::filesystem::remove(tmp, ec);
            return err(Error(Errc::InternalError, "WAL rotate fsync failed"));
        }

        {
            std::error_code ec;
            std::filesystem::rename(tmp, path, ec);
            if (ec) {
                std::filesystem::remove(tmp, ec);
                return err(Error(Errc::InternalError, "WAL rotate failed: " + ec.message()));
            }
        }

        if (!detail::syncDirectory(path.string())) {
            return err(Error(Errc::InternalError, "WAL dir fsync failed"));
        }

        wal_.reset();
        wal_ = std::move(fresh);
        std::error_code size_ec;
        const auto fresh_bytes = std::filesystem::file_size(path, size_ec);

        wal_entry_count_.store(0, std::memory_order_relaxed);
        wal_bytes_.store(size_ec ? 0 : static_cast<size_t>(fresh_bytes), std::memory_order_relaxed);
        return ok();
    }

    auto PersistenceManager::sealActiveWalLocked() -> Result<void> {
        const auto wal_path = std::filesystem::path(opts_.data_dir) / K_WAL_FILENAME;
        wal_->flush();
        std::error_code size_ec;
        const auto sealed_bytes = std::filesystem::file_size(wal_path, size_ec);
        const size_t sealed_entries = wal_entry_count_.load(std::memory_order_relaxed);
        // Close before rename: Windows cannot rename an open file, and on
        // POSIX the open handles would keep pointing at the sealed inode.
        wal_.reset();
        const auto seg_path = std::filesystem::path(opts_.data_dir) / walSegmentName(wal_next_seq_);
        {
            std::error_code ec;
            std::filesystem::rename(wal_path, seg_path, ec);
            if (ec) {
                // Roll back: reopen whatever is at wal.log (likely nothing)
                // so wal_ is non-null and queued writes are not dropped.
                wal_ = std::make_unique<WalWriter>(wal_path.string(), WalOpenMode::Append);
                return err(Error(Errc::InternalError, "WAL seal failed: " + ec.message()));
            }
        }

        if (!detail::syncDirectory(seg_path.string())) {
            wal_ = std::make_unique<WalWriter>(wal_path.string(), WalOpenMode::Append);
            return err(Error(Errc::InternalError, "WAL seal dir fsync failed"));
        }
        if (auto fresh = replaceWalLocked(wal_path); !fresh.has_value()) {
            // Fresh-file staging failed after the seal rename; move the
            // segment back and reopen it so no writer state is lost.
            std::error_code ec;
            std::filesystem::rename(seg_path, wal_path, ec);
            wal_ = std::make_unique<WalWriter>(wal_path.string(), WalOpenMode::Append);
            return err(fresh.error());
        }
        if (!size_ec) {
            wal_sealed_bytes_.fetch_add(
                static_cast<uint64_t>(sealed_bytes),
                std::memory_order_relaxed
            );
        }

        wal_sealed_entries_.fetch_add(sealed_entries, std::memory_order_relaxed);
        ++wal_next_seq_;
        return ok();
    }

    void PersistenceManager::deleteSealedSegmentsLocked() {
        for (const auto& file : findWalFiles()) {
            if (file.filename() == K_WAL_FILENAME) {
                continue;
            }

            std::error_code ec;
            std::filesystem::remove(file, ec);
            if (ec) {
                CINDER_WARN("sealed WAL delete failed", {"path", file.string()});
            }
        }

        wal_sealed_entries_.store(0, std::memory_order_relaxed);
        wal_sealed_bytes_.store(0, std::memory_order_relaxed);
        if (!detail::syncDirectory(opts_.data_dir)) {
            CINDER_WARN("sealed WAL dir fsync failed", {"path", opts_.data_dir});
        }
    }
} // namespace cinder
