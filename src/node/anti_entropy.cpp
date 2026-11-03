#include "cinder/node/anti_entropy.hpp"

#include <algorithm>
#include <array>
#include <cstring>
#include <set>
#include <shared_mutex>
#include <unordered_map>
#include <utility>
#include <vector>
#include <xxhash.h>

#include "cinder/common/tracing.hpp"

namespace cinder {
    namespace {

        // Little-endian append helpers for the digest/sync blobs. The cluster is
        // homogeneous, but a fixed endianness keeps the wire format well-defined.
        void appendU32(std::string& out, uint32_t v) {
            std::array<char, sizeof(v)> buf{};
            std::memcpy(buf.data(), &v, buf.size());
            out.append(buf.data(), buf.size());
        }

        void appendU64(std::string& out, uint64_t v) {
            std::array<char, sizeof(v)> buf{};
            std::memcpy(buf.data(), &v, buf.size());
            out.append(buf.data(), buf.size());
        }

        auto readU32(const char*& p, const char* end, uint32_t& out) -> bool {
            if (static_cast<size_t>(end - p) < sizeof(uint32_t)) {
                return false;
            }

            std::memcpy(&out, p, sizeof(out));
            p += sizeof(out);
            return true;
        }

        auto readU64(const char*& p, const char* end, uint64_t& out) -> bool {
            if (static_cast<size_t>(end - p) < sizeof(uint64_t)) {
                return false;
            }

            std::memcpy(&out, p, sizeof(out));
            p += sizeof(out);
            return true;
        }

        auto readBytes(const char*& p, const char* end, size_t n, std::string& out) -> bool {
            if (n > static_cast<size_t>(end - p)) {
                return false;
            }

            out.assign(p, n);
            p += n;
            return true;
        }

        // Wire size of one serialized entry (values are never hashed).
        auto entryWireSize(const std::string& key, const VersionedEntry& entry) -> size_t {
            size_t n = sizeof(uint32_t)
                + key.size()
                + sizeof(uint64_t)
                + sizeof(uint64_t)
                + sizeof(uint8_t)
                + sizeof(uint32_t)
                + entry.value.size();
            if (entry.hasTtl()) {
                n += sizeof(uint64_t);
            }
            return n;
        }
    } // namespace

    AntiEntropyManager::AntiEntropyManager(
        CacheStore& store,
        ConsistentHashRing& ring,
        NodeId self,
        Clock& clock,
        Transport& transport,
        uint32_t num_buckets,
        MetricsCollector* metrics,
        size_t fetch_bytes,
        std::chrono::milliseconds full_rehash_interval
    ) :
        store_(store),
        ring_(ring),
        self_(std::move(self)),
        clock_(clock),
        transport_(transport),
        num_buckets_(num_buckets == 0 ? 1 : num_buckets),
        fetch_bytes_(fetch_bytes),
        full_rehash_interval_(full_rehash_interval),
        metrics_(metrics),
        xor_(std::make_unique<std::atomic<uint64_t>[]>(num_buckets_ == 0 ? 1 : num_buckets_)) {
        // Feed the incremental digest (P1). Unregistered in the destructor;
        // the store may outlive or predate this manager in tests.
        store_.setMutationSink(
            [this](std::string_view key, uint64_t version_and_ttl, uint64_t writer, bool removed) {
            onStoreMutation(key, version_and_ttl, writer, removed);
        }
        );
    }

    AntiEntropyManager::~AntiEntropyManager() {
        store_.setMutationSink(CacheStore::MutationSink{});
    }

    auto AntiEntropyManager::bucketFor(const std::string& key) const -> uint32_t {
        return static_cast<uint32_t>(XXH3_64bits(key.data(), key.size()) % num_buckets_);
    }

    auto AntiEntropyManager::entryHashRaw(
        std::string_view key,
        uint64_t version_and_ttl,
        uint64_t writer
    ) -> uint64_t {
        // Stateless mixer: XXH3 over the key, then a splitmix64 finalize over
        // the combined state. No streaming state (no malloc, no TLS, no
        // mutex), so the write-path sink stays allocation-free and lock-free
        // apart from the bucket atomics. The digest only needs 64-bit equality
        // comparison, for which splitmix64's avalanche is well-established.
        uint64_t h = XXH3_64bits(key.data(), key.size());
        h += 0x9E3779B97F4A7C15ULL + (version_and_ttl << 1U) + version_and_ttl;
        h ^= writer + 0x9E3779B97F4A7C15ULL + (h << 6U) + (h >> 2U);
        h ^= h >> 30U;
        h *= 0xBF58476D1CE4E5B9ULL;
        h ^= h >> 27U;
        h *= 0x94D049BB133111EBULL;
        h ^= h >> 31U;
        return h;
    }

    auto AntiEntropyManager::entryHash(const std::string& key, const VersionedEntry& entry)
        -> uint64_t {
        return entryHashRaw(key, entry.version_and_ttl, entry.writer_node_hash);
    }

    void AntiEntropyManager::onStoreMutation(
        std::string_view key,
        uint64_t version_and_ttl,
        uint64_t writer,
        bool /*removed*/
    ) {
        // XOR is self-inverse: add and remove are the same operation, so the
        // flag only documents intent. Leaf-only: never touch the store here
        // (invoked under the store's shard lock).
        std::shared_lock lock(acc_mutex_);
        uint64_t h = entryHashRaw(key, version_and_ttl, writer);
        uint32_t bucket = static_cast<uint32_t>(XXH3_64bits(key.data(), key.size()) % num_buckets_);
        if (capturing_.load(std::memory_order_acquire)) {
            std::scoped_lock log_lock(pending_mutex_);
            pending_.push_back({bucket, h});
            return;
        }
        xor_[bucket].fetch_xor(h, std::memory_order_relaxed);
    }

    auto AntiEntropyManager::computeDigest() const -> std::vector<uint64_t> {
        // Ground truth: full scan, XOR per entry. Order-independent (no
        // sort); the incremental accumulators must always equal this.
        std::vector<uint64_t> digest(num_buckets_, 0);
        store_.forEach([this, &digest](const std::string& key, const VersionedEntry& entry) {
            digest[bucketFor(key)] ^= entryHash(key, entry);
        });
        return digest;
    }

    auto AntiEntropyManager::digestForRound() -> std::vector<uint64_t> {
        bool expected = false;
        if (!rehashing_.compare_exchange_strong(expected, true, std::memory_order_acq_rel)) {
            // Another thread is rehashing; serve the current accumulators.
            // Cold-start zeros just look fully divergent once (self-healing).
            return snapshotXor();
        }

        bool need_rehash = !xor_valid_.load(std::memory_order_acquire);
        if (!need_rehash) {
            auto now_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(
                clock_.now().time_since_epoch()
            );
            auto last_ns = last_full_rehash_ns_.load(std::memory_order_acquire);
            auto interval = full_rehash_interval_.load(std::memory_order_acquire);
            need_rehash = (now_ns.count() - last_ns)
                >= std::chrono::duration_cast<std::chrono::nanoseconds>(interval).count();
        }
        if (!need_rehash) {
            rehashing_.store(false, std::memory_order_release);
            return snapshotXor();
        }

        fullRehash();
        rehashing_.store(false, std::memory_order_release);
        return snapshotXor();
    }

    auto AntiEntropyManager::snapshotXor() const -> std::vector<uint64_t> {
        std::vector<uint64_t> out;
        out.reserve(num_buckets_);
        for (uint32_t i = 0; i < num_buckets_; ++i) {
            out.push_back(xor_[i].load(std::memory_order_relaxed));
        }
        return out;
    }

    void AntiEntropyManager::fullRehash() {
        // Exactness protocol (see the P1 spec): divert-while-scanning. The
        // opening write section drains in-flight sink calls, so every later
        // mutation diverts to the pending log instead of the accumulators;
        // the drain+publish under the write lock is the only
        // writer-blocking window (~microseconds), so no scan-long stall and
        // no lost updates. Bounded work: the log only spans one scan.
        // Lock order (sink: shard -> shared; here: write only, never while
        // holding a shard lock) is deadlock-free by construction.
        {
            std::unique_lock lock(acc_mutex_);
            // Draining in-flight sinks first is what makes the divert exact:
            // any mutation that already checked capturing completed before
            // the scan starts, so the scan necessarily includes it.
            std::scoped_lock log_lock(pending_mutex_);
            pending_.clear();
            capturing_.store(true, std::memory_order_release);
        }
        std::vector<uint64_t> local(num_buckets_, 0);
        store_.forEach([this, &local](const std::string& key, const VersionedEntry& entry) {
            local[bucketFor(key)] ^= entryHash(key, entry);
        });

        {
            std::unique_lock lock(acc_mutex_);
            capturing_.store(false, std::memory_order_release);
            {
                std::scoped_lock log_lock(pending_mutex_);
                for (const auto& [bucket, h] : pending_) {
                    if (bucket < local.size()) {
                        local[bucket] ^= h;
                    }
                }
                pending_.clear();
            }
            // Self-check: accumulators and the fresh scan must agree — both
            // are exact by construction, so a mismatch is always a bug.
            // Publish unconditionally: a check must never gate the publish,
            // or buckets past the first mismatch stay stale forever.
            bool match = local.size() == num_buckets_;
            for (size_t i = 0; i < local.size(); ++i) {
                if (xor_[i].load(std::memory_order_relaxed) != local[i]) {
                    match = false;
                }
                xor_[i].store(local[i], std::memory_order_relaxed);
            }
            if (!match && metrics_ != nullptr) {
                metrics_->replicationMetrics().anti_entropy_rehash_mismatches.fetch_add(
                    1,
                    std::memory_order_relaxed
                );
            }
            xor_valid_.store(true, std::memory_order_release);
            last_full_rehash_ns_.store(
                std::chrono::duration_cast<std::chrono::nanoseconds>(
                    clock_.now().time_since_epoch()
                )
                    .count(),
                std::memory_order_release
            );
        }
        if (metrics_ != nullptr) {
            metrics_->replicationMetrics().anti_entropy_full_rehashes.fetch_add(
                1,
                std::memory_order_relaxed
            );
        }
        // Defer the partner-set refresh to pickPartner (timer thread): the
        // scan would otherwise also run on connection strands via
        // onDigestRequest.
        partners_dirty_.store(true, std::memory_order_release);
    }

    auto AntiEntropyManager::encodeDigest(const std::vector<uint64_t>& digest) -> std::string {
        std::string out;
        out.reserve(sizeof(uint32_t) * 2 + digest.size() * sizeof(uint64_t));
        appendU32(out, K_DIGEST_MAGIC);
        appendU32(out, static_cast<uint32_t>(digest.size()));
        for (auto h : digest) {
            appendU64(out, h);
        }
        return out;
    }

    auto AntiEntropyManager::decodeDigest(std::string_view data, size_t expected_buckets)
        -> std::optional<std::vector<uint64_t>> {
        const char* p = data.data();
        const char* end = p + data.size();
        uint32_t magic = 0;
        uint32_t n = 0;
        if (!readU32(p, end, magic) || magic != K_DIGEST_MAGIC) {
            return std::nullopt;
        }
        if (!readU32(p, end, n) || n != expected_buckets) {
            return std::nullopt;
        }
        if (static_cast<size_t>(end - p) != static_cast<size_t>(n) * sizeof(uint64_t)) {
            return std::nullopt;
        }

        std::vector<uint64_t> digest;
        digest.reserve(n);
        for (uint32_t i = 0; i < n; ++i) {
            uint64_t h = 0;
            if (!readU64(p, end, h)) {
                return std::nullopt;
            }
            digest.push_back(h);
        }
        return digest;
    }

    // One row: bucket u32 + key_hash/version/writer u64 (28 bytes).
    constexpr size_t K_META_ROW_BYTES = sizeof(uint32_t) + 3 * sizeof(uint64_t);

    auto AntiEntropyManager::encodeMetaRows(const std::vector<MetaRow>& rows) -> std::string {
        std::string out;
        out.reserve(sizeof(uint32_t) + rows.size() * K_META_ROW_BYTES);
        appendU32(out, static_cast<uint32_t>(rows.size()));
        for (const auto& r : rows) {
            appendU32(out, r.bucket);
            appendU64(out, r.key_hash);
            appendU64(out, r.version);
            appendU64(out, r.writer);
        }
        return out;
    }

    auto AntiEntropyManager::decodeMetaRows(std::string_view data)
        -> std::optional<std::vector<MetaRow>> {
        const char* p = data.data();
        const char* end = p + data.size();
        uint32_t n = 0;
        if (!readU32(p, end, n)) {
            return std::nullopt;
        }
        if (static_cast<size_t>(end - p) != static_cast<size_t>(n) * K_META_ROW_BYTES) {
            return std::nullopt;
        }

        std::vector<MetaRow> rows;
        rows.reserve(n);
        for (uint32_t i = 0; i < n; ++i) {
            MetaRow r;
            if (!readU32(p, end, r.bucket)
                || !readU64(p, end, r.key_hash)
                || !readU64(p, end, r.version)
                || !readU64(p, end, r.writer)) {
                return std::nullopt;
            }
            rows.push_back(r);
        }
        return rows;
    }

    auto AntiEntropyManager::encodeSync(const SyncMessage& msg) const -> std::string {
        std::string out;
        appendU32(out, K_SYNC_MAGIC);
        appendU32(out, static_cast<uint32_t>(msg.entries.size()));
        for (const auto& item : msg.entries) {
            appendU32(out, static_cast<uint32_t>(item.key.size()));
            out.append(item.key);
            appendU64(out, item.entry.version());
            appendU64(out, item.entry.writer_node_hash);
            out.push_back(item.entry.hasTtl() ? static_cast<char>(1) : static_cast<char>(0));
            if (item.entry.hasTtl()) {
                appendU64(out, toSystemMs(clock_, item.entry.expires_at));
            }
            appendU32(out, static_cast<uint32_t>(item.entry.value.size()));
            out.append(item.entry.value);
        }
        appendU32(out, static_cast<uint32_t>(msg.needs.size()));
        for (auto h : msg.needs) {
            appendU64(out, h);
        }
        return out;
    }

    auto AntiEntropyManager::decodeSync(std::string_view data) const -> std::optional<SyncMessage> {
        const char* p = data.data();
        const char* end = p + data.size();
        uint32_t magic = 0;
        if (!readU32(p, end, magic) || magic != K_SYNC_MAGIC) {
            return std::nullopt;
        }

        SyncMessage msg;
        uint32_t entry_count = 0;
        if (!readU32(p, end, entry_count)) {
            return std::nullopt;
        }
        msg.entries.reserve(entry_count);
        for (uint32_t i = 0; i < entry_count; ++i) {
            SyncMessage::Item item;
            std::string key;
            VersionedEntry entry;
            if (auto reason = parseSingleEntry(p, end, key, entry); reason.has_value()) {
                return std::nullopt;
            }
            item.key = std::move(key);
            item.entry = std::move(entry);
            msg.entries.push_back(std::move(item));
        }

        uint32_t need_count = 0;
        if (!readU32(p, end, need_count)) {
            return std::nullopt;
        }
        if (static_cast<size_t>(end - p) != static_cast<size_t>(need_count) * sizeof(uint64_t)) {
            return std::nullopt;
        }
        msg.needs.reserve(need_count);
        for (uint32_t i = 0; i < need_count; ++i) {
            uint64_t h = 0;
            if (!readU64(p, end, h)) {
                return std::nullopt;
            }
            msg.needs.push_back(h);
        }
        return msg;
    }

    auto AntiEntropyManager::splitDigestResponse(std::string_view payload, size_t num_buckets)
        -> std::optional<std::pair<std::string_view, std::string_view>> {
        size_t digest_len = sizeof(uint32_t) * 2 + num_buckets * sizeof(uint64_t);
        if (payload.size() < digest_len) {
            return std::nullopt;
        }
        return std::make_pair(
            std::string_view(payload.data(), digest_len),
            std::string_view(payload.data() + digest_len, payload.size() - digest_len)
        );
    }

    auto AntiEntropyManager::collectMetadata(
        const std::vector<uint32_t>& bucket_ids,
        size_t max_bytes
    ) const -> std::string {
        std::vector<char> wanted(num_buckets_, 0);
        for (auto id : bucket_ids) {
            if (id < num_buckets_) {
                wanted[id] = 1;
            }
        }

        // Rows grouped per bucket so the byte budget fills bucket-atomically:
        // a bucket is either fully covered (converges next round) or fully
        // deferred (retried next round) — never silently truncated mid-bucket.
        // The degenerate first-bucket-overflow case is included anyway:
        // over-budget once beats a convergence stall forever.
        std::vector<std::vector<MetaRow>> per_bucket(num_buckets_);
        store_.forEach(
            [this, &wanted, &per_bucket](const std::string& key, const VersionedEntry& entry) {
            uint32_t b = bucketFor(key);
            if (wanted[b]) {
                per_bucket[b].push_back(
                    {b,
                     XXH3_64bits(key.data(), key.size()),
                     entry.version(),
                     entry.writer_node_hash}
                );
            }
        }
        );

        std::vector<MetaRow> rows;
        size_t bytes = sizeof(uint32_t);
        for (uint32_t b = 0; b < num_buckets_; ++b) {
            if (!wanted[b] || per_bucket[b].empty()) {
                continue;
            }
            auto& vec = per_bucket[b];
            std::sort(vec.begin(), vec.end(), [](const MetaRow& a, const MetaRow& b) {
                return a.key_hash < b.key_hash;
            });
            size_t need = vec.size() * K_META_ROW_BYTES;
            if (bytes + need > max_bytes && !rows.empty()) {
                break;
            }
            bytes += need;
            rows.insert(rows.end(), vec.begin(), vec.end());
            if (bytes > max_bytes) {
                break;
            }
        }
        return encodeMetaRows(rows);
    }

    auto AntiEntropyManager::selectEntriesForPairs(
        const std::vector<std::pair<uint64_t, std::string>>& pairs,
        size_t max_bytes
    ) const -> std::vector<SyncMessage::Item> {
        std::vector<SyncMessage::Item> items;
        size_t bytes = 0;
        for (const auto& [hash, key] : pairs) {
            (void)hash;
            auto existing = store_.getVersioned(key);
            if (!existing.has_value()) {
                continue;
            }
            size_t cost = entryWireSize(key, *existing);
            if (bytes + cost > max_bytes && !items.empty()) {
                break;
            }
            bytes += cost;
            items.push_back({key, std::move(*existing)});
            if (bytes > max_bytes) {
                break;
            }
        }
        return items;
    }

    auto AntiEntropyManager::selectEntriesForHashes(
        const std::vector<uint64_t>& hashes,
        size_t max_bytes
    ) const -> std::vector<SyncMessage::Item> {
        if (hashes.empty()) {
            return {};
        }
        std::vector<uint64_t> sorted = hashes;
        std::sort(sorted.begin(), sorted.end());
        sorted.erase(std::unique(sorted.begin(), sorted.end()), sorted.end());

        // Resolve hashes to keys with one scan (point lookups would need the
        // keys, which is what we're resolving).
        std::unordered_map<uint64_t, std::string> resolved;
        resolved.reserve(sorted.size() * 2);
        store_.forEach([&sorted, &resolved](const std::string& key, const VersionedEntry&) {
            uint64_t h = XXH3_64bits(key.data(), key.size());
            if (std::binary_search(sorted.begin(), sorted.end(), h)) {
                resolved.try_emplace(h, key);
            }
        });

        std::vector<std::pair<uint64_t, std::string>> pairs;
        pairs.reserve(resolved.size());
        for (auto& [h, key] : resolved) {
            pairs.emplace_back(h, std::move(key));
        }
        std::sort(pairs.begin(), pairs.end(), [](const auto& a, const auto& b) {
            return a.first < b.first;
        });
        return selectEntriesForPairs(pairs, max_bytes);
    }

    auto AntiEntropyManager::collectEntriesForHashes(
        const std::vector<uint64_t>& hashes,
        size_t max_bytes
    ) const -> std::string {
        return encodeSync({selectEntriesForHashes(hashes, max_bytes), {}});
    }

    auto AntiEntropyManager::collectEntriesByKeys(
        const std::vector<std::string>& keys,
        size_t max_bytes
    ) const -> std::string {
        std::vector<std::pair<uint64_t, std::string>> pairs;
        pairs.reserve(keys.size());
        for (const auto& key : keys) {
            pairs.emplace_back(XXH3_64bits(key.data(), key.size()), key);
        }
        std::sort(pairs.begin(), pairs.end(), [](const auto& a, const auto& b) {
            return a.first < b.first;
        });
        return encodeSync({selectEntriesForPairs(pairs, max_bytes), {}});
    }

    auto AntiEntropyManager::diffRows(
        const std::vector<MetaRow>& rows,
        const std::vector<uint32_t>& divergent
    ) const -> MetaDiff {
        MetaDiff diff;
        if (rows.empty() || divergent.empty()) {
            return diff;
        }

        std::vector<char> wanted(num_buckets_, 0);
        for (auto id : divergent) {
            if (id < num_buckets_) {
                wanted[id] = 1;
            }
        }
        // Row lookup by hash. Duplicate hashes (2^-64 collision class, same
        // as the digest itself) keep the last row — deterministic.
        std::unordered_map<uint64_t, const MetaRow*> by_hash;
        by_hash.reserve(rows.size() * 2);
        for (const auto& row : rows) {
            by_hash[row.key_hash] = &row;
        }

        // Single scan over divergent buckets. Entries absent from the rows
        // are wins by absence (the partner lacks them); rows left over at
        // the end are entries the partner has and we lack (fetch).
        store_.forEach(
            [this, &wanted, &by_hash, &diff](const std::string& key, const VersionedEntry& entry) {
            if (!wanted[bucketFor(key)]) {
                return;
            }
            uint64_t h = XXH3_64bits(key.data(), key.size());
            auto it = by_hash.find(h);
            if (it == by_hash.end()) {
                diff.have.emplace_back(h, key);
                return;
            }
            const MetaRow* row = it->second;
            if (row->version
                > entry.version()
                || (row->version == entry.version() && row->writer > entry.writer_node_hash)) {
                diff.need.push_back(h);
            } else if (
                entry.version()
                > row->version
                || (entry.version() == row->version && entry.writer_node_hash > row->writer)
            ) {
                diff.have.emplace_back(h, key);
            }
            by_hash.erase(it);
        }
        );

        for (const auto& [h, row] : by_hash) {
            (void)row;
            diff.need.push_back(h);
        }
        std::sort(diff.need.begin(), diff.need.end());
        std::sort(diff.have.begin(), diff.have.end(), [](const auto& a, const auto& b) {
            return a.first < b.first;
        });
        return diff;
    }

    auto AntiEntropyManager::parseSingleEntry(
        const char*& p,
        const char* end,
        std::string& key,
        VersionedEntry& entry
    ) const -> std::optional<std::string_view> {
        uint32_t key_len = 0;
        uint64_t version = 0;
        uint64_t writer_hash = 0;
        uint32_t val_len = 0;
        std::string value;
        if (!readU32(p, end, key_len)
            || !readBytes(p, end, key_len, key)
            || !readU64(p, end, version)
            || !readU64(p, end, writer_hash)
            || static_cast<size_t>(end - p)
            < 1) {
            return "truncated entry";
        }

        uint8_t has_ttl = static_cast<uint8_t>(*p);
        ++p;
        if (has_ttl != 0 && has_ttl != 1) {
            return "bad ttl flag";
        }

        uint64_t expires_ms = 0;
        if (has_ttl == 1 && !readU64(p, end, expires_ms)) {
            return "truncated expiry";
        }
        if (!readU32(p, end, val_len) || !readBytes(p, end, val_len, value)) {
            return "truncated value";
        }

        entry.setVersion(version);
        entry.writer_node_hash = writer_hash;
        entry.value = std::move(value);
        if (has_ttl == 1) {
            entry.setHasTtl(true);
            entry.expires_at =
                toSteadyExpiry(clock_, system_clock::time_point(milliseconds(expires_ms)));
        }
        return std::nullopt;
    }

    auto AntiEntropyManager::isNewer(const std::string& key, uint64_t version, uint64_t writer_hash)
        -> bool {
        auto existing = store_.getVersioned(key);
        if (!existing.has_value()) {
            return true;
        }
        return version
            > existing->version()
            || (version == existing->version() && writer_hash > existing->writer_node_hash);
    }

    auto AntiEntropyManager::applySync(const SyncMessage& msg) -> size_t {
        size_t applied = 0;
        for (const auto& item : msg.entries) {
            // Only count as repaired if the entry is actually newer than what
            // we have. putVersioned always returns ok() (LWW accepts or
            // discards), so pre-check to get an accurate repair count.
            bool is_new = isNewer(item.key, item.entry.version(), item.entry.writer_node_hash);
            [[maybe_unused]]
            auto put_result = store_.putVersioned(item.key, std::move(item.entry));
            if (is_new) {
                ++applied;
            }
        }
        return applied;
    }

    auto AntiEntropyManager::applyEntries(std::string_view data) -> size_t {
        auto msg = decodeSync(data);
        if (!msg.has_value() || !msg->needs.empty()) {
            if (!msg.has_value()) {
                CINDER_WARN("malformed entries blob", {"reason", "decode failed"});
            } else {
                CINDER_WARN("malformed entries blob", {"reason", "unexpected needlist"});
            }
            return 0;
        }
        return applySync(*msg);
    }

    void AntiEntropyManager::refreshPartners(int replica_factor) {
        partners_.clear();
        if (replica_factor <= 1) {
            return;
        }

        std::set<NodeId> candidates;
        auto consider = [&](const std::string& key) {
            for (const auto& node : ring_.getNodes(key, replica_factor)) {
                if (node != self_) {
                    candidates.insert(node);
                }
            }
            return candidates.size() > 64;
        };
        // forEach visits copies; stop early once the candidate set is ample.
        // No early-exit in the visitor API, so bound by key count instead:
        // iterate live keys directly and break at 64 candidates.
        for (const auto& [key, entry] : store_.liveEntries()) {
            (void)entry;
            if (consider(key)) {
                break;
            }
        }
        partners_.assign(candidates.begin(), candidates.end());
    }

    auto AntiEntropyManager::pickPartner(int replica_factor) -> std::optional<NodeId> {
        if (replica_factor <= 1) {
            return std::nullopt;
        }
        if (partners_.empty() || partners_dirty_.exchange(false, std::memory_order_acq_rel)) {
            refreshPartners(replica_factor);
        }
        if (partners_.empty()) {
            return std::nullopt;
        }

        auto partner = partners_[partner_idx_ % partners_.size()];
        ++partner_idx_;
        return partner;
    }

    void AntiEntropyManager::onDigestRequest(
        const NodeId& from,
        const net::Request& req,
        std::function<void(net::Response)> respond
    ) {
        Span span("anti_entropy.digest", captureTraceContext());
        auto remote = decodeDigest(req.value, num_buckets_);
        if (!remote.has_value()) {
            CINDER_WARN("bad digest", {"from", from});
            respond(
                net::Response{
                    .status = Errc::InvalidArgument,
                    .value = std::nullopt,
                    .trace_id = req.trace_id,
                    .span_id = req.span_id,
                }
            );
            return;
        }

        auto local = digestForRound();
        std::vector<uint32_t> divergent;
        for (uint32_t i = 0; i < num_buckets_; ++i) {
            if (local[i] != (*remote)[i]) {
                divergent.push_back(i);
            }
        }

        // Response: our digest (so the initiator derives the same divergent
        // set) plus our metadata rows for those buckets.
        std::string payload = encodeDigest(local);
        std::string meta = collectMetadata(divergent, K_MAX_METADATA_BYTES);
        auto rows = decodeMetaRows(meta);
        if (metrics_ != nullptr) {
            metrics_->replicationMetrics().anti_entropy_rounds.fetch_add(
                1,
                std::memory_order_relaxed
            );
            metrics_->replicationMetrics().anti_entropy_meta_rows_sent.fetch_add(
                rows.has_value() ? rows->size() : 0,
                std::memory_order_relaxed
            );
        }
        payload += meta;
        CINDER_DEBUG("digest received", {"from", from}, {"divergent_buckets", divergent.size()});
        respond(
            net::Response{
                .status = Errc::OK,
                .value = std::move(payload),
                .trace_id = req.trace_id,
                .span_id = req.span_id,
            }
        );
    }

    void AntiEntropyManager::onSyncRequest(
        const NodeId& from,
        const net::Request& req,
        std::function<void(net::Response)> respond
    ) {
        Span span("anti_entropy.sync", captureTraceContext());
        auto respond_error = [&](Errc code) {
            respond(
                net::Response{
                    .status = code,
                    .value = std::nullopt,
                    .trace_id = req.trace_id,
                    .span_id = req.span_id,
                }
            );
        };

        auto msg = decodeSync(req.value);
        if (!msg.has_value()) {
            CINDER_WARN("bad sync", {"from", from});
            respond_error(Errc::InvalidArgument);
            return;
        }
        size_t repaired = applySync(*msg);
        if (metrics_ != nullptr && repaired > 0) {
            metrics_->replicationMetrics().anti_entropy_keys_repaired.fetch_add(
                repaired,
                std::memory_order_relaxed
            );
        }

        // Serve the needlist (if any) with an entries-only response. The
        // entries the sender pushed are already applied above.
        SyncMessage resp;
        if (!msg->needs.empty()) {
            resp.entries = selectEntriesForHashes(msg->needs, fetchBytes());
            CINDER_DEBUG(
                "sync serving needlist",
                {"from", from},
                {"requested", msg->needs.size()},
                {"served", resp.entries.size()}
            );
        }
        CINDER_DEBUG("sync received", {"from", from}, {"entries", repaired});
        respond(
            net::Response{
                .status = Errc::OK,
                .value = encodeSync(resp),
                .trace_id = req.trace_id,
                .span_id = req.span_id,
            }
        );
    }

    void AntiEntropyManager::runRound(int replica_factor) {
        Span span("anti_entropy.round", captureTraceContext());
        auto partner = pickPartner(replica_factor);
        if (!partner.has_value()) {
            CINDER_DEBUG(("no partner available, skipping round"));
            span.setAttribute({"outcome", "skipped"});
            span.setAttribute({"reason", "no-partner"});
            return;
        }

        span.setAttribute({"partner", *partner});
        auto local = digestForRound();
        net::Request req;

        req.opcode = net::Opcode::AntiEntropyDigest;
        req.value = encodeDigest(local);
        req.trace_id = span.traceId();
        req.span_id = span.spanId();

        CINDER_DEBUG("starting round", {"partner", *partner});
        // The digest callback runs on a transport completion thread with
        // unrelated TLS; capture the round identity so its events stay on
        // the same trace. (No TraceScope can span the co_await inside the
        // transport — attach at callback entry instead.)
        SpanContext round_ctx = span.context();
        transport_.sendRequestAsync(
            *partner,
            req,
            [this, local = std::move(local), partner = *partner, round_ctx](
                Result<net::Response> r
            ) mutable {
            TraceScope scope(round_ctx);
            // The initiating round span is a stack local that died when
            // runRound returned; report the async outcome on a child that
            // joins the same trace.
            Span result_span("anti_entropy.round.result", round_ctx);
            result_span.setAttribute({"partner", partner});
            if (!r.has_value() || r->status != Errc::OK || !r->value.has_value()) {
                CINDER_DEBUG("digest round failed", {"partner", partner});
                result_span.setAttribute({"outcome", "digest-failed"});
                result_span.setStatus(SpanStatus::Error);
                return;
            }

            auto split = splitDigestResponse(*r->value, num_buckets_);
            if (!split.has_value()) {
                CINDER_WARN("short digest response", {"from", partner});
                result_span.setAttribute({"outcome", "short-response"});
                result_span.setStatus(SpanStatus::Error);
                return;
            }

            auto remote = decodeDigest(split->first, num_buckets_);
            if (!remote.has_value()) {
                CINDER_WARN("bad digest response", {"from", partner});
                result_span.setAttribute({"outcome", "bad-digest"});
                result_span.setStatus(SpanStatus::Error);
                return;
            }

            // Derive the divergent set from the two digests and diff the
            // partner's metadata rows against the local store.
            std::vector<uint32_t> divergent;
            for (uint32_t i = 0; i < num_buckets_; ++i) {
                if (local[i] != (*remote)[i]) {
                    divergent.push_back(i);
                }
            }
            if (divergent.empty()) {
                result_span.setAttribute({"repaired", 0});
                result_span.setAttribute({"divergent", 0});
                result_span.setAttribute({"outcome", "completed"});
                result_span.setStatus(SpanStatus::Ok);
                CINDER_INFO("round completed", {"partner", partner}, {"repaired", 0});
                return;
            }

            auto rows = decodeMetaRows(split->second);
            if (!rows.has_value()) {
                CINDER_WARN("bad metadata response", {"from", partner});
                result_span.setAttribute({"outcome", "bad-meta"});
                result_span.setStatus(SpanStatus::Error);
                return;
            }
            if (metrics_ != nullptr) {
                metrics_->replicationMetrics().anti_entropy_meta_rows_recv.fetch_add(
                    rows->size(),
                    std::memory_order_relaxed
                );
            }
            MetaDiff diff = diffRows(*rows, divergent);

            // Push entries where we win; ask for entries where we lose.
            // Both directions are byte-budgeted; the next round re-diffs
            // any remainder, so convergence is monotonic without a cursor.
            SyncMessage sync;
            sync.entries = selectEntriesForPairs(diff.have, fetchBytes());
            sync.needs = std::move(diff.need);
            result_span.setAttribute({"push", sync.entries.size()});
            result_span.setAttribute({"fetch", sync.needs.size()});
            net::Request sync_req;
            sync_req.opcode = net::Opcode::AntiEntropySync;
            sync_req.value = encodeSync(sync);
            sync_req.trace_id = round_ctx.trace_id;
            sync_req.span_id = round_ctx.span_id;

            transport_.sendRequestAsync(
                partner,
                sync_req,
                [this, partner, round_ctx](Result<net::Response> sr) mutable {
                TraceScope inner_scope(round_ctx);
                Span fetch_span("anti_entropy.round.fetch", round_ctx);
                fetch_span.setAttribute({"partner", partner});
                if (!sr.has_value() || sr->status != Errc::OK || !sr->value.has_value()) {
                    CINDER_DEBUG("sync phase failed", {"partner", partner});
                    fetch_span.setAttribute({"outcome", "sync-failed"});
                    fetch_span.setStatus(SpanStatus::Error);
                    return;
                }
                // Response carries entries only; a needlist here is a
                // protocol violation.
                auto resp = decodeSync(*sr->value);
                if (!resp.has_value() || !resp->needs.empty()) {
                    CINDER_WARN("bad sync response", {"from", partner});
                    fetch_span.setAttribute({"outcome", "bad-sync"});
                    fetch_span.setStatus(SpanStatus::Error);
                    return;
                }
                size_t repaired = applySync(*resp);
                if (metrics_ != nullptr) {
                    if (repaired > 0) {
                        metrics_->replicationMetrics().anti_entropy_keys_repaired.fetch_add(
                            repaired,
                            std::memory_order_relaxed
                        );
                    }
                    metrics_->replicationMetrics().anti_entropy_entries_fetched.fetch_add(
                        resp->entries.size(),
                        std::memory_order_relaxed
                    );
                }
                CINDER_INFO(
                    "fetch completed",
                    {"partner", partner},
                    {"repaired", repaired},
                    {"entries", resp->entries.size()}
                );
                CINDER_INFO("round completed", {"partner", partner}, {"repaired", repaired});
                fetch_span.setAttribute({"repaired", repaired});
                fetch_span.setAttribute({"outcome", "completed"});
                fetch_span.setStatus(SpanStatus::Ok);
            }
            );
            result_span.setAttribute({"divergent", divergent.size()});
            result_span.setAttribute({"outcome", "completed"});
            result_span.setStatus(SpanStatus::Ok);
        }
        );
    }
} // namespace cinder
