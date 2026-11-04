#include "cinder/node/anti_entropy.hpp"

#include <algorithm>
#include <ankerl/unordered_dense.h>
#include <array>
#include <bit>
#include <chrono>
#include <cstring>
#include <limits>
#include <set>
#include <shared_mutex>
#include <utility>
#include <vector>
#include <xxhash.h>

#include "cinder/common/tracing.hpp"

using std::chrono::duration_cast;
using std::chrono::milliseconds;
using std::chrono::nanoseconds;
using std::chrono::system_clock;

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

        auto ibltItemChecksum(uint64_t key_hash, uint64_t version, uint64_t writer, bool tombstone)
            -> uint64_t {
            std::array<uint64_t, 4> words{key_hash, version, writer, tombstone ? 1U : 0U};
            return XXH3_64bits(words.data(), sizeof(words));
        }

        auto ibltPositions(
            uint64_t key_hash,
            uint64_t version,
            uint64_t writer,
            bool tombstone,
            size_t count
        ) -> std::array<size_t, AntiEntropyManager::K_IBLT_HASH_COUNT> {
            std::array<size_t, AntiEntropyManager::K_IBLT_HASH_COUNT> positions{};
            std::array<uint64_t, 4> words{key_hash, version, writer, tombstone ? 1U : 0U};
            uint64_t hash = XXH3_64bits(words.data(), sizeof(words));
            for (size_t i = 0; i < positions.size(); ++i) {
                uint64_t candidate = hash + (i + 1) * 0x9E3779B97F4A7C15ULL;
                candidate ^= candidate >> 30U;
                candidate *= 0xBF58476D1CE4E5B9ULL;
                candidate ^= candidate >> 27U;
                candidate *= 0x94D049BB133111EBULL;
                candidate ^= candidate >> 31U;
                positions[i] = static_cast<size_t>(candidate % count);
                while (std::find(
                           positions.begin(),
                           positions.begin() + static_cast<ptrdiff_t>(i),
                           positions[i]
                       )
                       != positions.begin()
                       + static_cast<ptrdiff_t>(i)) {
                    ++positions[i];
                    if (positions[i] == count) {
                        positions[i] = 0;
                    }
                }
            }
            return positions;
        }

        void ibltApply(
            AntiEntropyManager::IbltCell& cell,
            int64_t sign,
            uint64_t key_hash,
            uint64_t version,
            uint64_t writer,
            bool tombstone
        ) {
            cell.count += sign;
            cell.key_hash_xor ^= key_hash;
            cell.version_xor ^= version;
            cell.writer_xor ^= writer;
            cell.tombstone_xor ^= tombstone ? 1U : 0U;
            cell.checksum_xor ^= ibltItemChecksum(key_hash, version, writer, tombstone);
        }

        auto ibltPure(const AntiEntropyManager::IbltCell& cell) -> bool {
            if (cell.count != 1 && cell.count != -1) {
                return false;
            }
            if (cell.tombstone_xor != 0 && cell.tombstone_xor != 1) {
                return false;
            }
            return cell.checksum_xor
                == ibltItemChecksum(
                       cell.key_hash_xor,
                       cell.version_xor,
                       cell.writer_xor,
                       cell.tombstone_xor != 0
                );
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
        milliseconds full_rehash_interval
    ) :
        store_(store),
        ring_(ring),
        clock_(clock),
        transport_(transport),
        fetch_bytes_(fetch_bytes),
        full_rehash_interval_(full_rehash_interval),
        metrics_(metrics),
        xor_(std::make_unique<std::atomic<uint64_t>[]>(num_buckets == 0 ? 1 : num_buckets)),
        self_(std::move(self)),
        num_buckets_(num_buckets == 0 ? 1 : num_buckets) {
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
            auto now_ns = duration_cast<nanoseconds>(clock_.now().time_since_epoch());
            auto last_ns = last_full_rehash_ns_.load(std::memory_order_acquire);
            auto interval = full_rehash_interval_.load(std::memory_order_acquire);
            need_rehash =
                (now_ns.count() - last_ns) >= duration_cast<nanoseconds>(interval).count();
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
                duration_cast<nanoseconds>(clock_.now().time_since_epoch()).count(),
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

    auto AntiEntropyManager::groupDigests(const std::vector<uint64_t>& bucket_hashes)
        -> std::vector<uint64_t> {
        const size_t group_count =
            (bucket_hashes.size() + K_BUCKETS_PER_GROUP - 1) / K_BUCKETS_PER_GROUP;
        std::vector<uint64_t> groups(group_count, 0);
        for (size_t group = 0; group < group_count; ++group) {
            // Include each absolute bucket index to make the fold sensitive to
            // placement, not only the multiset of bucket hashes.
            std::string material;
            const size_t begin = group * K_BUCKETS_PER_GROUP;
            const size_t end = std::min(begin + K_BUCKETS_PER_GROUP, bucket_hashes.size());
            material.reserve((end - begin) * (sizeof(uint32_t) + sizeof(uint64_t)));
            for (size_t bucket = begin; bucket < end; ++bucket) {
                appendU32(material, static_cast<uint32_t>(bucket));
                appendU64(material, bucket_hashes[bucket]);
            }
            groups[group] = XXH3_64bits(material.data(), material.size());
        }
        return groups;
    }

    auto AntiEntropyManager::encodeGroupDigest(const std::vector<uint64_t>& groups) -> std::string {
        std::string out;
        out.reserve(2 * sizeof(uint32_t) + groups.size() * sizeof(uint64_t));
        appendU32(out, K_GROUP_DIGEST_MAGIC);
        appendU32(out, static_cast<uint32_t>(groups.size()));
        for (uint64_t hash : groups) {
            appendU64(out, hash);
        }
        return out;
    }

    auto AntiEntropyManager::decodeGroupDigest(std::string_view data, size_t expected_groups)
        -> std::optional<std::vector<uint64_t>> {
        const char* p = data.data();
        const char* end = p + data.size();
        uint32_t magic = 0;
        uint32_t count = 0;
        if (!readU32(p, end, magic)
            || magic
            != K_GROUP_DIGEST_MAGIC
            || !readU32(p, end, count)
            || count
            != expected_groups
            || static_cast<size_t>(end - p)
            != static_cast<size_t>(count)
            * sizeof(uint64_t)) {
            return std::nullopt;
        }

        std::vector<uint64_t> result;
        result.reserve(count);
        for (uint32_t i = 0; i < count; ++i) {
            uint64_t hash = 0;
            if (!readU64(p, end, hash)) {
                return std::nullopt;
            }
            result.push_back(hash);
        }
        return result;
    }

    auto AntiEntropyManager::encodeBucketRefinement(const BucketRefinement& request)
        -> std::string {
        std::string out;
        appendU32(out, K_BUCKET_REFINEMENT_MAGIC);
        appendU32(out, request.force_metadata ? 1U : 0U);
        appendU32(out, static_cast<uint32_t>(request.bucket_hashes.size()));
        for (const auto& [bucket, hash] : request.bucket_hashes) {
            appendU32(out, bucket);
            appendU64(out, hash);
        }
        return out;
    }

    auto AntiEntropyManager::decodeBucketRefinement(std::string_view data, size_t expected_groups)
        -> std::optional<BucketRefinement> {
        const char* p = data.data();
        const char* end = p + data.size();
        uint32_t magic = 0;
        uint32_t flags = 0;
        uint32_t bucket_count = 0;
        if (!readU32(p, end, magic)
            || magic
            != K_BUCKET_REFINEMENT_MAGIC
            || !readU32(p, end, flags)
            || (flags & ~1U)
            != 0
            || !readU32(p, end, bucket_count)
            || static_cast<size_t>(end - p)
            != static_cast<size_t>(bucket_count)
            * (sizeof(uint32_t) + sizeof(uint64_t))) {
            return std::nullopt;
        }

        BucketRefinement request;
        request.force_metadata = (flags & 1U) != 0;
        request.bucket_hashes.reserve(bucket_count);
        for (uint32_t i = 0; i < bucket_count; ++i) {
            uint32_t bucket = 0;
            uint64_t hash = 0;
            if (!readU32(p, end, bucket) || bucket >= expected_groups || !readU64(p, end, hash)) {
                return std::nullopt;
            }
            request.bucket_hashes.emplace_back(bucket, hash);
        }
        return request;
    }

    auto AntiEntropyManager::encodeDigestResponse(const DigestResponse& response) -> std::string {
        std::string rows = encodeMetaRows(response.rows);
        std::string out;
        out.reserve(
            3
            * sizeof(uint32_t)
            + response.group_hashes.size()
            * sizeof(uint64_t)
            + response.bucket_hashes.size()
            * (sizeof(uint32_t) + sizeof(uint64_t))
            + response.covered_buckets.size()
            * sizeof(uint32_t)
            + sizeof(uint32_t)
            + rows.size()
            + sizeof(uint32_t)
        );

        appendU32(out, K_DIGEST_RESPONSE_MAGIC);
        appendU32(out, static_cast<uint32_t>(response.group_hashes.size()));
        for (uint64_t hash : response.group_hashes) {
            appendU64(out, hash);
        }

        appendU32(out, static_cast<uint32_t>(response.bucket_hashes.size()));
        for (const auto& [bucket, hash] : response.bucket_hashes) {
            appendU32(out, bucket);
            appendU64(out, hash);
        }

        appendU32(out, static_cast<uint32_t>(response.covered_buckets.size()));
        for (uint32_t bucket : response.covered_buckets) {
            appendU32(out, bucket);
        }

        appendU32(out, static_cast<uint32_t>(rows.size()));
        out += rows;
        appendU32(out, static_cast<uint32_t>(response.iblt_sketches.size()));
        for (const auto& [bucket, sketch] : response.iblt_sketches) {
            appendU32(out, bucket);
            appendU32(out, static_cast<uint32_t>(sketch.size()));
            out += sketch;
        }
        return out;
    }

    auto AntiEntropyManager::decodeDigestResponse(std::string_view data, size_t expected_groups)
        -> std::optional<DigestResponse> {
        const char* p = data.data();
        const char* end = p + data.size();
        uint32_t magic = 0;
        uint32_t group_count = 0;
        if (!readU32(p, end, magic)
            || magic
            != K_DIGEST_RESPONSE_MAGIC
            || !readU32(p, end, group_count)
            || group_count
            != expected_groups) {
            return std::nullopt;
        }

        DigestResponse response;
        response.group_hashes.reserve(group_count);
        for (uint32_t i = 0; i < group_count; ++i) {
            uint64_t hash = 0;
            if (!readU64(p, end, hash)) {
                return std::nullopt;
            }
            response.group_hashes.push_back(hash);
        }

        uint32_t bucket_hash_count = 0;
        if (!readU32(p, end, bucket_hash_count)
            || static_cast<size_t>(end - p)
            < static_cast<size_t>(bucket_hash_count)
            * (sizeof(uint32_t) + sizeof(uint64_t))) {
            return std::nullopt;
        }

        response.bucket_hashes.reserve(bucket_hash_count);
        for (uint32_t i = 0; i < bucket_hash_count; ++i) {
            uint32_t bucket = 0;
            uint64_t hash = 0;
            if (!readU32(p, end, bucket) || !readU64(p, end, hash)) {
                return std::nullopt;
            }
            response.bucket_hashes.emplace_back(bucket, hash);
        }

        uint32_t covered_count = 0;
        if (!readU32(p, end, covered_count)
            || static_cast<size_t>(end - p)
            < static_cast<size_t>(covered_count)
            * sizeof(uint32_t)
            + sizeof(uint32_t)) {
            return std::nullopt;
        }

        response.covered_buckets.reserve(covered_count);
        for (uint32_t i = 0; i < covered_count; ++i) {
            uint32_t bucket = 0;
            if (!readU32(p, end, bucket)) {
                return std::nullopt;
            }
            response.covered_buckets.push_back(bucket);
        }

        uint32_t rows_size = 0;
        if (!readU32(p, end, rows_size)
            || static_cast<size_t>(end - p)
            < rows_size
            + sizeof(uint32_t)) {
            return std::nullopt;
        }

        auto rows = decodeMetaRows(std::string_view(p, rows_size));
        if (!rows.has_value()) {
            return std::nullopt;
        }

        response.rows = std::move(*rows);
        p += rows_size;
        uint32_t sketch_count = 0;
        if (!readU32(p, end, sketch_count)
            || static_cast<size_t>(sketch_count)
            > static_cast<size_t>(end - p)
            / (2 * sizeof(uint32_t))) {
            return std::nullopt;
        }

        response.iblt_sketches.reserve(sketch_count);
        for (uint32_t i = 0; i < sketch_count; ++i) {
            uint32_t bucket = 0;
            uint32_t sketch_size = 0;
            std::string sketch_blob;
            if (!readU32(p, end, bucket)
                || !readU32(p, end, sketch_size)
                || !readBytes(p, end, sketch_size, sketch_blob)
                || !decodeIblt(sketch_blob).has_value()) {
                return std::nullopt;
            }
            response.iblt_sketches.emplace_back(bucket, std::move(sketch_blob));
        }
        if (p != end) {
            return std::nullopt;
        }
        for (const auto& row : response.rows) {
            if (!std::binary_search(
                    response.covered_buckets.begin(),
                    response.covered_buckets.end(),
                    row.bucket
                )) {
                return std::nullopt;
            }
        }

        std::vector<char> sketch_bucket_seen(
            response.covered_buckets.empty()
                ? 0
                : static_cast<size_t>(response.covered_buckets.back()) + 1,
            0
        );

        std::vector<uint32_t> sorted_covered = response.covered_buckets;
        std::sort(sorted_covered.begin(), sorted_covered.end());
        std::vector<uint32_t> sketch_buckets;
        sketch_buckets.reserve(response.iblt_sketches.size());
        for (const auto& [bucket, sketch] : response.iblt_sketches) {
            (void)sketch;
            if (!std::binary_search(sorted_covered.begin(), sorted_covered.end(), bucket)) {
                return std::nullopt;
            }
            if (bucket >= sketch_bucket_seen.size() || sketch_bucket_seen[bucket]) {
                return std::nullopt;
            }

            sketch_bucket_seen[bucket] = 1;
            sketch_buckets.push_back(bucket);
        }

        std::sort(sketch_buckets.begin(), sketch_buckets.end());
        if (std::adjacent_find(sketch_buckets.begin(), sketch_buckets.end())
            != sketch_buckets.end()) {
            return std::nullopt;
        }
        for (const auto& row : response.rows) {
            if (row.bucket < sketch_bucket_seen.size() && sketch_bucket_seen[row.bucket]) {
                return std::nullopt;
            }
        }
        return response;
    }

    auto AntiEntropyManager::buildIblt(const std::vector<MetaRow>& rows, size_t cell_count)
        -> std::optional<IbltSketch> {
        if (cell_count < K_IBLT_HASH_COUNT || cell_count > K_MAX_IBLT_CELLS) {
            return std::nullopt;
        }

        IbltSketch sketch;
        sketch.cells.resize(cell_count);
        for (const auto& row : rows) {
            const auto positions =
                ibltPositions(row.key_hash, row.version, row.writer, row.tombstone, cell_count);
            for (size_t pos : positions) {
                ibltApply(
                    sketch.cells[pos],
                    1,
                    row.key_hash,
                    row.version,
                    row.writer,
                    row.tombstone
                );
            }
        }
        return sketch;
    }

    auto AntiEntropyManager::encodeIblt(const IbltSketch& sketch) -> std::string {
        if (sketch.cells.size() < K_IBLT_HASH_COUNT || sketch.cells.size() > K_MAX_IBLT_CELLS) {
            return {};
        }

        std::string out;
        out.reserve(2 * sizeof(uint32_t) + sketch.cells.size() * 48U);
        appendU32(out, K_IBLT_MAGIC);
        appendU32(out, static_cast<uint32_t>(sketch.cells.size()));
        for (const auto& cell : sketch.cells) {
            appendU64(out, std::bit_cast<uint64_t>(cell.count));
            appendU64(out, cell.key_hash_xor);
            appendU64(out, cell.version_xor);
            appendU64(out, cell.writer_xor);
            appendU64(out, cell.tombstone_xor);
            appendU64(out, cell.checksum_xor);
        }
        return out;
    }

    auto AntiEntropyManager::decodeIblt(std::string_view data) -> std::optional<IbltSketch> {
        const char* p = data.data();
        const char* end = p + data.size();
        uint32_t magic = 0;
        uint32_t count = 0;
        constexpr size_t K_CELL_BYTES = sizeof(uint64_t) + 5 * sizeof(uint64_t);
        if (!readU32(p, end, magic)
            || magic
            != K_IBLT_MAGIC
            || !readU32(p, end, count)
            || count
            < K_IBLT_HASH_COUNT
            || count
            > K_MAX_IBLT_CELLS
            || static_cast<size_t>(end - p)
            != static_cast<size_t>(count)
            * K_CELL_BYTES) {
            return std::nullopt;
        }

        IbltSketch sketch;
        sketch.cells.reserve(count);
        for (uint32_t i = 0; i < count; ++i) {
            uint64_t count_bits = 0;
            IbltCell cell;
            if (!readU64(p, end, count_bits)
                || !readU64(p, end, cell.key_hash_xor)
                || !readU64(p, end, cell.version_xor)
                || !readU64(p, end, cell.writer_xor)
                || !readU64(p, end, cell.tombstone_xor)
                || !readU64(p, end, cell.checksum_xor)) {
                return std::nullopt;
            }
            if (count_bits < (uint64_t{1} << 63U) && count_bits > (uint64_t{1} << 40U)) {
                return std::nullopt;
            }
            if (count_bits
                >= (uint64_t{1} << 63U)
                && (std::numeric_limits<uint64_t>::max() - count_bits)
                >= (uint64_t{1} << 40U)) {
                return std::nullopt;
            }

            cell.count = std::bit_cast<int64_t>(count_bits);
            sketch.cells.push_back(cell);
        }
        return sketch;
    }

    auto AntiEntropyManager::decodeIbltDifference(
        const IbltSketch& remote,
        const IbltSketch& local,
        uint32_t bucket
    ) -> std::optional<IbltDiff> {
        if (remote.cells.size()
            != local.cells.size()
            || remote.cells.size()
            < K_IBLT_HASH_COUNT
            || remote.cells.size()
            > K_MAX_IBLT_CELLS) {
            return std::nullopt;
        }

        IbltSketch difference = remote;
        for (size_t i = 0; i < difference.cells.size(); ++i) {
            difference.cells[i].count -= local.cells[i].count;
            difference.cells[i].key_hash_xor ^= local.cells[i].key_hash_xor;
            difference.cells[i].version_xor ^= local.cells[i].version_xor;
            difference.cells[i].writer_xor ^= local.cells[i].writer_xor;
            difference.cells[i].tombstone_xor ^= local.cells[i].tombstone_xor;
            difference.cells[i].checksum_xor ^= local.cells[i].checksum_xor;
        }

        IbltDiff decoded;
        std::vector<size_t> queue;
        std::vector<char> queued(difference.cells.size(), 0);
        queue.reserve(difference.cells.size());
        for (size_t i = 0; i < difference.cells.size(); ++i) {
            if (ibltPure(difference.cells[i])) {
                queue.push_back(i);
                queued[i] = 1;
            }
        }
        for (size_t cursor = 0; cursor < queue.size(); ++cursor) {
            const size_t pos = queue[cursor];
            const IbltCell cell = difference.cells[pos];
            if (!ibltPure(cell)) {
                queued[pos] = 0;
                continue;
            }

            queued[pos] = 0;
            MetaRow row{
                .bucket = bucket,
                .tombstone = cell.tombstone_xor != 0,
                .key_hash = cell.key_hash_xor,
                .version = cell.version_xor,
                .writer = cell.writer_xor,
            };

            const bool is_remote = cell.count == 1;
            (is_remote ? decoded.remote_only : decoded.local_only).push_back(row);
            const auto positions = ibltPositions(
                row.key_hash,
                row.version,
                row.writer,
                row.tombstone,
                difference.cells.size()
            );
            for (size_t target : positions) {
                ibltApply(
                    difference.cells[target],
                    is_remote ? -1 : 1,
                    row.key_hash,
                    row.version,
                    row.writer,
                    row.tombstone
                );
                if (!queued[target] && ibltPure(difference.cells[target])) {
                    queue.push_back(target);
                    queued[target] = 1;
                }
            }
        }
        for (const auto& cell : difference.cells) {
            if (cell.count
                != 0
                || cell.key_hash_xor
                != 0
                || cell.version_xor
                != 0
                || cell.writer_xor
                != 0
                || cell.tombstone_xor
                != 0
                || cell.checksum_xor
                != 0) {
                return std::nullopt;
            }
        }
        return decoded;
    }

    // One row: bucket u32 + key_hash/version/writer u64 + tombstone u8
    // (29 bytes).
    constexpr size_t K_META_ROW_BYTES = sizeof(uint32_t) + 3 * sizeof(uint64_t) + sizeof(uint8_t);

    auto AntiEntropyManager::encodeMetaRows(const std::vector<MetaRow>& rows) -> std::string {
        std::string out;
        out.reserve(sizeof(uint32_t) + rows.size() * K_META_ROW_BYTES);
        appendU32(out, static_cast<uint32_t>(rows.size()));
        for (const auto& r : rows) {
            appendU32(out, r.bucket);
            appendU64(out, r.key_hash);
            appendU64(out, r.version);
            appendU64(out, r.writer);
            out.push_back(r.tombstone ? static_cast<char>(1) : static_cast<char>(0));
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
                || !readU64(p, end, r.writer)
                || p
                >= end) {
                return std::nullopt;
            }

            // Exact-size validation above bounds p: one flag byte remains.
            const auto tombstone = static_cast<uint8_t>(*p++);
            if (tombstone > 1) {
                return std::nullopt;
            }

            r.tombstone = tombstone != 0;
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
            // Entry flags bitfield: bit 0 = has_ttl, bit 1 = tombstone.
            // Unknown bits are rejected (clean fail on mixed versions).
            uint8_t entry_flags = 0;
            if (item.entry.hasTtl()) {
                entry_flags |= 1U;
            }
            if (item.entry.isTombstone()) {
                entry_flags |= 2U;
            }

            out.push_back(static_cast<char>(entry_flags));
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

    auto AntiEntropyManager::collectMetadata(
        const std::vector<uint32_t>& bucket_ids,
        size_t max_bytes,
        std::vector<uint32_t>* covered_buckets
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
                per_bucket[b].push_back({
                    b,
                    entry.isTombstone(),
                    XXH3_64bits(key.data(), key.size()),
                    entry.version(),
                    entry.writer_node_hash,
                });
            }
        }
        );

        std::vector<MetaRow> rows;
        // Outer metadata-row count and covered-bucket count.
        size_t bytes = 2 * sizeof(uint32_t);
        for (uint32_t b = 0; b < num_buckets_; ++b) {
            if (!wanted[b]) {
                continue;
            }

            auto& vec = per_bucket[b];
            std::sort(vec.begin(), vec.end(), [](const MetaRow& lhs, const MetaRow& rhs) {
                return lhs.key_hash < rhs.key_hash;
            });
            // Coverage is explicit even for empty buckets. Without this, a
            // peer cannot distinguish "empty" from "omitted by byte budget".
            const size_t coverage_cost = sizeof(uint32_t);
            if (bytes
                + coverage_cost
                > max_bytes
                && covered_buckets
                != nullptr
                && !covered_buckets->empty()) {
                break;
            }

            bytes += coverage_cost;
            if (covered_buckets != nullptr) {
                covered_buckets->push_back(b);
            }

            size_t need = vec.size() * K_META_ROW_BYTES;
            if (bytes
                + need
                > max_bytes
                && covered_buckets
                != nullptr
                && covered_buckets->size()
                > 1) {
                // Over budget: drop this bucket's coverage (the pop) and stop.
                // No need to adjust `bytes`: it is only read while filling.
                covered_buckets->pop_back();
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
        ankerl::unordered_dense::map<uint64_t, std::string> resolved;
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
        const std::vector<uint32_t>& divergent,
        const std::vector<uint32_t>& covered_buckets
    ) const -> MetaDiff {
        MetaDiff diff;
        if (divergent.empty()) {
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
        ankerl::unordered_dense::map<uint64_t, const MetaRow*> by_hash;
        by_hash.reserve(rows.size() * 2);
        for (const auto& row : rows) {
            by_hash[row.key_hash] = &row;
        }

        // Single scan over divergent buckets. Entries absent from the rows
        // are wins by absence (the partner lacks them); rows left over at
        // the end are entries the partner has and we lack (fetch).
        std::vector<char> covered(num_buckets_, covered_buckets.empty() ? 1 : 0);
        for (uint32_t bucket : covered_buckets) {
            if (bucket < num_buckets_) {
                covered[bucket] = 1;
            }
        }

        store_.forEach([this, &wanted, &covered, &by_hash, &diff](
                           const std::string& key,
                           const VersionedEntry& entry
                       ) {
            if (!wanted[bucketFor(key)]) {
                return;
            }

            uint64_t h = XXH3_64bits(key.data(), key.size());
            auto it = by_hash.find(h);
            if (it == by_hash.end()) {
                // Only infer remote absence when this bucket's complete
                // metadata was sent. An uncovered bucket is deferred whole.
                if (covered[bucketFor(key)]) {
                    diff.have.emplace_back(h, key);
                }
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
            } else if (row->tombstone != entry.isTombstone()) {
                // Equal (version, writer) with contradictory delete state.
                // Unreachable through store paths (per-writer versions are
                // monotonic), except across standalone writers sharing
                // writer 0 with colliding Lamport values. Tombstone wins
                // ties so both sides converge instead of stalling divergent
                // with zero repair (the digests cover the tombstone bit, so
                // without a decision here the bucket disagrees forever).
                if (row->tombstone) {
                    diff.need.push_back(h);
                } else {
                    diff.have.emplace_back(h, key);
                }
            }
            by_hash.erase(it);
        });

        for (const auto& [h, row] : by_hash) {
            if (covered[row->bucket]) {
                diff.need.push_back(h);
            }
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

        uint8_t entry_flags = static_cast<uint8_t>(*p);
        ++p;
        if ((entry_flags & uint8_t{3}) != entry_flags) {
            return "bad entry flags";
        }

        const bool has_ttl = (entry_flags & 1U) != 0;
        uint64_t expires_ms = 0;
        if (has_ttl && !readU64(p, end, expires_ms)) {
            return "truncated expiry";
        }
        if (!readU32(p, end, val_len) || !readBytes(p, end, val_len, value)) {
            return "truncated value";
        }

        entry.setVersion(version);
        entry.writer_node_hash = writer_hash;
        entry.value = std::move(value);
        entry.setTombstone((entry_flags & 2U) != 0);
        if (has_ttl) {
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

    auto AntiEntropyManager::applySync(SyncMessage msg) -> size_t {
        size_t applied = 0;
        for (auto& item : msg.entries) {
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
        return applySync(std::move(*msg));
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
        if (req.key == "bucket-refinement-v3") {
            onBucketRefinement(from, req, std::move(respond));
            return;
        }

        Span span("anti_entropy.digest", captureTraceContext());
        auto remote_groups = decodeGroupDigest(
            req.value,
            (num_buckets_ + K_BUCKETS_PER_GROUP - 1) / K_BUCKETS_PER_GROUP
        );
        if (!remote_groups.has_value()) {
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

        DigestResponse response;
        response.group_hashes = groupDigests(digestForRound());
        if (metrics_ != nullptr) {
            metrics_->replicationMetrics().anti_entropy_rounds.fetch_add(
                1,
                std::memory_order_relaxed
            );
        }

        net::Response digest_response{
            .status = Errc::OK,
            .value = encodeDigestResponse(response),
            .trace_id = req.trace_id,
            .span_id = req.span_id,
        };

        CINDER_DEBUG("digest received", {"from", from}, {"groups", response.group_hashes.size()});
        respond(std::move(digest_response));
    }

    void AntiEntropyManager::onBucketRefinement(
        const NodeId& from,
        const net::Request& req,
        std::function<void(net::Response)> respond
    ) {
        Span span("anti_entropy.refine", captureTraceContext());
        auto request = decodeBucketRefinement(req.value, num_buckets_);
        if (!request.has_value()) {
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
        DigestResponse response;
        response.group_hashes = groupDigests(local);
        std::vector<uint32_t> requested_buckets;
        for (const auto& [bucket, requester_hash] : request->bucket_hashes) {
            response.bucket_hashes.emplace_back(bucket, local[bucket]);
            if (local[bucket] != requester_hash) {
                requested_buckets.push_back(bucket);
            }
        }

        std::sort(requested_buckets.begin(), requested_buckets.end());
        std::sort(response.bucket_hashes.begin(), response.bucket_hashes.end());
        if (request->force_metadata) {
            std::vector<uint32_t> metadata_covered;
            auto meta = collectMetadata(requested_buckets, K_MAX_METADATA_BYTES, &metadata_covered);
            auto metadata_rows = decodeMetaRows(meta);
            if (!metadata_rows.has_value()) {
                respond(
                    net::Response{
                        .status = Errc::InternalError,
                        .value = std::nullopt,
                        .trace_id = req.trace_id,
                        .span_id = req.span_id,
                    }
                );
                return;
            }

            response.covered_buckets = std::move(metadata_covered);
            response.rows = std::move(*metadata_rows);
        } else {
            struct BucketBuild {
                std::vector<MetaRow> sample;
                IbltSketch sketch;
                bool sketching = false;
            };

            std::vector<char> requested(num_buckets_, 0);
            std::vector<std::optional<BucketBuild>> builds(num_buckets_);
            for (uint32_t bucket : requested_buckets) {
                requested[bucket] = 1;
                builds[bucket].emplace();
                builds[bucket]->sample.reserve(K_IBLT_MIN_ROWS);
            }

            store_.forEach(
                [this, &requested, &builds](const std::string& key, const VersionedEntry& entry) {
                const uint32_t bucket = bucketFor(key);
                if (!requested[bucket]) {
                    return;
                }

                MetaRow row{
                    .bucket = bucket,
                    .tombstone = entry.isTombstone(),
                    .key_hash = XXH3_64bits(key.data(), key.size()),
                    .version = entry.version(),
                    .writer = entry.writer_node_hash,
                };

                auto& build = *builds[bucket];
                if (!build.sketching) {
                    build.sample.push_back(row);
                    if (build.sample.size() == K_IBLT_MIN_ROWS) {
                        auto sketch = buildIblt(build.sample, K_IBLT_DEFAULT_CELLS);
                        if (sketch.has_value()) {
                            build.sketch = std::move(*sketch);
                            build.sample.clear();
                            build.sketching = true;
                        }
                    }
                } else {
                    const auto positions = ibltPositions(
                        row.key_hash,
                        row.version,
                        row.writer,
                        row.tombstone,
                        K_IBLT_DEFAULT_CELLS
                    );
                    for (size_t pos : positions) {
                        ibltApply(
                            build.sketch.cells[pos],
                            1,
                            row.key_hash,
                            row.version,
                            row.writer,
                            row.tombstone
                        );
                    }
                }
            }
            );
            for (uint32_t bucket : requested_buckets) {
                auto& build = *builds[bucket];
                response.covered_buckets.push_back(bucket);
                if (build.sketching) {
                    response.iblt_sketches.emplace_back(bucket, encodeIblt(build.sketch));
                } else {
                    response.rows
                        .insert(response.rows.end(), build.sample.begin(), build.sample.end());
                }
            }
        }
        if (metrics_ != nullptr) {
            metrics_->replicationMetrics().anti_entropy_meta_rows_sent.fetch_add(
                response.rows.size(),
                std::memory_order_relaxed
            );
        }

        CINDER_DEBUG(
            "bucket refinement received",
            {"from", from},
            {"bucket_hashes", request->bucket_hashes.size()},
            {"rows", response.rows.size()}
        );
        respond(
            net::Response{
                .status = Errc::OK,
                .value = encodeDigestResponse(response),
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
        // NOTE: entries are moved into applySync; pull the needlist out first —
        // reading msg->needs after the move sees an empty vector.
        std::vector<uint64_t> needs = std::move(msg->needs);
        size_t repaired = applySync(std::move(*msg));
        if (metrics_ != nullptr && repaired > 0) {
            metrics_->replicationMetrics().anti_entropy_keys_repaired.fetch_add(
                repaired,
                std::memory_order_relaxed
            );
        }

        // Serve the needlist (if any) with an entries-only response. The
        // entries the sender pushed are already applied above.
        SyncMessage resp;
        if (!needs.empty()) {
            resp.entries = selectEntriesForHashes(needs, fetchBytes());
            CINDER_DEBUG(
                "sync serving needlist",
                {"from", from},
                {"requested", needs.size()},
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

    void AntiEntropyManager::finishRefinement(
        const std::vector<uint64_t>& local,
        const NodeId& partner,
        SpanContext round_ctx,
        const DigestResponse& response
    ) {
        TraceScope scope(round_ctx);
        Span result_span("anti_entropy.round.result", round_ctx);
        result_span.setAttribute({"partner", partner});

        // Reconstruct the partner's full metadata set from the local set plus
        // the IBLT symmetric difference. If any sketch fails to peel, request
        // explicit rows for those buckets and use the established path.
        std::vector<uint32_t> failed_sketch_buckets;
        std::vector<MetaRow> reconstructed_remote_rows = response.rows;
        std::vector<uint32_t> reconstructed_covered_buckets = response.covered_buckets;
        std::vector<char> sketch_has_diff(num_buckets_, 0);
        size_t successful_sketches = 0;
        for (const auto& [bucket, blob] : response.iblt_sketches) {
            if (bucket >= num_buckets_) {
                failed_sketch_buckets.push_back(bucket);
                continue;
            }
            if (!std::binary_search(
                    reconstructed_covered_buckets.begin(),
                    reconstructed_covered_buckets.end(),
                    bucket
                )) {
                failed_sketch_buckets.push_back(bucket);
                continue;
            }

            std::vector<uint32_t> only_bucket{bucket};
            std::vector<uint32_t> local_covered;
            auto local_blob = collectMetadata(only_bucket, K_MAX_METADATA_BYTES, &local_covered);
            auto local_rows = decodeMetaRows(local_blob);
            auto remote_sketch = decodeIblt(blob);
            std::optional<IbltSketch> local_sketch = local_rows.has_value()
                ? buildIblt(*local_rows, K_IBLT_DEFAULT_CELLS)
                : std::nullopt;

            std::optional<IbltDiff> difference;
            if (remote_sketch.has_value()
                && local_sketch.has_value()
                && std::binary_search(local_covered.begin(), local_covered.end(), bucket)) {
                difference = decodeIbltDifference(*remote_sketch, *local_sketch, bucket);
            }
            if (!difference.has_value()) {
                failed_sketch_buckets.push_back(bucket);
                continue;
            }
            if (!std::binary_search(
                    reconstructed_covered_buckets.begin(),
                    reconstructed_covered_buckets.end(),
                    bucket
                )) {
                reconstructed_covered_buckets.push_back(bucket);
            }

            auto remote_bucket = std::find_if(
                response.bucket_hashes.begin(),
                response.bucket_hashes.end(),
                [bucket](const auto& item) { return item.first == bucket; }
            );
            if (remote_bucket
                == response.bucket_hashes.end()
                || (local[bucket]
                    != remote_bucket->second
                    && difference->remote_only.empty()
                    && difference->local_only.empty())) {
                failed_sketch_buckets.push_back(bucket);
                continue;
            }

            ankerl::unordered_dense::set<uint64_t> removed;
            removed.reserve(difference->local_only.size() * 2);
            for (const auto& row : difference->local_only) {
                removed.insert(row.key_hash);
            }
            for (const auto& row : *local_rows) {
                if (!removed.contains(row.key_hash)) {
                    reconstructed_remote_rows.push_back(row);
                }
            }

            reconstructed_remote_rows.insert(
                reconstructed_remote_rows.end(),
                difference->remote_only.begin(),
                difference->remote_only.end()
            );
            sketch_has_diff[bucket] =
                !difference->remote_only.empty() || !difference->local_only.empty();
            ++successful_sketches;
        }

        std::sort(failed_sketch_buckets.begin(), failed_sketch_buckets.end());
        failed_sketch_buckets.erase(
            std::unique(failed_sketch_buckets.begin(), failed_sketch_buckets.end()),
            failed_sketch_buckets.end()
        );
        std::sort(reconstructed_covered_buckets.begin(), reconstructed_covered_buckets.end());
        if (!failed_sketch_buckets.empty()) {
            if (metrics_ != nullptr) {
                metrics_->replicationMetrics().anti_entropy_iblt_fallbacks.fetch_add(
                    failed_sketch_buckets.size(),
                    std::memory_order_relaxed
                );
            }

            BucketRefinement fallback;
            fallback.force_metadata = true;
            for (uint32_t bucket : failed_sketch_buckets) {
                if (bucket < local.size()) {
                    fallback.bucket_hashes.emplace_back(bucket, local[bucket]);
                }
            }

            net::Request fallback_req;
            fallback_req.opcode = net::Opcode::AntiEntropyDigest;
            fallback_req.key = "bucket-refinement-v3";
            fallback_req.value = encodeBucketRefinement(fallback);
            fallback_req.trace_id = round_ctx.trace_id;
            fallback_req.span_id = round_ctx.span_id;
            transport_.sendRequestAsync(
                partner,
                fallback_req,
                [this,
                 partner,
                 round_ctx,
                 local,
                 response,
                 failed_sketch_buckets,
                 reconstructed_covered_buckets,
                 rows =
                     std::move(reconstructed_remote_rows)](Result<net::Response> result) mutable {
                if (!result.has_value()
                    || result->status
                    != Errc::OK
                    || !result->value.has_value()) {
                    CINDER_DEBUG("anti-entropy IBLT fallback failed", {"partner", partner});
                    return;
                }

                const size_t groups =
                    (num_buckets_ + K_BUCKETS_PER_GROUP - 1) / K_BUCKETS_PER_GROUP;
                auto fallback_response = decodeDigestResponse(*result->value, groups);
                if (!fallback_response.has_value()) {
                    CINDER_WARN("bad anti-entropy IBLT fallback", {"partner", partner});
                    return;
                }

                DigestResponse merged = response;
                merged.iblt_sketches.clear();
                merged.covered_buckets = reconstructed_covered_buckets;
                merged.covered_buckets.erase(
                    std::remove_if(
                        merged.covered_buckets.begin(),
                        merged.covered_buckets.end(),
                        [&failed_sketch_buckets](uint32_t bucket) {
                    return std::binary_search(
                        failed_sketch_buckets.begin(),
                        failed_sketch_buckets.end(),
                        bucket
                    );
                }
                    ),
                    merged.covered_buckets.end()
                );

                std::sort(
                    reconstructed_covered_buckets.begin(),
                    reconstructed_covered_buckets.end()
                );
                for (uint32_t bucket : reconstructed_covered_buckets) {
                    if (!std::binary_search(
                            failed_sketch_buckets.begin(),
                            failed_sketch_buckets.end(),
                            bucket
                        )
                        && !std::binary_search(
                            merged.covered_buckets.begin(),
                            merged.covered_buckets.end(),
                            bucket
                        )) {
                        merged.covered_buckets.push_back(bucket);
                    }
                }

                merged.rows = std::move(rows);
                merged.rows.erase(
                    std::remove_if(
                        merged.rows.begin(),
                        merged.rows.end(),
                        [&failed_sketch_buckets](const MetaRow& row) {
                    return std::binary_search(
                        failed_sketch_buckets.begin(),
                        failed_sketch_buckets.end(),
                        row.bucket
                    );
                }
                    ),
                    merged.rows.end()
                );
                merged.rows.insert(
                    merged.rows.end(),
                    fallback_response->rows.begin(),
                    fallback_response->rows.end()
                );
                merged.covered_buckets.insert(
                    merged.covered_buckets.end(),
                    fallback_response->covered_buckets.begin(),
                    fallback_response->covered_buckets.end()
                );

                std::sort(merged.covered_buckets.begin(), merged.covered_buckets.end());
                for (const auto& [bucket, hash] : fallback_response->bucket_hashes) {
                    auto it = std::find_if(
                        merged.bucket_hashes.begin(),
                        merged.bucket_hashes.end(),
                        [bucket](const auto& entry) { return entry.first == bucket; }
                    );

                    if (it == merged.bucket_hashes.end()) {
                        merged.bucket_hashes.emplace_back(bucket, hash);
                    } else {
                        it->second = hash;
                    }
                }

                std::sort(merged.covered_buckets.begin(), merged.covered_buckets.end());
                merged.covered_buckets.erase(
                    std::unique(merged.covered_buckets.begin(), merged.covered_buckets.end()),
                    merged.covered_buckets.end()
                );
                finishRefinement(local, partner, round_ctx, merged);
            }
            );
            return;
        }
        if (metrics_ != nullptr && !response.iblt_sketches.empty() && successful_sketches > 0) {
            metrics_->replicationMetrics().anti_entropy_iblt_successes.fetch_add(
                successful_sketches,
                std::memory_order_relaxed
            );
        }

        std::vector<uint32_t> divergent_buckets;
        std::vector<uint64_t> remote_buckets(num_buckets_, 0);
        std::vector<char> has_remote_bucket(num_buckets_, 0);
        for (const auto& [bucket, hash] : response.bucket_hashes) {
            if (bucket >= num_buckets_ || has_remote_bucket[bucket]) {
                CINDER_WARN("bad bucket digest response", {"from", partner});
                result_span.setAttribute({"outcome", "bad-buckets"});
                result_span.setStatus(SpanStatus::Error);
                return;
            }

            remote_buckets[bucket] = hash;
            has_remote_bucket[bucket] = 1;
        }

        std::vector<char> has_remote_sketch(num_buckets_, 0);
        for (const auto& [bucket, blob] : response.iblt_sketches) {
            (void)blob;
            if (bucket >= num_buckets_ || has_remote_sketch[bucket]) {
                CINDER_WARN("bad bucket IBLT response", {"from", partner});
                result_span.setAttribute({"outcome", "bad-iblt-buckets"});
                result_span.setStatus(SpanStatus::Error);
                return;
            }
            has_remote_sketch[bucket] = 1;
        }

        const size_t group_count = (num_buckets_ + K_BUCKETS_PER_GROUP - 1) / K_BUCKETS_PER_GROUP;
        auto local_groups = groupDigests(local);
        if (response.group_hashes.size() != group_count) {
            result_span.setAttribute({"outcome", "bad-refinement"});
            result_span.setStatus(SpanStatus::Error);
            return;
        }

        std::vector<uint32_t> divergent_groups;
        for (uint32_t group = 0; group < group_count; ++group) {
            if (local_groups[group] != response.group_hashes[group]) {
                divergent_groups.push_back(group);
            }
        }

        std::vector<uint32_t> sorted_covered_buckets = reconstructed_covered_buckets;
        std::sort(sorted_covered_buckets.begin(), sorted_covered_buckets.end());
        for (uint32_t bucket = 0; bucket < num_buckets_; ++bucket) {
            const bool bucket_differs = local[bucket] != remote_buckets[bucket];
            if (has_remote_bucket[bucket] && bucket_differs) {
                divergent_buckets.push_back(bucket);
            } else if (
                has_remote_sketch[bucket]
                && std::binary_search(
                    sorted_covered_buckets.begin(),
                    sorted_covered_buckets.end(),
                    bucket
                )
                && sketch_has_diff[bucket]
            ) {
                divergent_buckets.push_back(bucket);
            }
        }
        if (divergent_buckets.empty()) {
            result_span.setAttribute({"divergent_groups", divergent_groups.size()});
            result_span.setAttribute({"divergent", 0});
            result_span.setAttribute({"outcome", "completed"});
            result_span.setStatus(SpanStatus::Ok);
            CINDER_INFO("round completed", {"partner", partner}, {"repaired", 0});
            return;
        }
        for (uint32_t bucket = 0; bucket < num_buckets_; ++bucket) {
            if (has_remote_sketch[bucket]
                && !std::binary_search(
                    reconstructed_covered_buckets.begin(),
                    reconstructed_covered_buckets.end(),
                    bucket
                )) {
                reconstructed_covered_buckets.push_back(bucket);
            }
        }

        std::sort(reconstructed_covered_buckets.begin(), reconstructed_covered_buckets.end());
        std::vector<char> covered(num_buckets_, 0);
        for (uint32_t bucket : reconstructed_covered_buckets) {
            if (bucket >= num_buckets_) {
                CINDER_WARN("bad metadata coverage", {"from", partner});
                result_span.setAttribute({"outcome", "bad-meta-coverage"});
                result_span.setStatus(SpanStatus::Error);
                return;
            }
            covered[bucket] = 1;
        }
        for (const auto& row : reconstructed_remote_rows) {
            if (row.bucket
                >= num_buckets_
                || !covered[row.bucket]
                || !std::binary_search(
                    divergent_buckets.begin(),
                    divergent_buckets.end(),
                    row.bucket
                )) {
                CINDER_WARN("bad metadata row", {"from", partner});
                result_span.setAttribute({"outcome", "bad-meta-row"});
                result_span.setStatus(SpanStatus::Error);
                return;
            }
        }
        if (metrics_ != nullptr) {
            metrics_->replicationMetrics().anti_entropy_group_mismatches.fetch_add(
                divergent_groups.size(),
                std::memory_order_relaxed
            );
            metrics_->replicationMetrics().anti_entropy_bucket_mismatches.fetch_add(
                divergent_buckets.size(),
                std::memory_order_relaxed
            );
            metrics_->replicationMetrics().anti_entropy_meta_rows_recv.fetch_add(
                reconstructed_remote_rows.size(),
                std::memory_order_relaxed
            );
        }

        MetaDiff diff =
            diffRows(reconstructed_remote_rows, divergent_buckets, reconstructed_covered_buckets);
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

        // The digest result span is closed before this async callback; use a
        // separate fetch span that retains the original round context.
        result_span.setAttribute({"divergent_groups", divergent_groups.size()});
        result_span.setAttribute({"divergent_buckets", divergent_buckets.size()});
        result_span.setAttribute({"outcome", "completed"});
        result_span.setStatus(SpanStatus::Ok);
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

            auto resp = decodeSync(*sr->value);
            if (!resp.has_value() || !resp->needs.empty()) {
                CINDER_WARN("bad sync response", {"from", partner});
                fetch_span.setAttribute({"outcome", "bad-sync"});
                fetch_span.setStatus(SpanStatus::Error);
                return;
            }

            size_t fetched = resp->entries.size();
            size_t repaired = applySync(std::move(*resp));
            if (metrics_ != nullptr) {
                if (repaired > 0) {
                    metrics_->replicationMetrics().anti_entropy_keys_repaired.fetch_add(
                        repaired,
                        std::memory_order_relaxed
                    );
                }
                metrics_->replicationMetrics().anti_entropy_entries_fetched.fetch_add(
                    fetched,
                    std::memory_order_relaxed
                );
            }

            CINDER_INFO(
                "fetch completed",
                {"partner", partner},
                {"repaired", repaired},
                {"entries", fetched}
            );
            CINDER_INFO("round completed", {"partner", partner}, {"repaired", repaired});

            fetch_span.setAttribute({"repaired", repaired});
            fetch_span.setAttribute({"outcome", "completed"});
            fetch_span.setStatus(SpanStatus::Ok);
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
        req.value = encodeGroupDigest(groupDigests(local));
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

            const size_t group_count =
                (num_buckets_ + K_BUCKETS_PER_GROUP - 1) / K_BUCKETS_PER_GROUP;
            auto response = decodeDigestResponse(*r->value, group_count);
            if (!response.has_value()) {
                CINDER_WARN("short digest response", {"from", partner});
                result_span.setAttribute({"outcome", "short-response"});
                result_span.setStatus(SpanStatus::Error);
                return;
            }

            auto local_groups = groupDigests(local);
            std::vector<uint32_t> divergent_groups;
            for (uint32_t group = 0; group < group_count; ++group) {
                if (local_groups[group] != response->group_hashes[group]) {
                    divergent_groups.push_back(group);
                }
            }
            if (divergent_groups.empty()) {
                result_span.setAttribute({"repaired", 0});
                result_span.setAttribute({"divergent_groups", 0});
                result_span.setAttribute({"divergent", 0});
                result_span.setAttribute({"outcome", "completed"});
                result_span.setStatus(SpanStatus::Ok);
                CINDER_INFO("round completed", {"partner", partner}, {"repaired", 0});
                return;
            }
            if (metrics_ != nullptr) {
                metrics_->replicationMetrics().anti_entropy_group_mismatches.fetch_add(
                    divergent_groups.size(),
                    std::memory_order_relaxed
                );
            }
            // Phase 2: request hashes only for buckets in divergent groups.
            // This refines one 128-byte group hash into at most 16 bucket
            // hashes; values and metadata remain absent until the exact
            // divergent buckets are known.
            BucketRefinement refinement;
            for (uint32_t group : divergent_groups) {
                const uint32_t begin = group * K_BUCKETS_PER_GROUP;
                const uint32_t end = std::min(begin + K_BUCKETS_PER_GROUP, num_buckets_);
                for (uint32_t bucket = begin; bucket < end; ++bucket) {
                    refinement.bucket_hashes.emplace_back(bucket, local[bucket]);
                }
            }

            const SpanContext refinement_parent = result_span.context();
            result_span.setAttribute({"divergent_groups", divergent_groups.size()});
            result_span.setAttribute({"outcome", "refining"});
            result_span.setStatus(SpanStatus::Ok);

            net::Request refine_req;
            refine_req.opcode = net::Opcode::AntiEntropyDigest;
            refine_req.key = "bucket-refinement-v3";
            refine_req.value = encodeBucketRefinement(refinement);
            refine_req.trace_id = round_ctx.trace_id;
            refine_req.span_id = round_ctx.span_id;
            transport_.sendRequestAsync(
                partner,
                refine_req,
                [this, local = std::move(local), partner, refinement_parent](
                    Result<net::Response> refined
                ) mutable {
                TraceScope refinement_scope(refinement_parent);
                if (!refined.has_value()
                    || refined->status
                    != Errc::OK
                    || !refined->value.has_value()) {
                    Span fail_span("anti_entropy.round.refinement", refinement_parent);
                    fail_span.setAttribute({"partner", partner});
                    fail_span.setAttribute({"outcome", "refinement-failed"});
                    fail_span.setStatus(SpanStatus::Error);
                    return;
                }

                const size_t groups =
                    (num_buckets_ + K_BUCKETS_PER_GROUP - 1) / K_BUCKETS_PER_GROUP;
                auto decoded = decodeDigestResponse(*refined->value, groups);
                if (!decoded.has_value()) {
                    Span fail_span("anti_entropy.round.refinement", refinement_parent);
                    fail_span.setAttribute({"partner", partner});
                    fail_span.setAttribute({"outcome", "bad-refinement"});
                    fail_span.setStatus(SpanStatus::Error);
                    return;
                }

                Span refinement_span("anti_entropy.round.refinement", refinement_parent);
                refinement_span.setAttribute({"partner", partner});
                refinement_span.setAttribute({"bucket_hashes", decoded->bucket_hashes.size()});
                refinement_span.setStatus(SpanStatus::Ok);
                finishRefinement(local, partner, refinement_parent, *decoded);
            }
            );
        }
        );
    }
} // namespace cinder
