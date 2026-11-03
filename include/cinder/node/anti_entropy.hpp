#pragma once

#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <memory>
#include <mutex>
#include <optional>
#include <shared_mutex>
#include <string>
#include <string_view>
#include <vector>

#include "cinder/cluster/clock.hpp"
#include "cinder/cluster/transport.hpp"
#include "cinder/common/metrics.hpp"
#include "cinder/common/tracing.hpp"
#include "cinder/common/types.hpp"
#include "cinder/hashing/consistent_hash_ring.hpp"
#include "cinder/net/protocol.hpp"
#include "cinder/store/cache_store.hpp"

using std::chrono::milliseconds;

namespace cinder {

    // Periodic anti-entropy repair. Each round compares a compact digest with
    // a replica partner, then reconciles only the difference:
    //
    //   Phase 1 (digest + metadata): initiator sends its digest (one xxHash
    //   per bucket). Partner responds with its digest plus compact metadata
    //   rows (key_hash, version, writer — 24 bytes each, no values) for
    //   divergent buckets.
    //   Phase 2 (targeted fetch): both sides derive the same divergent set.
    //   The initiator pushes full entries only where it wins and sends a
    //   needlist (key hashes) where it loses; the partner replies with full
    //   entries for the needlist. Values cross the wire solely for true
    //   deltas. Both sides apply received entries via LWW (putVersioned),
    //   making the protocol idempotent and commutative.
    //
    // Digests cover (key, version, writer) only — never values. (writer,
    // version) uniquely identifies a write (per-writer Lamport clock, single
    // store version authority), so hashing values is pure overhead.
    //
    // Wire schema v2 (magic-prefixed blobs). The v1 layout (bare
    // count-first blobs) is rejected: old peers read the magic as a count,
    // fail the size check, and report InvalidArgument — a clean failed round
    // that resumes once the cluster is homogeneous.
    class AntiEntropyManager {
    public:

        // Schema magics (u32 LE) prefixing every anti-entropy payload.
        static constexpr uint32_t K_DIGEST_MAGIC = 0x41454D32; // "AEM2"
        static constexpr uint32_t K_GROUP_DIGEST_MAGIC = 0x41454D33; // "AEM3"
        static constexpr uint32_t K_DIGEST_RESPONSE_MAGIC = 0x41455233; // "AER3"
        static constexpr uint32_t K_BUCKET_REFINEMENT_MAGIC = 0x41454233; // "AEB3"
        static constexpr uint32_t K_SYNC_MAGIC = 0x41454D53; // "AEMS"
        static constexpr uint32_t K_NEED_MAGIC = 0x41454D4E; // "AEMN"
        static constexpr uint32_t K_BUCKETS_PER_GROUP = 16;

        // Upper bound on metadata rows per digest response. Buckets fill
        // atomically (a bucket's rows are all included or the bucket is
        // deferred to a later round), so truncation can delay but never stall
        // convergence.
        static constexpr size_t K_MAX_METADATA_BYTES = 4U << 20U;

        // Backstop cadence for incremental digests (P1): full rehash at least
        // this often, bounding the silent-divergence window to wall-clock
        // time rather than round counts.
        static constexpr milliseconds K_DEFAULT_FULL_REHASH_INTERVAL{300'000};

        // One metadata row: bucket id + key identity + recency, no value.
        // 4 + 8×3 = 28 bytes on the wire.
        struct MetaRow {
            uint32_t bucket = 0;
            uint64_t key_hash = 0;
            uint64_t version = 0;
            uint64_t writer = 0;
        };

        // A decoded sync message: full entries to apply plus key hashes the
        // sender needs back. Responses carry entries only (needs empty).
        struct SyncMessage {
            struct Item {
                std::string key;
                VersionedEntry entry;
            };

            std::vector<Item> entries;
            std::vector<uint64_t> needs;
        };

        // Diff of remote metadata against the local store: hashes to fetch
        // (remote wins) and local keys to push (local wins).
        struct MetaDiff {
            std::vector<uint64_t> need;
            // (key_hash, key) sorted by key_hash for deterministic output.
            std::vector<std::pair<uint64_t, std::string>> have;
        };

        struct DigestResponse {
            std::vector<uint64_t> group_hashes;
            std::vector<std::pair<uint32_t, uint64_t>> bucket_hashes;
            std::vector<uint32_t> covered_buckets;
            std::vector<MetaRow> rows;
        };

        struct BucketRefinement {
            std::vector<std::pair<uint32_t, uint64_t>> bucket_hashes;
        };

        AntiEntropyManager(
            CacheStore& store,
            ConsistentHashRing& ring,
            NodeId self,
            Clock& clock,
            Transport& transport,
            uint32_t num_buckets,
            MetricsCollector* metrics = nullptr,
            size_t fetch_bytes = 1U << 20U,
            milliseconds full_rehash_interval = K_DEFAULT_FULL_REHASH_INTERVAL
        );

        ~AntiEntropyManager();

        AntiEntropyManager(const AntiEntropyManager&) = delete;
        auto operator=(const AntiEntropyManager&) -> AntiEntropyManager& = delete;
        AntiEntropyManager(AntiEntropyManager&&) = delete;
        auto operator=(AntiEntropyManager&&) -> AntiEntropyManager& = delete;

        // Full-scan ground-truth digest: XOR of per-entry hashes per bucket.
        // Order-independent (no sort); the incremental accumulators (P1) must
        // always equal this — see the equivalence property test.
        [[nodiscard]]
        auto computeDigest() const -> std::vector<uint64_t>;

        // Cheap per-round digest: accumulator copy, or a full rehash on cold
        // start, bucket-count change, or backstop interval expiry.
        auto digestForRound() -> std::vector<uint64_t>;

        // Metadata rows for the given buckets, sorted by key_hash, filled
        // bucket-atomically up to max_bytes. Layout: [MAGIC][row count u32]
        // [rows: key_hash u64, version u64, writer u64]*.
        [[nodiscard]]
        auto collectMetadata(
            const std::vector<uint32_t>& bucket_ids,
            size_t max_bytes,
            std::vector<uint32_t>* covered_buckets = nullptr
        ) const -> std::string;

        // Full entries for the given key hashes (needlist serving), sorted by
        // key_hash, bounded by max_bytes. Layout: entries-only sync blob.
        [[nodiscard]]
        auto collectEntriesForHashes(const std::vector<uint64_t>& hashes, size_t max_bytes) const
            -> std::string;

        // Full entries for the given keys, sorted by key hash, bounded by
        // max_bytes. Entries-only sync blob.
        [[nodiscard]]
        auto collectEntriesByKeys(const std::vector<std::string>& keys, size_t max_bytes) const
            -> std::string;

        // Diff remote metadata rows against the local store, scoped to the
        // divergent buckets. Remote wins (fetch) iff remote.version >
        // local.version, or == with remote.writer > local.writer; absent
        // locally always loses. Local entries in divergent buckets missing
        // from the rows are wins by absence (push). need/have sorted by key
        // hash for deterministic, convergent fetch order.
        [[nodiscard]]
        auto diffRows(
            const std::vector<MetaRow>& rows,
            const std::vector<uint32_t>& divergent,
            const std::vector<uint32_t>& covered_buckets = {}
        ) const -> MetaDiff;

        // Apply a decoded sync message's entries via putVersioned (LWW).
        // Takes the message by value: entries are moved into the store.
        // Returns count of entries actually newer than what we have.
        auto applySync(SyncMessage msg) -> size_t;

        // Legacy entry point: parses an entries-only sync blob and applies it.
        // Kept for tests/fuzz; production flows use decodeSync + applySync.
        auto applyEntries(std::string_view data) -> size_t;

        // Legacy flat digest codec retained for fuzz and diagnostics.
        [[nodiscard]]
        static auto encodeDigest(const std::vector<uint64_t>& digest) -> std::string;

        // Parse a digest blob. Rejects wrong magic, wrong bucket count, and
        // size mismatches. Unknown schema ⇒ nullopt (failed round, retried
        // once the cluster is homogeneous).
        [[nodiscard]]
        static auto decodeDigest(std::string_view data, size_t expected_buckets)
            -> std::optional<std::vector<uint64_t>>;

        // Bucket indices are grouped in fixed consecutive runs
        // of 16. Group hash folds (bucket index, bucket hash) in order.
        [[nodiscard]]
        static auto groupDigests(const std::vector<uint64_t>& bucket_hashes)
            -> std::vector<uint64_t>;

        [[nodiscard]]
        static auto encodeGroupDigest(const std::vector<uint64_t>& groups) -> std::string;

        [[nodiscard]]
        static auto decodeGroupDigest(std::string_view data, size_t expected_groups)
            -> std::optional<std::vector<uint64_t>>;

        [[nodiscard]]
        static auto encodeBucketRefinement(const BucketRefinement& request) -> std::string;

        [[nodiscard]]
        static auto decodeBucketRefinement(std::string_view data, size_t expected_groups)
            -> std::optional<BucketRefinement>;

        [[nodiscard]]
        static auto encodeDigestResponse(const DigestResponse& response) -> std::string;

        [[nodiscard]]
        static auto decodeDigestResponse(std::string_view data, size_t expected_groups)
            -> std::optional<DigestResponse>;

        // Serialize metadata rows: [MAGIC][u32 count][rows...].
        [[nodiscard]]
        static auto encodeMetaRows(const std::vector<MetaRow>& rows) -> std::string;

        // Parse metadata rows with exact-size validation.
        [[nodiscard]]
        static auto decodeMetaRows(std::string_view data) -> std::optional<std::vector<MetaRow>>;

        // Serialize a sync message: [MAGIC][entry count u32][entries...]
        // [need count u32][need key_hash u64...]. Entries use the variable
        // length encoding documented in protocol.md.
        [[nodiscard]]
        auto encodeSync(const SyncMessage& msg) const -> std::string;

        // Parse a sync message with exact-size validation at every step.
        [[nodiscard]]
        auto decodeSync(std::string_view data) const -> std::optional<SyncMessage>;

        // Select next partner round-robin from the cached replica set. The
        // set refreshes on full-rehash rounds (which scan anyway) and when
        // empty, so steady-state rounds never scan for partners. Returns
        // nullopt when no other node shares a key with this node.
        [[nodiscard]]
        auto pickPartner(int replica_factor) -> std::optional<NodeId>;

        // Run one full initiator round: digest, pick a partner, exchange
        // digests + metadata, push winners, fetch losers via needlist.
        // Best-effort: transport failures only log.
        void runRound(int replica_factor);

        // Protocol handlers — called by TcpConnection::handleRequest, always
        // on that connection's strand.
        // onDigestRequest: receives initiator's digest, compares, responds
        //   with our digest plus our metadata rows for divergent buckets.
        // onSyncRequest: applies entries, serves the needlist with our
        //   entries, responds entries-only.
        // Contract: `respond` is always invoked synchronously, before return.
        // TcpConnection captures raw `this` in the callback and assumes
        // strand confinement for sendResponse/maybeRead, so any future change
        // that defers the callback (post, async store access) must switch to
        // shared_from_this capture and re-enter via the connection's strand.
        void onDigestRequest(
            const NodeId& from,
            const net::Request& req,
            std::function<void(net::Response)> respond
        );

        void onBucketRefinement(
            const NodeId& from,
            const net::Request& req,
            std::function<void(net::Response)> respond
        );

        void onSyncRequest(
            const NodeId& from,
            const net::Request& req,
            std::function<void(net::Response)> respond
        );

        void setMetrics(MetricsCollector* m) {
            metrics_ = m;
        }

        void setFetchBytes(size_t bytes) {
            fetch_bytes_.store(bytes, std::memory_order_relaxed);
        }

        [[nodiscard]]
        auto fetchBytes() const -> size_t {
            return fetch_bytes_.load(std::memory_order_relaxed);
        }

        void setFullRehashInterval(milliseconds interval) {
            full_rehash_interval_.store(interval, std::memory_order_relaxed);
        }

        [[nodiscard]]
        auto numBuckets() const -> uint32_t {
            return num_buckets_;
        }

    private:

        // Copy the accumulators (atomic loads; a torn cross-bucket read only
        // looks divergent and self-heals next round).
        [[nodiscard]]
        auto snapshotXor() const -> std::vector<uint64_t>;

        // Exact full rehash with divert-while-scanning (see the P1 spec):
        // mutations during the scan divert to a pending log instead of the
        // accumulators; drain+publish under the write lock is the only
        // writer-blocking window (~microseconds). Compares scan vs live
        // accumulators first and counts mismatches (always a bug).
        void fullRehash();

        // Recompute the cached replica-partner set via a store scan. Called on
        // full-rehash rounds (which scan anyway) and when the cache is empty,
        // so steady-state rounds never scan for partners.
        void refreshPartners(int replica_factor);

        void finishRefinement(
            const std::vector<uint64_t>& local_bucket_hashes,
            const NodeId& partner,
            SpanContext round_ctx,
            const DigestResponse& response
        );

        // Fill entries for sorted (hash, key) pairs up to max_bytes, via
        // point lookups (deterministic order, cursor-free convergence: the
        // next round re-diffs the remainder).
        [[nodiscard]]
        auto selectEntriesForPairs(
            const std::vector<std::pair<uint64_t, std::string>>& pairs,
            size_t max_bytes
        ) const -> std::vector<SyncMessage::Item>;

        // Resolve key hashes to keys with one scan, then select as above.
        [[nodiscard]]
        auto selectEntriesForHashes(const std::vector<uint64_t>& hashes, size_t max_bytes) const
            -> std::vector<SyncMessage::Item>;

        // Compute which bucket a key belongs to.
        [[nodiscard]]
        auto bucketFor(const std::string& key) const -> uint32_t;

        // Entry hash: XXH3 over (key_len, key, version_and_ttl, writer).
        // Order-independent digest atom; the P1 accumulator XORs these.
        [[nodiscard]]
        static auto entryHash(const std::string& key, const VersionedEntry& entry) -> uint64_t;
        [[nodiscard]]
        static auto entryHashRaw(std::string_view key, uint64_t version_and_ttl, uint64_t writer)
            -> uint64_t;

        // Store-mutation observer (P1): XORs the entry hash into the
        // accumulator. Leaf-only: never calls back into the store (invoked
        // under the store's shard lock — see the lock-ordering rule on
        // CacheStore::setMutationSink).
        void onStoreMutation(
            std::string_view key,
            uint64_t version_and_ttl,
            uint64_t writer,
            bool removed
        );

        // Parse one entry from [p,end), advancing p. Returns failure reason or
        // nullopt on success; key/entry are valid only on success.
        auto parseSingleEntry(
            const char*& p,
            const char* end,
            std::string& key,
            VersionedEntry& entry
        ) const -> std::optional<std::string_view>;
        [[nodiscard]]
        auto isNewer(const std::string& key, uint64_t version, uint64_t writer_hash) -> bool;

        CacheStore& store_;
        ConsistentHashRing& ring_;
        Clock& clock_;
        Transport& transport_;
        std::atomic<size_t> fetch_bytes_;
        std::atomic<milliseconds> full_rehash_interval_;
        size_t partner_idx_ = 0;
        MetricsCollector* metrics_ = nullptr;
        // P1 incremental state. xor_[b] is the XOR of entry hashes in bucket b,
        // updated lock-free on the write path (atomic fetch_xor) and blessed
        // by fullRehash. No mutex on the fast path: torn cross-bucket reads
        // only look divergent and self-heal next round. Fixed-size array:
        // vector<atomic> is neither movable nor copyable.
        std::unique_ptr<std::atomic<uint64_t>[]> xor_;
        // Nanoseconds since steady-clock epoch of the last blessed rehash.
        std::atomic<int64_t> last_full_rehash_ns_{0};
        // Pending-log for divert-while-scanning (see fullRehash).
        std::vector<std::pair<uint32_t, uint64_t>> pending_;
        std::vector<NodeId> partners_;
        NodeId self_;
        std::mutex pending_mutex_;
        std::shared_mutex acc_mutex_;
        uint32_t num_buckets_;
        // Cold start / post-rehash refresh flag; consumed on the timer thread
        // in pickPartner, so refresh scans never run on connection strands.
        std::atomic<bool> partners_dirty_{true};
        std::atomic<bool> xor_valid_{false};
        // Rehash mutual exclusion: concurrent digestForRound calls must not
        // rehash simultaneously; losers serve the current accumulators.
        std::atomic<bool> rehashing_{false};
        // Divert-while-scanning flag (see fullRehash).
        std::atomic<bool> capturing_{false};
    };
} // namespace cinder
