#pragma once

#include <cstdint>
#include <functional>
#include <optional>
#include <string>
#include <vector>

#include "cinder/cluster/clock.hpp"
#include "cinder/cluster/transport.hpp"
#include "cinder/common/metrics.hpp"
#include "cinder/common/types.hpp"
#include "cinder/hashing/consistent_hash_ring.hpp"
#include "cinder/net/protocol.hpp"
#include "cinder/store/cache_store.hpp"

namespace cinder {

// Periodic anti-entropy repair using range-hash bucketing. Keys are
// partitioned into N hash buckets; xxHash fingerprints are exchanged between
// replica partners; only divergent buckets trigger full entry sync. Both sides
// apply received entries via LWW (putVersioned), making the protocol
// idempotent and commutative.
//
// Two-phase exchange (initiated by primary every anti_entropy_interval):
//   Phase 1: Initiator sends its digest (N bucket hashes) to a replica partner.
//            Partner compares, responds with divergent bucket IDs + its entries.
//   Phase 2: Initiator applies received entries (LWW), then sends its entries
//            for the same divergent buckets back to the partner.
class AntiEntropyManager {
  public:

    AntiEntropyManager(CacheStore& store, ConsistentHashRing& ring, NodeId self, Clock& clock,
        Transport& transport, uint32_t num_buckets, MetricsCollector* metrics = nullptr);

    ~AntiEntropyManager() = default;

    AntiEntropyManager(const AntiEntropyManager&) = delete;
    auto operator=(const AntiEntropyManager&) -> AntiEntropyManager& = delete;
    AntiEntropyManager(AntiEntropyManager&&) = delete;
    auto operator=(AntiEntropyManager&&) -> AntiEntropyManager& = delete;

    // Compute per-bucket xxHash digest from a store snapshot. Returns a vector
    // of num_buckets hashes (one per bucket).
    [[nodiscard]] auto computeDigest() const -> std::vector<uint64_t>;

    // Collect all entries belonging to the given bucket IDs. Each entry is
    // serialized as: [key_len(4)][key][version(8)][writer_hash(8)]
    //                [has_ttl(1)][expires_at_ms?(8)][val_len(4)][value]
    [[nodiscard]] auto collectEntries(const std::vector<uint32_t>& bucket_ids) const -> std::string;

    // Apply received entries via putVersioned (LWW). Returns count of entries
    // applied or skipped.
    auto applyEntries(std::string_view data) -> size_t;

    // Serialize a digest vector into a blob: [u32 num][u64 hash * num].
    [[nodiscard]] static auto encodeDigest(const std::vector<uint64_t>& digest) -> std::string;

    // Parse a serialized digest blob into a vector of hashes.
    [[nodiscard]] static auto decodeDigest(std::string_view data, size_t expected_buckets)
        -> std::optional<std::vector<uint64_t>>;

    // Serialize divergent bucket IDs: [u32 count][u32 id * count].
    [[nodiscard]] static auto encodeBucketIds(const std::vector<uint32_t>& ids) -> std::string;

    // Parse serialized divergent bucket IDs.
    [[nodiscard]] static auto decodeBucketIds(std::string_view data)
        -> std::optional<std::vector<uint32_t>>;

    // Select next partner round-robin from replica set. Returns nullopt when
    // no other node shares a key with this node (e.g. empty store).
    [[nodiscard]] auto pickPartner(int replica_factor) -> std::optional<NodeId>;

    // Run one full initiator round: compute digest, pick a partner, exchange
    // digests, apply the partner's entries, and push back our entries for the
    // same divergent buckets. Best-effort: transport failures only log.
    void runRound(int replica_factor);

    // Protocol handlers — called by TcpConnection::handleRequest.
    // onDigestRequest: receives initiator's digest, compares, responds with
    //   divergent bucket IDs + this node's entries for those buckets.
    // onSyncRequest: receives entries for divergent buckets, applies via LWW.
    void onDigestRequest(
        const NodeId& from, const net::Request& req, std::function<void(net::Response)> respond);

    void onSyncRequest(
        const NodeId& from, const net::Request& req, std::function<void(net::Response)> respond);

    void setMetrics(MetricsCollector* m) { metrics_ = m; }

    [[nodiscard]] auto numBuckets() const -> uint32_t { return num_buckets_; }

  private:

    // Compute which bucket a key belongs to.
    [[nodiscard]] auto bucketFor(const std::string& key) const -> uint32_t;

    CacheStore& store_;
    ConsistentHashRing& ring_;
    NodeId self_;
    Clock& clock_;
    Transport& transport_;
    uint32_t num_buckets_;
    size_t partner_idx_ = 0;
    MetricsCollector* metrics_ = nullptr;
};
} // namespace cinder
