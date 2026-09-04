#include "cinder/node/anti_entropy.hpp"

#include <algorithm>
#include <array>
#include <cstring>
#include <set>
#include <utility>
#include <vector>
#include <xxhash.h>

#include "cinder/common/logger.hpp"

namespace cinder {
namespace {

// Little-endian append helpers for the digest/sync blobs. The cluster is
// homogeneous, but a fixed endianness keeps the wire format well-defined.
void
appendU32(std::string& out, uint32_t v) {
    std::array<char, sizeof(v)> buf{};
    std::memcpy(buf.data(), &v, buf.size());
    out.append(buf.data(), buf.size());
}

void
appendU64(std::string& out, uint64_t v) {
    std::array<char, sizeof(v)> buf{};
    std::memcpy(buf.data(), &v, buf.size());
    out.append(buf.data(), buf.size());
}

auto
readU32(const char*& p, const char* end, uint32_t& out) -> bool {
    if (static_cast<size_t>(end - p) < sizeof(uint32_t)) {
        return false;
    }

    std::memcpy(&out, p, sizeof(out));
    p += sizeof(out);
    return true;
}

auto
readU64(const char*& p, const char* end, uint64_t& out) -> bool {
    if (static_cast<size_t>(end - p) < sizeof(uint64_t)) {
        return false;
    }

    std::memcpy(&out, p, sizeof(out));
    p += sizeof(out);
    return true;
}

auto
readBytes(const char*& p, const char* end, size_t n, std::string& out) -> bool {
    if (n > static_cast<size_t>(end - p)) {
        return false;
    }

    out.assign(p, n);
    p += n;
    return true;
}

// Steady expiries are node-local (clocks differ), so the digest covers only
// the data that must converge: key, value, version, writer hash, and the
// TTL-presence flag. Exact expiry instants are intentionally excluded.
void
hashEntry(XXH3_state_t* state, const std::string& key, const VersionedEntry& entry) {
    auto kl = static_cast<uint32_t>(key.size());
    XXH3_64bits_update(state, &kl, sizeof(kl));
    if (!key.empty()) {
        XXH3_64bits_update(state, key.data(), key.size());
    }

    auto vl = static_cast<uint32_t>(entry.value.size());
    XXH3_64bits_update(state, &vl, sizeof(vl));
    if (!entry.value.empty()) {
        XXH3_64bits_update(state, entry.value.data(), entry.value.size());
    }

    XXH3_64bits_update(state, &entry.version, sizeof(entry.version));
    XXH3_64bits_update(state, &entry.writer_node_hash, sizeof(entry.writer_node_hash));
    uint8_t ht = entry.has_ttl ? 1 : 0;
    XXH3_64bits_update(state, &ht, sizeof(ht));
}
} // namespace

AntiEntropyManager::AntiEntropyManager(CacheStore& store, ConsistentHashRing& ring, NodeId self,
    Clock& clock, Transport& transport, uint32_t num_buckets, MetricsCollector* metrics)
    : store_(store),
      ring_(ring),
      self_(std::move(self)),
      clock_(clock),
      transport_(transport),
      num_buckets_(num_buckets == 0 ? 1 : num_buckets),
      metrics_(metrics) {}

auto
AntiEntropyManager::bucketFor(const std::string& key) const -> uint32_t {
    return static_cast<uint32_t>(XXH3_64bits(key.data(), key.size()) % num_buckets_);
}

auto
AntiEntropyManager::computeDigest() const -> std::vector<uint64_t> {
    // Bucket (key, entry) pairs; sorting each bucket by key makes the digest
    // independent of store iteration order (LRU recency differs per node).
    std::vector<std::vector<std::pair<std::string, VersionedEntry>>> buckets(num_buckets_);
    store_.forEach([this, &buckets](const std::string& key, const VersionedEntry& entry) {
        buckets[bucketFor(key)].emplace_back(key, entry);
    });

    std::vector<uint64_t> digest(num_buckets_, 0);
    XXH3_state_t* state = XXH3_createState();
    for (uint32_t bucket = 0; bucket < num_buckets_; ++bucket) {
        auto& vec = buckets[bucket];
        if (vec.empty()) {
            digest[bucket] = 0;
            continue;
        }

        std::sort(vec.begin(), vec.end(), [](const auto& lhs, const auto& rhs) {
            return lhs.first < rhs.first;
        });
        XXH3_64bits_reset(state);
        for (const auto& [key, entry] : vec) {
            hashEntry(state, key, entry);
        }
        digest[bucket] = XXH3_64bits_digest(state);
    }
    XXH3_freeState(state);
    return digest;
}

auto
AntiEntropyManager::encodeDigest(const std::vector<uint64_t>& digest) -> std::string {
    std::string out;
    out.reserve(sizeof(uint32_t) + digest.size() * sizeof(uint64_t));
    appendU32(out, static_cast<uint32_t>(digest.size()));
    for (auto h : digest) {
        appendU64(out, h);
    }
    return out;
}

auto
AntiEntropyManager::decodeDigest(std::string_view data, size_t expected_buckets)
    -> std::optional<std::vector<uint64_t>> {
    const char* p = data.data();
    const char* end = p + data.size();
    uint32_t n = 0;
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

auto
AntiEntropyManager::encodeBucketIds(const std::vector<uint32_t>& ids) -> std::string {
    std::string out;
    out.reserve(sizeof(uint32_t) + ids.size() * sizeof(uint32_t));
    appendU32(out, static_cast<uint32_t>(ids.size()));
    for (auto id : ids) {
        appendU32(out, id);
    }
    return out;
}

auto
AntiEntropyManager::decodeBucketIds(std::string_view data) -> std::optional<std::vector<uint32_t>> {
    const char* p = data.data();
    const char* end = p + data.size();
    uint32_t n = 0;
    if (!readU32(p, end, n)) {
        return std::nullopt;
    }
    if (static_cast<size_t>(end - p) != static_cast<size_t>(n) * sizeof(uint32_t)) {
        return std::nullopt;
    }

    std::vector<uint32_t> ids;
    ids.reserve(n);
    for (uint32_t i = 0; i < n; ++i) {
        uint32_t id = 0;
        if (!readU32(p, end, id)) {
            return std::nullopt;
        }
        ids.push_back(id);
    }
    return ids;
}

auto
AntiEntropyManager::collectEntries(const std::vector<uint32_t>& bucket_ids) const -> std::string {
    std::vector<char> wanted(num_buckets_, 0);
    for (auto id : bucket_ids) {
        if (id < num_buckets_) {
            wanted[id] = 1;
        }
    }

    struct Item {
        std::string key;
        VersionedEntry entry;
    };

    std::vector<Item> items;
    store_.forEach([this, &wanted, &items](const std::string& key, const VersionedEntry& entry) {
        if (wanted[bucketFor(key)]) {
            items.push_back({key, entry});
        }
    });
    // Deterministic wire order so both sides hash/apply identically.
    std::sort(
        items.begin(), items.end(), [](const Item& a, const Item& b) { return a.key < b.key; });

    std::string out;
    appendU32(out, static_cast<uint32_t>(items.size()));
    for (const auto& item : items) {
        appendU32(out, static_cast<uint32_t>(item.key.size()));
        out.append(item.key);
        appendU64(out, item.entry.version);
        appendU64(out, item.entry.writer_node_hash);
        out.push_back(item.entry.has_ttl ? static_cast<char>(1) : static_cast<char>(0));
        if (item.entry.has_ttl) {
            appendU64(out, toSystemMs(clock_, item.entry.expires_at));
        }
        appendU32(out, static_cast<uint32_t>(item.entry.value.size()));
        out.append(item.entry.value);
    }
    return out;
}

auto
AntiEntropyManager::applyEntries(std::string_view data) -> size_t {
    const char* p = data.data();
    const char* end = p + data.size();
    uint32_t count = 0;
    if (!readU32(p, end, count)) {
        Logger::warn("cinder anti_entropy: malformed entries blob (no count)");
        return 0;
    }

    size_t applied = 0;
    for (uint32_t i = 0; i < count; ++i) {
        uint32_t key_len = 0;
        std::string key;
        uint64_t version = 0;
        uint64_t writer_hash = 0;
        uint8_t has_ttl = 0;
        uint64_t expires_ms = 0;
        uint32_t val_len = 0;
        std::string value;
        if (!readU32(p, end, key_len) || !readBytes(p, end, key_len, key)
            || !readU64(p, end, version) || !readU64(p, end, writer_hash)
            || static_cast<size_t>(end - p) < 1) {
            Logger::warn("cinder anti_entropy: truncated entry at index={}", i);
            break;
        }

        has_ttl = static_cast<uint8_t>(*p);
        ++p;
        if (has_ttl != 0 && has_ttl != 1) {
            Logger::warn("cinder anti_entropy: bad ttl flag at index={}", i);
            break;
        }
        if (has_ttl == 1 && !readU64(p, end, expires_ms)) {
            Logger::warn("cinder anti_entropy: truncated expiry at index={}", i);
            break;
        }
        if (!readU32(p, end, val_len) || !readBytes(p, end, val_len, value)) {
            Logger::warn("cinder anti_entropy: truncated value at index={}", i);
            break;
        }

        VersionedEntry entry;
        entry.version = version;
        entry.writer_node_hash = writer_hash;
        entry.value = std::move(value);
        if (has_ttl == 1) {
            entry.has_ttl = true;
            entry.expires_at =
                toSteadyExpiry(clock_, system_clock::time_point(milliseconds(expires_ms)));
        }
        // LWW apply is idempotent: stale entries are no-ops, so replaying the
        // same sync blob is always safe.
        if (store_.putVersioned(key, std::move(entry)).has_value()) {
            ++applied;
        }
    }
    return applied;
}

auto
AntiEntropyManager::pickPartner(int replica_factor) -> std::optional<NodeId> {
    if (replica_factor <= 1) {
        return std::nullopt;
    }

    std::set<NodeId> candidates;
    for (const auto& [key, entry] : store_.liveEntries()) {
        (void)entry;
        for (const auto& node : ring_.getNodes(key, replica_factor)) {
            if (node != self_) {
                candidates.insert(node);
            }
        }
        if (candidates.size() > 64) {
            break; // enough for fair rotation; avoid full scans on huge stores
        }
    }
    if (candidates.empty()) {
        return std::nullopt;
    }

    std::vector<NodeId> ordered(candidates.begin(), candidates.end());
    auto partner = ordered[partner_idx_ % ordered.size()];
    ++partner_idx_;
    return partner;
}

void
AntiEntropyManager::onDigestRequest(
    const NodeId& from, const net::Request& req, std::function<void(net::Response)> respond) {
    auto remote = decodeDigest(req.value, num_buckets_);
    if (!remote.has_value()) {
        Logger::warn("cinder anti_entropy: bad digest from={}", from);
        respond(net::Response{.status = Errc::InvalidArgument, .value = std::nullopt});
        return;
    }

    auto local = computeDigest();
    std::vector<uint32_t> divergent;
    for (uint32_t i = 0; i < num_buckets_; ++i) {
        if (local[i] != (*remote)[i]) {
            divergent.push_back(i);
        }
    }

    // Response carries our digest (so the initiator derives the same divergent
    // set for phase 2) followed by our entries for those buckets.
    std::string payload = encodeDigest(local);
    payload += collectEntries(divergent);
    Logger::debug(
        "cinder anti_entropy: digest from={} divergent_buckets={}", from, divergent.size());
    if (metrics_) {
        metrics_->replicationMetrics().anti_entropy_rounds.fetch_add(1, std::memory_order_relaxed);
    }
    respond(net::Response{.status = Errc::OK, .value = std::move(payload)});
}

void
AntiEntropyManager::onSyncRequest(
    const NodeId& from, const net::Request& req, std::function<void(net::Response)> respond) {
    size_t n = applyEntries(req.value);
    Logger::debug("cinder anti_entropy: sync from={} entries={}", from, n);
    if (metrics_ && n > 0) {
        metrics_->replicationMetrics().anti_entropy_keys_repaired.fetch_add(
            n, std::memory_order_relaxed);
    }
    respond(net::Response{.status = Errc::OK, .value = std::nullopt});
}

void
AntiEntropyManager::runRound(int replica_factor) {
    auto partner = pickPartner(replica_factor);
    if (!partner.has_value()) {
        Logger::debug("cinder anti_entropy: no partner available, skipping round");
        return;
    }

    auto local = computeDigest();
    net::Request req;
    req.opcode = net::Opcode::AntiEntropyDigest;
    req.value = encodeDigest(local);

    Logger::debug("cinder anti_entropy: starting round with partner={}", *partner);
    transport_.sendRequestAsync(*partner,
        req,
        [this, local = std::move(local), partner = *partner](Result<net::Response> r) mutable {
        if (!r.has_value() || r->status != Errc::OK || !r->value.has_value()) {
            Logger::debug("cinder anti_entropy: digest round failed partner={}", partner);
            return;
        }

        // Response layout: [digest section][entries section]. The digest
        // section has a fixed size derived from our bucket count.
        const std::string& payload = *r->value;
        size_t digest_len = sizeof(uint32_t) + static_cast<size_t>(num_buckets_) * sizeof(uint64_t);
        if (payload.size() < digest_len) {
            Logger::warn("cinder anti_entropy: short digest response from={}", partner);
            return;
        }

        auto remote = decodeDigest(std::string_view(payload.data(), digest_len), num_buckets_);
        if (!remote.has_value()) {
            Logger::warn("cinder anti_entropy: bad digest response from={}", partner);
            return;
        }

        size_t repaired = applyEntries(
            std::string_view(payload.data() + digest_len, payload.size() - digest_len));
        if (metrics_) {
            metrics_->replicationMetrics().anti_entropy_rounds.fetch_add(
                1, std::memory_order_relaxed);
            if (repaired > 0) {
                metrics_->replicationMetrics().anti_entropy_keys_repaired.fetch_add(
                    repaired, std::memory_order_relaxed);
            }
        }

        Logger::info("cinder anti_entropy: round with partner={} repaired={}", partner, repaired);
        // Phase 2: both sides derive the same divergent set from the two
        // digests; push our entries for those buckets back to the partner.
        std::vector<uint32_t> divergent;
        for (uint32_t i = 0; i < num_buckets_; ++i) {
            if (local[i] != (*remote)[i]) {
                divergent.push_back(i);
            }
        }
        if (divergent.empty()) {
            return;
        }

        net::Request sync;
        sync.opcode = net::Opcode::AntiEntropySync;
        sync.value = collectEntries(divergent);
        transport_.sendRequestAsync(partner, sync, [partner](Result<net::Response> sr) {
            if (!sr.has_value() || sr->status != Errc::OK) {
                Logger::debug("cinder anti_entropy: sync phase failed partner={}", partner);
            }
        });
    });
}
} // namespace cinder
