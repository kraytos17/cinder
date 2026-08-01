#include "cinder/node/replication_manager.hpp"

#include <cstdint>
#include <string_view>

namespace cinder {

using namespace std::chrono;

namespace {

// Stable FNV-1a 64-bit hash — deterministic across compilers/platforms, used
// purely as a per-node write tiebreaker (not for the hash ring).
uint64_t
fnv1a64(std::string_view s) {
    uint64_t h = 14'695'981'039'346'656'037ULL;
    for (char c : s) {
        h ^= static_cast<uint8_t>(c);
        h *= 1'099'511'628'211ULL;
    }
    return h;
}
} // namespace

ReplicationManager::ReplicationManager(
    CacheStore& local, NodeId self, Clock& clock, Transport& transport)
    : local_(local),
      self_(std::move(self)),
      clock_(clock),
      transport_(transport) {}

auto
ReplicationManager::write(const std::string& key, std::string value,
    std::optional<milliseconds> ttl, const std::vector<NodeId>& replica_nodes, ConsistencyMode mode)
    -> Result<void> {
    Version version = version_.fetch_add(1, std::memory_order_relaxed);
    uint64_t writer = fnv1a64(self_);

    VersionedEntry entry;
    entry.value = value;
    entry.version = version;
    entry.writer_node_hash = writer;
    if (ttl.has_value()) {
        entry.expires_at = clock_.now() + *ttl;
        entry.has_ttl = true;
    }

    auto local_res = local_.putVersioned(key, std::move(entry));
    if (!local_res.has_value()) {
        return local_res;
    }

    net::Request req;
    req.opcode = net::Opcode::Replicate;
    req.key = key;
    req.value = std::move(value);
    req.ttl = ttl;
    req.version = version;
    req.writer_node_hash = writer;

    // Local write counts as one acknowledgement.
    size_t acks = 1;
    for (const auto& node : replica_nodes) {
        auto r = transport_.send(node, req);
        if (r.has_value()) {
            ++acks;
        } else {
            enqueueHint(node, req);
        }
    }
    if (mode == ConsistencyMode::Quorum) {
        size_t total = replica_nodes.size() + 1;
        size_t w = total / 2 + 1;
        if (acks < w) {
            return err(Error(Errc::NotReady, "quorum not reached"));
        }
    }
    return ok();
}

auto
ReplicationManager::replayHints() -> size_t {
    std::scoped_lock lock(hints_mutex_);
    auto now = clock_.now();
    size_t replayed = 0;
    for (auto it = hints_.begin(); it != hints_.end();) {
        if (it->expires_at <= now) {
            it = hints_.erase(it); // expired — drop
            continue;
        }

        auto r = transport_.send(it->target, it->req);
        if (r.has_value()) {
            it = hints_.erase(it);
            ++replayed;
        } else {
            ++it;
        }
    }
    return replayed;
}

auto
ReplicationManager::hintCount() const -> size_t {
    std::scoped_lock lock(hints_mutex_);
    return hints_.size();
}

void
ReplicationManager::enqueueHint(const NodeId& target, const net::Request& req) {
    std::scoped_lock lock(hints_mutex_);
    if (hints_.size() >= K_MAX_HINTS) {
        hints_.pop_front(); // drop oldest to bound memory
    }
    hints_.push_back({target, req, clock_.now() + K_HINT_TTL});
}
} // namespace cinder
