#include "cinder/node/replication_manager.hpp"

#include <algorithm>
#include <cstdint>
#include <memory>
#include <string_view>

namespace cinder {

using std::chrono::milliseconds;
using std::chrono::system_clock;

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

void
ReplicationManager::writeAsync(const std::string& key, std::string value,
    std::optional<milliseconds> ttl, const std::vector<NodeId>& replica_nodes, ConsistencyMode mode,
    WriteCallback on_done) {
    // Version comes from the store — it is the single version authority. The
    // store's counter has advanced past any observed peer versions (Lamport
    // bump) and is seeded from the clock, so a restarted node wins LWW.
    Version version = local_.mintVersion();
    uint64_t writer = fnv1a64(self_);

    VersionedEntry entry;
    entry.value = value;
    entry.version = version;
    entry.writer_node_hash = writer;
    if (ttl.has_value()) {
        entry.expires_at = clock_.now() + *ttl;
        entry.has_ttl = true;
    }

    // Capture the absolute wall-clock expiry before `entry` is moved into the
    // store below. Carried on the wire (not a relative ttl) so every replica
    // expires the key at the same instant regardless of delivery delay.
    std::optional<system_clock::time_point> wire_expiry;
    if (entry.has_ttl) {
        wire_expiry = toSystemExpiry(clock_, entry.expires_at);
    }

    auto local_res = local_.putVersioned(key, std::move(entry));
    if (!local_res.has_value()) {
        on_done(local_res);
        return;
    }

    net::Request req;
    req.opcode = net::Opcode::Replicate;
    req.key = key;
    req.value = std::move(value);
    req.expires_at = wire_expiry;
    req.version = version;
    req.writer_node_hash = writer;

    if (mode == ConsistencyMode::Async) {
        // Local commit counts; fan-out best-effort, hint on failure.
        for (const auto& node : replica_nodes) {
            transport_.sendAsync(node, req, [this, node, req](Result<void> r) {
                if (!r.has_value()) {
                    enqueueHint(node, req);
                }
            });
        }
        on_done(ok());
        return;
    }

    // Quorum: W = R/2 + 1 acknowledgements including the local write.
    size_t total = replica_nodes.size() + 1;
    size_t w = total / 2 + 1;
    auto state = std::make_shared<QuorumState>();
    state->done = std::make_shared<WriteCallback>(std::move(on_done));
    state->acks = 1; // local write
    state->pending = replica_nodes.size();

    for (const auto& node : replica_nodes) {
        transport_.sendAsync(node, req, [this, node, req, state, w](Result<void> r) {
            if (r.has_value()) {
                ++state->acks;
            } else {
                enqueueHint(node, req);
            }

            --state->pending;
            if (state->done_flag) {
                return;
            }
            if (state->acks >= w) {
                state->done_flag = true;
                (*state->done)(ok());
            } else if (state->pending == 0) {
                state->done_flag = true;
                (*state->done)(err(Error(Errc::NotReady, "quorum not reached")));
            }
        });
    }
    // No replicas: W == 1, the local ack suffices.
    if (state->pending == 0 && !state->done_flag) {
        state->done_flag = true;
        (*state->done)(ok());
    }
}

void
ReplicationManager::replayHints(ReplayCallback on_done) {
    std::vector<Hint> snapshot;
    {
        std::scoped_lock lock(hints_mutex_);
        snapshot.assign(hints_.begin(), hints_.end());
    }
    if (snapshot.empty()) {
        on_done(0);
        return;
    }

    auto state = std::make_shared<ReplayState>();
    state->done = std::make_shared<ReplayCallback>(std::move(on_done));
    state->pending = snapshot.size();
    auto now = clock_.now();

    for (const auto& hint : snapshot) {
        if (hint.expires_at <= now) {
            // Expired — drop.
            std::scoped_lock lock(hints_mutex_);
            removeHint(hint);
            if (--state->pending == 0) {
                (*state->done)(state->replayed);
            }
            continue;
        }

        transport_.sendAsync(hint.target, hint.req, [this, hint, state](Result<void> r) {
            if (r.has_value()) {
                std::scoped_lock lock(hints_mutex_);
                removeHint(hint);
                ++state->replayed;
            }
            if (--state->pending == 0) {
                (*state->done)(state->replayed);
            }
        });
    }
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

void
ReplicationManager::removeHint(const Hint& hint) {
    auto it = std::find_if(hints_.begin(), hints_.end(), [&](const Hint& h) {
        return h.target == hint.target && h.req.key == hint.req.key
               && h.req.version == hint.req.version;
    });
    if (it != hints_.end()) {
        hints_.erase(it);
    }
}
} // namespace cinder
