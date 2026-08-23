#include "cinder/node/replication_manager.hpp"

#include <algorithm>
#include <cstdint>
#include <memory>
#include <string_view>

#include "cinder/common/logger.hpp"

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
        Logger::debug("cinder replication: async write fan-out key={} replicas={}",
            key,
            replica_nodes.size());
        // Local commit counts; fan-out best-effort, hint on failure.
        for (const auto& node : replica_nodes) {
            transport_.sendAsync(node, req, [this, node, req](Result<void> r) {
                if (!r.has_value()) {
                    Logger::warn(
                        "cinder replication: replica unreachable node={} key={}", node, req.key);
                    enqueueHint(node, req);
                }
            });
        }
        on_done(ok());
        return;
    }

    // Quorum: W = R/2 + 1 acknowledgements including the local write.
    Logger::debug("cinder replication: quorum write fan-out key={} replicas={} W={}",
        key,
        replica_nodes.size(),
        replica_nodes.size() / 2 + 1);

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
ReplicationManager::readAsync(const std::string& key, const std::vector<NodeId>& replica_nodes,
    size_t R, ReadCallback on_done) {
    // Local read — always attempted first.
    auto local_entry = local_.getVersioned(key);
    if (replica_nodes.empty() || R <= 1) {
        // No replicas or trivial quorum: return whatever the local store has.
        if (local_entry.has_value()) {
            on_done(std::move(*local_entry));
        } else {
            on_done(err<VersionedEntry>(Error(Errc::NotFound, "key not found")));
        }
        return;
    }

    net::Request req;
    req.opcode = net::Opcode::GetVersioned;
    req.key = key;
    Logger::debug(
        "cinder replication: quorum read fan-out key={} replicas={}", key, replica_nodes.size());

    auto state = std::make_shared<ReadQuorumState>();
    state->done = std::make_shared<ReadCallback>(std::move(on_done));
    state->replicas = replica_nodes;
    state->pending = replica_nodes.size();
    if (local_entry.has_value()) {
        state->acks = 1;
        state->best_version = local_entry->version;
        state->best_writer_hash = local_entry->writer_node_hash;
        state->best_entry = std::move(*local_entry);
    }
    for (const auto& node : replica_nodes) {
        transport_.sendRequestAsync(
            node, req, [this, key, node, state, R](Result<net::Response> r) {
            if (state->done_flag) {
                return;
            }
            if (r.has_value() && r->status == Errc::OK && r->version != 0) {
                ++state->acks;
                if (r->version > state->best_version
                    || (r->version == state->best_version
                        && r->writer_node_hash > state->best_writer_hash)) {
                    state->needs_repair = true;
                    state->best_version = r->version;
                    state->best_writer_hash = r->writer_node_hash;

                    VersionedEntry entry;
                    entry.value = std::move(*r->value);
                    entry.version = r->version;
                    entry.writer_node_hash = r->writer_node_hash;
                    state->best_entry = std::move(entry);
                } else if (r->version != state->best_version
                           || r->writer_node_hash != state->best_writer_hash) {
                    state->needs_repair = true;
                }
            } else if (r.has_value() && r->status == Errc::NotFound) {
                // Replica doesn't have the key — stale, needs repair.
                state->needs_repair = true;
            }

            --state->pending;
            if (state->acks >= R) {
                state->done_flag = true;
                if (state->best_entry.has_value()) {
                    if (state->needs_repair) {
                        Logger::info("cinder replication: repair sent key={} version={}",
                            key,
                            state->best_entry->version);
                        for (const auto& target : state->replicas) {
                            net::Request repair;
                            repair.opcode = net::Opcode::Replicate;
                            repair.key = key;
                            repair.value = state->best_entry->value;
                            repair.version = state->best_entry->version;
                            repair.writer_node_hash = state->best_entry->writer_node_hash;
                            if (state->best_entry->has_ttl) {
                                repair.expires_at =
                                    toSystemExpiry(clock_, state->best_entry->expires_at);
                            }
                            transport_.sendAsync(target, repair, [](Result<void>) {});
                        }
                    }
                    (*state->done)(std::move(*state->best_entry));
                } else {
                    (*state->done)(err<VersionedEntry>(Error(Errc::NotFound, "key not found")));
                }
            } else if (state->pending == 0) {
                state->done_flag = true;
                if (state->best_entry.has_value()) {
                    (*state->done)(std::move(*state->best_entry));
                } else {
                    (*state->done)(err<VersionedEntry>(Error(Errc::NotFound, "key not found")));
                }
            }
        });
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
    Logger::debug("cinder replication: hint enqueued target={} key={}", target, req.key);
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
