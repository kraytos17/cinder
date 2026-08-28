#include "cinder/node/replication_manager.hpp"

#include <cstdint>
#include <memory>
#include <string_view>

#include "cinder/common/logger.hpp"

namespace cinder {

using std::chrono::milliseconds;
using std::chrono::system_clock;

namespace {

// Stable FNV-1a 64-bit hash — deterministic across compilers/platforms, used
// purely as a per-node write tiebreaker.
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
        Logger::warn("cinder replication: local write failed key={} reason={}",
            key,
            local_res.error().message());
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
        transport_.sendAsync(node, req, [this, state, w, node, req](Result<void> r) {
            if (!r.has_value()) {
                Logger::warn(
                    "cinder replication: replica unreachable node={} key={}", node, req.key);
                enqueueHint(node, req);
            }

            bool succeed = false;
            bool fail = false;
            {
                std::scoped_lock lock(state->mu);
                if (r.has_value()) {
                    ++state->acks;
                }

                --state->pending;
                if (!state->decided) {
                    if (state->acks >= w) {
                        state->decided = true;
                        succeed = true;
                    } else if (state->pending == 0) {
                        state->decided = true;
                        fail = true;
                    }
                }
            }

            if (succeed) {
                Logger::debug("cinder replication: quorum write succeeded key={} acks={}",
                    req.key,
                    state->acks);
                (*state->done)(ok());
            } else if (fail) {
                Logger::warn("cinder replication: quorum write failed key={} acks={} needed={}",
                    req.key,
                    state->acks,
                    w);
                (*state->done)(err(Error(Errc::NotReady, "quorum not reached")));
            }
        });
    }
    // No replicas: W == 1, the local ack suffices.
    bool no_replicas_ok = false;
    {
        std::scoped_lock lock(state->mu);
        if (state->pending == 0 && !state->decided) {
            state->decided = true;
            no_replicas_ok = true;
        }
    }

    if (no_replicas_ok) {
        (*state->done)(ok());
    }
}

void
ReplicationManager::readAsync(const std::string& key, const std::vector<NodeId>& replica_nodes,
    size_t R, ReadCallback on_done) {
    // Local read — always attempted first.
    auto local_entry = local_.getVersioned(key);
    if (!local_entry.has_value()) {
        Logger::debug("cinder replication: local read miss key={}", key);
    }
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
            bool decided = false;
            std::optional<VersionedEntry> result;
            bool needs_repair = false;
            {
                std::scoped_lock lock(state->mu);
                if (state->decided) {
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
                        if (r->expires_at.has_value()) {
                            entry.has_ttl = true;
                            entry.expires_at = toSteadyExpiry(clock_, *r->expires_at);
                        }
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
                    state->decided = true;
                    decided = true;
                } else if (state->pending == 0) {
                    state->decided = true;
                    decided = true;
                }

                if (decided) {
                    // Snapshot the outcome; the winner entry is delivered
                    // to the caller and used for repair fan-out below.
                    needs_repair = state->needs_repair;
                    if (state->best_entry.has_value()) {
                        result = std::move(state->best_entry);
                    }
                }
            }

            if (!decided) {
                return;
            }
            if (result.has_value() && needs_repair) {
                // Self-heal local store: if local was behind a replica, update it.
                auto heal = local_.putVersioned(key, *result);
                if (!heal.has_value()) {
                    Logger::debug("cinder replication: local self-heal skipped key={} reason={}",
                        key,
                        static_cast<int>(heal.error().code()));
                }
                sendRepairFanOut(key, state->replicas, *result);
            }
            if (result.has_value()) {
                (*state->done)(std::move(*result));
            } else {
                (*state->done)(err<VersionedEntry>(Error(Errc::NotFound, "key not found")));
            }
        });
    }
}

auto
ReplicationManager::hintCount() const -> size_t {
    return hints_.size();
}

void
ReplicationManager::enqueueHint(const NodeId& target, const net::Request& req) {
    Logger::debug("cinder replication: hint enqueued target={} key={}", target, req.key);
    hints_.push({target, req, clock_.now() + K_HINT_TTL});
}

void
ReplicationManager::sendRepairFanOut(
    const std::string& key, const std::vector<NodeId>& targets, const VersionedEntry& winner) {
    Logger::info("cinder replication: repair fan-out key={} version={} targets={}",
        key,
        winner.version,
        targets.size());

    net::Request repair;
    repair.opcode = net::Opcode::Replicate;
    repair.key = key;
    repair.value = winner.value;
    repair.version = winner.version;
    repair.writer_node_hash = winner.writer_node_hash;
    if (winner.has_ttl) {
        repair.expires_at = toSystemExpiry(clock_, winner.expires_at);
    }
    for (const auto& target : targets) {
        transport_.sendAsync(target, repair, [](Result<void>) {});
    }
}

void
ReplicationManager::replayHints(ReplayCallback on_done) {
    // Single-consumer: a replay already in flight (overlapping timer ticks or
    // concurrent pool threads) reports zero rather than double-sending hints.
    bool expected = false;
    if (!replaying_.compare_exchange_strong(expected, true)) {
        on_done(0);
        return;
    }

    auto now = clock_.now();
    auto state = std::make_shared<ReplayState>();
    state->done =
        std::make_shared<ReplayCallback>([this, done = std::move(on_done)](size_t n) mutable {
        Logger::info("cinder replication: replay completed replayed={}", n);
        replaying_.store(false);
        done(n);
    });

    bool has_hints = false;
    hints_.replay([&](const HintQueue::Hint& hint) {
        has_hints = true;
        {
            std::scoped_lock lock(state->mu);
            ++state->pending;
        }

        transport_.sendAsync(hint.target, hint.req, [this, hint, state](Result<void> r) {
            if (r.has_value()) {
                hints_.remove(hint.target, hint.req);
            }

            bool finish = false;
            size_t replayed = 0;
            {
                std::scoped_lock lock(state->mu);
                if (r.has_value()) {
                    ++state->replayed;
                }

                --state->pending;
                if (state->pending == 0 && !state->decided) {
                    state->decided = true;
                    finish = true;
                    replayed = state->replayed;
                }
            }

            if (finish) {
                (*state->done)(replayed);
            }
        });
    }, now);

    // Only call on_done here if the queue was empty (no async callbacks to wait for).
    // When hints exist, the sendAsync callbacks call on_done when all complete.
    if (!has_hints) {
        (*state->done)(0);
    }
}
} // namespace cinder
