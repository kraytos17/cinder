#include "cinder/cluster/failure_detector.hpp"

#include <algorithm>
#include <utility>

#include "cinder/common/logger.hpp"

namespace cinder {

using std::chrono::milliseconds;

FailureDetector::FailureDetector(Clock& clock, Transport& transport, MembershipTable& table,
    NodeId self, milliseconds suspect_timeout)
    : clock_(clock),
      transport_(transport),
      table_(table),
      self_(std::move(self)),
      suspect_timeout_(suspect_timeout) {}

void
FailureDetector::start() {
    {
        std::scoped_lock lock(state_mutex_);
        rebuildPeersLocked();
    }
    // Late joiners discovered via gossip/failure-detection must be added
    // dynamically after start(); each arrival fires this callback.
    table_.onChange([this] {
        std::scoped_lock lock(state_mutex_);
        rebuildPeersLocked();
    });
}

void
FailureDetector::rebuildPeersLocked() {
    peers_.clear();
    for (const auto& info : table_.snapshot()) {
        if (info.id != self_) {
            peers_.push_back(info.id);
        }
    }
    std::sort(peers_.begin(), peers_.end());
}

void
FailureDetector::tick() {
    // Work items deferred outside state_mutex_: membership mutations and the
    // ping send. sendAsync may complete synchronously (sim transport), and its
    // callback re-enters onProbeResult which takes state_mutex_ — so neither
    // may run while we hold it.
    std::vector<NodeId> timed_out;
    std::vector<NodeId> dead;
    NodeId probe_target;
    bool has_probe = false;
    {
        std::scoped_lock lock(state_mutex_);
        // Sweep pending probes for timeouts (covers blackholes that never ack).
        for (auto& [peer, probe] : probes_) {
            if (probe.pending && clock_.now() - probe.sent_at > suspect_timeout_) {
                probe.pending = false;
                timed_out.push_back(peer);
            }
        }

        dead = escalateSuspectsLocked();
        // Round-robin to the next peer that isn't self, pending, or dead.
        if (!peers_.empty()) {
            for (size_t attempt = 0; attempt < peers_.size(); ++attempt) {
                const NodeId& peer = peers_[next_peer_ % peers_.size()];
                next_peer_ = (next_peer_ + 1) % peers_.size();
                if (peer == self_) {
                    continue;
                }

                auto it = probes_.find(peer);
                if (it != probes_.end() && it->second.pending) {
                    continue;
                }

                auto info = table_.get(peer);
                if (info.has_value() && info->state == NodeState::Dead) {
                    continue;
                }

                auto& probe = probes_[peer];
                probe.pending = true;
                probe.sent_at = clock_.now();
                probe_target = peer;
                has_probe = true;
                break;
            }
        }
    }

    for (const auto& peer : timed_out) {
        Logger::info("cinder failure_detector: suspect marked peer={} reason=timeout", peer);
        table_.markSuspect(peer);
    }
    for (const auto& peer : dead) {
        table_.markDead(peer);
    }
    if (!has_probe) {
        return;
    }

    Logger::debug("cinder failure_detector: ping sent peer={}", probe_target);
    net::Request ping;
    ping.opcode = net::Opcode::Ping;
    transport_.sendAsync(probe_target, ping, [this, probe_target](Result<void> r) {
        onProbeResult(probe_target, r.has_value());
    });
}

void
FailureDetector::onProbeResult(const NodeId& peer, bool acked) {
    bool known_probe = false;
    {
        std::scoped_lock lock(state_mutex_);
        auto it = probes_.find(peer);
        if (it == probes_.end()) {
            return;
        }

        it->second.pending = false;
        known_probe = true;
        if (acked) {
            suspect_since_.erase(peer);
        } else {
            suspect_since_.emplace(peer, clock_.now());
        }
    }

    if (!known_probe) {
        return;
    }
    if (acked) {
        Logger::debug("cinder failure_detector: ping received peer={}", peer);
        auto info = table_.get(peer);
        table_.markAlive(peer, info.has_value() ? info->incarnation : 0);
        return;
    }

    // Unreachable: suspect now, record when; escalation promotes to Dead later.
    Logger::info("cinder failure_detector: suspect marked peer={} reason=unreachable", peer);
    table_.markSuspect(peer);
}

auto
FailureDetector::escalateSuspectsLocked() -> std::vector<NodeId> {
    // Caller holds state_mutex_; returns peers to mark Dead so the caller can
    // mutate MembershipTable without holding our lock.
    std::vector<NodeId> to_dead;
    for (const auto& [peer, since] : suspect_since_) {
        auto info = table_.get(peer);
        if (!info.has_value() || info->state != NodeState::Suspect) {
            continue;
        }
        if (clock_.now() - since > suspect_timeout_) {
            to_dead.push_back(peer);
        }
    }
    for (const auto& peer : to_dead) {
        suspect_since_.erase(peer);
    }
    return to_dead;
}
} // namespace cinder
