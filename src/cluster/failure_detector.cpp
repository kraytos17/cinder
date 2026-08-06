#include "cinder/cluster/failure_detector.hpp"

#include <algorithm>
#include <utility>

namespace cinder {

using namespace std::chrono;

FailureDetector::FailureDetector(Clock& clock, Transport& transport, MembershipTable& table,
    NodeId self, milliseconds suspect_timeout)
    : clock_(clock),
      transport_(transport),
      table_(table),
      self_(std::move(self)),
      suspect_timeout_(suspect_timeout) {}

void
FailureDetector::start() {
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
    // Sweep pending probes for timeouts (covers blackholes that never ack).
    for (auto& [peer, probe] : probes_) {
        (void)peer;
        if (probe.pending && clock_.now() - probe.sent_at > suspect_timeout_) {
            probe.pending = false;
            table_.markSuspect(peer);
        }
    }

    escalateSuspects();
    if (peers_.empty()) {
        return;
    }
    // Round-robin to the next peer that isn't self, pending, or already dead.
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

        net::Request ping;
        ping.opcode = net::Opcode::Ping;
        transport_.sendAsync(
            peer, ping, [this, peer](Result<void> r) { onProbeResult(peer, r.has_value()); });
        return;
    }
}

void
FailureDetector::onProbeResult(const NodeId& peer, bool acked) {
    auto it = probes_.find(peer);
    if (it == probes_.end()) {
        return;
    }

    it->second.pending = false;
    if (acked) {
        suspect_since_.erase(peer);
        auto info = table_.get(peer);
        table_.markAlive(peer, info.has_value() ? info->incarnation : 0);
        return;
    }

    // Unreachable: suspect now, record when; escalateSuspects() promotes to Dead.
    suspect_since_.emplace(peer, clock_.now());
    table_.markSuspect(peer);
}

void
FailureDetector::escalateSuspects() {
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
        table_.markDead(peer);
    }
}
} // namespace cinder
