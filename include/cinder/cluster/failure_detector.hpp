#pragma once

#include <chrono>
#include <mutex>
#include <unordered_map>

#include "cinder/cluster/clock.hpp"
#include "cinder/cluster/membership.hpp"
#include "cinder/cluster/transport.hpp"
#include "cinder/common/metrics.hpp"
#include "cinder/common/types.hpp"

using std::chrono::milliseconds;
using std::chrono::steady_clock;

namespace cinder {

// SWIM-style liveness probing. Periodically pings one peer (round-robin) using
// the existing Ping opcode; a successful reply keeps it Alive, consecutive
// failed replies (or a reply missing for longer than suspect_timeout) mark it
// Suspect, and a Suspect that persists past suspect_timeout is marked Dead.
//
// A single transient failure (e.g. a dropped Ping under load) must NOT flap
// the ring: suspect requires K_SUSPECT_THRESHOLD consecutive failed probes.
// Success resets the streak. A probe pending longer than suspect_timeout
// (blackhole that never acks) suspects immediately — it already waited out
// the full timeout.
//
// Thread-safety: tick() (timer handler) and onProbeResult() (transport
// completion) may run on different io-pool threads once the server goes
// multi-threaded, so all probe/round-robin state is guarded by state_mutex_.
// MembershipTable mutations and sendAsync() happen outside the lock — the
// transport may complete callbacks synchronously and re-enter this class.
class FailureDetector {
  public:

    FailureDetector(Clock& clock, Transport& transport, MembershipTable& table, NodeId self,
        milliseconds suspect_timeout);
    ~FailureDetector() = default;

    FailureDetector(const FailureDetector&) = delete;
    auto operator=(const FailureDetector&) -> FailureDetector& = delete;
    FailureDetector(FailureDetector&&) = delete;
    auto operator=(FailureDetector&&) -> FailureDetector& = delete;

    void start();
    void tick(); // check timeouts, then probe one peer; exposed for the sim harness

    void setMetrics(MetricsCollector* m) { metrics_ = m; }

    void setSuspectTimeout(milliseconds timeout) { suspect_timeout_ = timeout; }

  private:

    // Consecutive failed probes required before marking Suspect. Filters
    // single transient glitches without materially delaying real-failure
    // detection ((K-1) extra ping intervals).
    static constexpr int K_SUSPECT_THRESHOLD = 2;

    struct ProbeState {
        bool pending = false;
        steady_clock::time_point sent_at{};
    };

    // Fused per-peer state: one hash lookup per peer per tick instead of
    // three (probes_ + suspect_since_ + consecutive_failures_). All fields
    // for a peer live on adjacent lines so sweep + probe + escalate touch
    // one cache line instead of three scattered map nodes.
    struct PeerState {
        ProbeState probe;
        steady_clock::time_point suspect_since{};
        bool suspected = false;
        int consecutive_failures = 0;
    };

    void onProbeResult(const NodeId& peer, bool acked);
    void rebuildPeersLocked(); // populates peers_ from table snapshot (caller holds state_mutex_)
    // Caller must hold state_mutex_. Returns peers to mark Dead; the caller
    // performs MembershipTable mutations outside the lock.
    auto escalateSuspectsLocked() -> std::vector<NodeId>;

    Clock& clock_;
    Transport& transport_;
    MembershipTable& table_;
    NodeId self_;
    milliseconds suspect_timeout_;
    mutable std::mutex state_mutex_;
    std::vector<NodeId> peers_;
    size_t next_peer_ = 0;
    std::unordered_map<NodeId, PeerState> peer_state_;
    MetricsCollector* metrics_ = nullptr;
};
} // namespace cinder
