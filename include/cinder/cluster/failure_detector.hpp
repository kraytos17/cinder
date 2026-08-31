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
// the existing Ping opcode; a successful reply keeps it Alive, a connect-refused
// or missing reply (within suspect_timeout) marks it Suspect, and a Suspect that
// persists past suspect_timeout is marked Dead.
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

    struct ProbeState {
        bool pending = false;
        steady_clock::time_point sent_at;
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
    std::unordered_map<NodeId, ProbeState> probes_;
    std::unordered_map<NodeId, steady_clock::time_point> suspect_since_;
    MetricsCollector* metrics_ = nullptr;
};
} // namespace cinder
