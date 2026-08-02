#pragma once

#include <chrono>
#include <unordered_map>

#include "cinder/cluster/clock.hpp"
#include "cinder/cluster/membership.hpp"
#include "cinder/cluster/transport.hpp"
#include "cinder/common/types.hpp"

namespace cinder {

// SWIM-style liveness probing. Periodically pings one peer (round-robin) using
// the existing Ping opcode; a successful reply keeps it Alive, a connect-refused
// or missing reply (within suspect_timeout) marks it Suspect, and a Suspect that
// persists past suspect_timeout is marked Dead.
//
// All state lives on the node's io thread (the periodic timer fires there), so
// no internal locking is needed beyond what MembershipTable provides.
class FailureDetector {
  public:

    FailureDetector(Clock& clock, Transport& transport, MembershipTable& table, NodeId self,
        std::chrono::milliseconds ping_interval, std::chrono::milliseconds suspect_timeout);
    ~FailureDetector() = default;

    FailureDetector(const FailureDetector&) = delete;
    auto operator=(const FailureDetector&) -> FailureDetector& = delete;
    FailureDetector(FailureDetector&&) = delete;
    auto operator=(FailureDetector&&) -> FailureDetector& = delete;

    void start();
    void tick(); // check timeouts, then probe one peer; exposed for the sim harness

  private:

    struct ProbeState {
        bool pending = false;
        std::chrono::steady_clock::time_point sent_at;
    };

    void onProbeResult(const NodeId& peer, bool acked);
    void escalateSuspects();

    Clock& clock_;
    Transport& transport_;
    MembershipTable& table_;
    NodeId self_;
    std::chrono::milliseconds suspect_timeout_;
    std::vector<NodeId> peers_;
    size_t next_peer_ = 0;
    std::unordered_map<NodeId, ProbeState> probes_;
    std::unordered_map<NodeId, std::chrono::steady_clock::time_point> suspect_since_;
};
} // namespace cinder
