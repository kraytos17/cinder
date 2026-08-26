#pragma once

#include <chrono>
#include <mutex>
#include <string_view>

#include "cinder/cluster/clock.hpp"
#include "cinder/cluster/membership.hpp"
#include "cinder/cluster/transport.hpp"
#include "cinder/common/types.hpp"

using std::chrono::milliseconds;

namespace cinder {

// SWIM-style membership dissemination. Periodically sends this node's view
// (encoded as a ;-delimited string in a Gossip request) to a random peer;
// applies incoming gossip views to the local MembershipTable via
// incarnation-guarded applyRumor.
//
// Wire format (text, v1): "id@host:port:state:incarnation;id@host:port:..."
// state ∈ {alive, suspect, dead}.
class GossipManager {
  public:

    GossipManager(Clock& clock, Transport& transport, MembershipTable& table, const NodeId& self,
        milliseconds gossip_interval);
    ~GossipManager() = default;

    GossipManager(const GossipManager&) = delete;
    auto operator=(const GossipManager&) -> GossipManager& = delete;
    GossipManager(GossipManager&&) = delete;
    auto operator=(GossipManager&&) -> GossipManager& = delete;

    void start();
    void tick(); // send this node's view to a random peer; exposed for the sim harness
    void handleMessage(const NodeId& from, const net::Request& req);
    // Broadcast self as Dead to every peer so they don't suspect during teardown.
    void leave();

    [[nodiscard]] auto gossipInterval() const -> milliseconds { return gossip_interval_; }

  private:

    void sendView(const NodeId& to);
    void rebuildPeersLocked(); // populates peers_ from table snapshot
    static auto encodeView(const std::vector<NodeInfo>& view) -> std::string;
    static auto decodeView(std::string_view value) -> std::vector<NodeInfo>;

    Clock& clock_;
    Transport& transport_;
    MembershipTable& table_;
    NodeId self_;
    milliseconds gossip_interval_;
    mutable std::mutex peers_mutex_; // guards peers_ only
    std::vector<NodeId> peers_;
};
} // namespace cinder
