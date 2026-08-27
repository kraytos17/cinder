#pragma once

#include <cstdint>
#include <flat_map>
#include <functional>
#include <mutex>
#include <optional>
#include <string>
#include <vector>

#include "cinder/common/cluster_config.hpp"
#include "cinder/common/types.hpp"

using std::chrono::milliseconds;
using std::chrono::steady_clock;

namespace cinder {

enum class NodeState : uint8_t {
    Alive,
    Suspect,
    Dead
};

struct NodeInfo {
    NodeId id;
    std::string host;
    uint16_t port = 0;
    NodeState state = NodeState::Alive;
    uint64_t incarnation = 0;
    steady_clock::time_point joined_at{};
};

// Local view of cluster membership. Tracks (id, host, port, state, incarnation)
// for every known node. Rumor application is incarnation-guarded: a rumor is
// applied only if its incarnation >= the locally known one. A node that hears a
// Suspect/Dead rumor about itself refutes it by bumping its own incarnation and
// forcing itself Alive.
class MembershipTable {
  public:

    explicit MembershipTable(NodeId self);
    ~MembershipTable() = default;

    MembershipTable(const MembershipTable&) = delete;
    auto operator=(const MembershipTable&) -> MembershipTable& = delete;
    MembershipTable(MembershipTable&&) = delete;
    auto operator=(MembershipTable&&) -> MembershipTable& = delete;

    // Seed initial Alive members (incarnation 0) from a static peer list.
    void seed(const std::vector<ClusterConfig::NodeConfig>& peers);

    // Record this node's own host/port so its gossip view advertises a reachable
    // address for peers to adopt and migrate keys to.
    void setSelfAddress(const std::string& host, uint16_t port);

    // Apply a rumor from another node; ignored if its incarnation is stale.
    void applyRumor(const NodeId& from, const NodeInfo& rumor);
    void markSuspect(const NodeId& id);
    void markDead(const NodeId& id);
    void markAlive(const NodeId& id, uint64_t incarnation);

    [[nodiscard]] auto get(const NodeId& id) const -> std::optional<NodeInfo>;
    [[nodiscard]] auto aliveCount() const -> size_t;
    [[nodiscard]] auto visibleAliveCount() const -> size_t;
    [[nodiscard]] auto expectedClusterSize() const -> size_t;
    // Degraded when fewer than a majority of the expected cluster is visible.
    [[nodiscard]] auto isDegraded() const -> bool;
    [[nodiscard]] auto snapshot() const -> std::vector<NodeInfo>;

    // True if `id` joined (or re-joined after being Dead/Suspect) within the
    // quarantine window. `quarantine` of zero disables the check.
    [[nodiscard]] auto isQuarantined(
        const NodeId& id, steady_clock::time_point now, milliseconds quarantine) const -> bool;

    using ChangeCallback = std::function<void()>;
    void onChange(ChangeCallback cb);

  private:

    // Invoke the change observers without holding mutex_, so observers may
    // safely re-enter the table without self-deadlocking.
    void fireCallbacks();
    bool refuteSelfRumor(const NodeInfo& rumor);

    mutable std::mutex mutex_;
    NodeId self_;
    std::flat_map<NodeId, NodeInfo> nodes_;
    std::vector<ChangeCallback> cbs_;
};
} // namespace cinder
