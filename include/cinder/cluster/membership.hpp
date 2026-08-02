#pragma once

#include <cstdint>
#include <functional>
#include <mutex>
#include <string>
#include <unordered_map>
#include <vector>

#include "cinder/client/connection_pool.hpp"
#include "cinder/common/types.hpp"

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
};

// Local view of cluster membership. Tracks (id, host, port, state, incarnation)
// for every known node. Rumor application is incarnation-guarded: a rumor is
// applied only if its incarnation >= the locally known one. A node that hears a
// Suspect/Dead rumor about itself refutes it by bumping its own incarnation and
// forcing itself Alive (SWIM's "alive with higher incarnation" recovery).
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

    // Apply a rumor from another node; ignored if its incarnation is stale.
    void applyRumor(const NodeId& from, const NodeInfo& rumor);

    void markSuspect(const NodeId& id);
    void markDead(const NodeId& id);
    void markAlive(const NodeId& id, uint64_t incarnation);

    [[nodiscard]] auto get(const NodeId& id) const -> const NodeInfo*;
    [[nodiscard]] auto aliveCount() const -> size_t;
    [[nodiscard]] auto visibleAliveCount() const -> size_t;
    [[nodiscard]] auto expectedClusterSize() const -> size_t;
    // Degraded when fewer than a majority of the expected cluster is visible.
    [[nodiscard]] auto isDegraded() const -> bool;
    [[nodiscard]] auto snapshot() const -> std::vector<NodeInfo>;

    using ChangeCallback = std::move_only_function<void()>;
    void onChange(ChangeCallback cb);

  private:

    void refuteSelfRumor(const NodeInfo& rumor);

    mutable std::mutex mutex_;
    NodeId self_;
    std::unordered_map<NodeId, NodeInfo> nodes_;
    std::vector<ChangeCallback> cbs_;
};
} // namespace cinder
