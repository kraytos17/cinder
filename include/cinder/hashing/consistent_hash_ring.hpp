#pragma once

#include <atomic>
#include <memory>
#include <string_view>
#include <vector>

#include "cinder/common/types.hpp"

namespace cinder {

struct RingSnapshot {
    std::vector<std::pair<uint64_t, NodeId>> ring;
    std::vector<NodeId> physical_nodes;
};

// Consistent-hash ring with virtual nodes and lock-free reads.
//
// Concurrency contract: readers (getNode/getNodes) load an immutable
// RingSnapshot via std::atomic<std::shared_ptr<>> and never block or tear;
// mutators (addNode/removeNode) publish a fresh copy-on-write snapshot with a
// CAS retry loop, so concurrent mutators merge instead of losing updates.
// Readers may observe either the pre- or post-mutation view for keys in flight
// during a membership change (standard ring-linearization semantics).
class ConsistentHashRing {
  public:

    explicit ConsistentHashRing(int vnodes_per_node = 150);
    ~ConsistentHashRing() = default;

    ConsistentHashRing(const ConsistentHashRing&) = delete;
    auto operator=(const ConsistentHashRing&) -> ConsistentHashRing& = delete;
    ConsistentHashRing(ConsistentHashRing&&) = delete;
    auto operator=(ConsistentHashRing&&) -> ConsistentHashRing& = delete;

    void addNode(const NodeId& node_id);
    void removeNode(std::string_view node_id);

    auto getNode(std::string_view key) const -> NodeId;
    auto getNodes(std::string_view key, int replica_count) const -> std::vector<NodeId>;

  private:

    static auto hashVnode(std::string_view node_id, int vnode_index) -> uint64_t;
    static auto hashKey(std::string_view key) -> uint64_t;

    std::atomic<std::shared_ptr<const RingSnapshot>> snapshot_;
    int vnodes_per_node_;
};
} // namespace cinder
