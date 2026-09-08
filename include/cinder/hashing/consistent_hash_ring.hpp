#pragma once

#include <atomic>
#include <memory>
#include <shared_mutex>
#include <string_view>
#include <unordered_map>
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
//
// Bounded-load support: when bounded_load_factor > 0, getNodeBounded() skips
// nodes whose in-flight load exceeds ceil(avg_load * factor) and spills to
// the next ring position. Callers manage load counters via
// incrementLoad/decrementLoad.
class ConsistentHashRing {
  public:

    explicit ConsistentHashRing(int vnodes_per_node = 150, double bounded_load_factor = 0.0);
    ~ConsistentHashRing() = default;

    ConsistentHashRing(const ConsistentHashRing&) = delete;
    auto operator=(const ConsistentHashRing&) -> ConsistentHashRing& = delete;
    ConsistentHashRing(ConsistentHashRing&&) = delete;
    auto operator=(ConsistentHashRing&&) -> ConsistentHashRing& = delete;

    void addNode(const NodeId& node_id);
    void removeNode(std::string_view node_id);

    auto getNode(std::string_view key) const -> NodeId;
    auto getNodes(std::string_view key, int replica_count) const -> std::vector<NodeId>;

    // Bounded-load variant: spills to the next ring position when the
    // preferred node is overloaded (load > ceil(avg_load * factor)).
    // Falls back to getNode() when bounded_load_factor <= 0.
    auto getNodeBounded(std::string_view key) const -> NodeId;

    // Caller-managed in-flight load counters. incrementLoad must be called
    // before dispatching a request to a node; decrementLoad on completion.
    void incrementLoad(std::string_view node) const;
    void decrementLoad(std::string_view node) const;

    auto currentLoad(std::string_view node) const -> uint64_t;
    auto totalLoad() const -> uint64_t;
    auto loadFactor() const -> double;

  private:

    static auto hashVnode(std::string_view node_id, int vnode_index) -> uint64_t;
    static auto hashKey(std::string_view key) -> uint64_t;

    std::atomic<std::shared_ptr<const RingSnapshot>> snapshot_;
    int vnodes_per_node_;
    double bounded_load_factor_;

    // Per-node in-flight load counters. Mutable because load tracking is
    // logically orthogonal to the ring's snapshot immutability — readers
    // call incrementLoad/decrementLoad on a const ring reference.
    mutable std::shared_mutex load_mu_;
    mutable std::unordered_map<NodeId, std::atomic<uint64_t>> load_;
};
} // namespace cinder
