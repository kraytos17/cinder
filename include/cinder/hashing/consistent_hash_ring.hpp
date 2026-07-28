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

class ConsistentHashRing {
  public:

    explicit ConsistentHashRing(int vnodes_per_node = 150);

    ConsistentHashRing(const ConsistentHashRing&) = delete;
    auto operator=(const ConsistentHashRing&) -> ConsistentHashRing& = delete;

    void add_node(const NodeId& node_id);
    void remove_node(std::string_view node_id);

    auto get_node(std::string_view key) const -> NodeId;
    auto get_nodes(std::string_view key, int replica_count) const -> std::vector<NodeId>;

  private:

    static auto hash_vnode(std::string_view node_id, int vnode_index) -> uint64_t;
    static auto hash_key(std::string_view key) -> uint64_t;

    std::atomic<std::shared_ptr<const RingSnapshot>> snapshot_;
    int vnodes_per_node_;
};
} // namespace cinder
