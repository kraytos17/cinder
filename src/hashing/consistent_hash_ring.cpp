#include "cinder/hashing/consistent_hash_ring.hpp"

#include <algorithm>
#include <array>
#include <cstdio>
#include <cstring>
#include <unordered_set>
#include <xxhash.h>

namespace cinder {

static auto
hash_64(std::string_view data) -> uint64_t {
    return XXH3_64bits(data.data(), data.size());
}

ConsistentHashRing::ConsistentHashRing(int vnodes_per_node)
    : vnodes_per_node_(vnodes_per_node) {
    snapshot_.store(std::make_shared<const RingSnapshot>());
}

void
ConsistentHashRing::add_node(const NodeId& node_id) {
    auto old = snapshot_.load();
    auto snap = std::make_shared<RingSnapshot>();

    snap->ring = old->ring;
    snap->physical_nodes = old->physical_nodes;
    snap->physical_nodes.push_back(node_id);

    for (int i = 0; i < vnodes_per_node_; i++) {
        auto h = hash_vnode(node_id, i);
        snap->ring.emplace_back(h, node_id);
    }

    std::sort(snap->ring.begin(), snap->ring.end());
    snapshot_.store(std::move(snap));
}

void
ConsistentHashRing::remove_node(std::string_view node_id) {
    auto old = snapshot_.load();
    auto snap = std::make_shared<RingSnapshot>();
    for (auto& entry : old->ring) {
        if (entry.second != node_id) {
            snap->ring.push_back(entry);
        }
    }
    for (auto& id : old->physical_nodes) {
        if (id != node_id) {
            snap->physical_nodes.push_back(id);
        }
    }
    snapshot_.store(std::move(snap));
}

auto
ConsistentHashRing::get_node(std::string_view key) const -> NodeId {
    auto snap = snapshot_.load();
    auto h = hash_key(key);
    auto it = std::lower_bound(snap->ring.begin(),
        snap->ring.end(),
        h,
        [](const std::pair<uint64_t, NodeId>& entry, uint64_t val) { return entry.first < val; });

    if (it == snap->ring.end()) {
        it = snap->ring.begin();
    }
    return it->second;
}

auto
ConsistentHashRing::get_nodes(std::string_view key, int replica_count) const
    -> std::vector<NodeId> {
    auto snap = snapshot_.load();
    auto h = hash_key(key);
    auto it = std::lower_bound(snap->ring.begin(),
        snap->ring.end(),
        h,
        [](const std::pair<uint64_t, NodeId>& entry, uint64_t val) { return entry.first < val; });

    if (it == snap->ring.end()) {
        it = snap->ring.begin();
    }

    std::unordered_set<std::string_view> seen;
    std::vector<NodeId> result;
    auto cur = it;
    while (result.size() < static_cast<size_t>(replica_count)) {
        if (seen.insert(cur->second).second) {
            result.push_back(cur->second);
        }

        cur++;
        if (cur == snap->ring.end()) {
            cur = snap->ring.begin();
        }
        if (cur == it) {
            break;
        }
    }
    return result;
}

auto
ConsistentHashRing::hash_vnode(std::string_view node_id, int vnode_index) -> uint64_t {
    std::array<char, 64> buf;
    auto n = std::min(node_id.size(), buf.size() - 16);
    std::memcpy(buf.data(), node_id.data(), n);
    int len = static_cast<int>(n) + snprintf(buf.data() + n, buf.size() - n, "-%d", vnode_index);
    return hash_64(std::string_view(buf.data(), static_cast<size_t>(len)));
}

auto
ConsistentHashRing::hash_key(std::string_view key) -> uint64_t {
    return hash_64(key);
}
} // namespace cinder
