#include "cinder/hashing/consistent_hash_ring.hpp"

#include <algorithm>
#include <array>
#include <cstdio>
#include <cstring>
#include <format>
#include <xxhash.h>

namespace cinder {

static auto
hash64(std::string_view data) -> uint64_t {
    return XXH3_64bits(data.data(), data.size());
}

ConsistentHashRing::ConsistentHashRing(int vnodes_per_node)
    : vnodes_per_node_(vnodes_per_node) {
    snapshot_.store(std::make_shared<const RingSnapshot>());
}

void
ConsistentHashRing::addNode(const NodeId& node_id) {
    // Copy-on-write with a CAS publish: concurrent mutators retry against the
    // winner instead of losing updates. Readers are unaffected — they keep
    // working on whichever immutable snapshot they loaded.
    while (true) {
        auto old = snapshot_.load();
        bool present = false;
        for (const auto& existing : old->physical_nodes) {
            if (existing == node_id) {
                present = true;
                break;
            }
        }
        if (present) {
            return;
        }

        auto snap = std::make_shared<RingSnapshot>();
        snap->ring.reserve(old->ring.size() + static_cast<size_t>(vnodes_per_node_));
        snap->ring = old->ring;
        snap->physical_nodes = old->physical_nodes;
        snap->physical_nodes.push_back(node_id);
        for (int i = 0; i < vnodes_per_node_; i++) {
            auto h = hashVnode(node_id, i);
            snap->ring.emplace_back(h, node_id);
        }

        std::sort(snap->ring.begin(), snap->ring.end());
        if (snapshot_.compare_exchange_weak(old, std::move(snap))) {
            return;
        }
        // Another mutator published first — retry on its snapshot.
    }
}

void
ConsistentHashRing::removeNode(std::string_view node_id) {
    while (true) {
        auto old = snapshot_.load();
        auto snap = std::make_shared<RingSnapshot>(*old);
        std::erase_if(snap->ring, [&](const auto& entry) { return entry.second == node_id; });
        std::erase_if(snap->physical_nodes, [&](const auto& id) { return id == node_id; });

        if (snap->ring.size() == old->ring.size()
            && snap->physical_nodes.size() == old->physical_nodes.size()) {
            bool changed = false;
            for (const auto& id : old->physical_nodes) {
                if (id == node_id) {
                    changed = true;
                    break;
                }
            }
            if (!changed) {
                return;
            }
        }
        if (snapshot_.compare_exchange_weak(old, std::move(snap))) {
            return;
        }
    }
}

auto
ConsistentHashRing::getNode(std::string_view key) const -> NodeId {
    auto snap = snapshot_.load();
    auto h = hashKey(key);
    auto it = std::lower_bound(snap->ring.begin(),
        snap->ring.end(),
        h,
        [](const std::pair<uint64_t, NodeId>& entry, uint64_t val) static {
        return entry.first < val;
    });

    if (it == snap->ring.end()) {
        it = snap->ring.begin();
    }
    return it->second;
}

auto
ConsistentHashRing::getNodes(std::string_view key, int replica_count) const -> std::vector<NodeId> {
    auto snap = snapshot_.load();
    auto h = hashKey(key);
    auto it = std::lower_bound(snap->ring.begin(),
        snap->ring.end(),
        h,
        [](const std::pair<uint64_t, NodeId>& entry, uint64_t val) static {
        return entry.first < val;
    });

    if (it == snap->ring.end()) {
        it = snap->ring.begin();
    }

    constexpr size_t K_MAX_REPLICAS = 32;
    std::array<NodeId, K_MAX_REPLICAS> seen{};
    size_t seen_count = 0;

    std::vector<NodeId> result;
    result.reserve(static_cast<size_t>(replica_count));
    auto cur = it;
    while (result.size() < static_cast<size_t>(replica_count)) {
        const auto& node = cur->second;
        bool dup = false;
        for (size_t i = 0; i < seen_count; i++) {
            if (seen[i] == node) {
                dup = true;
                break;
            }
        }
        if (!dup) {
            seen[seen_count++] = node;
            result.push_back(node);
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
ConsistentHashRing::hashVnode(std::string_view node_id, int vnode_index) -> uint64_t {
    auto label = std::format("{}-{}", node_id, vnode_index);
    return hash64(label);
}

auto
ConsistentHashRing::hashKey(std::string_view key) -> uint64_t {
    return hash64(key);
}
} // namespace cinder
