#include "cinder/hashing/consistent_hash_ring.hpp"

#include <algorithm>
#include <array>
#include <cassert>
#include <cstdio>
#include <cstring>
#include <format>
#include <xxhash.h>

namespace cinder {

static auto
hash64(std::string_view data) -> uint64_t {
    return XXH3_64bits(data.data(), data.size());
}

ConsistentHashRing::ConsistentHashRing(int vnodes_per_node, double bounded_load_factor)
    : vnodes_per_node_(vnodes_per_node),
      bounded_load_factor_(bounded_load_factor) {
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
        auto old_size = old->hashes.size();
        auto new_vnodes = static_cast<size_t>(vnodes_per_node_);

        snap->hashes.reserve(old_size + new_vnodes);
        snap->node_index.reserve(old_size + new_vnodes);
        snap->physical_nodes = old->physical_nodes;
        snap->physical_nodes.push_back(node_id);
        auto phys_idx = static_cast<uint16_t>(snap->physical_nodes.size() - 1);

        snap->hashes = old->hashes;
        snap->node_index = old->node_index;
        for (int i = 0; i < vnodes_per_node_; i++) {
            snap->hashes.push_back(hashVnode(node_id, i));
            snap->node_index.push_back(phys_idx);
        }

        // Sort hashes and node_index together.
        // We use an index array to sort both vectors in lock-step.
        std::vector<size_t> order(snap->hashes.size());
        for (size_t i = 0; i < order.size(); i++) {
            order[i] = i;
        }
        std::sort(order.begin(), order.end(), [&](size_t a, size_t b) {
            return snap->hashes[a] < snap->hashes[b];
        });

        auto sorted_hashes = snap->hashes;
        auto sorted_indices = snap->node_index;
        for (size_t i = 0; i < order.size(); i++) {
            snap->hashes[i] = sorted_hashes[order[i]];
            snap->node_index[i] = sorted_indices[order[i]];
        }
        if (snapshot_.compare_exchange_weak(old, std::move(snap))) {
            // Initialize load counter for the new node (zero in-flight).
            {
                std::unique_lock lock(load_mu_);
                load_.try_emplace(node_id, 0ULL);
            }
            return;
        }
        // Another mutator published first — retry on its snapshot.
    }
}

void
ConsistentHashRing::removeNode(std::string_view node_id) {
    while (true) {
        auto old = snapshot_.load();
        // Find the index of this node in physical_nodes.
        auto phys_idx = static_cast<uint16_t>(old->physical_nodes.size());
        for (size_t i = 0; i < old->physical_nodes.size(); i++) {
            if (old->physical_nodes[i] == node_id) {
                phys_idx = static_cast<uint16_t>(i);
                break;
            }
        }
        if (phys_idx == old->physical_nodes.size()) {
            return; // not found
        }

        auto snap = std::make_shared<RingSnapshot>();
        snap->physical_nodes = old->physical_nodes;
        snap->physical_nodes.erase(snap->physical_nodes.begin() + phys_idx);
        // Remove all ring entries for this node, and remap indices > phys_idx down by 1.
        snap->hashes.reserve(old->hashes.size());
        snap->node_index.reserve(old->node_index.size());
        for (size_t i = 0; i < old->hashes.size(); i++) {
            if (old->node_index[i] == phys_idx) {
                continue; // skip vnodes for removed node
            }

            snap->hashes.push_back(old->hashes[i]);
            auto idx = old->node_index[i];
            snap->node_index.push_back(idx > phys_idx ? idx - 1 : idx);
        }
        if (snap->hashes.size() == old->hashes.size()) {
            return; // nothing changed
        }
        if (snapshot_.compare_exchange_weak(old, std::move(snap))) {
            // Remove load counter for the departed node.
            {
                std::unique_lock lock(load_mu_);
                load_.erase(std::string(node_id));
            }
            return;
        }
    }
}

auto
ConsistentHashRing::getNode(std::string_view key) const -> NodeId {
    auto snap = snapshot_.load();
    auto h = hashKey(key);
    auto idx = std::lower_bound(snap->hashes.begin(), snap->hashes.end(), h) - snap->hashes.begin();
    if (static_cast<size_t>(idx) == snap->hashes.size()) {
        idx = 0;
    }
    return snap->physical_nodes[snap->node_index[static_cast<size_t>(idx)]];
}

auto
ConsistentHashRing::getNodes(std::string_view key, int replica_count) const -> std::vector<NodeId> {
    constexpr size_t K_MAX_REPLICAS = 32;
    assert(replica_count <= static_cast<int>(K_MAX_REPLICAS)
           && "replica_count exceeds K_MAX_REPLICAS");

    auto snap = snapshot_.load();
    auto h = hashKey(key);
    auto start =
        std::lower_bound(snap->hashes.begin(), snap->hashes.end(), h) - snap->hashes.begin();
    if (static_cast<size_t>(start) == snap->hashes.size()) {
        start = 0;
    }

    std::array<NodeId, K_MAX_REPLICAS> seen{};
    size_t seen_count = 0;

    std::vector<NodeId> result;
    result.reserve(static_cast<size_t>(replica_count));
    auto cur = static_cast<size_t>(start);
    while (static_cast<int>(result.size()) < replica_count) {
        const auto& node = snap->physical_nodes[snap->node_index[cur]];
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
        if (cur == snap->hashes.size()) {
            cur = 0;
        }
        if (cur == static_cast<size_t>(start)) {
            break;
        }
    }
    return result;
}

auto
ConsistentHashRing::getNodeBounded(std::string_view key) const -> NodeId {
    if (bounded_load_factor_ <= 0.0) {
        return getNode(key);
    }

    auto snap = snapshot_.load();
    if (snap->hashes.empty()) {
        return {};
    }

    auto h = hashKey(key);
    auto idx = std::lower_bound(snap->hashes.begin(), snap->hashes.end(), h) - snap->hashes.begin();
    if (static_cast<size_t>(idx) == snap->hashes.size()) {
        idx = 0;
    }

    // Compute max load per node: ceil(avg_load * factor).
    const auto num_nodes = snap->physical_nodes.size();
    if (num_nodes == 0) {
        return {};
    }
    if (num_nodes == 1) {
        return snap->physical_nodes[snap->node_index[static_cast<size_t>(idx)]];
    }

    uint64_t total = 0;
    {
        std::shared_lock lock(load_mu_);
        for (const auto& [id, cnt] : load_) {
            total += cnt.load(std::memory_order_relaxed);
        }
    }

    const double avg = static_cast<double>(total) / static_cast<double>(num_nodes);
    const auto max_load = static_cast<uint64_t>(std::ceil(avg * bounded_load_factor_));
    // Walk the ring from the hash position. Return the first node whose
    // in-flight load is below the max. Wrap around at most once.
    auto start = idx;
    do {
        std::shared_lock lock(load_mu_);
        const auto& node = snap->physical_nodes[snap->node_index[static_cast<size_t>(idx)]];
        auto it2 = load_.find(node);
        uint64_t node_load = 0;
        if (it2 != load_.end()) {
            node_load = it2->second.load(std::memory_order_relaxed);
        }
        if (node_load < max_load) {
            return node;
        }

        idx++;
        if (static_cast<size_t>(idx) == snap->hashes.size()) {
            idx = 0;
        }
    } while (idx != start);
    return snap->physical_nodes[snap->node_index[static_cast<size_t>(start)]];
}

void
ConsistentHashRing::incrementLoad(std::string_view node) const {
    std::shared_lock lock(load_mu_);
    auto it = load_.find(node);
    if (it != load_.end()) {
        it->second.fetch_add(1, std::memory_order_relaxed);
    }
}

void
ConsistentHashRing::decrementLoad(std::string_view node) const {
    std::shared_lock lock(load_mu_);
    auto it = load_.find(node);
    if (it != load_.end()) {
        auto prev = it->second.fetch_sub(1, std::memory_order_relaxed);
        assert(prev > 0 && "decrementLoad called without matching incrementLoad");
        (void)prev;
    }
}

auto
ConsistentHashRing::currentLoad(std::string_view node) const -> uint64_t {
    std::shared_lock lock(load_mu_);
    auto it = load_.find(node);
    if (it != load_.end()) {
        return it->second.load(std::memory_order_relaxed);
    }
    return 0;
}

auto
ConsistentHashRing::totalLoad() const -> uint64_t {
    uint64_t total = 0;
    std::shared_lock lock(load_mu_);
    for (const auto& [id, cnt] : load_) {
        total += cnt.load(std::memory_order_relaxed);
    }
    return total;
}

auto
ConsistentHashRing::loadFactor() const -> double {
    std::shared_lock lock(load_mu_);
    const auto num_nodes = load_.size();
    if (num_nodes == 0) {
        return 0.0;
    }

    uint64_t total = 0;
    for (const auto& [id, cnt] : load_) {
        total += cnt.load(std::memory_order_relaxed);
    }
    const double avg = static_cast<double>(total) / static_cast<double>(num_nodes);
    return avg * bounded_load_factor_;
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
