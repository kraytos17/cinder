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
ConsistentHashRing::getNodeBounded(std::string_view key) const -> NodeId {
    if (bounded_load_factor_ <= 0.0) {
        return getNode(key);
    }

    auto snap = snapshot_.load();
    if (snap->ring.empty()) {
        return {};
    }

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

    // Compute max load per node: ceil(avg_load * factor).
    const auto num_nodes = snap->physical_nodes.size();
    if (num_nodes == 0) {
        return {};
    }
    if (num_nodes == 1) {
        return it->second;
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
    auto start = it;
    do {
        std::shared_lock lock(load_mu_);
        auto it2 = load_.find(it->second);
        uint64_t node_load = 0;
        if (it2 != load_.end()) {
            node_load = it2->second.load(std::memory_order_relaxed);
        }
        if (node_load < max_load) {
            return it->second;
        }

        ++it;
        if (it == snap->ring.end()) {
            it = snap->ring.begin();
        }
    } while (it != start);
    // All nodes overloaded — return the original match (graceful degradation).
    return start->second;
}

void
ConsistentHashRing::incrementLoad(std::string_view node) const {
    std::shared_lock lock(load_mu_);
    auto it = load_.find(std::string(node));
    if (it != load_.end()) {
        it->second.fetch_add(1, std::memory_order_relaxed);
    }
}

void
ConsistentHashRing::decrementLoad(std::string_view node) const {
    std::shared_lock lock(load_mu_);
    auto it = load_.find(std::string(node));
    if (it != load_.end()) {
        auto prev = it->second.fetch_sub(1, std::memory_order_relaxed);
        assert(prev > 0 && "decrementLoad called without matching incrementLoad");
        (void)prev;
    }
}

auto
ConsistentHashRing::currentLoad(std::string_view node) const -> uint64_t {
    std::shared_lock lock(load_mu_);
    auto it = load_.find(std::string(node));
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
