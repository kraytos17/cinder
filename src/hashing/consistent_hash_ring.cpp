#include "cinder/hashing/consistent_hash_ring.hpp"

#include <algorithm>
#include <array>
#include <cassert>
#include <cmath>
#include <cstdio>
#include <cstring>
#include <flat_set>
#include <format>
#include <stdexcept>
#include <xxhash.h>

namespace cinder {

    static auto hash64(std::string_view data) -> uint64_t {
        return XXH3_64bits(data.data(), data.size());
    }

    ConsistentHashRing::ConsistentHashRing(int vnodes_per_node, double bounded_load_factor) :
        vnodes_per_node_(vnodes_per_node),
        bounded_load_factor_(bounded_load_factor) {
        if (vnodes_per_node_ <= 0) {
            throw std::invalid_argument("vnodes_per_node must be positive");
        }
        if (!std::isfinite(bounded_load_factor_) || bounded_load_factor_ < 0.0) {
            throw std::invalid_argument("bounded_load_factor must be finite and non-negative");
        }
        snapshot_.store(std::make_shared<const RingSnapshot>());
    }

    void ConsistentHashRing::addNode(const NodeId& node_id) {
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
            if (old->physical_nodes.size() >= K_MAX_PHYSICAL_NODES) {
                throw std::length_error("consistent hash ring exceeds uint16 node index capacity");
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
            snap->load_cells = old->load_cells;
            snap->load_cells.push_back(std::make_shared<std::atomic<uint64_t>>(0ULL));
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

            std::ranges::sort(order, std::ranges::less{}, [&](size_t i) -> uint64_t {
                return snap->hashes[i];
            });

            auto sorted_hashes = snap->hashes;
            auto sorted_indices = snap->node_index;
            for (size_t i = 0; i < order.size(); i++) {
                snap->hashes[i] = sorted_hashes[order[i]];
                snap->node_index[i] = sorted_indices[order[i]];
            }
            if (snapshot_.compare_exchange_weak(old, std::move(snap))) {
                return;
            }
            // Another mutator published first — retry on its snapshot.
        }
    }

    void ConsistentHashRing::setNodes(std::span<const NodeId> node_ids) {
        // Normalize duplicates once. Existing nodes keep their order and load
        // cells so callers observing membership churn retain their counters.
        const std::flat_set<NodeId> desired(node_ids.begin(), node_ids.end());
        if (desired.size() > K_MAX_PHYSICAL_NODES) {
            throw std::length_error("consistent hash ring exceeds uint16 node index capacity");
        }

        struct VnodeEntry {
            uint64_t hash;
            uint16_t node_index;
        };

        while (true) {
            auto old = snapshot_.load();
            if (old->physical_nodes.size()
                == desired.size()
                && std::ranges::all_of(old->physical_nodes, [&](const NodeId& id) {
                return desired.contains(id);
            })) {
                return;
            }

            auto snap = std::make_shared<RingSnapshot>();
            snap->physical_nodes.reserve(desired.size());
            snap->load_cells.reserve(desired.size());
            ankerl::unordered_dense::map<NodeId, uint16_t> new_index;
            new_index.reserve(desired.size());

            std::vector<uint32_t> remap(old->physical_nodes.size(), UINT32_MAX);
            const size_t retained_nodes = static_cast<size_t>(
                std::ranges::count_if(old->physical_nodes, [&](const NodeId& id) {
                return desired.contains(id);
            })
            );
            const size_t added_nodes = desired.size() - retained_nodes;
            const auto vnode_count = static_cast<size_t>(vnodes_per_node_);
            if (desired.size() > std::vector<VnodeEntry>{}.max_size() / vnode_count) {
                throw std::length_error("consistent hash ring vnode count overflows storage");
            }

            std::vector<VnodeEntry> added_vnodes;
            added_vnodes.reserve(added_nodes * vnode_count);
            // Retain prior physical ordering/load counters for surviving nodes.
            for (size_t old_index = 0; old_index < old->physical_nodes.size(); ++old_index) {
                const auto& id = old->physical_nodes[old_index];
                if (!desired.contains(id)) {
                    continue;
                }

                auto index = static_cast<uint16_t>(snap->physical_nodes.size());
                new_index.emplace(id, index);
                remap[old_index] = index;
                snap->physical_nodes.push_back(id);
                snap->load_cells.push_back(old->load_cells[old_index]);
            }
            // New members append in supplied order; repeated ids are ignored.
            for (const auto& id : node_ids) {
                if (new_index.contains(id)) {
                    continue;
                }

                auto index = static_cast<uint16_t>(snap->physical_nodes.size());
                new_index.emplace(id, index);
                snap->physical_nodes.push_back(id);
                snap->load_cells.push_back(std::make_shared<std::atomic<uint64_t>>(0));
                for (int vnode = 0; vnode < vnodes_per_node_; ++vnode) {
                    added_vnodes.push_back({hashVnode(id, vnode), index});
                }
            }
            if (snap->physical_nodes.size() != desired.size()) {
                throw std::logic_error("consistent hash membership normalization mismatch");
            }

            // The retained old-ring entries are already hash-sorted. Sort only
            // the new vnode run, then merge both sorted runs into the snapshot.
            std::ranges::sort(added_vnodes, {}, &VnodeEntry::hash);
            const size_t retained_count =
                static_cast<size_t>(std::ranges::count_if(old->node_index, [&](uint16_t old_index) {
                return remap[old_index] != UINT32_MAX;
            }));

            snap->hashes.reserve(retained_count + added_vnodes.size());
            snap->node_index.reserve(retained_count + added_vnodes.size());
            size_t old_pos = 0;
            size_t added_pos = 0;
            auto skip_removed = [&] {
                while (
                    old_pos < old->hashes.size() && remap[old->node_index[old_pos]] == UINT32_MAX) {
                    ++old_pos;
                }
            };

            skip_removed();
            while (old_pos < old->hashes.size() || added_pos < added_vnodes.size()) {
                const bool take_old = added_pos
                    == added_vnodes.size()
                    || (old_pos
                        < old->hashes.size()
                        && old->hashes[old_pos]
                        <= added_vnodes[added_pos].hash);
                if (take_old) {
                    snap->hashes.push_back(old->hashes[old_pos]);
                    snap->node_index.push_back(
                        static_cast<uint16_t>(remap[old->node_index[old_pos]])
                    );

                    ++old_pos;
                    skip_removed();
                } else {
                    snap->hashes.push_back(added_vnodes[added_pos].hash);
                    snap->node_index.push_back(added_vnodes[added_pos].node_index);
                    ++added_pos;
                }
            }
            if (snapshot_.compare_exchange_weak(old, std::move(snap))) {
                return;
            }
        }
    }

    void ConsistentHashRing::removeNode(std::string_view node_id) {
        while (true) {
            auto old = snapshot_.load();
            // Find the index of this node in physical_nodes.
            size_t phys_idx = old->physical_nodes.size();
            for (size_t i = 0; i < old->physical_nodes.size(); ++i) {
                if (old->physical_nodes[i] == node_id) {
                    phys_idx = i;
                    break;
                }
            }
            if (phys_idx == old->physical_nodes.size()) {
                return; // not found
            }

            const auto phys_index = static_cast<uint16_t>(phys_idx);
            auto snap = std::make_shared<RingSnapshot>();
            snap->physical_nodes = old->physical_nodes;

            using PhysicalNodeDifference = std::vector<NodeId>::difference_type;
            const auto physical_node_offset = static_cast<PhysicalNodeDifference>(phys_idx);

            snap->physical_nodes.erase(snap->physical_nodes.begin() + physical_node_offset);
            snap->load_cells = old->load_cells;
            snap->load_cells.erase(snap->load_cells.begin() + physical_node_offset);
            // Remove all ring entries for this node, and remap indices above it.
            snap->hashes.reserve(old->hashes.size());
            snap->node_index.reserve(old->node_index.size());
            for (size_t i = 0; i < old->hashes.size(); i++) {
                if (old->node_index[i] == phys_index) {
                    continue; // skip vnodes for removed node
                }

                snap->hashes.push_back(old->hashes[i]);
                auto idx = old->node_index[i];
                snap->node_index.push_back(idx > phys_index ? idx - 1 : idx);
            }
            if (snap->hashes.size() == old->hashes.size()) {
                return; // nothing changed
            }
            if (snapshot_.compare_exchange_weak(old, std::move(snap))) {
                return;
            }
        }
    }

    auto ConsistentHashRing::getNode(std::string_view key) const -> NodeId {
        auto snap = snapshot_.load();
        if (snap->hashes.empty()) {
            return {};
        }

        auto h = hashKey(key);
        auto idx = std::ranges::lower_bound(snap->hashes, h) - snap->hashes.begin();
        if (static_cast<size_t>(idx) == snap->hashes.size()) {
            idx = 0;
        }
        return snap->physical_nodes[snap->node_index[static_cast<size_t>(idx)]];
    }

    auto ConsistentHashRing::getNodes(std::string_view key, int replica_count) const
        -> std::vector<NodeId> {
        if (replica_count <= 0 || static_cast<size_t>(replica_count) > K_MAX_REPLICAS) {
            throw std::invalid_argument("replica_count must be between 1 and 32");
        }

        auto snap = snapshot_.load();
        if (snap->hashes.empty()) {
            return {};
        }

        auto h = hashKey(key);
        auto start = std::ranges::lower_bound(snap->hashes, h) - snap->hashes.begin();
        if (static_cast<size_t>(start) == snap->hashes.size()) {
            start = 0;
        }

        std::array<uint16_t, K_MAX_REPLICAS> seen{};
        size_t seen_count = 0;
        std::vector<NodeId> result;

        result.reserve(static_cast<size_t>(replica_count));
        auto cur = static_cast<size_t>(start);
        while (static_cast<int>(result.size()) < replica_count) {
            const auto node_index = snap->node_index[cur];
            const auto& node = snap->physical_nodes[node_index];
            bool dup = false;
            for (size_t i = 0; i < seen_count; i++) {
                if (seen[i] == node_index) {
                    dup = true;
                    break;
                }
            }
            if (!dup) {
                seen[seen_count++] = node_index;
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

    auto ConsistentHashRing::getNodeBounded(std::string_view key) const -> NodeId {
        if (bounded_load_factor_ <= 0.0) {
            return getNode(key);
        }

        auto snap = snapshot_.load();
        if (snap->hashes.empty()) {
            return {};
        }

        auto h = hashKey(key);
        auto idx = std::ranges::lower_bound(snap->hashes, h) - snap->hashes.begin();
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
        for (const auto& cell : snap->load_cells) {
            total += cell->load(std::memory_order_relaxed);
        }

        const double avg = static_cast<double>(total) / static_cast<double>(num_nodes);
        const auto max_load = static_cast<uint64_t>(std::ceil(avg * bounded_load_factor_));
        // Walk the ring from the hash position. Return the first node whose
        // in-flight load is below the max. Wrap around at most once.
        // Single immutable snapshot for the whole walk — no per-step lock.
        auto start = idx;
        do {
            const auto phys = snap->node_index[static_cast<size_t>(idx)];
            const uint64_t node_load = snap->load_cells[phys]->load(std::memory_order_relaxed);
            if (node_load < max_load) {
                return snap->physical_nodes[phys];
            }

            idx++;
            if (static_cast<size_t>(idx) == snap->hashes.size()) {
                idx = 0;
            }
        } while (idx != start);
        return snap->physical_nodes[snap->node_index[static_cast<size_t>(start)]];
    }

    void ConsistentHashRing::incrementLoad(std::string_view node) const {
        auto snap = snapshot_.load();
        if (auto idx = findIndex(snap, node); idx.has_value()) {
            snap->load_cells[*idx]->fetch_add(1, std::memory_order_relaxed);
        }
    }

    void ConsistentHashRing::decrementLoad(std::string_view node) const {
        auto snap = snapshot_.load();
        if (auto idx = findIndex(snap, node); idx.has_value()) {
            auto prev = snap->load_cells[*idx]->fetch_sub(1, std::memory_order_relaxed);
            assert(prev > 0 && "decrementLoad called without matching incrementLoad");
            (void)prev;
        }
    }

    auto ConsistentHashRing::currentLoad(std::string_view node) const -> uint64_t {
        auto snap = snapshot_.load();
        if (auto idx = findIndex(snap, node); idx.has_value()) {
            return snap->load_cells[*idx]->load(std::memory_order_relaxed);
        }
        return 0;
    }

    auto ConsistentHashRing::totalLoad() const -> uint64_t {
        uint64_t total = 0;
        auto snap = snapshot_.load();
        for (const auto& cell : snap->load_cells) {
            total += cell->load(std::memory_order_relaxed);
        }
        return total;
    }

    auto ConsistentHashRing::loadFactor() const -> double {
        auto snap = snapshot_.load();
        const auto num_nodes = snap->load_cells.size();
        if (num_nodes == 0) {
            return 0.0;
        }

        uint64_t total = 0;
        for (const auto& cell : snap->load_cells) {
            total += cell->load(std::memory_order_relaxed);
        }

        const double avg = static_cast<double>(total) / static_cast<double>(num_nodes);
        return avg * bounded_load_factor_;
    }

    auto ConsistentHashRing::hashVnode(std::string_view node_id, int vnode_index) -> uint64_t {
        auto label = std::format("{}-{}", node_id, vnode_index);
        return hash64(label);
    }

    auto ConsistentHashRing::hashKey(std::string_view key) -> uint64_t {
        return hash64(key);
    }
} // namespace cinder
