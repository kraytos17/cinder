#pragma once

#include <ankerl/unordered_dense.h>
#include <atomic>
#include <cstdint>
#include <functional>
#include <memory>
#include <optional>
#include <string_view>
#include <vector>

#include "cinder/common/types.hpp"

namespace cinder {

    // Transparent hash for heterogeneous lookup — avoids std::string allocation
    // when callers already hold std::string_view (incrementLoad/decrementLoad).
    struct TransparentStringHash {
        using is_transparent = void;

        auto operator()(std::string_view s) const noexcept -> size_t {
            return std::hash<std::string_view>{}(s);
        }
    };

    // Per-node in-flight load counters, carried inside the snapshot.
    // Cells are shared_ptr so in-flight counts survive membership changes:
    // addNode/removeNode copy the cell pointers into the new snapshot, so an
    // increment issued against an older snapshot still lands in the same
    // counter the latest snapshot reads. Mutable because load tracking is
    // logically orthogonal to the ring's snapshot immutability — readers call
    // increment/decrement on a const ring reference.
    using LoadCell = std::shared_ptr<std::atomic<uint64_t>>;

    struct RingSnapshot {
        std::vector<uint64_t> hashes; // sorted, binary-searched
        std::vector<uint16_t> node_index; // parallel: index into physical_nodes
        std::vector<NodeId> physical_nodes;
        std::vector<LoadCell> load_cells; // parallel: per-node in-flight load
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

        // Load cells live in RingSnapshot (parallel to physical_nodes) and
        // are shared across snapshots, so in-flight counts survive membership
        // changes. Writers need no separate loads mutex: the snapshot CAS is
        // the linearization point.

        // Linear scan: cluster size is tiny (indexed by uint16_t), so a
        // cache-friendly scan of short node ids beats hashing + probing.
        // Miss (concurrent removeNode) returns nullopt; callers treat it as
        // zero load / no-op, matching the old map-lookup miss semantics.
        static auto findIndex(
            const std::shared_ptr<const RingSnapshot>& snap,
            std::string_view node
        ) -> std::optional<size_t> {
            for (size_t i = 0; i < snap->physical_nodes.size(); ++i) {
                if (snap->physical_nodes[i] == node) {
                    return i;
                }
            }
            return std::nullopt;
        }
    };
} // namespace cinder
