#include <array>
#include <benchmark/benchmark.h>
#include <span>
#include <string>
#include <vector>

#include "cinder/hashing/consistent_hash_ring.hpp"

namespace cinder {
    namespace {

        auto makeLookupKeys() -> std::array<std::string, 1024> {
            std::array<std::string, 1024> keys;
            for (size_t i = 0; i < keys.size(); ++i) {
                keys[i] = "ring-benchmark-key-" + std::to_string(i);
            }
            return keys;
        }

        void bmRingGetNode(benchmark::State& state) {
            ConsistentHashRing ring(static_cast<int>(state.range(0)));
            ring.addNode("node1");
            ring.addNode("node2");
            ring.addNode("node3");
            ring.addNode("node4");
            const auto keys = makeLookupKeys();
            size_t i = 0;
            // NOLINTNEXTLINE
            for (auto _ : state) {
                auto node = ring.getNode(keys[i++ % keys.size()]);
                benchmark::DoNotOptimize(node);
            }
            state.SetItemsProcessed(state.iterations());
        }

        // NOLINTNEXTLINE
        BENCHMARK(bmRingGetNode)->Arg(50)->Arg(150)->Arg(300);

        void bmRingGetNodes(benchmark::State& state) {
            ConsistentHashRing ring(static_cast<int>(state.range(0)));
            ring.addNode("node1");
            ring.addNode("node2");
            ring.addNode("node3");
            ring.addNode("node4");
            const auto keys = makeLookupKeys();
            size_t i = 0;
            // NOLINTNEXTLINE
            for (auto _ : state) {
                auto nodes = ring.getNodes(keys[i++ % keys.size()], 3);
                benchmark::DoNotOptimize(nodes);
            }
            state.SetItemsProcessed(state.iterations());
        }

        // NOLINTNEXTLINE
        BENCHMARK(bmRingGetNodes)->Arg(50)->Arg(150)->Arg(300);

        void bmRingIndividualMembershipChurn(benchmark::State& state) {
            ConsistentHashRing ring(static_cast<int>(state.range(0)));
            ring.addNode("node1");
            ring.addNode("node2");
            ring.addNode("node3");
            ring.addNode("node4");
            bool transient_present = false;
            // NOLINTNEXTLINE
            for (auto _ : state) {
                if (transient_present) {
                    ring.removeNode("transient");
                } else {
                    ring.addNode("transient");
                }
                transient_present = !transient_present;
            }
            state.SetItemsProcessed(state.iterations());
        }

        // NOLINTNEXTLINE
        BENCHMARK(bmRingIndividualMembershipChurn)->Arg(50)->Arg(150)->Arg(300);

        void bmRingBulkMembershipChurn(benchmark::State& state) {
            ConsistentHashRing ring(static_cast<int>(state.range(0)));
            const std::vector<NodeId> base{"node1", "node2", "node3", "node4"};
            const std::vector<NodeId>
                with_transient{"node1", "node2", "node3", "node4", "transient"};
            ring.setNodes(base);
            bool transient_present = false;
            // NOLINTNEXTLINE
            for (auto _ : state) {
                ring.setNodes(
                    transient_present ? std::span<const NodeId>(base)
                                      : std::span<const NodeId>(with_transient)
                );
                transient_present = !transient_present;
            }
            state.SetItemsProcessed(state.iterations());
        }

        // NOLINTNEXTLINE
        BENCHMARK(bmRingBulkMembershipChurn)->Arg(50)->Arg(150)->Arg(300);

        // Production-shaped contention: N threads doing the per-request
        // getNode on one shared ring. Each lookup takes the spinlock-backed
        // atomic<shared_ptr> snapshot load.
        void bmRingGetNodeContended(benchmark::State& state) {
            ConsistentHashRing ring(150);
            ring.addNode("node1");
            ring.addNode("node2");
            ring.addNode("node3");
            ring.addNode("node4");
            const auto keys = makeLookupKeys();
            int i = state.thread_index();
            const int stride = state.threads();
            // NOLINTNEXTLINE
            for (auto _ : state) {
                const auto& key = keys[static_cast<size_t>(i) % keys.size()];
                i += stride;
                auto node = ring.getNode(key);
                benchmark::DoNotOptimize(node);
            }
            state.SetItemsProcessed(state.iterations());
        }

        // NOLINTNEXTLINE
        BENCHMARK(bmRingGetNodeContended)->ThreadRange(1, 8);

        // Full per-replica cost: lookup + increment + decrement, as
        // TcpConnection does around each request. Three locked snapshot
        // loads per iteration.
        void bmRingLoadIncDecContended(benchmark::State& state) {
            ConsistentHashRing ring(150);
            ring.addNode("node1");
            ring.addNode("node2");
            ring.addNode("node3");
            ring.addNode("node4");
            const auto keys = makeLookupKeys();
            int i = state.thread_index();
            const int stride = state.threads();
            // NOLINTNEXTLINE
            for (auto _ : state) {
                const auto& key = keys[static_cast<size_t>(i) % keys.size()];
                i += stride;
                auto node = ring.getNode(key);
                ring.incrementLoad(node);
                ring.decrementLoad(node);
            }
            state.SetItemsProcessed(state.iterations());
        }

        // NOLINTNEXTLINE
        BENCHMARK(bmRingLoadIncDecContended)->ThreadRange(1, 8);

        // Bounded-load variant: adds the whole-ring total scan plus the walk
        // over cells, still on one shared snapshot.
        void bmRingGetNodeBoundedContended(benchmark::State& state) {
            ConsistentHashRing ring(150, 2.0);
            ring.addNode("node1");
            ring.addNode("node2");
            ring.addNode("node3");
            ring.addNode("node4");
            const auto keys = makeLookupKeys();
            int i = state.thread_index();
            const int stride = state.threads();
            // NOLINTNEXTLINE
            for (auto _ : state) {
                const auto& key = keys[static_cast<size_t>(i) % keys.size()];
                i += stride;
                auto node = ring.getNodeBounded(key);
                benchmark::DoNotOptimize(node);
            }
            state.SetItemsProcessed(state.iterations());
        }

        // NOLINTNEXTLINE
        BENCHMARK(bmRingGetNodeBoundedContended)->ThreadRange(1, 8);
    } // namespace
} // namespace cinder
