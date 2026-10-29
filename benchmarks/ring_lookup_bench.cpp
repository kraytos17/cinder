#include <benchmark/benchmark.h>
#include <string>

#include "cinder/hashing/consistent_hash_ring.hpp"

namespace cinder {
    namespace {

        void bmRingGetNode(benchmark::State& state) {
            ConsistentHashRing ring(static_cast<int>(state.range(0)));
            ring.addNode("node1");
            ring.addNode("node2");
            ring.addNode("node3");
            ring.addNode("node4");
            int i = 0;
            // NOLINTNEXTLINE
            for (auto _ : state) {
                (void)ring.getNode("key" + std::to_string(i++));
            }
        }

        // NOLINTNEXTLINE
        BENCHMARK(bmRingGetNode)->Arg(50)->Arg(150)->Arg(300);

        void bmRingGetNodes(benchmark::State& state) {
            ConsistentHashRing ring(static_cast<int>(state.range(0)));
            ring.addNode("node1");
            ring.addNode("node2");
            ring.addNode("node3");
            ring.addNode("node4");
            int i = 0;
            // NOLINTNEXTLINE
            for (auto _ : state) {
                (void)ring.getNodes("key" + std::to_string(i++), 3);
            }
        }

        // NOLINTNEXTLINE
        BENCHMARK(bmRingGetNodes)->Arg(50)->Arg(150)->Arg(300);

        void bmRingAddNode(benchmark::State& state) {
            ConsistentHashRing ring(static_cast<int>(state.range(0)));
            ring.addNode("node1");
            int i = 0;
            // NOLINTNEXTLINE
            for (auto _ : state) {
                ring.addNode("node" + std::to_string(i++));
            }
        }

        // NOLINTNEXTLINE
        BENCHMARK(bmRingAddNode)->Arg(50)->Arg(150);

        // Production-shaped contention: N threads doing the per-request
        // getNode on one shared ring. Each lookup takes the spinlock-backed
        // atomic<shared_ptr> snapshot load.
        void bmRingGetNodeContended(benchmark::State& state) {
            ConsistentHashRing ring(150);
            ring.addNode("node1");
            ring.addNode("node2");
            ring.addNode("node3");
            ring.addNode("node4");
            int i = state.thread_index();
            const int stride = state.threads();
            // NOLINTNEXTLINE
            for (auto _ : state) {
                auto key = "key" + std::to_string(i);
                i += stride;
                benchmark::DoNotOptimize(ring.getNode(key));
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
            int i = state.thread_index();
            const int stride = state.threads();
            // NOLINTNEXTLINE
            for (auto _ : state) {
                auto key = "key" + std::to_string(i);
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
            int i = state.thread_index();
            const int stride = state.threads();
            // NOLINTNEXTLINE
            for (auto _ : state) {
                auto key = "key" + std::to_string(i);
                i += stride;
                benchmark::DoNotOptimize(ring.getNodeBounded(key));
            }
            state.SetItemsProcessed(state.iterations());
        }

        // NOLINTNEXTLINE
        BENCHMARK(bmRingGetNodeBoundedContended)->ThreadRange(1, 8);
    } // namespace
} // namespace cinder
