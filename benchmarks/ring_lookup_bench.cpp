#include <benchmark/benchmark.h>
#include <string>

#include "cinder/hashing/consistent_hash_ring.hpp"

namespace cinder {
namespace {

// Baseline: raw hash + binary search on the ring for a single key.
void
bmRingGetNode(benchmark::State& state) {
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

// Replica selection with dedup (the ring's own fixed-array dedup path).
void
bmRingGetNodes(benchmark::State& state) {
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

// Add-node churn: the copy-on-write snapshot rebuild cost.
void
bmRingAddNode(benchmark::State& state) {
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

} // namespace
} // namespace cinder
