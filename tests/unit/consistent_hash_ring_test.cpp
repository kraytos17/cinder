#include <atomic>
#include <gtest/gtest.h>
#include <set>
#include <string>
#include <thread>
#include <vector>

#include "cinder/hashing/consistent_hash_ring.hpp"

namespace cinder {
namespace {

TEST(ConsistentHashRingTest, SingleNode) {
    ConsistentHashRing ring(10);
    ring.addNode("node1");

    for (int i = 0; i < 100; i++) {
        auto key = "key" + std::to_string(i);
        EXPECT_EQ(ring.getNode(key), "node1");
    }
}

TEST(ConsistentHashRingTest, MultipleNodes) {
    ConsistentHashRing ring(50);
    ring.addNode("node1");
    ring.addNode("node2");
    ring.addNode("node3");

    std::set<std::string> seen;
    for (int i = 0; i < 1'000; i++) {
        auto key = "key" + std::to_string(i);
        seen.insert(ring.getNode(key));
    }
    EXPECT_EQ(seen.size(), 3);
}

TEST(ConsistentHashRingTest, AddNodeDistributes) {
    ConsistentHashRing ring(100);
    ring.addNode("node1");
    ring.addNode("node2");

    std::map<std::string, int> counts;
    for (int i = 0; i < 10'000; i++) {
        auto key = "key" + std::to_string(i);
        counts[ring.getNode(key)]++;
    }

    EXPECT_NEAR(counts["node1"], 5'000, 1'000);
    EXPECT_NEAR(counts["node2"], 5'000, 1'000);
}

TEST(ConsistentHashRingTest, RemoveNode) {
    ConsistentHashRing ring(50);
    ring.addNode("node1");
    ring.addNode("node2");
    ring.removeNode("node1");

    for (int i = 0; i < 1'000; i++) {
        auto key = "key" + std::to_string(i);
        EXPECT_EQ(ring.getNode(key), "node2");
    }
}

TEST(ConsistentHashRingTest, GetNodesReplicaCount) {
    ConsistentHashRing ring(50);
    ring.addNode("node1");
    ring.addNode("node2");
    ring.addNode("node3");

    for (int i = 0; i < 100; i++) {
        auto key = "key" + std::to_string(i);
        auto nodes = ring.getNodes(key, 2);
        ASSERT_EQ(nodes.size(), 2);
        EXPECT_NE(nodes[0], nodes[1]);
    }
}

TEST(ConsistentHashRingTest, GetNodesNotExceedingAlive) {
    ConsistentHashRing ring(50);
    ring.addNode("node1");

    for (int i = 0; i < 100; i++) {
        auto key = "key" + std::to_string(i);
        auto nodes = ring.getNodes(key, 3);
        EXPECT_EQ(nodes.size(), 1);
    }
}

TEST(ConsistentHashRingTest, MinimalRemappingOnAdd) {
    ConsistentHashRing ring(150);

    ring.addNode("node1");
    ring.addNode("node2");

    std::map<std::string, std::string> before;
    for (int i = 0; i < 10'000; i++) {
        auto key = "key" + std::to_string(i);
        before[key] = ring.getNode(key);
    }

    ring.addNode("node3");
    int moved = 0;
    for (auto& [key, old_node] : before) {
        if (ring.getNode(key) != old_node) {
            moved++;
        }
    }

    // Expected: roughly 1/N of keys move when adding a node (1/3 ≈ 33%)
    // should be below 45%
    EXPECT_LT(moved, 4'500);
}

TEST(ConsistentHashRingTest, ConcurrentReads) {
    ConsistentHashRing ring(100);
    ring.addNode("node1");
    ring.addNode("node2");
    ring.addNode("node3");

    std::vector<std::thread> threads;
    threads.reserve(4);
    for (int t = 0; t < 4; t++) {
        threads.emplace_back([&ring]() {
            for (int i = 0; i < 1'000; i++) {
                auto key = "key" + std::to_string(i);
                auto node = ring.getNode(key);
                EXPECT_TRUE(node == "node1" || node == "node2" || node == "node3");
            }
        });
    }
    for (auto& th : threads) {
        th.join();
    }
}

// Readers hammering the ring while a writer churns membership: every response
// must be internally consistent (non-empty, distinct replicas, drawn from the
// node set of SOME snapshot). Exercises the RCU snapshot + CAS publish path
// under TSan.
TEST(ConsistentHashRingTest, ConcurrentReadersDuringMembershipChurn) {
    ConsistentHashRing ring(64);
    ring.addNode("n0");
    ring.addNode("n1");

    constexpr int K_READER_THREADS = 4;
    std::atomic<bool> stop{false};
    std::vector<std::thread> readers;
    readers.reserve(K_READER_THREADS);
    for (int t = 0; t < K_READER_THREADS; t++) {
        readers.emplace_back([&ring, &stop, t]() {
            for (int i = 0; !stop.load(std::memory_order_relaxed); i++) {
                auto key = "k" + std::to_string(t) + "-" + std::to_string(i);
                auto primary = ring.getNode(key);
                EXPECT_FALSE(primary.empty());

                auto replicas = ring.getNodes(key, 2);
                EXPECT_EQ(replicas.size(), 2U);
                EXPECT_NE(replicas[0], replicas[1]);
            }
        });
    }

    // Writer: add/remove a rotating cast of nodes until readers have had a
    // good workout. Every published snapshot keeps n0/n1 present.
    std::thread writer([&ring, &stop]() {
        for (int round = 0; round < 200; round++) {
            auto name = "churn" + std::to_string(round % 8);
            ring.addNode(name);
            std::this_thread::yield();
            ring.removeNode(name);
        }
        stop.store(true, std::memory_order_relaxed);
    });

    writer.join();
    for (auto& th : readers) {
        th.join();
    }

    // Post-churn invariants: churn nodes are gone, seeds remain.
    auto key = std::string_view("final");
    auto primary = ring.getNode(key);
    EXPECT_TRUE(primary == "n0" || primary == "n1");
}

TEST(ConsistentHashRingTest, GetNodesAllDistinct) {
    ConsistentHashRing ring(50);
    ring.addNode("node1");
    ring.addNode("node2");
    ring.addNode("node3");

    for (int i = 0; i < 200; i++) {
        auto key = "key" + std::to_string(i);
        auto nodes = ring.getNodes(key, 3);
        ASSERT_EQ(nodes.size(), 3);
        EXPECT_NE(nodes[0], nodes[1]);
        EXPECT_NE(nodes[0], nodes[2]);
        EXPECT_NE(nodes[1], nodes[2]);
    }
}

TEST(ConsistentHashRingTest, GetNodesReplicaCountExceedsAlive) {
    ConsistentHashRing ring(50);
    ring.addNode("node1");
    ring.addNode("node2");

    // Requesting more replicas than alive nodes must not overrun the fixed
    // dedup buffer or fabricate nodes.
    for (int i = 0; i < 200; i++) {
        auto key = "key" + std::to_string(i);
        auto nodes = ring.getNodes(key, 8);
        EXPECT_LE(nodes.size(), 2);
    }
}

TEST(ConsistentHashRingTest, AddNodeIdempotent) {
    ConsistentHashRing ring(10);
    ring.addNode("node1");
    ring.addNode("node1"); // duplicate add must not duplicate vnodes

    // Duplicate physical nodes would break getNodes dedup for replica>1.
    ring.addNode("node2");
    for (int i = 0; i < 200; i++) {
        auto key = "key" + std::to_string(i);
        auto nodes = ring.getNodes(key, 2);
        ASSERT_EQ(nodes.size(), 2);
        EXPECT_NE(nodes[0], nodes[1]);
    }

    // Re-add after remove works (add→remove→add round-trip).
    ring.removeNode("node2");
    ring.addNode("node2");
    for (int i = 0; i < 200; i++) {
        auto key = "key" + std::to_string(i);
        EXPECT_EQ(ring.getNodes(key, 2).size(), 2);
    }
}

TEST(BoundedLoadTest, SingleNodeAlwaysReturnsThatNode) {
    ConsistentHashRing ring(10, 1.0);
    ring.addNode("node1");

    for (int i = 0; i < 100; i++) {
        auto key = "key" + std::to_string(i);
        EXPECT_EQ(ring.getNodeBounded(key), "node1");
    }
}

TEST(BoundedLoadTest, BalancedLoadDistributes) {
    ConsistentHashRing ring(50, 1.0);
    ring.addNode("node1");
    ring.addNode("node2");

    // With no load, getNodeBounded behaves like getNode.
    std::map<std::string, int> counts;
    for (int i = 0; i < 10'000; i++) {
        auto key = "key" + std::to_string(i);
        counts[ring.getNodeBounded(key)]++;
    }

    EXPECT_NEAR(counts["node1"], 5'000, 1'500);
    EXPECT_NEAR(counts["node2"], 5'000, 1'500);
}

TEST(BoundedLoadTest, OverloadedNodeSpills) {
    ConsistentHashRing ring(50, 1.0);
    ring.addNode("node1");
    ring.addNode("node2");

    // Find a key that maps to node1 without load.
    std::string spill_key;
    for (int i = 0; i < 10'000; i++) {
        auto key = "spill" + std::to_string(i);
        if (ring.getNode(key) == "node1" && ring.getNode(key) != ring.getNodeBounded(key)) {
            // Not useful yet — no load.
        }
        if (ring.getNode(key) == "node1") {
            spill_key = key;
            break;
        }
    }

    ASSERT_FALSE(spill_key.empty());
    // With no load, getNodeBounded returns node1.
    EXPECT_EQ(ring.getNodeBounded(spill_key), "node1");

    // Artificially overload node1 so its load exceeds the bounded limit.
    // avg_load = 0, max_load = ceil(0 * 1.0) = 0, so load >= 1 triggers spillover.
    ring.incrementLoad("node1");
    ring.incrementLoad("node1");

    // Now getNodeBounded should spill to node2.
    EXPECT_EQ(ring.getNodeBounded(spill_key), "node2");

    ring.decrementLoad("node1");
    ring.decrementLoad("node1");
}

TEST(BoundedLoadTest, AllOverloadedWrapsAround) {
    ConsistentHashRing ring(50, 1.0);
    ring.addNode("node1");
    ring.addNode("node2");

    // Overload both nodes.
    for (int i = 0; i < 10; i++) {
        ring.incrementLoad("node1");
        ring.incrementLoad("node2");
    }

    // All overloaded — should still return a valid node (graceful degradation).
    for (int i = 0; i < 100; i++) {
        auto key = "key" + std::to_string(i);
        auto node = ring.getNodeBounded(key);
        EXPECT_TRUE(node == "node1" || node == "node2");
    }
    for (int i = 0; i < 10; i++) {
        ring.decrementLoad("node1");
        ring.decrementLoad("node2");
    }
}

TEST(BoundedLoadTest, LoadCounterTracking) {
    ConsistentHashRing ring(10, 1.0);
    ring.addNode("node1");

    EXPECT_EQ(ring.currentLoad("node1"), 0);
    EXPECT_EQ(ring.totalLoad(), 0);

    ring.incrementLoad("node1");
    ring.incrementLoad("node1");
    EXPECT_EQ(ring.currentLoad("node1"), 2);
    EXPECT_EQ(ring.totalLoad(), 2);

    ring.decrementLoad("node1");
    EXPECT_EQ(ring.currentLoad("node1"), 1);
    EXPECT_EQ(ring.totalLoad(), 1);

    ring.decrementLoad("node1");
    EXPECT_EQ(ring.currentLoad("node1"), 0);
    EXPECT_EQ(ring.totalLoad(), 0);
}

TEST(BoundedLoadTest, BackwardCompatFactorZero) {
    // When bounded_load_factor is 0 (default), getNodeBounded behaves like getNode.
    ConsistentHashRing ring(50, 0.0);
    ring.addNode("node1");
    ring.addNode("node2");

    for (int i = 0; i < 100; i++) {
        auto key = "key" + std::to_string(i);
        EXPECT_EQ(ring.getNodeBounded(key), ring.getNode(key));
    }
}

TEST(BoundedLoadTest, LoadTrackingPersistsAcrossMembershipChange) {
    ConsistentHashRing ring(50, 1.0);
    ring.addNode("node1");
    ring.addNode("node2");

    ring.incrementLoad("node1");
    ring.incrementLoad("node1");
    EXPECT_EQ(ring.currentLoad("node1"), 2);

    // Adding a node should not reset existing load counters.
    ring.addNode("node3");
    EXPECT_EQ(ring.currentLoad("node1"), 2);

    // Removing a node should erase its load counter.
    ring.removeNode("node2");
    EXPECT_EQ(ring.currentLoad("node2"), 0);
    EXPECT_EQ(ring.currentLoad("node1"), 2);

    ring.decrementLoad("node1");
    ring.decrementLoad("node1");
}

TEST(BoundedLoadTest, ConcurrentBoundedReadsUnderLoadChurn) {
    ConsistentHashRing ring(64, 1.0);
    ring.addNode("n0");
    ring.addNode("n1");

    std::atomic<bool> stop{false};
    std::vector<std::thread> threads;

    // Readers: hammer getNodeBounded while load fluctuates.
    for (int t = 0; t < 4; t++) {
        threads.emplace_back([&ring, &stop, t]() {
            for (int i = 0; !stop.load(std::memory_order_relaxed); i++) {
                auto key = "k" + std::to_string(t) + "-" + std::to_string(i);
                auto node = ring.getNodeBounded(key);
                EXPECT_TRUE(node == "n0" || node == "n1");
            }
        });
    }

    // Load chummers: rapidly increment/decrement to stress the mutex.
    for (int t = 0; t < 2; t++) {
        threads.emplace_back([&ring, &stop]() {
            for (int i = 0; !stop.load(std::memory_order_relaxed); i++) {
                const auto* node = (i % 2 == 0) ? "n0" : "n1";
                ring.incrementLoad(node);
                ring.decrementLoad(node);
            }
        });
    }

    std::this_thread::sleep_for(std::chrono::milliseconds(200));
    stop.store(true, std::memory_order_relaxed);
    for (auto& th : threads) {
        th.join();
    }
    EXPECT_EQ(ring.totalLoad(), 0);
}
} // namespace
} // namespace cinder
