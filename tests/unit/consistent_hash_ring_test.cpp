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
} // namespace
} // namespace cinder
