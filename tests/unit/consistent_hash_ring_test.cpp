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
} // namespace
} // namespace cinder
