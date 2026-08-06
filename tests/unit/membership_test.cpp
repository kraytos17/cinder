#include <gtest/gtest.h>
#include <string>
#include <vector>

#include "cinder/client/connection_pool.hpp"
#include "cinder/cluster/membership.hpp"

namespace cinder {
namespace {

NodeInfo
makeInfo(const std::string& id, NodeState state, uint64_t incarnation) {
    return {.id = id, .host = "", .state = state, .incarnation = incarnation};
}

TEST(MembershipTest, HigherIncarnationWins) {
    MembershipTable table("node1");
    table.seed({{"node2", "127.0.0.1", 17'901}});

    table.applyRumor("node1", makeInfo("node2", NodeState::Suspect, 3));
    EXPECT_EQ(table.get("node2")->state, NodeState::Suspect);
    EXPECT_EQ(table.get("node2")->incarnation, 3);

    table.applyRumor("node1", makeInfo("node2", NodeState::Dead, 7));
    EXPECT_EQ(table.get("node2")->state, NodeState::Dead);
    EXPECT_EQ(table.get("node2")->incarnation, 7);
}

TEST(MembershipTest, StaleRumorIgnored) {
    MembershipTable table("node1");
    table.seed({{"node2", "127.0.0.1", 17'901}});
    table.markAlive("node2", 5);

    table.applyRumor("node1", makeInfo("node2", NodeState::Dead, 3));
    EXPECT_EQ(table.get("node2")->state, NodeState::Alive);
    EXPECT_EQ(table.get("node2")->incarnation, 5);
}

TEST(MembershipTest, SelfRumorRefuted) {
    MembershipTable table("node1");
    table.seed({{"node2", "127.0.0.1", 17'901}});

    // A peer claims node1 is dead → node1 refutes with a higher incarnation.
    table.applyRumor("node2", makeInfo("node1", NodeState::Dead, 9));
    auto self = table.get("node1");
    ASSERT_TRUE(self.has_value());
    EXPECT_EQ(self->state, NodeState::Alive);
    EXPECT_GT(self->incarnation, 9);
}

TEST(MembershipTest, DegradedThreshold) {
    MembershipTable table("node1");
    std::vector<ClusterConfig::NodeConfig> peers{
        {"node1", "127.0.0.1", 17'900},
        {"node2", "127.0.0.1", 17'901},
        {"node3", "127.0.0.1", 17'902},
    };
    table.seed(peers);
    EXPECT_FALSE(table.isDegraded()); // 3/3 visible

    table.markDead("node2");
    EXPECT_FALSE(table.isDegraded()); // 2/3 ≥ majority (2)

    table.markDead("node3");
    EXPECT_TRUE(table.isDegraded()); // 1/3 < majority
}

TEST(MembershipTest, OnChangeFires) {
    MembershipTable table("node1");
    table.seed({{"node2", "127.0.0.1", 17'901}});

    int calls = 0;
    table.onChange([&calls] { ++calls; });

    table.markSuspect("node2");
    EXPECT_EQ(calls, 1);

    table.markAlive("node2", 1);
    EXPECT_EQ(calls, 2);
}

TEST(MembershipTest, SetSelfAddress) {
    MembershipTable table("node1");
    table.setSelfAddress("127.0.0.1", 7'000);

    auto self = table.get("node1");
    ASSERT_TRUE(self.has_value());
    EXPECT_EQ(self->host, "127.0.0.1");
    EXPECT_EQ(self->port, 7'000);
}

TEST(MembershipTest, QuarantineAfterRejoin) {
    using namespace std::chrono_literals;
    MembershipTable table("node1");
    table.seed({{"node2", "127.0.0.1", 17'901}});
    auto t0 = std::chrono::steady_clock::now();

    // A seeded node is quarantined briefly after seeding.
    EXPECT_TRUE(table.isQuarantined("node2", t0, 1'000ms));
    EXPECT_FALSE(table.isQuarantined("node2", t0 + 2'000ms, 1'000ms));

    // Re-join resets the window: Dead → Alive starts a fresh quarantine.
    table.markDead("node2");
    EXPECT_FALSE(table.isQuarantined("node2", t0, 1'000ms)); // not alive
    table.markAlive("node2", 10);
    EXPECT_TRUE(table.isQuarantined("node2", t0, 1'000ms));
    EXPECT_FALSE(table.isQuarantined("node2", t0 + 2'000ms, 1'000ms));

    // Zero interval disables quarantine.
    EXPECT_FALSE(table.isQuarantined("node2", t0, 0ms));
}
} // namespace
} // namespace cinder
