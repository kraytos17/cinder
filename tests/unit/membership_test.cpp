#include <gtest/gtest.h>
#include <string>
#include <vector>

#include "cinder/cluster/membership.hpp"

using std::chrono::steady_clock;

namespace cinder {
    namespace {

        NodeInfo makeInfo(const std::string& id, NodeState state, uint64_t incarnation) {
            return {.id = id, .host = {}, .state = state, .incarnation = incarnation};
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

        TEST(MembershipTest, HostAdoptedFromRumor) {
            MembershipTable table("node1");

            // Gossip-discovered node whose address was unknown to the forwarding peer.
            table.applyRumor("node1", makeInfo("node3", NodeState::Alive, 0));
            ASSERT_TRUE(table.get("node3").has_value());
            EXPECT_EQ(table.get("node3")->host, "");

            // A later rumor carries the reachable address → must be adopted.
            NodeInfo addressed = makeInfo("node3", NodeState::Alive, 1);
            addressed.host = "10.0.0.7";
            addressed.port = 9001;
            table.applyRumor("node1", addressed);

            auto n3 = table.get("node3");
            ASSERT_TRUE(n3.has_value());
            EXPECT_EQ(n3->host, "10.0.0.7");
            EXPECT_EQ(n3->port, 9001);
        }

        TEST(MembershipTest, KnownHostNotClobbered) {
            MembershipTable table("node1");

            NodeInfo addressed = makeInfo("node2", NodeState::Alive, 0);
            addressed.host = "10.0.0.5";
            addressed.port = 9000;
            table.applyRumor("node1", addressed);

            // A rumor with no address must never blank out a known-good one.
            table.applyRumor("node1", makeInfo("node2", NodeState::Suspect, 5));
            auto n2 = table.get("node2");

            ASSERT_TRUE(n2.has_value());
            EXPECT_EQ(n2->state, NodeState::Suspect);
            EXPECT_EQ(n2->host, "10.0.0.5");
            EXPECT_EQ(n2->port, 9000);
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
            table.setSelfAddress("127.0.0.1", 7000);

            auto self = table.get("node1");
            ASSERT_TRUE(self.has_value());
            EXPECT_EQ(self->host, "127.0.0.1");
            EXPECT_EQ(self->port, 7000);
        }

        TEST(MembershipTest, QuarantineAfterRejoin) {
            using namespace std::chrono_literals;
            MembershipTable table("node1");
            table.seed({{"node2", "127.0.0.1", 17'901}});
            auto t0 = steady_clock::now();

            // A seeded node is quarantined briefly after seeding.
            EXPECT_TRUE(table.isQuarantined("node2", t0, 1000ms));
            EXPECT_FALSE(table.isQuarantined("node2", t0 + 2000ms, 1000ms));

            // Re-join resets the window: Dead → Alive starts a fresh quarantine.
            table.markDead("node2");
            EXPECT_FALSE(table.isQuarantined("node2", t0, 1000ms)); // not alive
            table.markAlive("node2", 10);
            EXPECT_TRUE(table.isQuarantined("node2", t0, 1000ms));
            EXPECT_FALSE(table.isQuarantined("node2", t0 + 2000ms, 1000ms));

            // Zero interval disables quarantine.
            EXPECT_FALSE(table.isQuarantined("node2", t0, 0ms));
        }

        // The one refutation case the tests above miss: a well-formed rumor
        // about ourselves whose claimed incarnation exceeds ours. Accepting
        // it would drop the node out of its own ring, so it must still be
        // refuted — and the refutation must beat the claimed incarnation.
        TEST(MembershipTest, SelfRumorWithHigherIncarnationIsStillRefuted) {
            MembershipTable table("self");
            table.seed({{"node2", "127.0.0.1", 7001}});

            auto before = table.get("self");
            ASSERT_TRUE(before.has_value());

            NodeInfo rumor;
            rumor.id = "self";
            rumor.host = "127.0.0.1";
            rumor.port = 7000;
            rumor.state = NodeState::Dead;
            rumor.incarnation = before->incarnation + 100;
            table.applyRumor("node2", rumor);

            auto after = table.get("self");
            ASSERT_TRUE(after.has_value());
            EXPECT_EQ(after->state, NodeState::Alive) << "self must never accept its own death";
            EXPECT_GT(after->incarnation, rumor.incarnation)
                << "refuting incarnation must exceed the claimed one, or the rumor "
                   "re-applies on the next gossip round";
        }
    } // namespace
} // namespace cinder
