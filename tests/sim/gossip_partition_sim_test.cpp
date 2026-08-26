#include <gtest/gtest.h>
#include <memory>
#include <string>
#include <vector>

#include "cinder/cluster/failure_detector.hpp"
#include "cinder/cluster/gossip.hpp"
#include "cinder/cluster/membership.hpp"
#include "sim_clock.hpp"
#include "sim_transport.hpp"

namespace cinder {
namespace {

using namespace std::chrono_literals;
using namespace std::chrono;

// A node view: table + detector + gossip over a shared bus. SimTransport
// resolves sendAsync synchronously (ok when the bus accepts, err when the
// target is down), so Ping probes need no explicit ACK handler.
struct GossipNode {
    NodeId id;
    SimTransport transport;
    MembershipTable table;
    FailureDetector detector;
    GossipManager gossip;

    GossipNode(SimClock& c, SimBus& b, NodeId node_id, NodeId self)
        : id(node_id),
          transport(b, node_id),
          table(self),
          detector(c, transport, table, self, 3'000ms),
          gossip(c, transport, table, self, 1'000ms) {
        // Forward incoming Gossip requests to this node's manager.
        transport.onMessage([this](const NodeId& from, const net::Request& req) {
            if (req.opcode == net::Opcode::Gossip) {
                gossip.handleMessage(from, req);
            }
        });
    }
};

// Three nodes wired into one bus. `seedAll` adds every node to each table.
struct GossipCluster {
    SimClock clock;
    SimBus bus{clock, 7};

    std::vector<std::unique_ptr<GossipNode>> nodes;

    GossipCluster() {
        for (const auto& name : {"node1", "node2", "node3"}) {
            nodes.push_back(std::make_unique<GossipNode>(clock, bus, name, "node1"));
        }
    }

    void seedAll() {
        std::vector<ClusterConfig::NodeConfig> peers{
            {"node1", "127.0.0.1", 17'900},
            {"node2", "127.0.0.1", 17'901},
            {"node3", "127.0.0.1", 17'902},
        };
        for (auto& n : nodes) {
            n->table.seed(peers);
        }
        for (auto& n : nodes) {
            n->detector.start();
            n->gossip.start();
        }
    }

    void tickAll() {
        for (auto& n : nodes) {
            n->detector.tick();
            n->gossip.tick();
        }
        bus.deliver();
    }
};

TEST(GossipPartitionSimTest, SuspectThenDead) {
    GossipCluster c;
    c.seedAll();

    // node2 goes down: probe fails → suspect, then escalated to dead.
    c.bus.setNodeDown("node2");
    c.tickAll();

    auto info = c.nodes[0]->table.get("node2");
    ASSERT_TRUE(info.has_value());
    EXPECT_EQ(info->state, NodeState::Suspect);

    // Advance past suspect_timeout (3s); escalation happens on a tick.
    c.clock.advance(4s);
    c.tickAll();
    info = c.nodes[0]->table.get("node2");
    EXPECT_EQ(info->state, NodeState::Dead);
}

TEST(GossipPartitionSimTest, IncarnationRefutation) {
    GossipCluster c;
    c.seedAll();

    // node2 marked dead locally, then "recovers" with a higher incarnation.
    c.bus.setNodeDown("node2");
    c.tickAll();
    c.clock.advance(4s);
    c.tickAll();
    ASSERT_EQ(c.nodes[0]->table.get("node2")->state, NodeState::Dead);

    // Recovery: node2's own higher-incarnation Alive rumor.
    c.bus.setNodeUp("node2");
    NodeInfo recovery;
    recovery.id = "node2";
    recovery.host = "127.0.0.1";
    recovery.port = 17'901;
    recovery.state = NodeState::Alive;
    recovery.incarnation = 5;
    c.nodes[0]->table.applyRumor("node2", recovery);

    auto info = c.nodes[0]->table.get("node2");
    ASSERT_TRUE(info.has_value());
    EXPECT_EQ(info->state, NodeState::Alive);
    EXPECT_EQ(info->incarnation, 5);
}

TEST(GossipPartitionSimTest, StaleRumorIgnored) {
    GossipCluster c;
    c.seedAll();

    // Local incarnation is high (5); a lower-incarnation Dead rumor is ignored.
    c.nodes[0]->table.markAlive("node2", 5);

    NodeInfo stale;
    stale.id = "node2";
    stale.state = NodeState::Dead;
    stale.incarnation = 3;
    c.nodes[0]->table.applyRumor("node2", stale);

    auto info = c.nodes[0]->table.get("node2");
    ASSERT_TRUE(info.has_value());
    EXPECT_EQ(info->state, NodeState::Alive);
    EXPECT_EQ(info->incarnation, 5);
}

TEST(GossipPartitionSimTest, GossipPropagates) {
    GossipCluster c;
    c.seedAll();

    // node1 learns node2 is dead via the detector, then gossips to node3.
    c.bus.setNodeDown("node2");
    c.tickAll();
    c.clock.advance(4s);
    c.tickAll();
    ASSERT_EQ(c.nodes[0]->table.get("node2")->state, NodeState::Dead);

    c.nodes[0]->gossip.tick();
    c.clock.advance(1ms);
    c.bus.deliver();

    auto info = c.nodes[2]->table.get("node2");
    ASSERT_TRUE(info.has_value());
    EXPECT_EQ(info->state, NodeState::Dead);
}

TEST(GossipPartitionSimTest, PartitionDegraded) {
    GossipCluster c;
    c.seedAll();

    // Two of three nodes unreachable → below majority → degraded.
    c.bus.setNodeDown("node2");
    c.bus.setNodeDown("node3");
    c.tickAll();
    c.clock.advance(4s);
    c.tickAll();
    EXPECT_TRUE(c.nodes[0]->table.isDegraded());

    // Both return → back to a majority → not degraded.
    c.bus.setNodeUp("node2");
    c.bus.setNodeUp("node3");
    c.nodes[0]->table.markAlive("node2", 1);
    c.nodes[0]->table.markAlive("node3", 1);
    EXPECT_FALSE(c.nodes[0]->table.isDegraded());
}

// Regression: an onChange observer that re-enters the table (like
// CacheNodeServer::rebuildRing, which takes a snapshot) must not deadlock when
// the table fires it from inside a mutating call.
TEST(GossipPartitionSimTest, OnChangeReentrantSnapshot) {
    GossipCluster c;
    c.seedAll();

    // Observe membership changes by taking a full snapshot (re-enters the
    // mutex). Before the fix, firing this from applyRumor/markSuspect under the
    // lock deadlocked.
    int notifications = 0;
    c.nodes[0]->table.onChange([&] {
        (void)c.nodes[0]->table.snapshot(); // would self-deadlock if under lock
        ++notifications;
    });

    // A state transition must complete (not hang) and notify the observer.
    c.nodes[0]->table.markSuspect("node2");
    EXPECT_GE(notifications, 1);

    // An unknown-Alive rumor (gossip join path) also fires onChange.
    NodeInfo rumor;
    rumor.id = "node9";
    rumor.host = "127.0.0.1";
    rumor.port = 17'909;
    rumor.state = NodeState::Alive;
    rumor.incarnation = 1;
    c.nodes[0]->table.applyRumor("node9", rumor);
    EXPECT_GE(notifications, 2);
}

TEST(GossipPartitionSimTest, LateJoinerGetsGossipAndProbes) {
    SimClock clock;
    SimBus bus(clock, 42);

    // node1 + node2: fully seeded and started.
    auto n1 = std::make_unique<GossipNode>(clock, bus, "node1", "node1");
    auto n2 = std::make_unique<GossipNode>(clock, bus, "node2", "node2");
    std::vector<ClusterConfig::NodeConfig> seed_peers{
        {"node1", "127.0.0.1", 17'900},
        {"node2", "127.0.0.1", 17'901},
    };

    n1->table.seed(seed_peers);
    n2->table.seed(seed_peers);
    n1->detector.start();
    n1->gossip.start();
    n2->detector.start();
    n2->gossip.start();

    // node3: late joiner — constructed (onMessage registered) but NOT seeded
    // and NOT started (no detector/gossip ticks). Bus is up so sendAsync
    // succeeds.
    auto n3 = std::make_unique<GossipNode>(clock, bus, "node3", "node3");

    // Simulate late join: node3 announces itself to node1 via applyRumor.
    // MembershipTable::fireCallbacks fires onChange → both managers rebuild
    // peers_ to include "node3".
    NodeInfo late;
    late.id = "node3";
    late.host = "127.0.0.1";
    late.port = 17'902;
    late.state = NodeState::Alive;
    late.incarnation = 1;
    n1->table.applyRumor("node3", late);

    // Verify onChange rebuilt peers: snapshot node1's gossip peers indirectly
    // by checking that the table knows about node3.
    ASSERT_TRUE(n1->table.get("node3").has_value());
    EXPECT_EQ(n1->table.get("node3")->state, NodeState::Alive);

    // Tick several rounds. FD round-robin alternates [node2, node3] so two
    // ticks cover both. Gossip uses random selection; a handful of rounds
    // ensures node3 is hit at least once.
    for (int i = 0; i < 6; ++i) {
        clock.advance(1s);
        n1->detector.tick();
        n1->gossip.tick();
        n2->detector.tick();
        n2->gossip.tick();
        bus.deliver();
    }

    // node3 received a gossip view and learned about node1.
    auto node3_sees_n1 = n3->table.get("node1");
    ASSERT_TRUE(node3_sees_n1.has_value()) << "late joiner never received gossip view";
    EXPECT_EQ(node3_sees_n1->state, NodeState::Alive);

    // node1 successfully probed node3 (node3 was up on the bus, so probe
    // succeeded) — node3 remains Alive in node1's table.
    auto n1_sees_n3 = n1->table.get("node3");
    ASSERT_TRUE(n1_sees_n3.has_value());
    EXPECT_EQ(n1_sees_n3->state, NodeState::Alive);
}

// Late joiner that goes down immediately: probe fails → suspect → dead.
TEST(GossipPartitionSimTest, LateJoinerGoesDownImmediately) {
    SimClock clock;
    SimBus bus(clock, 99);

    auto n1 = std::make_unique<GossipNode>(clock, bus, "node1", "node1");
    std::vector<ClusterConfig::NodeConfig> seed_peers{
        {"node1", "127.0.0.1", 17'900},
    };

    n1->table.seed(seed_peers);
    n1->detector.start();
    n1->gossip.start();
    auto n3 = std::make_unique<GossipNode>(clock, bus, "node3", "node3");

    // Late join via rumor.
    NodeInfo late;
    late.id = "node3";
    late.host = "127.0.0.1";
    late.port = 17'902;
    late.state = NodeState::Alive;
    late.incarnation = 1;
    n1->table.applyRumor("node3", late);

    // Bring node3 down before the first probe.
    bus.setNodeDown("node3");

    // Tick: FD picks node3 (only peer), probe fails → suspect.
    clock.advance(1s);
    n1->detector.tick();
    bus.deliver();

    auto info = n1->table.get("node3");
    ASSERT_TRUE(info.has_value());
    EXPECT_EQ(info->state, NodeState::Suspect);

    // Advance past suspect timeout → escalation marks dead.
    clock.advance(4s);
    n1->detector.tick();
    bus.deliver();
    info = n1->table.get("node3");
    ASSERT_TRUE(info.has_value());
    EXPECT_EQ(info->state, NodeState::Dead);
}

// Graceful leave: node3 calls leave() → broadcasts self as Dead to all peers.
// After delivery, node1 sees node3 as Dead immediately — no probe/suspect cycle.
TEST(GossipPartitionSimTest, GracefulLeaveBroadcastsDead) {
    SimClock clock;
    SimBus bus(clock, 42);

    auto n1 = std::make_unique<GossipNode>(clock, bus, "node1", "node1");
    auto n2 = std::make_unique<GossipNode>(clock, bus, "node2", "node2");
    auto n3 = std::make_unique<GossipNode>(clock, bus, "node3", "node3");

    std::vector<ClusterConfig::NodeConfig> seed_peers{
        {"node1", "127.0.0.1", 17'900},
        {"node2", "127.0.0.1", 17'901},
        {"node3", "127.0.0.1", 17'902},
    };
    n1->table.seed(seed_peers);
    n2->table.seed(seed_peers);
    n3->table.seed(seed_peers);
    n1->detector.start();
    n1->gossip.start();
    n2->detector.start();
    n2->gossip.start();
    n3->detector.start();
    n3->gossip.start();

    // Confirm node3 is alive in all tables before leave.
    EXPECT_EQ(n1->table.get("node3")->state, NodeState::Alive);
    EXPECT_EQ(n2->table.get("node3")->state, NodeState::Alive);

    // Node3 broadcasts leave.
    n3->gossip.leave();

    // Deliver the leave gossip to node1 and node2.
    bus.deliver();

    // node1 and node2 see node3 as Dead immediately.
    EXPECT_EQ(n1->table.get("node3")->state, NodeState::Dead);
    EXPECT_EQ(n2->table.get("node3")->state, NodeState::Dead);

    // node3's own table: markDead was called internally.
    EXPECT_EQ(n3->table.get("node3")->state, NodeState::Dead);
}
} // namespace
} // namespace cinder
