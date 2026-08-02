#include <gtest/gtest.h>
#include <string>
#include <vector>

#include "cinder/node/replication_manager.hpp"
#include "cinder/store/lru_store.hpp"
#include "sim_clock.hpp"
#include "sim_transport.hpp"

namespace cinder {
namespace {

using namespace std::chrono_literals;

// Two-node cluster: node1 (primary) + node2 (replica). Both store nodes apply
// incoming Replicate messages via putVersioned — idempotent per version.
class TwoNodeCluster {
  public:

    SimClock clock;
    SimBus bus{clock, 42};
    SimTransport t1{bus, "node1"};
    SimTransport t2{bus, "node2"};
    LruStore store1{1'024, &clock};
    LruStore store2{1'024, &clock};
    ReplicationManager mgr1{store1, "node1", clock, t1};
    ReplicationManager mgr2{store2, "node2", clock, t2};

    TwoNodeCluster() {
        bus.registerHandler("node1", applyHandler(store1, clock));
        bus.registerHandler("node2", applyHandler(store2, clock));
    }

  private:

    static auto applyHandler(LruStore& store, SimClock& clock) -> SimBus::Handler {
        return [&store, &clock](const NodeId&, const net::Request& req) {
            if (req.opcode != net::Opcode::Replicate) {
                return;
            }

            VersionedEntry e;
            e.value = req.value;
            e.version = req.version;
            e.writer_node_hash = req.writer_node_hash;
            if (req.ttl.has_value()) {
                e.has_ttl = true;
                e.expires_at = clock.now() + *req.ttl;
            }
            // NOLINTNEXTLINE(bugprone-unused-return-value, cert-err33-c)
            (void)store.putVersioned(req.key, std::move(e));
        };
    }
};

// Synchronous shim over writeAsync: SimTransport resolves synchronously, so the
// captured result is valid as soon as writeAsync returns.
auto
runWrite(ReplicationManager& mgr, const std::string& key, std::string value,
    std::optional<std::chrono::milliseconds> ttl, const std::vector<NodeId>& replicas,
    ConsistencyMode mode) -> Result<void> {
    Result<void> result = err(Error(Errc::InternalError));
    mgr.writeAsync(
        key, std::move(value), ttl, replicas, mode, [&](Result<void> r) { result = std::move(r); });
    return result;
}

auto
runReplay(ReplicationManager& mgr) -> size_t {
    size_t replayed = 0;
    mgr.replayHints([&](size_t n) { replayed = n; });
    return replayed;
}

TEST(ReplicationSimTest, AsyncWriteReachesReplica) {
    TwoNodeCluster c;
    c.bus.setDelay(10ms);
    ASSERT_TRUE(
        runWrite(c.mgr1, "k", "v", std::nullopt, {"node2"}, ConsistencyMode::Async).has_value());

    // Scheduled but not yet delivered.
    EXPECT_FALSE(c.store2.get("k").has_value());
    c.clock.advance(10ms);
    c.bus.deliver();

    ASSERT_TRUE(c.store2.get("k").has_value());
    EXPECT_EQ(c.store2.get("k").value(), "v");
    EXPECT_EQ(c.bus.deliveredCount(), 1);
}

TEST(ReplicationSimTest, OutOfOrderDeliveryHighestVersionWins) {
    TwoNodeCluster c;
    c.bus.setDelay(10ms);
    c.bus.setReorder(true);

    ASSERT_TRUE(
        runWrite(c.mgr1, "k", "old", std::nullopt, {"node2"}, ConsistencyMode::Async).has_value());
    ASSERT_TRUE(
        runWrite(c.mgr1, "k", "new", std::nullopt, {"node2"}, ConsistencyMode::Async).has_value());

    c.clock.advance(10ms);
    c.bus.deliver(); // reorder → [v2, v1]; stale v1 rejected by versioning

    ASSERT_TRUE(c.store2.get("k").has_value());
    EXPECT_EQ(c.store2.get("k").value(), "new");
}

TEST(ReplicationSimTest, MessageLossReplicaStaleNoCrash) {
    TwoNodeCluster c;
    c.bus.setLossRate(1.0);

    ASSERT_TRUE(
        runWrite(c.mgr1, "k", "v", std::nullopt, {"node2"}, ConsistencyMode::Async).has_value());
    c.clock.advance(10ms);
    c.bus.deliver();

    EXPECT_EQ(c.bus.droppedCount(), 1);
    EXPECT_EQ(c.bus.deliveredCount(), 0);
    EXPECT_FALSE(c.store2.get("k").has_value()); // lost — no crash
    EXPECT_TRUE(c.store1.get("k").has_value());  // primary still has it
}

TEST(ReplicationSimTest, TtlPropagatesToReplica) {
    TwoNodeCluster c;
    ASSERT_TRUE(runWrite(c.mgr1, "k", "v", 100ms, {"node2"}, ConsistencyMode::Async).has_value());
    c.bus.deliver();

    ASSERT_TRUE(c.store2.get("k").has_value());
    c.clock.advance(200ms);
    EXPECT_FALSE(c.store1.get("k").has_value());
    EXPECT_FALSE(c.store2.get("k").has_value());
}

TEST(ReplicationSimTest, FanoutToMultipleReplicas) {
    SimClock clock;
    SimBus bus{clock, 7};
    SimTransport t1{bus, "node1"};
    SimTransport t2{bus, "node2"};
    SimTransport t3{bus, "node3"};
    LruStore s1{1'024, &clock};
    LruStore s2{1'024, &clock};
    LruStore s3{1'024, &clock};
    ReplicationManager m1{s1, "node1", clock, t1};

    auto apply = [&](LruStore& store, const char* id) {
        bus.registerHandler(id, [&store, &clock](const NodeId&, const net::Request& req) {
            if (req.opcode != net::Opcode::Replicate) {
                return;
            }

            VersionedEntry e;
            e.value = req.value;
            e.version = req.version;
            e.writer_node_hash = req.writer_node_hash;
            if (req.ttl.has_value()) {
                e.has_ttl = true;
                e.expires_at = clock.now() + *req.ttl;
            }
            // NOLINTNEXTLINE(bugprone-unused-return-value, cert-err33-c)
            (void)store.putVersioned(req.key, std::move(e));
        });
    };
    apply(s2, "node2");
    apply(s3, "node3");

    ASSERT_TRUE(runWrite(m1, "k", "v", std::nullopt, {"node2", "node3"}, ConsistencyMode::Async)
            .has_value());
    clock.advance(5ms);
    bus.deliver();

    EXPECT_TRUE(s2.get("k").has_value());
    EXPECT_TRUE(s3.get("k").has_value());
}

TEST(ReplicationSimTest, QuorumMetWithOneReplicaUp) {
    TwoNodeCluster c;
    // R = primary + 1 replica = 2, W = 2/2 + 1 = 2. Local ack + replica ack.
    auto res = runWrite(c.mgr1, "k", "v", std::nullopt, {"node2"}, ConsistencyMode::Quorum);
    ASSERT_TRUE(res.has_value());

    c.clock.advance(5ms);
    c.bus.deliver();
    ASSERT_TRUE(c.store2.get("k").has_value());
    EXPECT_EQ(c.store2.get("k").value(), "v");
}

TEST(ReplicationSimTest, QuorumNotMetWithReplicaDown) {
    TwoNodeCluster c;
    c.bus.setNodeDown("node2");
    // R = 2, W = 2. Local ack only (1 < 2) → fail closed.
    auto res = runWrite(c.mgr1, "k", "v", std::nullopt, {"node2"}, ConsistencyMode::Quorum);
    ASSERT_FALSE(res.has_value());
    EXPECT_EQ(res.error().code(), Errc::NotReady);

    // Replica is down, so the write is hinted for later replay.
    EXPECT_EQ(c.mgr1.hintCount(), 1);
}

TEST(ReplicationSimTest, QuorumMetMajorityWithThreeNodes) {
    TwoNodeCluster c;
    // Add a third node: R = 3, W = 3/2 + 1 = 2. Local + 1 replica suffices.
    SimTransport t3{c.bus, "node3"};
    LruStore store3{1'024, &c.clock};
    ReplicationManager mgr3{store3, "node3", c.clock, t3};
    c.bus.registerHandler("node3", [&c, &store3](const NodeId&, const net::Request& req) {
        if (req.opcode != net::Opcode::Replicate) {
            return;
        }

        VersionedEntry e;
        e.value = req.value;
        e.version = req.version;
        e.writer_node_hash = req.writer_node_hash;
        if (req.ttl.has_value()) {
            e.has_ttl = true;
            e.expires_at = c.clock.now() + *req.ttl;
        }
        // NOLINTNEXTLINE
        (void)store3.putVersioned(req.key, std::move(e));
    });

    // node2 down, node3 up → acks = local + node3 = 2 = W → success.
    c.bus.setNodeDown("node2");
    auto res =
        runWrite(c.mgr1, "k", "v", std::nullopt, {"node2", "node3"}, ConsistencyMode::Quorum);
    ASSERT_TRUE(res.has_value());

    c.clock.advance(5ms);
    c.bus.deliver();
    EXPECT_TRUE(store3.get("k").has_value());
    // node2 was down → hinted.
    EXPECT_EQ(c.mgr1.hintCount(), 1);
}

TEST(ReplicationSimTest, HintedHandoffReplaysWhenReplicaReturns) {
    TwoNodeCluster c;
    c.bus.setNodeDown("node2");

    // Async write to a down replica → local ok, hint queued, replica stale.
    ASSERT_TRUE(
        runWrite(c.mgr1, "k", "v", std::nullopt, {"node2"}, ConsistencyMode::Async).has_value());
    ASSERT_EQ(c.mgr1.hintCount(), 1);
    EXPECT_FALSE(c.store2.get("k").has_value());

    // Replica returns; replay delivers the hint.
    c.bus.setNodeUp("node2");
    EXPECT_EQ(runReplay(c.mgr1), 1);
    EXPECT_EQ(c.mgr1.hintCount(), 0);

    c.clock.advance(5ms);
    c.bus.deliver();
    ASSERT_TRUE(c.store2.get("k").has_value());
    EXPECT_EQ(c.store2.get("k").value(), "v");
}

TEST(ReplicationSimTest, HintExpiresAndIsDropped) {
    TwoNodeCluster c;
    c.bus.setNodeDown("node2");
    ASSERT_TRUE(
        runWrite(c.mgr1, "k", "v", std::nullopt, {"node2"}, ConsistencyMode::Async).has_value());
    ASSERT_EQ(c.mgr1.hintCount(), 1);

    // Hint TTL is 30s — advance past it; replay drops the expired hint.
    c.clock.advance(std::chrono::seconds(31));
    EXPECT_EQ(runReplay(c.mgr1), 0);
    EXPECT_EQ(c.mgr1.hintCount(), 0);
}
} // namespace
} // namespace cinder
