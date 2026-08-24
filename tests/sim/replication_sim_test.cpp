#include <gtest/gtest.h>
#include <string>
#include <vector>

#include "cinder/node/replication_manager.hpp"
#include "cinder/store/lru_store.hpp"
#include "sim_clock.hpp"
#include "sim_transport.hpp"

using std::chrono::milliseconds;
using std::chrono::seconds;

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
            if (req.expires_at.has_value()) {
                e.has_ttl = true;
                e.expires_at = toSteadyExpiry(clock, *req.expires_at);
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
    std::optional<milliseconds> ttl, const std::vector<NodeId>& replicas, ConsistencyMode mode)
    -> Result<void> {
    Result<void> result = err(Error(Errc::InternalError));
    mgr.writeAsync(
        key, std::move(value), ttl, replicas, mode, [&](Result<void> r) { result = r; });
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

TEST(ReplicationSimTest, TtlAbsoluteExpirySharedAcrossReplicas) {
    TwoNodeCluster c;
    // Nonzero delivery delay: the replica receives the write after the primary
    // committed it. With an absolute wall-clock expiry on the wire, both nodes
    // must still expire the key at the SAME instant (not primary + delay).
    // TTL is chosen larger than the delay so the entry is still live at delivery.
    c.bus.setDelay(200ms);

    ASSERT_TRUE(runWrite(c.mgr1, "k", "v", 500ms, {"node2"}, ConsistencyMode::Async).has_value());

    // Let the delayed Replicate message actually be delivered.
    c.clock.advance(200ms);
    c.bus.deliver();

    ASSERT_TRUE(c.store1.getVersioned("k").has_value());
    ASSERT_TRUE(c.store2.getVersioned("k").has_value());
    EXPECT_EQ(c.store2.getVersioned("k")->expires_at, c.store1.getVersioned("k")->expires_at);

    // Both expire together after the TTL, regardless of the delivery delay.
    c.clock.advance(300ms);
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
            if (req.expires_at.has_value()) {
                e.has_ttl = true;
                e.expires_at = toSteadyExpiry(clock, *req.expires_at);
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
        if (req.expires_at.has_value()) {
            e.has_ttl = true;
            e.expires_at = toSteadyExpiry(c.clock, *req.expires_at);
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
    c.clock.advance(seconds(31));
    EXPECT_EQ(runReplay(c.mgr1), 0);
    EXPECT_EQ(c.mgr1.hintCount(), 0);
}

TEST(ReplicationSimTest, HintRetriesOnFailedReplay) {
    TwoNodeCluster c;
    c.bus.setNodeDown("node2");
    ASSERT_TRUE(
        runWrite(c.mgr1, "k", "v", std::nullopt, {"node2"}, ConsistencyMode::Async).has_value());
    ASSERT_EQ(c.mgr1.hintCount(), 1);

    // Replica still down: replay fails, hint must be retained for retry.
    EXPECT_EQ(runReplay(c.mgr1), 0);
    EXPECT_EQ(c.mgr1.hintCount(), 1);

    // Replica returns: replay succeeds, hint drained, value delivered.
    c.bus.setNodeUp("node2");
    EXPECT_EQ(runReplay(c.mgr1), 1);
    EXPECT_EQ(c.mgr1.hintCount(), 0);

    c.clock.advance(5ms);
    c.bus.deliver();
    ASSERT_TRUE(c.store2.get("k").has_value());
    EXPECT_EQ(c.store2.get("k").value(), "v");
}

TEST(ReplicationSimTest, ObservedPeerVersionThenLocalWriteWins) {
    TwoNodeCluster c;
    // node1 (primary) writes; node2 applies version V via replication.
    ASSERT_TRUE(
        runWrite(c.mgr1, "k", "v1", std::nullopt, {"node2"}, ConsistencyMode::Async).has_value());
    c.clock.advance(5ms);
    c.bus.deliver();
    ASSERT_TRUE(c.store2.get("k").has_value());
    Version observed = c.store2.getVersioned("k")->version;

    // The store's counter must have advanced past the observed version
    // (Lamport bump), so a local write on node2 outranks the primary's.
    EXPECT_GT(c.store2.mintVersion(), observed);

    // Simulate node2 now becoming primary and writing the same key: its
    // store-minted version must win LWW against node1's earlier write.
    Version fresh = c.store2.mintVersion();
    VersionedEntry entry;
    entry.value = "v2";
    entry.version = fresh;
    entry.writer_node_hash = 0x42;
    ASSERT_TRUE(c.store2.putVersioned("k", std::move(entry)).has_value());
    EXPECT_EQ(c.store2.get("k").value(), "v2");
    EXPECT_GT(c.store2.getVersioned("k")->version, observed);
}

// Cluster with both Replicate (fire-and-forget) and GetVersioned
// (request-response) handlers registered on each node.
class ReadRepairCluster {
  public:

    SimClock clock;
    SimBus bus{clock, 99};
    SimTransport t1{bus, "node1"};
    SimTransport t2{bus, "node2"};
    LruStore store1{1'024, &clock};
    LruStore store2{1'024, &clock};
    ReplicationManager mgr1{store1, "node1", clock, t1};
    ReplicationManager mgr2{store2, "node2", clock, t2};

    ReadRepairCluster() {
        bus.registerHandler("node1", applyHandler(store1, clock));
        bus.registerHandler("node2", applyHandler(store2, clock));
        bus.registerRequestHandler("node1", versionedHandler(store1));
        bus.registerRequestHandler("node2", versionedHandler(store2));
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
            if (req.expires_at.has_value()) {
                e.has_ttl = true;
                e.expires_at = toSteadyExpiry(clock, *req.expires_at);
            }
            // NOLINTNEXTLINE
            (void)store.putVersioned(req.key, std::move(e));
        };
    }

    static auto versionedHandler(LruStore& store) -> SimBus::RequestHandler {
        return [&store](const NodeId&, const net::Request& req) -> net::Response {
            if (req.opcode != net::Opcode::GetVersioned) {
                return {.status = Errc::InvalidArgument, .value = std::nullopt};
            }

            auto entry = store.getVersioned(req.key);
            if (!entry.has_value()) {
                return {.status = Errc::NotFound, .value = std::nullopt};
            }
            return {.status = Errc::OK,
                .value = entry->value,
                .version = entry->version,
                .writer_node_hash = entry->writer_node_hash};
        };
    }
};

auto
runRead(ReplicationManager& mgr, const std::string& key, const std::vector<NodeId>& replicas,
    size_t R) -> Result<VersionedEntry> {
    Result<VersionedEntry> result = err<VersionedEntry>(Error(Errc::InternalError));
    mgr.readAsync(key, replicas, R, [&](Result<VersionedEntry> r) { result = std::move(r); });
    return result;
}

TEST(ReadRepairSimTest, ReadRepairFixesStaleReplica) {
    ReadRepairCluster c;

    // Write v1 to node1 (primary), replicate to node2.
    auto wr = runWrite(c.mgr1, "key1", "v1", std::nullopt, {"node2"}, ConsistencyMode::Async);
    ASSERT_TRUE(wr.has_value());
    c.bus.deliver();
    ASSERT_EQ(c.store2.get("key1"), "v1");

    // Overwrite on node1 with v2 (higher version), replicate to node2.
    auto wr2 = runWrite(c.mgr1, "key1", "v2", std::nullopt, {"node2"}, ConsistencyMode::Async);
    ASSERT_TRUE(wr2.has_value());
    c.bus.deliver();
    ASSERT_EQ(c.store2.get("key1"), "v2");

    // Record the current version on node1 (from the second write).
    auto v2_entry = c.store1.getVersioned("key1");
    ASSERT_TRUE(v2_entry.has_value());
    Version v2_version = v2_entry->version;

    // Now simulate a stale replica: manually revert node2 to an older version.
    c.store2.remove("key1");
    VersionedEntry stale;
    stale.value = "v1";
    stale.version = v2_version - 1;
    stale.writer_node_hash = 100;
    ASSERT_TRUE(c.store2.putVersioned("key1", std::move(stale)).has_value());
    ASSERT_EQ(c.store2.get("key1"), "v1");

    // Quorum read from node1 with R=2: should return v2 and repair node2.
    auto result = runRead(c.mgr1, "key1", {"node2"}, 2);
    ASSERT_TRUE(result.has_value());
    EXPECT_EQ(result->value, "v2");
    EXPECT_EQ(result->version, v2_version);

    // Deliver the repair Replicate from node1 to node2.
    c.bus.deliver();
    EXPECT_EQ(c.store2.get("key1"), "v2");
    EXPECT_EQ(c.store2.getVersioned("key1")->version, v2_version);
}

TEST(ReadRepairSimTest, ReadRepairSkipsWhenAllAgree) {
    ReadRepairCluster c;

    auto wr = runWrite(c.mgr1, "key1", "v1", std::nullopt, {"node2"}, ConsistencyMode::Async);
    ASSERT_TRUE(wr.has_value());
    c.bus.deliver();
    ASSERT_EQ(c.store2.get("key1"), "v1");

    // Quorum read: both agree on v1, no repair needed.
    auto result = runRead(c.mgr1, "key1", {"node2"}, 2);
    ASSERT_TRUE(result.has_value());
    EXPECT_EQ(result->value, "v1");

    // No Replicate messages should have been sent (bus should be empty).
    EXPECT_EQ(c.bus.pending(), 0);
}

TEST(ReadRepairSimTest, ReadRepairReturnsBestVersion) {
    ReadRepairCluster c;

    // Seed node1 with v3, node2 with v1 (node2 is behind).
    VersionedEntry e1;
    e1.value = "v3";
    e1.version = 3;
    e1.writer_node_hash = 100;
    ASSERT_TRUE(c.store1.putVersioned("key1", std::move(e1)).has_value());

    VersionedEntry e2;
    e2.value = "v1";
    e2.version = 1;
    e2.writer_node_hash = 200;
    ASSERT_TRUE(c.store2.putVersioned("key1", std::move(e2)).has_value());

    // Quorum read from node1: should return v3 (highest version).
    auto result = runRead(c.mgr1, "key1", {"node2"}, 2);
    ASSERT_TRUE(result.has_value());
    EXPECT_EQ(result->value, "v3");
    EXPECT_EQ(result->version, 3);
}

TEST(ReadRepairSimTest, ReadRepairFallsBackToLocalOnReplicaFailure) {
    ReadRepairCluster c;

    VersionedEntry e1;
    e1.value = "local-v";
    e1.version = 5;
    e1.writer_node_hash = 100;
    ASSERT_TRUE(c.store1.putVersioned("key1", std::move(e1)).has_value());

    // Bring node2 down so sendRequestAsync fails.
    c.bus.setNodeDown("node2");

    // R=2 but node2 is down — should fall back to local read.
    auto result = runRead(c.mgr1, "key1", {"node2"}, 2);
    ASSERT_TRUE(result.has_value());
    EXPECT_EQ(result->value, "local-v");
}
} // namespace
} // namespace cinder
