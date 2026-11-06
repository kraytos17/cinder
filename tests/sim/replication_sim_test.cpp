#include <ankerl/unordered_dense.h>
#include <array>
#include <atomic>
#include <deque>
#include <gtest/gtest.h>
#include <memory>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include "cinder/node/replication_manager.hpp"
#include "cinder/store/lru_store.hpp"
#include "sim/sim_test_helpers.hpp"
#include "sim_clock.hpp"
#include "sim_transport.hpp"

using std::chrono::milliseconds;
using std::chrono::seconds;
using std::chrono::steady_clock;

namespace cinder {
    namespace {

        using namespace std::chrono_literals;

        class TwoNodeCluster {
        public:

            SimClock clock;
            SimBus bus{clock, 42};
            SimTransport t1{bus, "node1"};
            SimTransport t2{bus, "node2"};
            LruStore store1{1024, &clock};
            LruStore store2{1024, &clock};
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
                    e.setVersion(req.version);
                    e.writer_node_hash = req.writer_node_hash;
                    if (req.expires_at.has_value()) {
                        e.setHasTtl(true);
                        e.expires_at = toSteadyExpiry(clock, *req.expires_at);
                    }
                    // NOLINTNEXTLINE(bugprone-unused-return-value, cert-err33-c)
                    (void)store.putVersioned(req.key, std::move(e));
                };
            }
        };

        auto runReplay(ReplicationManager& mgr) -> size_t {
            size_t replayed = 0;
            mgr.replayHints([&](size_t n) { replayed = n; });
            return replayed;
        }

        TEST(ReplicationSimTest, AsyncWriteReachesReplica) {
            TwoNodeCluster c;
            c.bus.setDelay(10ms);
            ASSERT_TRUE(runWrite(c.mgr1, "k", "v", std::nullopt, {"node2"}, ConsistencyMode::Async)
                            .has_value());

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
                runWrite(c.mgr1, "k", "old", std::nullopt, {"node2"}, ConsistencyMode::Async)
                    .has_value()
            );
            ASSERT_TRUE(
                runWrite(c.mgr1, "k", "new", std::nullopt, {"node2"}, ConsistencyMode::Async)
                    .has_value()
            );

            c.clock.advance(10ms);
            c.bus.deliver(); // reorder → [v2, v1]; stale v1 rejected by versioning

            ASSERT_TRUE(c.store2.get("k").has_value());
            EXPECT_EQ(c.store2.get("k").value(), "new");
        }

        TEST(ReplicationSimTest, MessageLossReplicaStaleNoCrash) {
            TwoNodeCluster c;
            c.bus.setLossRate(1.0);

            ASSERT_TRUE(runWrite(c.mgr1, "k", "v", std::nullopt, {"node2"}, ConsistencyMode::Async)
                            .has_value());
            c.clock.advance(10ms);
            c.bus.deliver();

            EXPECT_EQ(c.bus.droppedCount(), 1);
            EXPECT_EQ(c.bus.deliveredCount(), 0);
            EXPECT_FALSE(c.store2.get("k").has_value()); // lost — no crash
            EXPECT_TRUE(c.store1.get("k").has_value()); // primary still has it
        }

        TEST(ReplicationSimTest, TtlPropagatesToReplica) {
            TwoNodeCluster c;
            ASSERT_TRUE(
                runWrite(c.mgr1, "k", "v", 100ms, {"node2"}, ConsistencyMode::Async).has_value()
            );
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

            ASSERT_TRUE(
                runWrite(c.mgr1, "k", "v", 500ms, {"node2"}, ConsistencyMode::Async).has_value()
            );

            // Let the delayed Replicate message actually be delivered.
            c.clock.advance(200ms);
            c.bus.deliver();

            ASSERT_TRUE(c.store1.getVersioned("k").has_value());
            ASSERT_TRUE(c.store2.getVersioned("k").has_value());
            EXPECT_EQ(
                c.store2.getVersioned("k")->expires_at,
                c.store1.getVersioned("k")->expires_at
            );

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
            LruStore s1{1024, &clock};
            LruStore s2{1024, &clock};
            LruStore s3{1024, &clock};
            ReplicationManager m1{s1, "node1", clock, t1};

            auto apply = [&](LruStore& store, const char* id) {
                bus.registerHandler(id, [&store, &clock](const NodeId&, const net::Request& req) {
                    if (req.opcode != net::Opcode::Replicate) {
                        return;
                    }

                    VersionedEntry e;
                    e.value = req.value;
                    e.setVersion(req.version);
                    e.writer_node_hash = req.writer_node_hash;
                    if (req.expires_at.has_value()) {
                        e.setHasTtl(true);
                        e.expires_at = toSteadyExpiry(clock, *req.expires_at);
                    }
                    // NOLINTNEXTLINE(bugprone-unused-return-value, cert-err33-c)
                    (void)store.putVersioned(req.key, std::move(e));
                });
            };
            apply(s2, "node2");
            apply(s3, "node3");

            ASSERT_TRUE(
                runWrite(m1, "k", "v", std::nullopt, {"node2", "node3"}, ConsistencyMode::Async)
                    .has_value()
            );
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
            LruStore store3{1024, &c.clock};
            ReplicationManager mgr3{store3, "node3", c.clock, t3};
            c.bus.registerHandler("node3", [&c, &store3](const NodeId&, const net::Request& req) {
                if (req.opcode != net::Opcode::Replicate) {
                    return;
                }

                VersionedEntry e;
                e.value = req.value;
                e.setVersion(req.version);
                e.writer_node_hash = req.writer_node_hash;
                if (req.expires_at.has_value()) {
                    e.setHasTtl(true);
                    e.expires_at = toSteadyExpiry(c.clock, *req.expires_at);
                }
                // NOLINTNEXTLINE
                (void)store3.putVersioned(req.key, std::move(e));
            });

            // node2 down, node3 up → acks = local + node3 = 2 = W → success.
            c.bus.setNodeDown("node2");
            auto res = runWrite(
                c.mgr1,
                "k",
                "v",
                std::nullopt,
                {"node2", "node3"},
                ConsistencyMode::Quorum
            );
            ASSERT_TRUE(res.has_value());

            c.clock.advance(5ms);
            c.bus.deliver();
            EXPECT_TRUE(store3.get("k").has_value());
            // node2 was down → hinted.
            EXPECT_EQ(c.mgr1.hintCount(), 1);
        }

        // Mixed sync/async quorum write: node2 acks inline (inline drain),
        // node3/node4 report while the detached tail is draining. The decision
        // must fire exactly once at quorum despite completions arriving on
        // both sides of the handoff.
        TEST(ReplicationSimTest, QuorumWriteMixedSyncAsyncCompletesOnce) {
            DeferredCluster c;
            c.transport.sync_nodes.insert("node2");

            std::atomic<int> done_count{0};
            Result<void> result = err(Error(Errc::InternalError, "unset"));
            c.mgr.writeAsync(
                "k",
                "v",
                std::nullopt,
                {"node2", "node3", "node4"},
                ConsistencyMode::Quorum,
                [&](Result<void> r) {
                // Assign before counting: the counter synchronizes the
                // handoff to the waiting test thread.
                result = std::move(r);
                ++done_count;
            }
            );
            // All three sends launched; the sync one completed inline.
            EXPECT_EQ(c.transport.pendingSends(), 2u);
            EXPECT_EQ(done_count.load(), 0);

            // First deferred ack reaches quorum (local + node2 + this one).
            // The decision lands on the async tail, off the calling thread.
            c.transport.completeSends(1);
            ASSERT_TRUE(awaitDecision(done_count));
            EXPECT_TRUE(result.has_value());

            // Straggler arrives after the decision: still exactly one
            // delivery, and the tail drains it so no callback is left
            // outstanding.
            c.transport.completeSends(1);
            std::this_thread::sleep_for(milliseconds(100));
            EXPECT_EQ(done_count.load(), 1);
            EXPECT_EQ(c.transport.pendingSends(), 0u);
        }

        // Quorum write fails fast when the decision becomes unreachable:
        // 4 nodes (W=3, local ack=1) with two failures outstanding means
        // 1 + 1 pending < 3, so NotReady fires while the third replica is
        // still in flight — no waiting out the straggler. The drain still
        // runs, so the late ack is consumed and both failures are hinted.
        TEST(ReplicationSimTest, QuorumWriteFailsFastWhenQuorumImpossible) {
            DeferredCluster c;
            std::atomic<int> done_count{0};
            Result<void> result = err(Error(Errc::InternalError, "unset"));
            c.mgr.writeAsync(
                "k",
                "v",
                std::nullopt,
                {"node2", "node3", "node4"},
                ConsistencyMode::Quorum,
                [&](Result<void> r) {
                result = std::move(r);
                ++done_count;
            }
            );
            EXPECT_EQ(c.transport.pendingSends(), 3u);

            // Two failures: acks(1) + pending(1) < W(3) → decide now.
            // Pre-fix this did not fire until the third send completed.
            c.transport.completeSends(2, false);
            ASSERT_TRUE(awaitDecision(done_count));
            EXPECT_FALSE(result.has_value());
            EXPECT_EQ(c.transport.pendingSends(), 1u);

            // Straggler acks late: still exactly one delivery, queue drains,
            // and only the two genuine failures were hinted.
            c.transport.completeSends(1, true);
            std::this_thread::sleep_for(milliseconds(100));
            EXPECT_EQ(done_count.load(), 1);
            EXPECT_EQ(c.transport.pendingSends(), 0u);
            EXPECT_EQ(c.mgr.hintCount(), 2u);
        }

        // Duplicate transport delivery is ignored: the same completion fired
        // twice must not double-decrement pending (which both corrupts the
        // counter and can force a premature fail). Pre-fix the repeated
        // failure below drove pending to zero and failed the write while a
        // success was still outstanding.
        TEST(ReplicationSimTest, QuorumWriteIgnoresDuplicateCompletion) {
            DeferredCluster c;
            std::atomic<int> done_count{0};
            Result<void> result = err(Error(Errc::InternalError, "unset"));
            c.mgr.writeAsync(
                "k",
                "v",
                std::nullopt,
                {"node2", "node3"},
                ConsistencyMode::Quorum,
                [&](Result<void> r) {
                result = std::move(r);
                ++done_count;
            }
            );
            EXPECT_EQ(c.transport.pendingSends(), 2u);

            // node2 fails once (hinted, pending 2→1: no decision either way).
            auto dup = c.transport.peekSend(0);
            (*dup)(cinder::err(Error(Errc::NotReady, "deferred failure")));
            auto hint_deadline = steady_clock::now() + seconds(10);
            while (c.mgr.hintCount() != 1u && steady_clock::now() < hint_deadline) {
                std::this_thread::sleep_for(milliseconds(1));
            }
            ASSERT_EQ(c.mgr.hintCount(), 1u);
            EXPECT_EQ(done_count.load(), 0);

            // Same completion delivered again: must be a no-op. Pre-fix this
            // decremented pending a second time and failed the write here.
            (*dup)(cinder::err(Error(Errc::NotReady, "deferred failure")));
            EXPECT_EQ(done_count.load(), 0);
            EXPECT_EQ(c.mgr.hintCount(), 1u);

            // node3 acks (local + node3 = W=2): success, exactly once. The
            // re-fired node2 callback still queued is ignored as a duplicate.
            c.transport.completeSends(2, true);
            ASSERT_TRUE(awaitDecision(done_count));
            EXPECT_TRUE(result.has_value());
            std::this_thread::sleep_for(milliseconds(100));
            EXPECT_EQ(done_count.load(), 1);
            EXPECT_EQ(c.transport.pendingSends(), 0u);
            EXPECT_EQ(c.mgr.hintCount(), 1u);
        }

        // Concurrent duplicate delivery: the same completion fired from two
        // threads at once must still reduce exactly once. The atomic exchange
        // admits a single winner; the loser is dropped before accounting.
        // Pre-fix this raced on a plain byte (TSan flags it) and could admit
        // both deliveries, double-decrementing pending.
        TEST(ReplicationSimTest, QuorumWriteIgnoresConcurrentDuplicateCompletion) {
            DeferredCluster c;
            std::atomic<int> done_count{0};
            Result<void> result = err(Error(Errc::InternalError, "unset"));
            c.mgr.writeAsync(
                "k",
                "v",
                std::nullopt,
                {"node2", "node3"},
                ConsistencyMode::Quorum,
                [&](Result<void> r) {
                result = std::move(r);
                ++done_count;
            }
            );
            EXPECT_EQ(c.transport.pendingSends(), 2u);

            // Same node2 success from two threads: exactly one wins the
            // exchange, so acks goes 1→2 once and the write succeeds.
            auto dup = c.transport.peekSend(0);
            std::thread t1([dup] { (*dup)(cinder::ok()); });
            std::thread t2([dup] { (*dup)(cinder::ok()); });
            t1.join();
            t2.join();

            ASSERT_TRUE(awaitDecision(done_count));
            EXPECT_TRUE(result.has_value());
            EXPECT_EQ(done_count.load(), 1);
            EXPECT_EQ(c.transport.pendingSends(), 2u);

            // Drain both queued callbacks: node2's re-fire is a duplicate and
            // ignored, node3's ack is consumed. Still exactly one delivery.
            c.transport.completeSends(2, true);
            std::this_thread::sleep_for(milliseconds(100));
            EXPECT_EQ(done_count.load(), 1);
            EXPECT_EQ(c.transport.pendingSends(), 0u);
        }

        // Quorum write with no replicas: W == 1, so the local ack alone
        // suffices and completes synchronously with no bus traffic.
        TEST(ReplicationSimTest, QuorumWriteWithNoReplicasCompletesImmediately) {
            DeferredCluster c;
            auto wr = runWrite(c.mgr, "k0", "v0", std::nullopt, {}, ConsistencyMode::Quorum);
            ASSERT_TRUE(wr.has_value());
            EXPECT_EQ(c.transport.pendingSends(), 0u);
            ASSERT_TRUE(c.store.get("k0").has_value());
            EXPECT_EQ(c.store.get("k0").value(), "v0");
        }

        // Async write with no replicas: best-effort fan-out over zero nodes is
        // a no-op; the local commit still counts and completes synchronously.
        TEST(ReplicationSimTest, AsyncWriteWithNoReplicasCompletesImmediately) {
            DeferredCluster c;
            auto wr = runWrite(c.mgr, "k0", "v0", std::nullopt, {}, ConsistencyMode::Async);
            ASSERT_TRUE(wr.has_value());
            EXPECT_EQ(c.transport.pendingSends(), 0u);
            ASSERT_TRUE(c.store.get("k0").has_value());
            EXPECT_EQ(c.store.get("k0").value(), "v0");
        }

        // Empty replay reports zero AND releases the single-consumer guard: a
        // stuck guard would silently disable all later replays, so prove the
        // manager still round-trips a real hint afterward.
        TEST(ReplicationSimTest, EmptyReplayReportsZeroAndReleasesGuard) {
            DeferredCluster c;
            EXPECT_EQ(runReplay(c.mgr), 0u);

            // Enqueue a hint with a failed quorum write (node2 never acks, so
            // the error completion goes through the async tail).
            std::atomic<int> wdone{0};
            c.mgr.writeAsync(
                "k1",
                "v1",
                std::nullopt,
                {"node2"},
                ConsistencyMode::Quorum,
                [&](Result<void>) { ++wdone; }
            );
            EXPECT_EQ(wdone.load(), 0);
            EXPECT_EQ(c.transport.pendingSends(), 1u);
            c.transport.completeSends(1, false);
            ASSERT_TRUE(awaitDecision(wdone));
            ASSERT_EQ(c.mgr.hintCount(), 1u);

            // The guard was released by the empty replay above, so this
            // replay runs instead of reporting zero.
            // Zero initial value: awaitDecision waits for any completion, and a
            // zero result would mean the empty fast path fired instead of the
            // tail -- either way the assertions below catch a wrong path.
            std::atomic<int> replayed{0};
            c.mgr.replayHints([&](size_t n) { replayed = static_cast<int>(n); });
            c.transport.completeSends(1, true);
            ASSERT_TRUE(awaitDecision(replayed));
            EXPECT_EQ(replayed.load(), 1);
            EXPECT_EQ(c.mgr.hintCount(), 0u);
        }

        TEST(ReplicationSimTest, HintedHandoffReplaysWhenReplicaReturns) {
            TwoNodeCluster c;
            c.bus.setNodeDown("node2");

            // Async write to a down replica → local ok, hint queued, replica stale.
            ASSERT_TRUE(runWrite(c.mgr1, "k", "v", std::nullopt, {"node2"}, ConsistencyMode::Async)
                            .has_value());
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
            ASSERT_TRUE(runWrite(c.mgr1, "k", "v", std::nullopt, {"node2"}, ConsistencyMode::Async)
                            .has_value());
            ASSERT_EQ(c.mgr1.hintCount(), 1);

            // Hint TTL is 30s — advance past it; replay drops the expired hint.
            c.clock.advance(seconds(31));
            EXPECT_EQ(runReplay(c.mgr1), 0);
            EXPECT_EQ(c.mgr1.hintCount(), 0);
        }

        TEST(ReplicationSimTest, HintRetriesOnFailedReplay) {
            TwoNodeCluster c;
            c.bus.setNodeDown("node2");
            ASSERT_TRUE(runWrite(c.mgr1, "k", "v", std::nullopt, {"node2"}, ConsistencyMode::Async)
                            .has_value());
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
            ASSERT_TRUE(runWrite(c.mgr1, "k", "v1", std::nullopt, {"node2"}, ConsistencyMode::Async)
                            .has_value());
            c.clock.advance(5ms);
            c.bus.deliver();
            ASSERT_TRUE(c.store2.get("k").has_value());
            Version observed = c.store2.getVersioned("k")->version();

            // The store's counter must have advanced past the observed version
            // (Lamport bump), so a local write on node2 outranks the primary's.
            EXPECT_GT(c.store2.mintVersion(), observed);

            // Simulate node2 now becoming primary and writing the same key: its
            // store-minted version must win LWW against node1's earlier write.
            Version fresh = c.store2.mintVersion();
            VersionedEntry entry;
            entry.value = "v2";
            entry.setVersion(fresh);
            entry.writer_node_hash = 0x42;
            ASSERT_TRUE(c.store2.putVersioned("k", std::move(entry)).has_value());
            EXPECT_EQ(c.store2.get("k").value(), "v2");
            EXPECT_GT(c.store2.getVersioned("k")->version(), observed);
        }

    } // namespace
} // namespace cinder
