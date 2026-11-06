// Direct unit tests for ReplicationManager behavior and quorum traces.
#include <algorithm>
#include <atomic>
#include <chrono>
#include <fcntl.h>
#include <filesystem>
#include <future>
#include <gtest/gtest.h>
#include <random>
#include <string>
#include <thread>
#include <unistd.h>
#include <utility>
#include <vector>

#include "cinder/cluster/membership.hpp"
#include "cinder/cluster/transport.hpp"
#include "cinder/common/metrics.hpp"
#include "cinder/common/tracing.hpp"
#include "cinder/node/hint_journal.hpp"
#include "cinder/node/hint_queue.hpp"
#include "cinder/node/replication_manager.hpp"
#include "cinder/store/lru_store.hpp"
#include "test_scratch.hpp"
#include "unit/trace_test_helpers.hpp"

namespace cinder {
    namespace {

        // ReplicationManager hint budget
        // -----------------------------------------------------------------

        class FailingTransport : public Transport {
        public:

            void sendAsync(
                const NodeId& /*to*/,
                const net::Request& /*req*/,
                SendCallback on_done
            ) override {
                on_done(err<void>(Error(Errc::NotReady, "peer down")));
            }

            void sendRequestAsync(
                const NodeId& /*to*/,
                const net::Request& /*req*/,
                RequestCallback on_done
            ) override {
                on_done(err<net::Response>(Error(Errc::NotReady, "peer down")));
            }

            void onMessage(MessageHandler /*handler*/) override {}
        };

        // Toggleable variant of FailingTransport: fail closed while `fail`
        // is set (to build a backlog), then ack everything inline (to
        // replay it).
        class FailThenAckTransport : public Transport {
        public:

            std::atomic<bool> fail{true};

            void sendAsync(
                const NodeId& /*to*/,
                const net::Request& /*req*/,
                SendCallback on_done
            ) override {
                if (fail.load(std::memory_order_relaxed)) {
                    on_done(err<void>(Error(Errc::NotReady, "peer down")));
                } else {
                    on_done(ok());
                }
            }

            void sendRequestAsync(
                const NodeId& /*to*/,
                const net::Request& /*req*/,
                RequestCallback on_done
            ) override {
                on_done(err<net::Response>(Error(Errc::NotReady, "peer down")));
            }

            void onMessage(MessageHandler /*handler*/) override {}
        };

        // -----------------------------------------------------------------
        // HintJournal durability
        // -----------------------------------------------------------------

        TEST(ReplicationHintBoundTest, ByteBudgetBoundsQueueAndCountsDrops) {
            RealClock clock;
            LruStore store(1U << 20U);
            FailingTransport transport;
            MetricsCollector metrics;
            ReplicationManager repl(store, "n1", clock, transport);
            repl.setMetrics(&metrics);
            // ~4KB budget; each hint carries a 1KB value.
            repl.setMaxHintBytes(4096);

            const std::string big(1024, 'v');
            for (int i = 0; i < 20; ++i) {
                bool done = false;
                repl.writeAsync(
                    "k" + std::to_string(i),
                    big,
                    std::nullopt,
                    {"n2"},
                    ConsistencyMode::Async,
                    [&](Result<void> r) {
                    EXPECT_TRUE(r.has_value());
                    done = true;
                }
                );
                EXPECT_TRUE(done);
            }

            // Async writes succeed locally even as hints drop.
            EXPECT_LT(repl.hintCount(), 20U);
            EXPECT_GT(repl.hintsDropped(), 0U);
            EXPECT_EQ(
                metrics.replicationMetrics().hints_enqueued.load(),
                static_cast<uint64_t>(20)
            );
            EXPECT_EQ(metrics.replicationMetrics().hints_dropped.load(), repl.hintsDropped());
        }

        TEST(ReplicationHintBoundTest, ReplayReplaysAtMostOneBatchPerCall) {
            RealClock clock;
            LruStore store(1U << 20U);
            FailThenAckTransport transport;
            ReplicationManager repl(store, "n1", clock, transport);

            const size_t total = ReplicationManager::K_REPLAY_BATCH + 44;
            transport.fail.store(true, std::memory_order_relaxed);
            for (size_t i = 0; i < total; ++i) {
                bool done = false;
                repl.writeAsync(
                    "k" + std::to_string(i),
                    "v",
                    std::nullopt,
                    {"n2"},
                    ConsistencyMode::Async,
                    [&](Result<void> r) {
                    EXPECT_TRUE(r.has_value());
                    done = true;
                }
                );
                EXPECT_TRUE(done);
            }
            ASSERT_EQ(repl.hintCount(), total);

            // One call replays one batch; the remainder stays queued for
            // the next tick instead of fanning out hundreds of concurrent
            // sends at once.
            transport.fail.store(false, std::memory_order_relaxed);
            size_t replayed = 0;
            repl.replayHints([&](size_t n) { replayed = n; });
            EXPECT_EQ(replayed, ReplicationManager::K_REPLAY_BATCH);
            EXPECT_EQ(repl.hintCount(), total - ReplicationManager::K_REPLAY_BATCH);

            repl.replayHints([&](size_t n) { replayed = n; });
            EXPECT_EQ(replayed, total - ReplicationManager::K_REPLAY_BATCH);
            EXPECT_EQ(repl.hintCount(), 0U);
        }

        // Transport that records Replicate/Delete fan-out requests and acks ok
        // inline, so delete tests can assert the tombstone bit and LWW
        // identity carried on the wire.
        class CapturingTransport : public Transport {
        public:

            void sendAsync(
                const NodeId& /*to*/,
                const net::Request& req,
                SendCallback on_done
            ) override {
                std::scoped_lock lock(mutex_);
                last_ = req;
                on_done(ok());
            }

            void sendRequestAsync(
                const NodeId& /*to*/,
                const net::Request& /*req*/,
                RequestCallback on_done
            ) override {
                on_done(err<net::Response>(Error(Errc::NotReady, "unused")));
            }

            void onMessage(MessageHandler /*handler*/) override {}

            auto last() const -> net::Request {
                std::scoped_lock lock(mutex_);
                return last_;
            }

        private:

            mutable std::mutex mutex_;
            net::Request last_;
        };

        TEST(ReplicationDeleteTest, DeleteAsyncFansOutTombstone) {
            RealClock clock;
            LruStore store(1U << 20U);
            CapturingTransport transport;
            MetricsCollector metrics;
            ReplicationManager repl(store, "n1", clock, transport);
            repl.setMetrics(&metrics);

            VersionedEntry live;
            live.value = "v";
            live.setVersion(3);
            live.writer_node_hash = 7;
            ASSERT_TRUE(store.putVersioned("k", std::move(live)).has_value());

            bool done = false;
            repl.deleteAsync("k", {"n2"}, ConsistencyMode::Async, [&](Result<void> r) {
                EXPECT_TRUE(r.has_value());
                done = true;
            });
            EXPECT_TRUE(done);

            // Local delete is a versioned tombstone, hidden from live reads.
            EXPECT_FALSE(store.get("k").has_value());
            auto tomb = store.getVersioned("k");
            ASSERT_TRUE(tomb.has_value());
            EXPECT_TRUE(tomb->isTombstone());
            EXPECT_GT(tomb->version(), 3U);

            // Fan-out carries the marker with the same LWW identity.
            auto sent = transport.last();
            EXPECT_EQ(sent.opcode, net::Opcode::Replicate);
            EXPECT_EQ(sent.key, "k");
            EXPECT_TRUE(sent.tombstone);
            EXPECT_EQ(sent.version, tomb->version());
            EXPECT_EQ(sent.writer_node_hash, tomb->writer_node_hash);
            EXPECT_TRUE(sent.expires_at.has_value()); // converged GC deadline
            EXPECT_EQ(metrics.replicationMetrics().async_deletes.load(), 1U);
        }

        TEST(ReplicationDeleteTest, DeleteAsyncQuorumFailClosedHints) {
            RealClock clock;
            LruStore store(1U << 20U);
            FailingTransport transport;
            MetricsCollector metrics;
            ReplicationManager repl(store, "n1", clock, transport);
            repl.setMetrics(&metrics);
            repl.setMaxHintBytes(1U << 20U);

            VersionedEntry live;
            live.value = "v";
            live.setVersion(3);
            live.writer_node_hash = 7;
            ASSERT_TRUE(store.putVersioned("k", std::move(live)).has_value());
            bool done = false;
            repl.deleteAsync("k", {"n2"}, ConsistencyMode::Quorum, [&](Result<void> r) {
                EXPECT_FALSE(r.has_value());
                EXPECT_EQ(r.error().code(), Errc::NotReady);
                done = true;
            });
            EXPECT_TRUE(done);

            // Local delete still applied; the failed replica got a hint
            // carrying the tombstone so it heals on return.
            EXPECT_FALSE(store.get("k").has_value());
            EXPECT_TRUE(store.getVersioned("k")->isTombstone());
            EXPECT_EQ(repl.hintCount(), 1U);
            EXPECT_EQ(metrics.replicationMetrics().quorum_deletes_failed.load(), 1U);
        }

        TEST(ReplicationDeleteTest, DeleteAsyncQuorumOkCountsAcks) {
            RealClock clock;
            LruStore store(1U << 20U);
            CapturingTransport transport;
            MetricsCollector metrics;
            ReplicationManager repl(store, "n1", clock, transport);
            repl.setMetrics(&metrics);

            VersionedEntry live2;
            live2.value = "v";
            live2.setVersion(3);
            live2.writer_node_hash = 7;
            ASSERT_TRUE(store.putVersioned("k", std::move(live2)).has_value());
            bool done = false;
            repl.deleteAsync("k", {"n2"}, ConsistencyMode::Quorum, [&](Result<void> r) {
                EXPECT_TRUE(r.has_value());
                done = true;
            });
            EXPECT_TRUE(done);
            EXPECT_EQ(metrics.replicationMetrics().quorum_deletes_ok.load(), 1U);
            EXPECT_TRUE(store.getVersioned("k")->isTombstone());
        }

        // Transport that answers every GetVersioned with a tombstone vote,
        // exercising the quorum-read delete path without a live peer.
        class TombstoneVotingTransport : public Transport {
        public:

            void sendAsync(
                const NodeId& /*to*/,
                const net::Request& /*req*/,
                SendCallback on_done
            ) override {
                on_done(ok());
            }

            void sendRequestAsync(
                const NodeId& /*to*/,
                const net::Request& /*req*/,
                RequestCallback on_done
            ) override {
                net::Response vote;
                vote.status = Errc::OK;
                vote.value = std::nullopt;
                vote.version = 5;
                vote.writer_node_hash = 7;
                vote.tombstone = true;
                on_done(ok(std::move(vote)));
            }

            void onMessage(MessageHandler /*handler*/) override {}
        };

        TEST(ReplicationDeleteTest, QuorumReadTombstoneVoteDeletesLocally) {
            RealClock clock;
            LruStore store(1U << 20U);
            TombstoneVotingTransport transport;
            ReplicationManager repl(store, "n1", clock, transport);

            VersionedEntry live;
            live.value = "v";
            live.setVersion(3);
            live.writer_node_hash = 7;
            ASSERT_TRUE(store.putVersioned("k", std::move(live)).has_value());

            std::promise<Result<VersionedEntry>> done;
            auto done_future = done.get_future();
            repl.readAsync("k", {"n2"}, 2, [&done](Result<VersionedEntry> r) {
                done.set_value(std::move(r));
            });

            auto result = done_future.get();
            ASSERT_TRUE(result.has_value());
            EXPECT_TRUE(result->isTombstone());
            EXPECT_EQ(result->version(), 5U);
            // Self-heal applied the tombstone locally (read repair).
            EXPECT_FALSE(store.get("k").has_value());
            EXPECT_TRUE(store.getVersioned("k")->isTombstone());
        }

        TEST(ReplicationDeleteTest, QuorumReadTombstoneTieBreaksDeterministically) {
            // Equal (version, writer), contradictory delete state: reachable
            // across standalone writers sharing writer 0. The read must pick
            // the tombstone and repair the live replica — never deadlock or
            // flap between the two.
            RealClock clock;
            LruStore store(1U << 20U);
            TombstoneVotingTransport transport;
            ReplicationManager repl(store, "n1", clock, transport);

            VersionedEntry live;
            live.value = "v";
            live.setVersion(5);
            live.writer_node_hash = 7;
            ASSERT_TRUE(store.putVersioned("k", std::move(live)).has_value());

            std::promise<Result<VersionedEntry>> done;
            auto done_future = done.get_future();
            repl.readAsync("k", {"n2"}, 2, [&done](Result<VersionedEntry> r) {
                done.set_value(std::move(r));
            });

            auto result = done_future.get();
            ASSERT_TRUE(result.has_value());
            EXPECT_TRUE(result->isTombstone()) << "tombstone must win exact-stamp ties";
            EXPECT_EQ(result->version(), 5U);
            // Loser repaired: local live copy converged to the tombstone.
            EXPECT_TRUE(store.getVersioned("k")->isTombstone());
        }

        TEST(ReadReducerTest, TombstoneTieBreaksDeterministically) {
            // Direct reducer test: equal (version, writer), one live local,
            // one tombstone vote. The tombstone must win and flag repair.
            RealClock clock;
            VersionedEntry live;
            live.value = "v";
            live.setVersion(5);
            live.writer_node_hash = 7;
            ReadReducer reducer(2, 1, std::move(live), clock);

            net::Response vote;
            vote.status = Errc::OK;
            vote.value = std::nullopt;
            vote.version = 5;
            vote.writer_node_hash = 7;
            vote.tombstone = true;
            reducer.noteRemote(std::move(vote), true);

            EXPECT_TRUE(reducer.shouldFire());
            EXPECT_TRUE(reducer.needsRepair());
            auto best = reducer.takeBest();
            ASSERT_TRUE(best.has_value());
            EXPECT_TRUE(best->isTombstone());
            EXPECT_EQ(best->version(), 5U);
        }

        TEST(ReadReducerTest, StaleVoteDoesNotWinButFlagsRepair) {
            RealClock clock;
            VersionedEntry live;
            live.value = "v";
            live.setVersion(5);
            live.writer_node_hash = 7;
            // R=2 with two outstanding remotes: one stale vote leaves the
            // reducer pending without a winner change.
            ReadReducer reducer(2, 2, std::move(live), clock);

            net::Response stale;
            stale.status = Errc::OK;
            stale.value = "old";
            stale.version = 3;
            stale.writer_node_hash = 7;
            reducer.noteRemote(std::move(stale), true);

            // Quorum met via local + stale vote (any response acks), but the
            // winner is unchanged and the divergent replica is flagged.
            EXPECT_TRUE(reducer.shouldFire());
            EXPECT_TRUE(reducer.needsRepair());
            auto best = reducer.takeBest();
            ASSERT_TRUE(best.has_value());
            EXPECT_FALSE(best->isTombstone());
            EXPECT_EQ(best->version(), 5U);
        }

        TEST(ReadReducerTest, MissingVoteCountsNoAckButNeedsRepair) {
            RealClock clock;
            ReadReducer reducer(2, 1, std::nullopt, clock);

            net::Response miss;
            miss.status = Errc::NotFound;
            reducer.noteRemote(std::move(miss), true);

            EXPECT_EQ(reducer.acks(), 0U);
            EXPECT_TRUE(reducer.needsRepair());
            EXPECT_FALSE(reducer.takeBest().has_value());
        }

        TEST(ReadFlightRegistryTest, DifferentRGetsSeparateFlights) {
            // Singleflight keys include the read quorum: different R values
            // must not share a leader's outcome.
            using Cb = std::function<void(int)>;
            ReadFlightRegistry<Cb, int> registry;
            bool first_called = false;
            bool second_called = false;
            auto [flight1, leader1] = registry.attach("k", 1, [&](int) { first_called = true; });
            auto [flight2, leader2] = registry.attach("k", 2, [&](int) { second_called = true; });
            EXPECT_TRUE(leader1);
            EXPECT_TRUE(leader2);
            EXPECT_NE(flight1, flight2);

            registry.finish("k", 1, flight1, 1);
            EXPECT_TRUE(first_called);
            EXPECT_FALSE(second_called);
            registry.finish("k", 2, flight2, 2);
            EXPECT_TRUE(second_called);
        }

        TEST(ReadFlightRegistryTest, SameKeySameRCoalesces) {
            using Cb = std::function<void(int)>;
            ReadFlightRegistry<Cb, int> registry;
            int calls = 0;
            auto [flight1, leader1] = registry.attach("k", 2, [&](int) { ++calls; });
            auto [flight2, leader2] = registry.attach("k", 2, [&](int) { ++calls; });
            EXPECT_TRUE(leader1);
            EXPECT_FALSE(leader2);
            EXPECT_EQ(flight1, flight2);

            // Finishing the wrong flight is a no-op for the table but still
            // notifies the passed flight's waiters (defensive path).
            registry.finish("k", 2, flight1, 7);
            EXPECT_EQ(calls, 2);
            // Entry erased: a later read begins a fresh flight.
            auto [flight3, leader3] = registry.attach("k", 2, [&](int) { ++calls; });
            EXPECT_TRUE(leader3);
            EXPECT_NE(flight3, flight1);
        }

        // Async quorum trace propagation (Phase 4)
        // -----------------------------------------------------------------

        // Transport that captures send callbacks instead of completing inline,
        // forcing fanOutQuorum's async tail (system-executor thread with
        // unrelated TLS) to deliver every completion. The test releases the
        // captured callbacks after the initiating call returns.
        class DeferredTransport : public Transport {
        public:

            void sendAsync(
                const NodeId& /*to*/,
                const net::Request& /*req*/,
                SendCallback on_done
            ) override {
                std::scoped_lock lock(mutex_);
                sends_.push_back(std::move(on_done));
            }

            void sendRequestAsync(
                const NodeId& /*to*/,
                const net::Request& /*req*/,
                RequestCallback on_done
            ) override {
                std::scoped_lock lock(mutex_);
                request_sends_.push_back(std::move(on_done));
            }

            void onMessage(MessageHandler /*handler*/) override {}

            void releaseSends(Result<void> result) {
                std::vector<SendCallback> pending;
                {
                    std::scoped_lock lock(mutex_);
                    pending = std::move(sends_);
                    sends_.clear();
                }
                for (auto& cb : pending) {
                    // Fresh Results per waiter: move_only callbacks each fire once.
                    cb(result.has_value() ? ok() : err<void>(result.error()));
                }
            }

            void releaseRequests(Result<net::Response> result) {
                std::vector<RequestCallback> pending;
                {
                    std::scoped_lock lock(mutex_);
                    pending = std::move(request_sends_);
                    request_sends_.clear();
                }
                for (auto& cb : pending) {
                    if (result.has_value()) {
                        cb(ok(std::move(result.value())));
                    } else {
                        cb(err<net::Response>(result.error()));
                    }
                }
            }

        private:

            std::mutex mutex_;
            std::vector<SendCallback> sends_;
            std::vector<RequestCallback> request_sends_;
        };

        TEST(ReplicationQuorumTraceTest, AsyncTailKeepsRequestTrace) {
            RealClock clock;
            LruStore store(1U << 20U);
            DeferredTransport transport;
            ReplicationManager repl(store, "n1", clock, transport);
            auto sub = std::make_shared<RecordingSubscriber>();

            constexpr uint64_t K_TRACE = 0x0BAD1;
            constexpr uint64_t K_PARENT = 0x0BAD2;
            std::promise<Result<void>> done;
            auto done_future = done.get_future();
            {
                TraceGuard guard(sub);
                repl.writeAsync(
                    "k",
                    "v",
                    std::nullopt,
                    {"n2"},
                    ConsistencyMode::Quorum,
                    [&done](Result<void> r) { done.set_value(std::move(r)); },
                    K_TRACE,
                    K_PARENT
                );
                // writeAsync returned with the replica outstanding: the inline
                // drain gave up, so releasing now must go through the tail.
                // The future is awaited inside the guard: the tail runs on a
                // system thread after this scope would otherwise end, and the
                // spans it creates need the subscriber still installed.
                transport.releaseSends(ok());
                ASSERT_TRUE(done_future.get().has_value());
            }

            auto spans = sub->spans();
            const auto* fanout = findSpan(spans, "replication.write.fanout");
            const auto* replica = findSpan(spans, "replication.write.replica");
            const auto* quorum = findSpan(spans, "replication.write.quorum.ok");
            ASSERT_NE(fanout, nullptr);
            ASSERT_NE(replica, nullptr);
            ASSERT_NE(quorum, nullptr);
            EXPECT_EQ(fanout->trace_id, K_TRACE);
            EXPECT_EQ(replica->trace_id, K_TRACE);
            EXPECT_EQ(replica->parent_span_id, fanout->span_id);
            EXPECT_EQ(replica->status, SpanStatus::Ok);
            EXPECT_TRUE(replica->closed);
            EXPECT_EQ(quorum->trace_id, K_TRACE);
            EXPECT_EQ(quorum->status, SpanStatus::Ok);
            // Decision evidence: acks/needed on the quorum span, replica id
            // on the per-replica span, shape on the fanout.
            const auto* q_acks = findAttr(*quorum, "acks");
            ASSERT_NE(q_acks, nullptr);
            EXPECT_EQ(q_acks->uint_val, 2U);
            const auto* q_needed = findAttr(*quorum, "needed");
            ASSERT_NE(q_needed, nullptr);
            EXPECT_EQ(q_needed->uint_val, 2U);
            const auto* r_replica = findAttr(*replica, "replica");
            ASSERT_NE(r_replica, nullptr);
            EXPECT_EQ(r_replica->str_val, "n2");
            const auto* f_replicas = findAttr(*fanout, "replicas");
            ASSERT_NE(f_replicas, nullptr);
            EXPECT_EQ(f_replicas->uint_val, 1U);
        }

        TEST(ReplicationQuorumTraceTest, FailedReplicaHintEnqueueKeepsTrace) {
            RealClock clock;
            LruStore store(1U << 20U);
            DeferredTransport transport;
            ReplicationManager repl(store, "n1", clock, transport);
            auto sub = std::make_shared<RecordingSubscriber>();

            constexpr uint64_t K_TRACE = 0x0BAD3;
            constexpr uint64_t K_PARENT = 0x0BAD4;
            std::promise<Result<void>> done;
            auto done_future = done.get_future();
            {
                TraceGuard guard(sub);
                repl.writeAsync(
                    "k",
                    "v",
                    std::nullopt,
                    {"n2"},
                    ConsistencyMode::Quorum,
                    [&done](Result<void> r) { done.set_value(std::move(r)); },
                    K_TRACE,
                    K_PARENT
                );
                transport.releaseSends(err<void>(Error(Errc::NotReady, "peer down")));
                // Awaited under the guard for the same reason as above: the
                // tail observes the request trace only while installed.
                EXPECT_FALSE(done_future.get().has_value());
            }
            EXPECT_EQ(repl.hintCount(), 1U);

            // The hint-enqueue event fired on the tail thread must still carry
            // the request trace — not the system pool's ambient TLS.
            bool hint_logged = false;
            for (const auto& log : sub->logEvents()) {
                if (log.message == "hint enqueued" && log.trace_id == K_TRACE) {
                    hint_logged = true;
                }
            }
            EXPECT_TRUE(hint_logged) << "hint enqueue lost the request trace";

            auto spans = sub->spans();
            const auto* replica = findSpan(spans, "replication.write.replica");
            const auto* quorum = findSpan(spans, "replication.write.quorum.fail");
            ASSERT_NE(replica, nullptr);
            ASSERT_NE(quorum, nullptr);
            EXPECT_EQ(replica->trace_id, K_TRACE);
            EXPECT_EQ(replica->status, SpanStatus::Error);
            EXPECT_EQ(quorum->trace_id, K_TRACE);
            EXPECT_EQ(quorum->status, SpanStatus::Error);
            // The failed decision still records how far the quorum got.
            const auto* q_acks = findAttr(*quorum, "acks");
            ASSERT_NE(q_acks, nullptr);
            EXPECT_EQ(q_acks->uint_val, 1U);
            const auto* r_replica = findAttr(*replica, "replica");
            ASSERT_NE(r_replica, nullptr);
            EXPECT_EQ(r_replica->str_val, "n2");
        }

        TEST(ReplicationQuorumTraceTest, ReadMissDecisionIsServedNotFailed) {
            RealClock clock;
            LruStore store(1U << 20U);
            DeferredTransport transport;
            ReplicationManager repl(store, "n1", clock, transport);
            auto sub = std::make_shared<RecordingSubscriber>();

            constexpr uint64_t K_TRACE = 0x0BAD5;
            constexpr uint64_t K_PARENT = 0x0BAD6;
            std::promise<Result<VersionedEntry>> done;
            auto done_future = done.get_future();
            {
                TraceGuard guard(sub);
                repl.readAsync("missing", {"n2"}, 2, [&done](Result<VersionedEntry> r) {
                    done.set_value(std::move(r));
                }, K_TRACE, K_PARENT);
                net::Response miss{.status = Errc::NotFound, .value = std::nullopt};
                transport.releaseRequests(ok(std::move(miss)));
                // Served miss: quorum answered, key simply absent everywhere.
                // Awaited under the guard so the tail's decision span lands.
                EXPECT_FALSE(done_future.get().has_value());
            }

            auto spans = sub->spans();
            const auto* quorum = findSpan(spans, "replication.read.quorum");
            ASSERT_NE(quorum, nullptr);
            EXPECT_EQ(quorum->trace_id, K_TRACE);
            EXPECT_EQ(quorum->status, SpanStatus::Ok);
        }

        // -----------------------------------------------------------------

    } // namespace
} // namespace cinder
