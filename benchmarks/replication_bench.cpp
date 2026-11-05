// Replication fan-out benchmarks (Phase 6): per-op cost of the extracted
// coordinator path — write/read quorum reduce across replica counts, and
// hint-replay batches at realistic backlog sizes. All transports complete
// synchronously inline, so this measures coordinator overhead (local
// commit, channel fan-out, LWW reduce, spans), not network I/O.

#include <atomic>
#include <benchmark/benchmark.h>
#include <cstddef>
#include <cstdint>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "cinder/cluster/clock.hpp"
#include "cinder/cluster/transport.hpp"
#include "cinder/common/status.hpp"
#include "cinder/common/tracing.hpp"
#include "cinder/node/replication_manager.hpp"
#include "cinder/store/lru_store.hpp"

namespace cinder {
    namespace {

        // Silence per-hint Warn/Info spam: at 1024-hint rebuilds the logging
        // dominates wall clock without touching timed results.
        void silenceBenchLogging() {
            static const bool once = [] {
                setLogLevel(LogLevel::Error);
                return true;
            }();
            (void)once;
        }

        // Acks every send inline; answers GetVersioned with a canned vote.
        class SyncAckTransport : public Transport {
        public:

            // When true, GetVersioned votes carry a newer version than local,
            // exercising the read-repair path. Otherwise NotFound (miss path).
            bool vote_newer = false;

            void sendAsync(
                const NodeId& /*to*/,
                const net::Request& /*req*/,
                SendCallback on_done
            ) override {
                on_done(ok());
            }

            void sendRequestAsync(
                const NodeId& /*to*/,
                const net::Request& req,
                RequestCallback on_done
            ) override {
                net::Response res;
                if (vote_newer) {
                    res.status = Errc::OK;
                    res.value = "newer";
                    res.version = 10;
                    res.writer_node_hash = 9;
                } else {
                    res.status = Errc::NotFound;
                }
                res.trace_id = req.trace_id;
                res.span_id = req.span_id;
                on_done(std::move(res));
            }

            void onMessage(MessageHandler /*handler*/) override {}
        };

        auto makeReplicas(size_t n) -> std::vector<NodeId> {
            std::vector<NodeId> replicas;
            replicas.reserve(n);
            for (size_t i = 0; i < n; ++i) {
                replicas.push_back("n" + std::to_string(i + 2));
            }
            return replicas;
        }

        struct WriteFixture {
            RealClock clock;
            LruStore store{1U << 28U};
            SyncAckTransport transport;
            ReplicationManager repl{store, "n1", clock, transport};
        };

        // Quorum write, fully synchronous transport: local commit + N sends +
        // channel reduce, all inline. Keys are unique per iteration so no
        // coalescing or LWW contention pollutes the timing.
        void bmWriteQuorumSync(benchmark::State& state) {
            silenceBenchLogging();
            WriteFixture fx;
            const auto replicas = makeReplicas(static_cast<size_t>(state.range(0)));
            uint64_t seq = 0;
            // NOLINTNEXTLINE
            for (auto _ : state) {
                const std::string key = "k" + std::to_string(seq++);
                bool done = false;
                fx.repl.writeAsync(
                    key,
                    "v",
                    std::nullopt,
                    replicas,
                    ConsistencyMode::Quorum,
                    [&](Result<void> r) {
                    benchmark::DoNotOptimize(r.has_value());
                    done = true;
                }
                );
                benchmark::DoNotOptimize(done);
            }
            state.SetItemsProcessed(state.iterations());
        }

        // NOLINTNEXTLINE
        BENCHMARK(bmWriteQuorumSync)->DenseRange(1, 5)->Unit(benchmark::kMicrosecond);

        void bmReadQuorumSync(benchmark::State& state, bool vote_newer) {
            silenceBenchLogging();
            WriteFixture fx;
            fx.transport.vote_newer = vote_newer;
            const auto replicas = makeReplicas(static_cast<size_t>(state.range(0)));
            // R = majority of (replicas + local).
            const size_t r = (replicas.size() + 1) / 2 + 1;
            if (!vote_newer) {
                // Miss path: pre-populate nothing; every vote is NotFound.
                uint64_t seq = 0;
                // NOLINTNEXTLINE
                for (auto _ : state) {
                    const std::string key = "m" + std::to_string(seq++);
                    bool done = false;
                    fx.repl.readAsync(key, replicas, r, [&](Result<VersionedEntry> res) {
                        benchmark::DoNotOptimize(res.has_value());
                        done = true;
                    });
                    benchmark::DoNotOptimize(done);
                }
            } else {
                // Repair path: local v1 present, replicas vote v10 newer.
                uint64_t seq = 0;
                // NOLINTNEXTLINE
                for (auto _ : state) {
                    const std::string key = "h" + std::to_string(seq++);
                    VersionedEntry local;
                    local.value = "old";
                    local.setVersion(1);
                    local.writer_node_hash = 7;
                    std::ignore = fx.store.putVersioned(key, std::move(local));
                    bool done = false;
                    fx.repl.readAsync(key, replicas, r, [&](Result<VersionedEntry> res) {
                        benchmark::DoNotOptimize(res.has_value());
                        done = true;
                    });
                    benchmark::DoNotOptimize(done);
                }
            }
            state.SetItemsProcessed(state.iterations());
        }

        void bmReadQuorumMiss(benchmark::State& state) {
            bmReadQuorumSync(state, false);
        }

        void bmReadQuorumRepair(benchmark::State& state) {
            bmReadQuorumSync(state, true);
        }

        // NOLINTNEXTLINE
        BENCHMARK(bmReadQuorumMiss)->DenseRange(1, 4)->Unit(benchmark::kMicrosecond);
        // NOLINTNEXTLINE
        BENCHMARK(bmReadQuorumRepair)->DenseRange(1, 4)->Unit(benchmark::kMicrosecond);

        // Hint replay at realistic backlog sizes. Backlog is built once
        // (paused timing) by failing every send; each timed iteration replays
        // one bounded batch against an acking transport, then re-queues it.
        // Arg: backlog size. Batches of 64/256 drain in one call; 1024 takes
        // four K_REPLAY_BATCH calls — the loop below reports per-call cost.
        void bmReplayBatch(benchmark::State& state) {
            silenceBenchLogging();
            const auto backlog = static_cast<size_t>(state.range(0));
            RealClock clock;
            LruStore store{1U << 28U};

            struct FailThenAck : public Transport {
                std::atomic<bool> fail{true};

                void sendAsync(
                    const NodeId& /*to*/,
                    const net::Request& /*req*/,
                    SendCallback on_done
                ) override {
                    if (fail.load(std::memory_order_relaxed)) {
                        on_done(err<void>(Error(Errc::NotReady, "down")));
                    } else {
                        on_done(ok());
                    }
                }

                void sendRequestAsync(
                    const NodeId& /*to*/,
                    const net::Request& /*req*/,
                    RequestCallback on_done
                ) override {
                    on_done(err<net::Response>(Error(Errc::NotReady, "down")));
                }

                void onMessage(MessageHandler /*handler*/) override {}
            };

            FailThenAck transport;
            ReplicationManager repl(store, "n1", clock, transport);
            for (size_t i = 0; i < backlog; ++i) {
                bool done = false;
                repl.writeAsync(
                    "k" + std::to_string(i),
                    "v",
                    std::nullopt,
                    {"n2"},
                    ConsistencyMode::Async,
                    [&](Result<void> /*r*/) { done = true; }
                );
                benchmark::DoNotOptimize(done);
            }

            transport.fail.store(false, std::memory_order_relaxed);
            // NOLINTNEXTLINE
            for (auto _ : state) {
                state.PauseTiming();
                // Rebuild the exact backlog every iteration (replay consumes).
                transport.fail.store(true, std::memory_order_relaxed);
                for (size_t i = 0; i < backlog; ++i) {
                    bool done = false;
                    repl.writeAsync(
                        "k" + std::to_string(i),
                        "v",
                        std::nullopt,
                        {"n2"},
                        ConsistencyMode::Async,
                        [&](Result<void> /*r*/) { done = true; }
                    );
                    benchmark::DoNotOptimize(done);
                }
                transport.fail.store(false, std::memory_order_relaxed);
                state.ResumeTiming();

                size_t replayed = 0;
                repl.replayHints([&](size_t n) { replayed = n; });
                benchmark::DoNotOptimize(replayed);
            }
            state.SetItemsProcessed(
                state.iterations()
                * static_cast<int64_t>(std::min(backlog, ReplicationManager::K_REPLAY_BATCH))
            );
        }

        // NOLINTNEXTLINE
        BENCHMARK(bmReplayBatch)->Arg(64)->Arg(256)->Arg(1024)->Unit(benchmark::kMicrosecond);

    } // namespace
} // namespace cinder
