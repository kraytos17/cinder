#include <algorithm>
#include <atomic>
#include <chrono>
#include <fstream>
#include <gtest/gtest.h>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

#include "cinder/client/cache_client.hpp"
#include "cinder/common/config.hpp"
#include "cinder/common/tracing.hpp"
#include "cinder/node/cache_node_server.hpp"
#include "integration/test_helpers.hpp"
#include "test_scratch.hpp"

namespace cinder {
    namespace {

        TEST(ParsePeerTest, ValidPeer) {
            ClusterConfig::NodeConfig out;
            EXPECT_TRUE(parsePeer("node2@127.0.0.1:17911", out));
            EXPECT_EQ(out.id, "node2");
            EXPECT_EQ(out.host, "127.0.0.1");
            EXPECT_EQ(out.port, 17'911);
        }

        TEST(ParsePeerTest, MissingSeparator) {
            ClusterConfig::NodeConfig out;
            EXPECT_FALSE(parsePeer("node2", out));
        }

        TEST(ParsePeerTest, MissingPort) {
            ClusterConfig::NodeConfig out;
            EXPECT_FALSE(parsePeer("node2@127.0.0.1", out));
        }

        TEST(ParsePeerTest, EmptyId) {
            ClusterConfig::NodeConfig out;
            EXPECT_FALSE(parsePeer("@127.0.0.1:17911", out));
        }

        // Gap §4 test: hot-reload racing live traffic under TSan. The server
        // runs multi-threaded (io_threads=4); the test hammers it with TCP
        // traffic while a reload loop rewrites the config file and triggers
        // AdminConfigReload on every round. Every reload mutates fields that
        // pool threads read concurrently:
        //   suspect_timeout_ms / gossip_interval_ms (FD/gossip tick reads),
        //   rpc_timeout_ms (transport send path),
        //   capacity (store setCapacity under traffic),
        //   current_config_ (admin info_getter reads).
        // Run under the tsan preset: halt_on_error turns any race into a
        // failure. A green run proves the reload path is race-free.
        TEST(ConfigReloadConcurrencyTest, ReloadUnderTrafficIsRaceFree) {
            const uint16_t port = net::test::pickHeldPort();
            ASSERT_NE(port, 0);
            // Held but never listened on: connections are refused, which still
            // exercises the failure detector's probe-failure path (and its
            // suspect_timeout_ms reads) without needing a second server.
            const uint16_t dead_peer_port = net::test::pickHeldPort();
            ASSERT_NE(dead_peer_port, 0);

            // Unique per process and per case (see test_scratch.hpp); the
            // port-derived name this replaces was unique too, but the pool
            // keeps every temp path in one place.
            test::ScratchFile cfg_file("cinder_reload_test", ".yaml");
            const std::string cfg_path = cfg_file.string();

            auto write_config = [&](int suspect_ms, int gossip_ms, int rpc_ms, size_t capacity) {
                std::ofstream out(cfg_path, std::ios::trunc);
                out
                    << "server:\n"
                    << "  node_id: \"reload-node\"\n"
                    << "  port: "
                    << port
                    << "\n"
                    << "  capacity: "
                    << capacity
                    << "\n"
                    << "  replication_factor: 1\n"
                    << "  consistency: \"async\"\n"
                    << "  rpc_timeout_ms: "
                    << rpc_ms
                    << "\n"
                    << "cluster:\n"
                    << "  peers:\n"
                    << R"(    - {id: "dead-peer", host: "127.0.0.1", port: )"
                    << dead_peer_port
                    << "}\n"
                    << "failure_detector:\n"
                    << "  ping_interval_ms: 100\n"
                    << "  suspect_timeout_ms: "
                    << suspect_ms
                    << "\n"
                    << "  gossip_interval_ms: "
                    << gossip_ms
                    << "\n";
                out.close();
            };
            // Fast timers (100 ms gossip/probe, 300 ms suspect timeout) so tick
            // reads of the reloaded fields overlap the reload storm many times
            // over instead of needing a multi-second test.
            constexpr int K_SUSPECT_A = 300;
            constexpr int K_SUSPECT_B = 301;
            constexpr int K_GOSSIP_A = 100;
            constexpr int K_GOSSIP_B = 101;
            constexpr int K_RPC_A = 5000;
            constexpr int K_RPC_B = 5001;
            constexpr size_t K_CAP_A = 1U << 20U;
            constexpr size_t K_CAP_B = (1U << 20U) + 1024;
            write_config(K_SUSPECT_A, K_GOSSIP_A, K_RPC_A, K_CAP_A);

            CacheNodeServerOptions opts;
            opts.node_id = "reload-node";
            opts.port = port;
            opts.listen_fd = net::test::takeHeldFd(port);
            ASSERT_GE(opts.listen_fd, 0);
            opts.capacity = K_CAP_A;
            opts.replica_factor = 1;
            opts.mode = ConsistencyMode::Async;
            opts.peers = {{"dead-peer", "127.0.0.1", dead_peer_port}};
            opts.ping_interval = std::chrono::milliseconds(100);
            opts.suspect_timeout = std::chrono::milliseconds(K_SUSPECT_A);
            opts.gossip_interval = std::chrono::milliseconds(K_GOSSIP_A);
            opts.io_threads = 4;
            opts.config_path = cfg_path;
            {
                auto cfg = loadConfig(cfg_path);
                ASSERT_TRUE(cfg.has_value());
                opts.config = *cfg;
            }

            CacheNodeServer server(std::move(opts));
            ASSERT_TRUE(server.start().has_value());
            std::thread runner([&] { server.run(); });

            auto admin = [&](net::Opcode op) {
                net::Request req;
                req.opcode = op;
                return net::test::rawRequest(port, req);
            };

            // Readiness: the listen socket is adopted synchronously, but the
            // io pool needs a moment before it accepts.
            bool ready = false;
            for (int i = 0; i < 100 && !ready; ++i) {
                auto res = admin(net::Opcode::AdminInfo);
                ready = res.has_value() && res->status == Errc::OK;
                if (!ready) {
                    std::this_thread::sleep_for(std::chrono::milliseconds(50));
                }
            }
            ASSERT_TRUE(ready);

            // Mirror of the server ring (same seeds, same 150 vnodes): only
            // keys owned by this node are used, so every op is local and must
            // succeed — any failure is a real regression, not a redirect.
            ConsistentHashRing mirror;
            mirror.addNode("reload-node");
            mirror.addNode("dead-peer");
            std::vector<std::string> owned;
            // Enough keys that traffic spans many 100 ms timer ticks: the
            // reload storm must overlap tick reads, not finish between them.
            for (int i = 0; i < 300'000 && owned.size() < 6000; ++i) {
                auto k = "rk" + std::to_string(i);
                if (mirror.getNode(k) == "reload-node") {
                    owned.push_back(k);
                }
            }
            ASSERT_FALSE(owned.empty());

            std::atomic<bool> failed{false};
            std::atomic<int> active_workers{4};
            auto worker = [&](size_t stride_begin) {
                for (size_t i = stride_begin; i < owned.size() && !failed.load(); i += 4) {
                    const auto& k = owned[i];
                    const auto v = "v" + std::to_string(i);
                    auto s = net::test::setKey(port, k, v);
                    if (!s.has_value() || s->status != Errc::OK) {
                        failed.store(true);
                        break;
                    }
                    auto g = net::test::getKey(port, k);
                    if (!g.has_value()
                        || g->status
                        != Errc::OK
                        || !g->value.has_value()
                        || *g->value
                        != v) {
                        failed.store(true);
                        break;
                    }
                }
                --active_workers;
            };
            std::thread t0([&] { worker(0); });
            std::thread t1([&] { worker(1); });
            std::thread t2([&] { worker(2); });
            std::thread t3([&] { worker(3); });

            // Deterministic cross-thread readers: these test threads drive the
            // same unsynchronised fields the reload mutates, with no common
            // lock between them and the io pool — any overlap is a reportable
            // race. (Relying only on pool timer ticks would be at the mercy of
            // the scheduler: all pool work could land on one thread and the
            // test would pass vacuously.)
            auto handle = server.gatewayHandle();
            std::thread fd_hammer([&] {
                while (active_workers.load() > 0) {
                    handle.failure_detector.tick();
                }
            });
            std::thread gossip_hammer([&] {
                while (active_workers.load() > 0) {
                    (void)handle.gossip.gossipInterval();
                }
            });

            // Reload storm spanning the whole traffic window: adjacent rounds
            // always differ (A/B alternation), so every round applies hot
            // fields on an io thread while pool threads serve traffic and
            // run the 100ms gossip/probe timers.
            bool use_b = false;
            int rounds = 0;
            while (active_workers.load() > 0 && !failed.load()) {
                use_b = !use_b;
                if (use_b) {
                    write_config(K_SUSPECT_B, K_GOSSIP_B, K_RPC_B, K_CAP_B);
                } else {
                    write_config(K_SUSPECT_A, K_GOSSIP_A, K_RPC_A, K_CAP_A);
                }
                auto r = admin(net::Opcode::AdminConfigReload);
                ASSERT_TRUE(r.has_value());
                EXPECT_EQ(r->status, Errc::OK);
                // Concurrent reader of current_config_ (admin info_getter).
                auto info = admin(net::Opcode::AdminInfo);
                ASSERT_TRUE(info.has_value());
                ++rounds;
            }
            // The storm must have overlapped traffic and timer ticks many
            // times over; a tiny count would mean the race window never opened.
            EXPECT_GE(rounds, 50);
            t0.join();
            t1.join();
            t2.join();
            t3.join();
            fd_hammer.join();
            gossip_hammer.join();
            EXPECT_FALSE(failed.load());

            // Deterministic tail: force set B and prove it applied end to end —
            // capacity_bytes in AdminInfo reflects the last reload (applyHotField
            // keeps the member in sync for syncEffectiveConfig), and the server
            // still serves reads/writes afterwards. The key is mirror-owned, so
            // it can never redirect.
            write_config(K_SUSPECT_B, K_GOSSIP_B, K_RPC_B, K_CAP_B);
            {
                auto r = admin(net::Opcode::AdminConfigReload);
                ASSERT_TRUE(r.has_value());
                EXPECT_EQ(r->status, Errc::OK);
            }
            {
                auto info = admin(net::Opcode::AdminInfo);
                ASSERT_TRUE(info.has_value());
                ASSERT_TRUE(info->value.has_value());
                EXPECT_NE(info->value->find("1049600"), std::string::npos);
            }
            {
                auto s = net::test::setKey(port, owned[0], "final");
                ASSERT_TRUE(s.has_value());
                EXPECT_EQ(s->status, Errc::OK);
                auto g = net::test::getKey(port, owned[0]);
                ASSERT_TRUE(g.has_value());
                ASSERT_TRUE(g->value.has_value());
                EXPECT_EQ(*g->value, "final");
            }

            server.shutdown();
            runner.join();
        }
    } // namespace

    // End-to-end trace propagation: a TCP request carrying (trace_id, span_id)
    // must be served under that trace, with the request span recorded as a
    // child of the caller span — not as a fresh unrelated trace.
    namespace {
        struct TraceProbe : public Subscriber {
            std::mutex mutex;

            struct SpanRecord {
                std::string operation;
                uint64_t trace_id = 0;
                uint64_t parent_span_id = 0;
            };

            std::vector<SpanRecord> spans;
            std::vector<uint64_t> event_traces;

            auto registerCallsite(const Metadata& /*meta*/) -> Interest override {
                return Interest::Always;
            }

            auto newSpan(
                const Metadata& meta,
                uint64_t parent_trace_id,
                uint64_t parent_span_id
            ) noexcept -> uint64_t override {
                std::scoped_lock lock(mutex);
                spans.push_back({std::string(meta.target), parent_trace_id, parent_span_id});
                return spans.size();
            }

            void enter(uint64_t /*span_id*/) noexcept override {}

            void exit(uint64_t /*span_id*/) noexcept override {}

            void tryClose(uint64_t /*span_id*/) noexcept override {}

            void event(const EventRecord& record) noexcept override {
                std::scoped_lock lock(mutex);
                event_traces.push_back(record.context.trace_id);
            }
        };

        TEST(TracePropagationTest, TcpRequestJoinsClientTrace) {
            const uint16_t port = net::test::pickHeldPort();
            ASSERT_NE(port, 0);

            CacheNodeServerOptions opts;
            opts.node_id = "trace-node";
            opts.port = port;
            opts.listen_fd = net::test::takeHeldFd(port);
            ASSERT_GE(opts.listen_fd, 0);
            opts.capacity = 1U << 20U;
            opts.replica_factor = 1;
            opts.mode = ConsistencyMode::Async;
            opts.io_threads = 1;

            CacheNodeServer server(std::move(opts));
            ASSERT_TRUE(server.start().has_value());
            std::thread runner([&] { server.run(); });

            constexpr uint64_t K_TRACE = 0x0BADF00DDEADBEEFULL;
            constexpr uint64_t K_PARENT = 0x0102030405060708ULL;
            {
                auto probe = std::make_shared<TraceProbe>();
                auto prev = globalSubscriber();
                setGlobalSubscriber(probe);
                net::Request req;
                req.opcode = net::Opcode::Ping;
                req.trace_id = K_TRACE;
                req.span_id = K_PARENT;
                auto res = net::test::rawRequest(port, req);
                ASSERT_TRUE(res.has_value());
                EXPECT_EQ(res->status, Errc::OK);
                // Response echoes the request trace context.
                EXPECT_EQ(res->trace_id, K_TRACE);
                setGlobalSubscriber(prev);

                std::scoped_lock lock(probe->mutex);
                bool joined = false;
                for (const auto& s : probe->spans) {
                    if (s.trace_id == K_TRACE && s.parent_span_id == K_PARENT) {
                        joined = true;
                    }
                }
                EXPECT_TRUE(joined) << "no server span joined the client trace";
                // Startup and background jobs legitimately mint fresh traces
                // (node.start, timer roots); every event must still belong to
                // a known trace — the request's, empty context, or a recorded
                // span's — never a dangling or mismatched one.
                std::vector<uint64_t> known_traces{K_TRACE, 0};
                for (const auto& s : probe->spans) {
                    known_traces.push_back(s.trace_id);
                }
                for (auto trace : probe->event_traces) {
                    EXPECT_TRUE(
                        std::find(known_traces.begin(), known_traces.end(), trace)
                        != known_traces.end()
                    ) << trace;
                }
            }

            server.shutdown();
            runner.join();
        }

        TEST(TracePropagationTest, ClientLibraryJoinsServerTrace) {
            const uint16_t port = net::test::pickHeldPort();
            ASSERT_NE(port, 0);

            CacheNodeServerOptions opts;
            opts.node_id = "trace-node";
            opts.port = port;
            opts.listen_fd = net::test::takeHeldFd(port);
            ASSERT_GE(opts.listen_fd, 0);
            opts.capacity = 1U << 20U;
            opts.replica_factor = 1;
            opts.mode = ConsistencyMode::Async;
            opts.io_threads = 1;

            CacheNodeServer server(std::move(opts));
            ASSERT_TRUE(server.start().has_value());
            std::thread runner([&] { server.run(); });

            ClusterConfig ccfg;
            ccfg.nodes.push_back({"trace-node", "127.0.0.1", port});
            CacheClient client(std::move(ccfg));

            auto recorder = std::make_shared<RecordingSubscriber>();
            {
                auto prev = globalSubscriber();
                setGlobalSubscriber(recorder);
                ASSERT_TRUE(client.set("trace-k", "trace-v").has_value());
                auto got = client.get("trace-k");
                ASSERT_TRUE(got.has_value());
                EXPECT_EQ(*got, "trace-v");
                setGlobalSubscriber(prev);
            }

            // The client stamps its span context onto every request, so the
            // server must serve under the same trace, parented at the
            // request's span id — no fresh server-side root.
            auto spans = recorder->spans();
            const RecordingSubscriber::RecordedSpan* set_span = nullptr;
            const RecordingSubscriber::RecordedSpan* get_span = nullptr;
            for (const auto& s : spans) {
                if (s.operation == "client.set") {
                    set_span = &s;
                }
                if (s.operation == "client.get") {
                    get_span = &s;
                }
            }
            ASSERT_NE(set_span, nullptr);
            ASSERT_NE(get_span, nullptr);

            bool set_joined = false;
            bool get_joined = false;
            for (const auto& s : spans) {
                if (s.operation
                    == "tcp.set"
                    && s.trace_id
                    == set_span->trace_id
                    && s.parent_span_id
                    == set_span->span_id) {
                    set_joined = true;
                }
                if (s.operation
                    == "tcp.get"
                    && s.trace_id
                    == get_span->trace_id
                    && s.parent_span_id
                    == get_span->span_id) {
                    get_joined = true;
                }
            }
            EXPECT_TRUE(set_joined) << "server did not join the client set trace";
            EXPECT_TRUE(get_joined) << "server did not join the client get trace";

            server.shutdown();
            runner.join();
        }

        TEST(TracePropagationTest, ServerLifecycleHasRoots) {
            const uint16_t port = net::test::pickHeldPort();
            ASSERT_NE(port, 0);

            CacheNodeServerOptions opts;
            opts.node_id = "trace-node-lifecycle";
            opts.port = port;
            opts.listen_fd = net::test::takeHeldFd(port);
            ASSERT_GE(opts.listen_fd, 0);
            opts.capacity = 1U << 20U;
            opts.replica_factor = 1;
            opts.mode = ConsistencyMode::Async;
            opts.io_threads = 1;

            auto recorder = std::make_shared<RecordingSubscriber>();
            {
                auto prev = globalSubscriber();
                setGlobalSubscriber(recorder);
                CacheNodeServer server(std::move(opts));
                ASSERT_TRUE(server.start().has_value());
                std::thread runner([&] { server.run(); });
                // Evict runs on a ~1s cadence; wait long enough to observe at
                // least one background pass.
                std::this_thread::sleep_for(std::chrono::milliseconds(2500));
                server.shutdown();
                runner.join();
                setGlobalSubscriber(prev);
            }

            auto spans = recorder->spans();
            const RecordingSubscriber::RecordedSpan* started = nullptr;
            const RecordingSubscriber::RecordedSpan* evicted = nullptr;
            const RecordingSubscriber::RecordedSpan* stopped = nullptr;
            for (const auto& s : spans) {
                if (s.operation == "node.start") {
                    started = &s;
                }
                if (s.operation == "timer.evict") {
                    evicted = &s;
                }
                if (s.operation == "node.shutdown") {
                    stopped = &s;
                }
            }
            ASSERT_NE(started, nullptr);
            EXPECT_EQ(started->status, SpanStatus::Ok);
            EXPECT_NE(started->trace_id, 0U);
            ASSERT_NE(evicted, nullptr);
            EXPECT_EQ(evicted->status, SpanStatus::Ok);
            ASSERT_NE(stopped, nullptr);
            EXPECT_EQ(stopped->status, SpanStatus::Ok);
            EXPECT_TRUE(stopped->closed);
            // Every background pass mints a strictly increasing run id, so
            // runs order on the backend even across timer types.
            uint64_t last_run = 0;
            size_t evict_runs = 0;
            for (const auto& s : spans) {
                if (s.operation != "timer.evict") {
                    continue;
                }
                uint64_t run = 0;
                for (const auto& a : s.attributes) {
                    if (a.key == "run") {
                        run = a.uint_val;
                    }
                }
                EXPECT_GT(run, last_run) << "run ids must increase per pass";
                last_run = run;
                ++evict_runs;
            }
            EXPECT_GE(evict_runs, 2U) << "2.5s at ~1s cadence must show 2+ passes";
        }
    } // namespace
} // namespace cinder
