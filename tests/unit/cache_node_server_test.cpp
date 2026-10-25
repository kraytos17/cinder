#include <atomic>
#include <cstdio>
#include <fstream>
#include <gtest/gtest.h>
#include <string>
#include <thread>
#include <vector>

#include "cinder/common/config.hpp"
#include "cinder/node/cache_node_server.hpp"
#include "integration/test_helpers.hpp"

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

            const std::string cfg_path =
                "/tmp/cinder_reload_test_" + std::to_string(port) + ".yaml";

            struct TempFile {
                std::string path;

                explicit TempFile(std::string p) :
                    path(std::move(p)) {}

                ~TempFile() {
                    (void)std::remove(path.c_str());
                }
            };

            TempFile cfg_file(cfg_path);

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
            constexpr size_t K_CAP_A = 1u << 20;
            constexpr size_t K_CAP_B = (1u << 20) + 1024;
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
} // namespace cinder
