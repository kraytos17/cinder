#include <chrono>
#include <gtest/gtest.h>
#include <string>
#include <thread>

#include "cinder/client/cache_client.hpp"
#include "cinder/common/hmac.hpp"
#include "cinder/net/protocol.hpp"
#include "cinder/node/cache_node_server.hpp"
#include "integration/test_helpers.hpp"

using std::chrono::milliseconds;

namespace cinder {
    namespace {

        struct AuthServer {
            std::optional<CacheNodeServer> server;
            std::thread runner;
            uint16_t port = 0;

            AuthServer(
                std::string node_id,
                std::string shared_secret,
                std::string client_shared_secret,
                bool tls_enabled
            ) {
                port = net::test::pickHeldPort();
                EXPECT_NE(port, 0);

                CacheNodeServerOptions opts;
                opts.node_id = std::move(node_id);
                opts.port = port;
                opts.listen_fd = net::test::takeHeldFd(port);
                EXPECT_GE(opts.listen_fd, 0);
                opts.capacity = 1U << 20U;
                opts.replica_factor = 1;
                opts.mode = ConsistencyMode::Async;
                opts.io_threads = 1;
                opts.tls_enabled = tls_enabled;
                opts.shared_secret = std::move(shared_secret);
                opts.client_shared_secret = std::move(client_shared_secret);

                server.emplace(std::move(opts));
            }

            ~AuthServer() {
                if (server.has_value()) {
                    server->shutdown();
                }
                if (runner.joinable()) {
                    runner.join();
                }
            }

            auto start() -> Result<void> {
                auto res = server->start();
                if (!res.has_value()) {
                    return res;
                }

                std::thread t([this] { server->run(); });
                runner = std::move(t);
                return ok();
            }

            // Readiness: the listen socket is adopted synchronously, but the
            // io pool needs a moment before it accepts.
            auto waitReady(std::string_view auth_token = {}) const -> bool {
                for (int i = 0; i < 100; ++i) {
                    net::Request req;
                    req.opcode = net::Opcode::Ping;
                    auto res = net::test::rawRequest(port, req, auth_token);
                    if (res.has_value() && res->status == Errc::OK) {
                        return true;
                    }

                    std::this_thread::sleep_for(milliseconds(50));
                }
                return false;
            }
        };

        // Plaintext harnesses below: the in-process server speaks plaintext
        // (no certificates) and CacheClient/rawRequest have no TLS mode.
        // TLS-build coverage lives in tls_test.cpp against real daemons.
#ifndef CINDER_ENABLE_TLS
        TEST(ClientAuthMatrixTest, ClientOpsRequireClientToken) {
            AuthServer srv("auth-node", "node-secret", "client-secret", true);
            ASSERT_TRUE(srv.start().has_value());
            auto client_token = generateClientAuthToken("client-secret");
            ASSERT_TRUE(srv.waitReady(client_token));

            // No token -> denied on every client-facing opcode.
            for (auto op : {
                     net::Opcode::Get,
                     net::Opcode::GetVersioned,
                     net::Opcode::Set,
                     net::Opcode::Del,
                     net::Opcode::Ping,
                     net::Opcode::AdminInfo,
                 }) {
                net::Request req;
                req.opcode = op;
                req.key = "k";
                req.value = "v";
                auto res = net::test::rawRequest(srv.port, req);
                ASSERT_TRUE(res.has_value());
                EXPECT_EQ(res->status, Errc::PermissionDenied) << static_cast<int>(op);
            }

            // Client token -> full data + admin roundtrip.
            net::Request set_req;
            set_req.opcode = net::Opcode::Set;
            set_req.key = "k";
            set_req.value = "v";
            auto set_res = net::test::rawRequest(srv.port, set_req, client_token);
            ASSERT_TRUE(set_res.has_value());
            EXPECT_EQ(set_res->status, Errc::OK);

            net::Request get_req;
            get_req.opcode = net::Opcode::Get;
            get_req.key = "k";
            auto get_res = net::test::rawRequest(srv.port, get_req, client_token);
            ASSERT_TRUE(get_res.has_value());
            EXPECT_EQ(get_res->status, Errc::OK);
            ASSERT_TRUE(get_res->value.has_value());
            EXPECT_EQ(*get_res->value, "v");

            net::Request info_req;
            info_req.opcode = net::Opcode::AdminInfo;
            auto info_res = net::test::rawRequest(srv.port, info_req, client_token);
            ASSERT_TRUE(info_res.has_value());
            EXPECT_EQ(info_res->status, Errc::OK);

            // Wrong secret -> denied.
            auto wrong_token = generateClientAuthToken("wrong-secret");
            auto wrong_res = net::test::rawRequest(srv.port, get_req, wrong_token);
            ASSERT_TRUE(wrong_res.has_value());
            EXPECT_EQ(wrong_res->status, Errc::PermissionDenied);

            // Node token does not satisfy client auth.
            auto node_token = generateAuthToken("node-secret", "auth-node");
            ASSERT_FALSE(node_token.empty());
            auto cross_res = net::test::rawRequest(srv.port, get_req, node_token);
            ASSERT_TRUE(cross_res.has_value());
            EXPECT_EQ(cross_res->status, Errc::PermissionDenied);
            auto admin_cross = net::test::rawRequest(srv.port, info_req, node_token);
            ASSERT_TRUE(admin_cross.has_value());
            EXPECT_EQ(admin_cross->status, Errc::PermissionDenied);
        }

        TEST(ClientAuthMatrixTest, ReplicationStillNeedsNodeToken) {
            AuthServer srv("auth-node", "node-secret", "client-secret", true);
            ASSERT_TRUE(srv.start().has_value());
            auto client_token = generateClientAuthToken("client-secret");
            ASSERT_TRUE(srv.waitReady(client_token));

            auto node_token = generateAuthToken("node-secret", "auth-node");
            ASSERT_FALSE(node_token.empty());

            // Replication with the node token still works (internal class).
            net::Request repl_req;
            repl_req.opcode = net::Opcode::Replicate;
            repl_req.key = "rk";
            repl_req.value = "rv";
            auto repl_res = net::test::rawRequest(srv.port, repl_req, node_token);
            ASSERT_TRUE(repl_res.has_value());
            EXPECT_EQ(repl_res->status, Errc::OK);

            // The client token does not satisfy internal auth.
            auto cross_res = net::test::rawRequest(srv.port, repl_req, client_token);
            ASSERT_TRUE(cross_res.has_value());
            EXPECT_EQ(cross_res->status, Errc::PermissionDenied);

            // No token denied.
            auto anon_res = net::test::rawRequest(srv.port, repl_req);
            ASSERT_TRUE(anon_res.has_value());
            EXPECT_EQ(anon_res->status, Errc::PermissionDenied);
        }
#endif // !CINDER_ENABLE_TLS (plaintext harnesses; see tls_test.cpp)

        TEST(ClientAuthMatrixTest, NodeOnlyDeploymentUnchanged) {
            // Only the node secret configured: client ops stay open, internal
            // ops keep the legacy node-token check.
            AuthServer srv("auth-node", "node-secret", "", false);
            ASSERT_TRUE(srv.start().has_value());
            ASSERT_TRUE(srv.waitReady());

            auto no_auth_get = net::test::getKey(srv.port, "missing");
            ASSERT_TRUE(no_auth_get.has_value());
            EXPECT_EQ(no_auth_get->status, Errc::NotFound);

            auto no_auth_set = net::test::setKey(srv.port, "k", "v");
            ASSERT_TRUE(no_auth_set.has_value());
            EXPECT_EQ(no_auth_set->status, Errc::OK);

            net::Request repl_req;
            repl_req.opcode = net::Opcode::Replicate;
            repl_req.key = "rk";
            repl_req.value = "rv";
            auto anon_repl = net::test::rawRequest(srv.port, repl_req);
            ASSERT_TRUE(anon_repl.has_value());
            EXPECT_EQ(anon_repl->status, Errc::PermissionDenied);

            auto node_token = generateAuthToken("node-secret", "auth-node");
            auto authed_repl = net::test::rawRequest(srv.port, repl_req, node_token);
            ASSERT_TRUE(authed_repl.has_value());
            EXPECT_EQ(authed_repl->status, Errc::OK);
        }

        TEST(ClientAuthMatrixTest, ClientAuthRequiresTlsAtStartup) {
            AuthServer srv("auth-node", "", "client-secret", false);
            EXPECT_FALSE(srv.start().has_value());
        }

#ifndef CINDER_ENABLE_CRYPTO
        TEST(ClientAuthMatrixTest, AuthPrimitivesFailClosedWithoutCrypto) {
            EXPECT_TRUE(generateAuthToken("secret", "node1").empty());
            EXPECT_TRUE(generateClientAuthToken("secret").empty());
            EXPECT_FALSE(verifyAuthToken("secret", "node1", std::string(32, 'x')));
            EXPECT_FALSE(verifyClientAuthToken("secret", std::string(32, 'x')));
        }
#endif

        // CacheClient has no TLS mode: signing covered here on plaintext.
#ifndef CINDER_ENABLE_TLS
        TEST(ClientAuthClientTest, LibraryRoundtripWithSecret) {
            AuthServer srv("auth-node", "node-secret", "client-secret", true);
            ASSERT_TRUE(srv.start().has_value());
            auto client_token = generateClientAuthToken("client-secret");
            ASSERT_TRUE(srv.waitReady(client_token));

            ClusterConfig authed_cfg;
            authed_cfg.nodes.push_back({"auth-node", "127.0.0.1", srv.port});
            authed_cfg.client_shared_secret = "client-secret";
            CacheClient authed(std::move(authed_cfg));

            auto set_res = authed.set("ck", "cv");
            ASSERT_TRUE(set_res.has_value()) << set_res.error().message();
            auto got = authed.get("ck");
            ASSERT_TRUE(got.has_value());
            EXPECT_EQ(*got, "cv");

            // Batch path signs too: all keys come back.
            auto batch_set = authed.multiSet({{"bk1", "bv1"}, {"bk2", "bv2"}});
            EXPECT_EQ(batch_set, 2U);
            auto batch = authed.multiGet({"bk1", "bk2"});
            EXPECT_EQ(batch.size(), 2U);
            EXPECT_EQ(batch["bk1"], "bv1");
            EXPECT_EQ(batch["bk2"], "bv2");
        }

        TEST(ClientAuthClientTest, LibraryWithoutSecretIsDenied) {
            AuthServer srv("auth-node", "node-secret", "client-secret", true);
            ASSERT_TRUE(srv.start().has_value());
            auto client_token = generateClientAuthToken("client-secret");
            ASSERT_TRUE(srv.waitReady(client_token));

            ClusterConfig anon_cfg;
            anon_cfg.nodes.push_back({"auth-node", "127.0.0.1", srv.port});
            CacheClient anon(std::move(anon_cfg));

            auto set_res = anon.set("ck", "cv");
            ASSERT_FALSE(set_res.has_value());
            EXPECT_EQ(set_res.error().code(), Errc::PermissionDenied);
            EXPECT_FALSE(anon.get("ck").has_value());
        }
#endif

    } // namespace
} // namespace cinder
