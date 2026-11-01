#include <array>
#include <cstdio>
#include <cstdlib>
#include <gtest/gtest.h>
#include <string>
#include <sys/wait.h>
#include <unistd.h>
#include <utility>
#include <vector>

#include "integration/test_helpers.hpp"

using cinder::net::test::NodeProcGuard;
using cinder::net::test::pickHeldPort;
using cinder::net::test::spawnNode;
using cinder::net::test::waitForNode;

namespace {

    // Extra env vars are set in the forked child before exec. When exit_code
    // is non-null it receives the child's exit status (or -1).
    auto runCli(
        uint16_t port,
        const std::vector<std::string>& args,
        const std::vector<std::pair<std::string, std::string>>& env = {},
        int* exit_code = nullptr
    ) -> std::pair<std::string, std::string> {
        std::array<int, 2> out_pipe{};
        std::array<int, 2> err_pipe{};
        if (pipe(out_pipe.data()) != 0 || pipe(err_pipe.data()) != 0) {
            return {};
        }

        pid_t pid = fork();
        if (pid == -1) {
            return {};
        }
        if (pid == 0) {
            for (const auto& [k, v] : env) {
                // NOLINTNEXTLINE(concurrency-mt-unsafe): forked child before exec.
                (void)setenv(k.c_str(), v.c_str(), 1);
            }

            close(out_pipe[0]);
            close(err_pipe[0]);
            dup2(out_pipe[1], STDOUT_FILENO);
            dup2(err_pipe[1], STDERR_FILENO);
            close(out_pipe[1]);
            close(err_pipe[1]);

            auto port_str = std::to_string(port);
            std::vector<const char*> argv{"cinder-cli", "--port", port_str.c_str()};
            for (const auto& a : args) {
                argv.push_back(a.c_str());
            }

            argv.push_back(nullptr);
            // NOLINTNEXTLINE
            execv(CINDER_TEST_CLI_PATH, const_cast<char* const*>(argv.data()));
            _exit(1);
        }

        close(out_pipe[1]);
        close(err_pipe[1]);

        std::array<char, 4096> buf{};
        std::string out;
        std::string err;
        ssize_t n = 0;
        while ((n = read(out_pipe[0], buf.data(), buf.size())) > 0) {
            out.append(buf.data(), static_cast<size_t>(n));
        }
        while ((n = read(err_pipe[0], buf.data(), buf.size())) > 0) {
            err.append(buf.data(), static_cast<size_t>(n));
        }

        close(out_pipe[0]);
        close(err_pipe[0]);

        int status = 0;
        waitpid(pid, &status, 0);
        if (exit_code != nullptr) {
            *exit_code = WIFEXITED(status) ? WEXITSTATUS(status) : -1;
        }

        return {out, err};
    }

    TEST(CliTest, Ping) {
        const uint16_t port = pickHeldPort();
        ASSERT_NE(port, 0);
        NodeProcGuard node{spawnNode(port, "node1", "")};
        ASSERT_TRUE(waitForNode(port, "node1"));
        auto [out, err] = runCli(port, {"ping"});
        EXPECT_EQ(out, "pong\n");
        EXPECT_TRUE(err.empty());
    }

    TEST(CliTest, SetGet) {
        const uint16_t port = pickHeldPort();
        ASSERT_NE(port, 0);
        NodeProcGuard node{spawnNode(port, "node1", "")};
        ASSERT_TRUE(waitForNode(port, "node1"));
        {
            auto [out, err] = runCli(port, {"set", "greeting", "hello"});
            EXPECT_EQ(out, "OK\n");
            EXPECT_TRUE(err.empty());
        }
        {
            auto [out, err] = runCli(port, {"get", "greeting"});
            EXPECT_EQ(out, "hello\n");
            EXPECT_TRUE(err.empty());
        }
    }

    TEST(CliTest, GetNotFound) {
        const uint16_t port = pickHeldPort();
        ASSERT_NE(port, 0);
        NodeProcGuard node{spawnNode(port, "node1", "")};
        ASSERT_TRUE(waitForNode(port, "node1"));
        auto [out, err] = runCli(port, {"get", "nope"});
        EXPECT_TRUE(out.contains("not found"));
        EXPECT_TRUE(err.empty());
    }

    TEST(CliTest, ConnectRefused) {
        auto [out, err] = runCli(9999, {"ping"});
        EXPECT_TRUE(err.contains("Connection refused"));
        EXPECT_TRUE(out.empty());
    }

    TEST(CliTest, Del) {
        const uint16_t port = pickHeldPort();
        ASSERT_NE(port, 0);
        NodeProcGuard node{spawnNode(port, "node1", "")};
        ASSERT_TRUE(waitForNode(port, "node1"));
        {
            auto [out, err] = runCli(port, {"set", "temp", "x"});
            EXPECT_EQ(out, "OK\n");
        }
        {
            auto [out, err] = runCli(port, {"del", "temp"});
            EXPECT_EQ(out, "OK\n");
        }
    }

    TEST(CliTest, Mget) {
        const uint16_t port = pickHeldPort();
        ASSERT_NE(port, 0);
        NodeProcGuard node{spawnNode(port, "node1", "")};
        ASSERT_TRUE(waitForNode(port, "node1"));
        {
            auto [out, err] = runCli(port, {"set", "a", "1"});
            EXPECT_EQ(out, "OK\n");
        }
        {
            auto [out, err] = runCli(port, {"set", "b", "2"});
            EXPECT_EQ(out, "OK\n");
        }
        {
            auto [out, err] = runCli(port, {"mget", "a", "b", "missing"});
            EXPECT_TRUE(out.contains("a: 1\n"));
            EXPECT_TRUE(out.contains("b: 2\n"));
            EXPECT_TRUE(out.contains("missing"));
            EXPECT_TRUE(err.empty());
        }
    }

    TEST(CliTest, Mset) {
        const uint16_t port = pickHeldPort();
        ASSERT_NE(port, 0);
        NodeProcGuard node{spawnNode(port, "node1", "")};
        ASSERT_TRUE(waitForNode(port, "node1"));
        {
            auto [out, err] = runCli(port, {"mset", "a", "1", "b", "2"});
            EXPECT_TRUE(out.contains("a: OK\n"));
            EXPECT_TRUE(out.contains("b: OK\n"));
            EXPECT_TRUE(err.empty());
        }
        {
            auto [out, err] = runCli(port, {"mget", "a", "b"});
            EXPECT_TRUE(out.contains("a: 1\n"));
            EXPECT_TRUE(out.contains("b: 2\n"));
            EXPECT_TRUE(err.empty());
        }
        {
            // Odd arg count is rejected.
            auto [out, err] = runCli(port, {"mset", "a"});
            EXPECT_TRUE(out.empty());
            EXPECT_TRUE(err.contains("mset requires key/value pairs"));
        }
        {
            // TTL applies to every pair.
            auto [out, err] = runCli(port, {"mset", "c", "3", "--ttl", "60000"});
            EXPECT_TRUE(out.contains("c: OK\n"));
            EXPECT_TRUE(err.empty());
        }
        {
            auto [out, err] = runCli(port, {"get", "c"});
            EXPECT_TRUE(out.contains("3\n"));
        }
    }

    TEST(CliTest, GetVersioned) {
        const uint16_t port = pickHeldPort();
        ASSERT_NE(port, 0);
        NodeProcGuard node{spawnNode(port, "node1", "")};
        ASSERT_TRUE(waitForNode(port, "node1"));
        auto get_version = [&](const std::string& key) {
            auto [out, err] = runCli(port, {"get-versioned", key});
            EXPECT_TRUE(err.empty());
            const auto pos = out.find("version: ");
            EXPECT_NE(pos, std::string::npos);
            return std::make_pair(out, std::stoull(out.substr(pos + 9)));
        };
        {
            auto [out, err] = runCli(port, {"set", "k", "v"});
            EXPECT_EQ(out, "OK\n");
        }
        // Same value as get, plus LWW metadata. Store versions come from a
        // global clock-seeded Lamport counter, so ordering and distinctness
        // are asserted, not literal values.
        unsigned long long v1 = 0;
        unsigned long long v2 = 0;
        {
            auto [out, ver] = get_version("k");
            EXPECT_TRUE(out.contains("v\n"));
            EXPECT_TRUE(out.contains("writer: "));
            v1 = ver;
        }
        {
            auto [out, err] = runCli(port, {"set", "k", "v2"});
            EXPECT_EQ(out, "OK\n");
        }
        {
            auto [out, ver] = get_version("k");
            EXPECT_TRUE(out.contains("v2\n"));
            EXPECT_GT(ver, v1);
            v2 = ver;
        }
        {
            // Miss behaves exactly like get miss.
            auto [out, err] = runCli(port, {"get-versioned", "nope"});
            EXPECT_TRUE(out.contains("not found"));
            EXPECT_TRUE(err.empty());
        }
        {
            // TTL-set keys report their expiry.
            auto [out, err] = runCli(port, {"set", "t", "x", "--ttl", "60000"});
            EXPECT_EQ(out, "OK\n");
        }
        {
            auto [out, ver] = get_version("t");
            EXPECT_TRUE(out.contains("x\n"));
            EXPECT_TRUE(out.contains("expires_at: "));
            // Distinct keys mint distinct versions (global counter).
            EXPECT_NE(ver, v2);
        }
    }

    TEST(CliTest, Info) {
        const uint16_t port = pickHeldPort();
        ASSERT_NE(port, 0);
        NodeProcGuard node{spawnNode(port, "node1", "")};
        ASSERT_TRUE(waitForNode(port, "node1"));
        auto [out, err] = runCli(port, {"info"});
        EXPECT_TRUE(out.contains("\"node_id\":\"node1\""));
        EXPECT_TRUE(err.empty());
    }

    TEST(CliTest, Cluster) {
        const uint16_t port = pickHeldPort();
        ASSERT_NE(port, 0);
        NodeProcGuard node{spawnNode(port, "node1", "")};
        ASSERT_TRUE(waitForNode(port, "node1"));
        auto [out, err] = runCli(port, {"cluster"});
        EXPECT_TRUE(out.contains("node1"));
        EXPECT_TRUE(err.empty());
    }

    TEST(CliTest, Ring) {
        const uint16_t port = pickHeldPort();
        ASSERT_NE(port, 0);
        NodeProcGuard node{spawnNode(port, "node1", "")};
        ASSERT_TRUE(waitForNode(port, "node1"));
        auto [out, err] = runCli(port, {"ring"});
        EXPECT_TRUE(out.contains("vnodes_per_node"));
        EXPECT_TRUE(err.empty());
    }
} // namespace

#ifdef CINDER_ENABLE_TLS
namespace {

    // Test certificates (see tests/fixtures). CMAKE_CURRENT_SOURCE_DIR for
    // this TU is tests/, matching the tls integration suite's layout.
    std::string cliFixturePath(const std::string& name) {
        auto src = std::filesystem::path(CMAKE_CURRENT_SOURCE_DIR) / "fixtures" / name;
        if (std::filesystem::exists(src)) {
            return src.string();
        }
        return std::filesystem::path(CMAKE_CURRENT_BINARY_DIR) / "fixtures" / name;
    }

    // TLS daemon with client auth enabled. Mirrors tls_test's spawner plus
    // --client-shared-secret.
    pid_t spawnTlsAuthDaemon(int port, const std::string& client_secret) {
        auto port_str = std::to_string(port);
        auto cert = cliFixturePath("server.pem");
        auto key = cliFixturePath("server-key.pem");
        auto ca = cliFixturePath("ca.pem");
        int held_fd = cinder::net::test::takeHeldFd(static_cast<uint16_t>(port));
        auto fd_str = std::to_string(held_fd);
        pid_t pid = fork();
        if (pid == 0) {
            // NOLINTNEXTLINE
            execl(
                CINDER_TEST_CINDERD_PATH,
                "cinderd",
                "--port",
                port_str.c_str(),
                "--tls",
                "--tls-cert",
                cert.c_str(),
                "--tls-key",
                key.c_str(),
                "--tls-ca",
                ca.c_str(),
                "--client-shared-secret",
                client_secret.c_str(),
                "--listen-fd",
                fd_str.c_str(),
                nullptr
            );
            _exit(1);
        }
        if (held_fd >= 0) {
            ::close(held_fd);
        }
        return pid;
    }

    void stopTlsAuthDaemon(pid_t pid) {
        if (pid <= 0) {
            return;
        }

        (void)kill(pid, SIGTERM);
        int status = 0;
        auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(2);
        while (std::chrono::steady_clock::now() < deadline) {
            pid_t r = waitpid(pid, &status, WNOHANG);
            if (r == pid) {
                return;
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(50));
        }

        (void)kill(pid, SIGKILL);
        (void)waitpid(pid, &status, 0);
    }

    // cinder-cli against a TLS + client-auth daemon. Extra CLI args and env
    // are forwarded to runCli; returns the child exit code.
    int runTlsCli(
        uint16_t port,
        const std::vector<std::string>& cmd,
        std::string& out,
        const std::string& client_secret = {},
        const std::vector<std::pair<std::string, std::string>>& env = {}
    ) {
        std::vector<std::string> args{
            "--tls",
            "--tls-ca",
            cliFixturePath("ca.pem"),
        };
        if (!client_secret.empty()) {
            args.push_back("--client-secret");
            args.push_back(client_secret);
        }

        args.insert(args.end(), cmd.begin(), cmd.end());
        int rc = -1;
        auto [o, e] = runCli(port, args, env, &rc);
        out = std::move(o);
        return rc;
    }

    bool waitForTlsAuthNode(uint16_t port, const std::string& client_secret) {
        for (int i = 0; i < 50; i++) {
            std::string out;
            int rc = runTlsCli(port, {"ping"}, out, client_secret);
            if (rc == 0 && out == "pong\n") {
                return true;
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
        }
        return false;
    }

    TEST(CliAuthTest, SetGetWithSecretOverTls) {
        const uint16_t port = pickHeldPort();
        ASSERT_NE(port, 0);
        pid_t daemon = spawnTlsAuthDaemon(port, "test-client-secret");
        ASSERT_TRUE(waitForTlsAuthNode(port, "test-client-secret"));

        std::string out;
        EXPECT_EQ(runTlsCli(port, {"set", "ck", "cv"}, out, "test-client-secret"), 0);
        EXPECT_EQ(out, "OK\n");
        EXPECT_EQ(runTlsCli(port, {"get", "ck"}, out, "test-client-secret"), 0);
        EXPECT_EQ(out, "cv\n");
        stopTlsAuthDaemon(daemon);
    }

    TEST(CliAuthTest, MissingSecretDenied) {
        const uint16_t port = pickHeldPort();
        ASSERT_NE(port, 0);
        pid_t daemon = spawnTlsAuthDaemon(port, "test-client-secret");
        ASSERT_TRUE(waitForTlsAuthNode(port, "test-client-secret"));

        std::string out;
        int rc = runTlsCli(port, {"get", "ck"}, out);
        EXPECT_NE(rc, 0);
        EXPECT_TRUE(out.contains("permission denied"));
        stopTlsAuthDaemon(daemon);
    }

    TEST(CliAuthTest, WrongSecretDenied) {
        const uint16_t port = pickHeldPort();
        ASSERT_NE(port, 0);
        pid_t daemon = spawnTlsAuthDaemon(port, "test-client-secret");
        ASSERT_TRUE(waitForTlsAuthNode(port, "test-client-secret"));

        std::string out;
        int rc = runTlsCli(port, {"ping"}, out, "wrong-secret");
        EXPECT_NE(rc, 0);
        stopTlsAuthDaemon(daemon);
    }

    TEST(CliAuthTest, SecretFromEnvironment) {
        const uint16_t port = pickHeldPort();
        ASSERT_NE(port, 0);
        pid_t daemon = spawnTlsAuthDaemon(port, "env-client-secret");
        ASSERT_TRUE(waitForTlsAuthNode(port, "env-client-secret"));

        std::string out;
        int rc =
            runTlsCli(port, {"ping"}, out, {}, {{"CINDER_CLIENT_SECRET", "env-client-secret"}});

        EXPECT_EQ(rc, 0);
        EXPECT_EQ(out, "pong\n");
        stopTlsAuthDaemon(daemon);
    }

    TEST(CliAuthTest, AdminInfoRequiresClientSecret) {
        const uint16_t port = pickHeldPort();
        ASSERT_NE(port, 0);
        pid_t daemon = spawnTlsAuthDaemon(port, "test-client-secret");
        ASSERT_TRUE(waitForTlsAuthNode(port, "test-client-secret"));

        std::string out;
        EXPECT_EQ(runTlsCli(port, {"info"}, out, "test-client-secret"), 0);
        EXPECT_TRUE(out.contains("\"node_id\""));

        std::string denied;
        EXPECT_NE(runTlsCli(port, {"info"}, denied), 0);
        stopTlsAuthDaemon(daemon);
    }
} // namespace
#endif // CINDER_ENABLE_TLS
