#include <array>
#include <cstdio>
#include <gtest/gtest.h>
#include <string>
#include <sys/wait.h>
#include <unistd.h>
#include <vector>

#include "integration/test_helpers.hpp"

using cinder::net::test::NodeProcGuard;
using cinder::net::test::spawnNode;
using cinder::net::test::waitForPort;

namespace {

auto
runCli(uint16_t port, const std::vector<std::string>& args) -> std::pair<std::string, std::string> {
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

    std::array<char, 4'096> buf{};
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
    return {out, err};
}

constexpr int K_CLI_PORT1 = 17'900;
constexpr int K_CLI_PORT2 = 17'901;
constexpr int K_CLI_PORT3 = 17'902;
constexpr int K_CLI_PORT5 = 17'903;

TEST(CliTest, Ping) {
    NodeProcGuard node{spawnNode(K_CLI_PORT1, "node1", "")};
    ASSERT_TRUE(waitForPort(K_CLI_PORT1));
    auto [out, err] = runCli(K_CLI_PORT1, {"ping"});
    EXPECT_EQ(out, "pong\n");
    EXPECT_TRUE(err.empty());
}

TEST(CliTest, SetGet) {
    NodeProcGuard node{spawnNode(K_CLI_PORT2, "node1", "")};
    ASSERT_TRUE(waitForPort(K_CLI_PORT2));
    {
        auto [out, err] = runCli(K_CLI_PORT2, {"set", "greeting", "hello"});
        EXPECT_EQ(out, "OK\n");
        EXPECT_TRUE(err.empty());
    }
    {
        auto [out, err] = runCli(K_CLI_PORT2, {"get", "greeting"});
        EXPECT_EQ(out, "hello\n");
        EXPECT_TRUE(err.empty());
    }
}

TEST(CliTest, GetNotFound) {
    NodeProcGuard node{spawnNode(K_CLI_PORT3, "node1", "")};
    ASSERT_TRUE(waitForPort(K_CLI_PORT3));
    auto [out, err] = runCli(K_CLI_PORT3, {"get", "nope"});
    EXPECT_TRUE(out.contains("not found"));
    EXPECT_TRUE(err.empty());
}

TEST(CliTest, ConnectRefused) {
    auto [out, err] = runCli(9'999, {"ping"});
    EXPECT_TRUE(err.contains("connect failed"));
    EXPECT_TRUE(out.empty());
}

TEST(CliTest, Del) {
    NodeProcGuard node{spawnNode(K_CLI_PORT5, "node1", "")};
    ASSERT_TRUE(waitForPort(K_CLI_PORT5));
    {
        auto [out, err] = runCli(K_CLI_PORT5, {"set", "temp", "x"});
        EXPECT_EQ(out, "OK\n");
    }
    {
        auto [out, err] = runCli(K_CLI_PORT5, {"del", "temp"});
        EXPECT_EQ(out, "OK\n");
    }
}
} // namespace
