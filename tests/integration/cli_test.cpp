#include <array>
#include <csignal>
#include <cstdio>
#include <gtest/gtest.h>
#include <string>
#include <sys/wait.h>
#include <unistd.h>
#include <vector>

#include "integration/test_helpers.hpp"

using cinder::net::test::waitForPort;

namespace {

static auto
runCli(uint16_t port, const std::vector<std::string>& args)
    -> std::pair<std::string, std::string> {
    int out_pipe[2];
    int err_pipe[2];
    if (pipe(out_pipe) != 0 || pipe(err_pipe) != 0) {
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

static auto
spawnDaemon(int port) -> pid_t {
    auto port_str = std::to_string(port);
    pid_t pid = fork();
    if (pid == 0) {
        // NOLINTNEXTLINE
        execl(CINDER_TEST_CINDERD_PATH, "cinderd", "--port", port_str.c_str(), nullptr);
        _exit(1);
    }
    return pid;
}

TEST(CliTest, Ping) {
    int port = 17'900;
    pid_t dpid = spawnDaemon(port);
    ASSERT_TRUE(waitForPort(port));
    auto [out, err] = runCli(port, {"ping"});
    EXPECT_EQ(out, "pong\n");
    EXPECT_TRUE(err.empty());
    (void)kill(dpid, SIGTERM);
    (void)waitpid(dpid, nullptr, 0);
}

TEST(CliTest, SetGet) {
    int port = 17'901;
    pid_t dpid = spawnDaemon(port);
    ASSERT_TRUE(waitForPort(port));
    {
        auto [out, err] = runCli(port, {"set", "greeting", "hello"});
        EXPECT_EQ(out, "hello\n");
        EXPECT_TRUE(err.empty());
    }
    {
        auto [out, err] = runCli(port, {"get", "greeting"});
        EXPECT_EQ(out, "hello\n");
        EXPECT_TRUE(err.empty());
    }
    (void)kill(dpid, SIGTERM);
    (void)waitpid(dpid, nullptr, 0);
}

TEST(CliTest, GetNotFound) {
    int port = 17'902;
    pid_t dpid = spawnDaemon(port);
    ASSERT_TRUE(waitForPort(port));
    auto [out, err] = runCli(port, {"get", "nope"});
    EXPECT_TRUE(out.find("not found") != std::string::npos);
    EXPECT_TRUE(err.empty());
    (void)kill(dpid, SIGTERM);
    (void)waitpid(dpid, nullptr, 0);
}

TEST(CliTest, ConnectRefused) {
    auto [out, err] = runCli(9'999, {"ping"});
    EXPECT_TRUE(err.find("connect failed") != std::string::npos);
    EXPECT_TRUE(out.empty());
}

TEST(CliTest, Del) {
    int port = 17'903;
    pid_t dpid = spawnDaemon(port);
    ASSERT_TRUE(waitForPort(port));
    {
        auto [out, err] = runCli(port, {"set", "temp", "x"});
        EXPECT_EQ(out, "x\n");
    }
    {
        auto [out, err] = runCli(port, {"del", "temp"});
        EXPECT_EQ(out, "(not found)\n");
    }
    (void)kill(dpid, SIGTERM);
    (void)waitpid(dpid, nullptr, 0);
}

} // namespace
