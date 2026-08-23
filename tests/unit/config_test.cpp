#include <cstdio>
#include <fstream>
#include <gtest/gtest.h>
#include <string>

#include "cinder/common/config.hpp"

namespace cinder {
namespace {

TEST(ConfigTest, LoadValidConfig) {
    // Write a temporary YAML file.
    const char* path = "/tmp/cinder_test_config.yaml";
    {
        std::ofstream f(path);
        f << R"(
server:
  port: 8080
  node_id: test-node
  capacity: 134217728
  replication_factor: 3
  consistency: quorum

cluster:
  peers:
    - id: node2
      host: 10.0.0.2
      port: 8081
    - id: node3
      host: 10.0.0.3
      port: 8082

failure_detector:
  ping_interval_ms: 500
  suspect_timeout_ms: 2000
  gossip_interval_ms: 500
  quarantine_interval_ms: 5000

logging:
  level: debug
)";
    }

    auto result = loadConfig(path);
    ASSERT_TRUE(result.has_value());
    const auto& cfg = result.value();

    EXPECT_EQ(cfg.node_id, "test-node");
    EXPECT_EQ(cfg.port, 8'080);
    EXPECT_EQ(cfg.capacity, 134'217'728);
    EXPECT_EQ(cfg.replica_factor, 3);
    EXPECT_EQ(cfg.consistency, "quorum");
    EXPECT_EQ(cfg.peers.size(), 2);
    EXPECT_EQ(cfg.peers[0].id, "node2");
    EXPECT_EQ(cfg.peers[0].host, "10.0.0.2");
    EXPECT_EQ(cfg.peers[0].port, 8'081);
    EXPECT_EQ(cfg.peers[1].id, "node3");
    EXPECT_EQ(cfg.peers[1].host, "10.0.0.3");
    EXPECT_EQ(cfg.peers[1].port, 8'082);
    EXPECT_EQ(cfg.ping_interval_ms, 500);
    EXPECT_EQ(cfg.suspect_timeout_ms, 2'000);
    EXPECT_EQ(cfg.gossip_interval_ms, 500);
    EXPECT_EQ(cfg.quarantine_interval_ms, 5'000);
    EXPECT_EQ(cfg.log_level, "debug");

    (void)std::remove(path);
}

TEST(ConfigTest, MissingFieldsUseDefaults) {
    const char* path = "/tmp/cinder_test_defaults.yaml";
    {
        std::ofstream f(path);
        f << R"(
server:
  port: 9090
)";
    }

    auto result = loadConfig(path);
    ASSERT_TRUE(result.has_value());
    const auto& cfg = result.value();

    EXPECT_EQ(cfg.port, 9'090);
    EXPECT_EQ(cfg.node_id, "node1");        // default
    EXPECT_EQ(cfg.capacity, 67'108'864);    // default
    EXPECT_EQ(cfg.replica_factor, 1);       // default
    EXPECT_EQ(cfg.consistency, "async");    // default
    EXPECT_TRUE(cfg.peers.empty());         // default
    EXPECT_EQ(cfg.ping_interval_ms, 1'000); // default
    EXPECT_EQ(cfg.log_level, "info");       // default

    (void)std::remove(path);
}

TEST(ConfigTest, EmptyFileUsesAllDefaults) {
    const char* path = "/tmp/cinder_test_empty.yaml";
    {
        std::ofstream f(path);
        f << "";
    }

    auto result = loadConfig(path);
    ASSERT_TRUE(result.has_value());
    const auto& cfg = result.value();
    EXPECT_EQ(cfg.node_id, "node1");
    EXPECT_EQ(cfg.port, 7'000);
    EXPECT_EQ(cfg.log_level, "info");

    (void)std::remove(path);
}

TEST(ConfigTest, InvalidYamlReturnsError) {
    const char* path = "/tmp/cinder_test_invalid.yaml";
    {
        std::ofstream f(path);
        f << "server:\n  port: [invalid yaml\n";
    }

    auto result = loadConfig(path);
    EXPECT_FALSE(result.has_value());
    EXPECT_EQ(result.error().code(), Errc::InvalidArgument);

    (void)std::remove(path);
}

TEST(ConfigTest, MissingFileReturnsError) {
    auto result = loadConfig("/tmp/nonexistent_config.yaml");
    EXPECT_FALSE(result.has_value());
    EXPECT_EQ(result.error().code(), Errc::NotFound);
}

TEST(ConfigTest, LogLevelFromString) {
    EXPECT_EQ(logLevelFromString("trace"), LogLevel::Trace);
    EXPECT_EQ(logLevelFromString("debug"), LogLevel::Debug);
    EXPECT_EQ(logLevelFromString("info"), LogLevel::Info);
    EXPECT_EQ(logLevelFromString("warn"), LogLevel::Warn);
    EXPECT_EQ(logLevelFromString("error"), LogLevel::Error);
    EXPECT_EQ(logLevelFromString("bogus"), LogLevel::Info); // default
}

TEST(ConfigTest, ParsePeersString) {
    Config cfg;
    auto count = parsePeersString("n1@10.0.0.1:7000,n2@10.0.0.2:7001", cfg);
    EXPECT_EQ(count, 2);
    EXPECT_EQ(cfg.peers.size(), 2);
    EXPECT_EQ(cfg.peers[0].id, "n1");
    EXPECT_EQ(cfg.peers[0].host, "10.0.0.1");
    EXPECT_EQ(cfg.peers[0].port, 7'000);
    EXPECT_EQ(cfg.peers[1].id, "n2");
    EXPECT_EQ(cfg.peers[1].host, "10.0.0.2");
    EXPECT_EQ(cfg.peers[1].port, 7'001);
}

TEST(ConfigTest, ParsePeersStringSkipsMalformed) {
    Config cfg;
    auto count = parsePeersString("n1@10.0.0.1:7000,bad,n2@10.0.0.2:7001", cfg);
    EXPECT_EQ(count, 2);
    EXPECT_EQ(cfg.peers.size(), 2);
}

TEST(ConfigTest, ParsePeersStringEmpty) {
    Config cfg;
    auto count = parsePeersString("", cfg);
    EXPECT_EQ(count, 0);
    EXPECT_TRUE(cfg.peers.empty());
}
} // namespace
} // namespace cinder
