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

TEST(ConfigTest, DiffConfigIdentical) {
    Config a;
    a.port = 7'000;
    a.node_id = "n1";
    a.log_level = "info";

    Config b = a;
    auto changed = diffConfig(a, b);
    EXPECT_TRUE(changed.empty());
}

TEST(ConfigTest, DiffConfigSingleChange) {
    Config a;
    a.port = 7'000;

    Config b = a;
    b.port = 8'080;
    auto changed = diffConfig(a, b);
    ASSERT_EQ(changed.size(), 1);
    EXPECT_EQ(changed[0], "port");
}

TEST(ConfigTest, DiffConfigMultipleChanges) {
    Config a;
    a.port = 7'000;
    a.log_level = "info";
    a.capacity = 67'108'864;

    Config b = a;
    b.port = 8'080;
    b.log_level = "debug";
    b.capacity = 134'217'728;
    auto changed = diffConfig(a, b);
    ASSERT_EQ(changed.size(), 3);
    EXPECT_EQ(changed[0], "port");
    EXPECT_EQ(changed[1], "capacity");
    EXPECT_EQ(changed[2], "log_level");
}

TEST(ConfigTest, DiffConfigPeersAdded) {
    Config a;
    Config b = a;
    b.peers.push_back({"n2", "10.0.0.2", 7'001});
    auto changed = diffConfig(a, b);
    ASSERT_EQ(changed.size(), 1);
    EXPECT_EQ(changed[0], "peers");
}

TEST(ConfigTest, DiffConfigPeersModified) {
    Config a;
    a.peers.push_back({"n2", "10.0.0.2", 7'001});

    Config b = a;
    b.peers[0].port = 9'090;
    auto changed = diffConfig(a, b);
    ASSERT_EQ(changed.size(), 1);
    EXPECT_EQ(changed[0], "peers");
}

TEST(ConfigTest, DiffConfigTlsFields) {
    Config a;
    a.tls.enabled = false;

    Config b = a;
    b.tls.enabled = true;
    b.tls.cert_file = "/path/cert.pem";
    auto changed = diffConfig(a, b);
    ASSERT_EQ(changed.size(), 2);
    EXPECT_EQ(changed[0], "tls_enabled");
    EXPECT_EQ(changed[1], "tls_cert_file");
}

TEST(ConfigTest, FormatConfigJsonRoundTrip) {
    const char* path = "/tmp/cinder_test_format.yaml";
    {
        std::ofstream f(path);
        f << R"(
server:
  port: 9090
  node_id: fmt-node
  capacity: 128
  replication_factor: 2
  consistency: quorum

cluster:
  peers:
    - id: n2
      host: 10.0.0.2
      port: 9091

failure_detector:
  ping_interval_ms: 500

logging:
  level: debug
)";
    }

    auto result = loadConfig(path);
    ASSERT_TRUE(result.has_value());

    auto json = formatConfigJson(*result);
    // String values are double-quoted because escapeJsonString wraps in quotes
    // and the format templates also include quotes.
    EXPECT_TRUE(json.contains(R"("node_id":""fmt-node"")"));
    EXPECT_TRUE(json.contains("\"port\":9090"));
    EXPECT_TRUE(json.contains("\"capacity\":128"));
    EXPECT_TRUE(json.contains("\"replica_factor\":2"));
    EXPECT_TRUE(json.contains(R"("consistency":""quorum"")"));
    EXPECT_TRUE(json.contains("\"ping_interval_ms\":500"));
    EXPECT_TRUE(json.contains(R"("log_level":""debug"")"));
    EXPECT_TRUE(json.contains(R"("id":""n2"")"));
    EXPECT_TRUE(json.contains(R"("host":""10.0.0.2"")"));

    (void)std::remove(path);
}

TEST(ConfigTest, FormatConfigJsonEscapesSpecialChars) {
    Config cfg;
    cfg.node_id = "node\"with\\special\nchars";
    auto json = formatConfigJson(cfg);
    EXPECT_TRUE(json.contains("node\\\"with\\\\special\\nchars"));
}
} // namespace
} // namespace cinder
