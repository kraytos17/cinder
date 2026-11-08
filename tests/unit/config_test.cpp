#include <fstream>
#include <gtest/gtest.h>
#include <string>

#include "cinder/common/config.hpp"
#include "test_scratch.hpp"

namespace cinder {
    namespace {

        TEST(ConfigTest, LoadValidConfig) {
            // Write a temporary YAML file.
            test::ScratchFile path_file("cinder_test_config", ".yaml");
            const std::string path = path_file.string();
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
            EXPECT_EQ(cfg.port, 8080);
            EXPECT_EQ(cfg.capacity, 134'217'728);
            EXPECT_EQ(cfg.replica_factor, 3);
            EXPECT_EQ(cfg.consistency, "quorum");
            EXPECT_EQ(cfg.peers.size(), 2);
            EXPECT_EQ(cfg.peers[0].id, "node2");
            EXPECT_EQ(cfg.peers[0].host, "10.0.0.2");
            EXPECT_EQ(cfg.peers[0].port, 8081);
            EXPECT_EQ(cfg.peers[1].id, "node3");
            EXPECT_EQ(cfg.peers[1].host, "10.0.0.3");
            EXPECT_EQ(cfg.peers[1].port, 8082);
            EXPECT_EQ(cfg.ping_interval_ms, 500);
            EXPECT_EQ(cfg.suspect_timeout_ms, 2000);
            EXPECT_EQ(cfg.gossip_interval_ms, 500);
            EXPECT_EQ(cfg.quarantine_interval_ms, 5000);
            EXPECT_EQ(cfg.log_level, "debug");
        }

        TEST(ConfigTest, MissingFieldsUseDefaults) {
            test::ScratchFile path_file("cinder_test_defaults", ".yaml");
            const std::string path = path_file.string();
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

            EXPECT_EQ(cfg.port, 9090);
            EXPECT_EQ(cfg.node_id, "node1"); // default
            EXPECT_EQ(cfg.capacity, 67'108'864); // default
            EXPECT_EQ(cfg.replica_factor, 1); // default
            EXPECT_EQ(cfg.consistency, "async"); // default
            EXPECT_TRUE(cfg.peers.empty()); // default
            EXPECT_EQ(cfg.ping_interval_ms, 1000); // default
            EXPECT_EQ(cfg.log_level, "info"); // default
        }

        TEST(ConfigTest, TuningSectionsParse) {
            test::ScratchFile path_file("cinder_test_tuning", ".yaml");
            const std::string path = path_file.string();
            {
                std::ofstream f(path);
                f << R"(
anti_entropy:
  interval_ms: 5000
  buckets: 64
  fetch_bytes: 4096
  full_rehash_interval_ms: 60000

persistence:
  enabled: true
  data_dir: /tmp/cinder-test
  snapshot_interval_s: 30
  max_wal_entries: 500
  max_wal_bytes: 8192
  max_wal_segment_bytes: 1024
  max_sealed_segments: 7
  wal_fsync: true
  snapshot_compression: true
)";
            }

            auto result = loadConfig(path);
            ASSERT_TRUE(result.has_value());
            const auto& cfg = result.value();

            EXPECT_EQ(cfg.anti_entropy.interval, milliseconds{5000});
            EXPECT_EQ(cfg.anti_entropy.buckets, 64U);
            EXPECT_EQ(cfg.anti_entropy.fetch_bytes, 4096U);
            EXPECT_EQ(cfg.anti_entropy.full_rehash_interval, milliseconds{60'000});
            EXPECT_TRUE(cfg.persistence_enabled);
            EXPECT_EQ(cfg.data_dir, "/tmp/cinder-test");
            EXPECT_EQ(cfg.persistence.snapshot_interval_s, 30U);
            EXPECT_EQ(cfg.persistence.max_wal_entries, 500U);
            EXPECT_EQ(cfg.persistence.max_wal_bytes, 8192U);
            EXPECT_EQ(cfg.persistence.max_wal_segment_bytes, 1024U);
            EXPECT_EQ(cfg.persistence.max_sealed_segments, 7U);
            EXPECT_TRUE(cfg.persistence.wal_fsync);
            EXPECT_TRUE(cfg.persistence.snapshot_compression);
        }

        TEST(ConfigTest, EmptyFileUsesAllDefaults) {
            test::ScratchFile path_file("cinder_test_empty", ".yaml");
            const std::string path = path_file.string();
            {
                std::ofstream f(path);
                f << "";
            }

            auto result = loadConfig(path);
            ASSERT_TRUE(result.has_value());
            const auto& cfg = result.value();
            EXPECT_EQ(cfg.node_id, "node1");
            EXPECT_EQ(cfg.port, 7000);
            EXPECT_EQ(cfg.log_level, "info");
        }

        TEST(ConfigTest, InvalidYamlReturnsError) {
            test::ScratchFile path_file("cinder_test_invalid", ".yaml");
            const std::string path = path_file.string();
            {
                std::ofstream f(path);
                f << "server:\n  port: [invalid yaml\n";
            }

            auto result = loadConfig(path);
            EXPECT_FALSE(result.has_value());
            EXPECT_EQ(result.error().code(), Errc::InvalidArgument);
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
            EXPECT_EQ(cfg.peers[0].port, 7000);
            EXPECT_EQ(cfg.peers[1].id, "n2");
            EXPECT_EQ(cfg.peers[1].host, "10.0.0.2");
            EXPECT_EQ(cfg.peers[1].port, 7001);
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
            a.port = 7000;
            a.node_id = "n1";
            a.log_level = "info";

            Config b = a;
            auto changed = diffConfig(a, b);
            EXPECT_TRUE(changed.empty());
        }

        TEST(ConfigTest, DiffConfigSingleChange) {
            Config a;
            a.port = 7000;

            Config b = a;
            b.port = 8080;
            auto changed = diffConfig(a, b);
            ASSERT_EQ(changed.size(), 1);
            EXPECT_EQ(changed[0], "port");
        }

        TEST(ConfigTest, DiffConfigMultipleChanges) {
            Config a;
            a.port = 7000;
            a.log_level = "info";
            a.capacity = 67'108'864;

            Config b = a;
            b.port = 8080;
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
            b.peers.push_back({"n2", "10.0.0.2", 7001});
            auto changed = diffConfig(a, b);
            ASSERT_EQ(changed.size(), 1);
            EXPECT_EQ(changed[0], "peers");
        }

        TEST(ConfigTest, DiffConfigPeersModified) {
            Config a;
            a.peers.push_back({"n2", "10.0.0.2", 7001});

            Config b = a;
            b.peers[0].port = 9090;
            auto changed = diffConfig(a, b);
            ASSERT_EQ(changed.size(), 1);
            EXPECT_EQ(changed[0], "peers");
        }

        TEST(ConfigTest, LoadReplicationMaxHintBytes) {
            test::ScratchFile path_file("cinder_test_hint_bytes", ".yaml");
            const std::string path = path_file.string();
            {
                std::ofstream f(path);
                f << R"(
replication:
  max_hint_bytes: 1048576
)";
            }

            auto result = loadConfig(path);
            ASSERT_TRUE(result.has_value());
            EXPECT_EQ(result->max_hint_bytes, 1'048'576U);
            EXPECT_TRUE(formatConfigJson(*result).contains("\"max_hint_bytes\":1048576"));

            Config modified = *result;
            modified.max_hint_bytes = 2'097'152;
            auto changed = diffConfig(*result, modified);
            ASSERT_EQ(changed.size(), 1);
            EXPECT_EQ(changed[0], "max_hint_bytes");

            // Absent section keeps the compiled default.
            test::ScratchFile empty_path_file("cinder_test_hint_bytes_empty", ".yaml");
            const std::string empty_path = empty_path_file.string();
            {
                std::ofstream f(empty_path);
                f << "";
            }

            auto empty_result = loadConfig(empty_path);
            ASSERT_TRUE(empty_result.has_value());
            EXPECT_EQ(empty_result->max_hint_bytes, 67'108'864U);
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
            test::ScratchFile path_file("cinder_test_format", ".yaml");
            const std::string path = path_file.string();
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
            EXPECT_TRUE(json.contains(R"("node_id":"fmt-node")"));
            EXPECT_TRUE(json.contains("\"port\":9090"));
            EXPECT_TRUE(json.contains("\"capacity\":128"));
            EXPECT_TRUE(json.contains("\"replica_factor\":2"));
            EXPECT_TRUE(json.contains(R"("consistency":"quorum")"));
            EXPECT_TRUE(json.contains("\"ping_interval_ms\":500"));
            EXPECT_TRUE(json.contains(R"("log_level":"debug")"));
            EXPECT_TRUE(json.contains(R"("id":"n2")"));
            EXPECT_TRUE(json.contains(R"("host":"10.0.0.2")"));
        }

        TEST(ConfigTest, FormatConfigJsonEscapesSpecialChars) {
            Config cfg;
            cfg.node_id = "node\"with\\special\nchars";
            auto json = formatConfigJson(cfg);
            EXPECT_TRUE(json.contains("node\\\"with\\\\special\\nchars"));
        }

        TEST(ConfigTest, FormatNodeInfoJsonKeepsAdminSchemaAndEscaping) {
            Config cfg;
            cfg.port = 7001;
            cfg.capacity = 8192;
            cfg.replica_factor = 2;
            cfg.consistency = "quorum";
            cfg.peers.push_back({"peer\"2", "host\\name", 7002});

            const auto json = formatNodeInfoJson("node\"1", cfg, 321, 12);
            EXPECT_EQ(
                json,
                R"({"node_id":"node\"1","port":7001,"capacity_bytes":8192,"current_bytes":321,"current_entries":12,"eviction_policy":"lru","replica_factor":2,"consistency":"quorum","persistence_enabled":false,"anti_entropy_interval_ms":30000,"anti_entropy_buckets":256,"anti_entropy_fetch_bytes":1048576,"anti_entropy_full_rehash_interval_ms":300000,"tls_enabled":false,"auth_enabled":false,"client_auth_enabled":false,"log_level":"info","peers":[{"id":"peer\"2","host":"host\\name","port":7002}]})"
            );
        }

        TEST(ConfigTest, FormatClusterJsonKeepsOrderAndEscaping) {
            const std::vector<NodeInfo> nodes{
                NodeInfo{
                    .id = "node\"1",
                    .host = "host\\one",
                    .port = 7000,
                    .state = NodeState::Alive,
                    .incarnation = 3,
                },
                NodeInfo{
                    .id = "node2",
                    .host = "host2",
                    .port = 7001,
                    .state = NodeState::Suspect,
                    .incarnation = 4,
                },
            };

            EXPECT_EQ(
                formatClusterJson(nodes),
                R"([{"id":"node\"1","host":"host\\one","port":7000,"state":"alive","incarnation":3},{"id":"node2","host":"host2","port":7001,"state":"suspect","incarnation":4}])"
            );
            EXPECT_EQ(formatClusterJson({}), "[]");
        }

        TEST(ConfigTest, FormatRingJsonKeepsSchemaAndEscaping) {
            EXPECT_EQ(
                formatRingJson("node\"1"),
                R"({"self":"node\"1","vnodes_per_node":150,"note":"ring is an immutable snapshot with 150 vnodes per physical node"})"
            );
        }
    } // namespace
} // namespace cinder
