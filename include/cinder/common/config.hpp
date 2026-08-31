#pragma once

#include <cstdint>
#include <string>
#include <vector>

#include "cinder/common/cluster_config.hpp"
#include "cinder/common/logger.hpp"
#include "cinder/common/status.hpp"

namespace cinder {

struct TlsConfig {
    bool enabled = false;
    std::string cert_file;
    std::string key_file;
    std::string ca_file;
};

struct Config {
    // Server
    std::string node_id = "node1";
    uint16_t port = 7'000;
    size_t capacity = 67'108'864;
    int replica_factor = 1;
    std::string consistency = "async";

    // Cluster
    std::vector<ClusterConfig::NodeConfig> peers;

    // Failure detector / gossip
    int ping_interval_ms = 1'000;
    int suspect_timeout_ms = 3'000;
    int gossip_interval_ms = 1'000;
    int quarantine_interval_ms = 10'000;

    // RPC
    int rpc_timeout_ms = 5'000;

    // Persistence
    bool persistence_enabled = false;
    std::string data_dir;
    int snapshot_interval_s = 60;
    int max_wal_entries = 10'000;

    // TLS
    TlsConfig tls;
    // Logging
    std::string log_level = "info";
    // Config file path (empty = no file, hot-reload disabled)
    std::string config_path;
    // Metrics HTTP port (0 = disabled)
    uint16_t metrics_port = 0;
};

// Load configuration from a YAML file. Missing fields use defaults.
auto
loadConfig(const std::string& path) -> Result<Config>;

// Parse a comma-separated peers string ("id@host:port,...") into
// Config::peers. Returns the count of valid peers parsed.
auto
parsePeersString(const std::string& peers, Config& cfg) -> size_t;

// Convert a log level string to LogLevel. Defaults to Info on bad input.
auto
logLevelFromString(std::string_view str) -> LogLevel;

// Produce a JSON representation of the current running config.
[[nodiscard]] auto
formatConfigJson(const Config& cfg) -> std::string;

// Compare old vs new config, return names of fields that changed.
[[nodiscard]] auto
diffConfig(const Config& old_cfg, const Config& new_cfg) -> std::vector<std::string>;
} // namespace cinder
