#pragma once

#include <cstdint>
#include <string>
#include <vector>

#include "cinder/client/connection_pool.hpp"
#include "cinder/common/logger.hpp"
#include "cinder/common/status.hpp"

namespace cinder {

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

    // Persistence
    bool persistence_enabled = false;
    std::string data_dir;
    int snapshot_interval_s = 60;
    int max_wal_entries = 10'000;

    // Logging
    std::string log_level = "info";
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
} // namespace cinder
