#include "cinder/common/config.hpp"

#include <fstream>
#include <string>
#include <yaml-cpp/yaml.h>

#include "cinder/node/cache_node_server.hpp"

namespace cinder {

auto
loadConfig(const std::string& path) -> Result<Config> {
    Config cfg;
    std::ifstream file(path);
    if (!file.is_open()) {
        return err<Config>(Error(Errc::NotFound, "cannot open config file: " + path));
    }

    YAML::Node root;
    try {
        root = YAML::Load(file);
    } catch (const YAML::Exception& e) {
        return err<Config>(
            Error(Errc::InvalidArgument, std::string("YAML parse error: ") + e.what()));
    }

    // Server
    if (auto server = root["server"]) {
        cfg.node_id = server["node_id"].as<std::string>(cfg.node_id);
        cfg.port = server["port"].as<uint16_t>(cfg.port);
        cfg.capacity = server["capacity"].as<size_t>(cfg.capacity);
        cfg.replica_factor = server["replication_factor"].as<int>(cfg.replica_factor);
        cfg.consistency = server["consistency"].as<std::string>(cfg.consistency);
    }
    // Cluster peers
    if (auto cluster = root["cluster"]) {
        if (auto peers_node = cluster["peers"]) {
            for (const auto& peer : peers_node) {
                ClusterConfig::NodeConfig nc;
                nc.id = peer["id"].as<std::string>();
                nc.host = peer["host"].as<std::string>();
                nc.port = peer["port"].as<uint16_t>();
                cfg.peers.push_back(nc);
            }
        }
    }
    // Failure detector
    if (auto fd = root["failure_detector"]) {
        cfg.ping_interval_ms = fd["ping_interval_ms"].as<int>(cfg.ping_interval_ms);
        cfg.suspect_timeout_ms = fd["suspect_timeout_ms"].as<int>(cfg.suspect_timeout_ms);
        cfg.gossip_interval_ms = fd["gossip_interval_ms"].as<int>(cfg.gossip_interval_ms);
        cfg.quarantine_interval_ms =
            fd["quarantine_interval_ms"].as<int>(cfg.quarantine_interval_ms);
    }
    // Persistence
    if (auto pers = root["persistence"]) {
        cfg.persistence_enabled = pers["enabled"].as<bool>(cfg.persistence_enabled);
        cfg.data_dir = pers["data_dir"].as<std::string>(cfg.data_dir);
        cfg.snapshot_interval_s = pers["snapshot_interval_s"].as<int>(cfg.snapshot_interval_s);
        cfg.max_wal_entries = pers["max_wal_entries"].as<int>(cfg.max_wal_entries);
    }
    // TLS
    if (auto tls = root["tls"]) {
        cfg.tls.enabled = tls["enabled"].as<bool>(cfg.tls.enabled);
        cfg.tls.cert_file = tls["cert_file"].as<std::string>(cfg.tls.cert_file);
        cfg.tls.key_file = tls["key_file"].as<std::string>(cfg.tls.key_file);
        cfg.tls.ca_file = tls["ca_file"].as<std::string>(cfg.tls.ca_file);
    }
    // Logging
    if (auto log = root["logging"]) {
        cfg.log_level = log["level"].as<std::string>(cfg.log_level);
    }
    return cfg;
}

auto
parsePeersString(const std::string& peers, Config& cfg) -> size_t {
    size_t count = 0;
    size_t start = 0;
    while (start < peers.size()) {
        auto end = peers.find(',', start);
        auto peer = peers.substr(start, end - start);
        ClusterConfig::NodeConfig nc;
        if (parsePeer(peer, nc)) {
            cfg.peers.push_back(nc);
            ++count;
        }
        if (end == std::string::npos) {
            break;
        }
        start = end + 1;
    }
    return count;
}

auto
logLevelFromString(std::string_view str) -> LogLevel {
    if (str == "trace") {
        return LogLevel::Trace;
    }
    if (str == "debug") {
        return LogLevel::Debug;
    }
    if (str == "warn") {
        return LogLevel::Warn;
    }
    if (str == "error") {
        return LogLevel::Error;
    }
    return LogLevel::Info;
}
} // namespace cinder
