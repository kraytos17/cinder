#include "cinder/common/config.hpp"

#include <fstream>
#include <string>
#include <yaml-cpp/yaml.h>

#include "cinder/node/cache_node_server.hpp"

namespace cinder {
namespace {

auto
escapeJsonString(std::string_view s) -> std::string {
    std::string out;
    out.reserve(s.size() + 2);
    for (char c : s) {
        switch (c) {
            case '"':
                out += "\\\"";
                break;
            case '\\':
                out += "\\\\";
                break;
            case '\n':
                out += "\\n";
                break;
            case '\r':
                out += "\\r";
                break;
            case '\t':
                out += "\\t";
                break;
            default:
                out += c;
                break;
        }
    }
    return out;
}
} // namespace

auto
formatConfigJson(const Config& cfg) -> std::string {
    std::string out = "{";
    out += R"("node_id":")" + escapeJsonString(cfg.node_id) + "\",";
    out += "\"port\":" + std::to_string(cfg.port) + ",";
    out += "\"capacity\":" + std::to_string(cfg.capacity) + ",";
    out += "\"replica_factor\":" + std::to_string(cfg.replica_factor) + ",";
    out += R"("consistency":")" + escapeJsonString(cfg.consistency) + "\",";
    out += "\"peers\":[";
    for (size_t i = 0; i < cfg.peers.size(); ++i) {
        if (i > 0) {
            out += ",";
        }

        out += R"({"id":")" + escapeJsonString(cfg.peers[i].id) + "\",";
        out += R"("host":")" + escapeJsonString(cfg.peers[i].host) + "\",";
        out += "\"port\":" + std::to_string(cfg.peers[i].port) + "}";
    }

    out += "],";
    out += "\"ping_interval_ms\":" + std::to_string(cfg.ping_interval_ms) + ",";
    out += "\"suspect_timeout_ms\":" + std::to_string(cfg.suspect_timeout_ms) + ",";
    out += "\"gossip_interval_ms\":" + std::to_string(cfg.gossip_interval_ms) + ",";
    out += "\"quarantine_interval_ms\":" + std::to_string(cfg.quarantine_interval_ms) + ",";
    out += "\"rpc_timeout_ms\":" + std::to_string(cfg.rpc_timeout_ms) + ",";
    out += "\"anti_entropy_interval_ms\":" + std::to_string(cfg.anti_entropy_interval_ms) + ",";
    out += "\"anti_entropy_buckets\":" + std::to_string(cfg.anti_entropy_buckets) + ",";
    out +=
        "\"persistence_enabled\":" + std::string(cfg.persistence_enabled ? "true" : "false") + ",";

    out += R"("data_dir":")" + escapeJsonString(cfg.data_dir) + "\",";
    out += "\"snapshot_interval_s\":" + std::to_string(cfg.snapshot_interval_s) + ",";
    out += "\"max_wal_entries\":" + std::to_string(cfg.max_wal_entries) + ",";
    out += "\"tls_enabled\":" + std::string(cfg.tls.enabled ? "true" : "false") + ",";
    out += "\"metrics_port\":" + std::to_string(cfg.metrics_port) + ",";
    out += R"("log_level":")" + escapeJsonString(cfg.log_level) + "\",";
    out += R"("config_path":")" + escapeJsonString(cfg.config_path) + "\"";
    out += "}";
    return out;
}

auto
diffConfig(const Config& old_cfg, const Config& new_cfg) -> std::vector<std::string> {
    std::vector<std::string> changed;
    if (old_cfg.node_id != new_cfg.node_id) {
        changed.emplace_back("node_id");
    }
    if (old_cfg.port != new_cfg.port) {
        changed.emplace_back("port");
    }
    if (old_cfg.capacity != new_cfg.capacity) {
        changed.emplace_back("capacity");
    }
    if (old_cfg.replica_factor != new_cfg.replica_factor) {
        changed.emplace_back("replica_factor");
    }
    if (old_cfg.consistency != new_cfg.consistency) {
        changed.emplace_back("consistency");
    }
    if (old_cfg.peers.size() != new_cfg.peers.size()) {
        changed.emplace_back("peers");
    } else {
        bool peers_changed = false;
        for (size_t i = 0; i < old_cfg.peers.size(); ++i) {
            if (old_cfg.peers[i].id != new_cfg.peers[i].id
                || old_cfg.peers[i].host != new_cfg.peers[i].host
                || old_cfg.peers[i].port != new_cfg.peers[i].port) {
                peers_changed = true;
                break;
            }
        }
        if (peers_changed) {
            changed.emplace_back("peers");
        }
    }
    if (old_cfg.ping_interval_ms != new_cfg.ping_interval_ms) {
        changed.emplace_back("ping_interval_ms");
    }
    if (old_cfg.suspect_timeout_ms != new_cfg.suspect_timeout_ms) {
        changed.emplace_back("suspect_timeout_ms");
    }
    if (old_cfg.gossip_interval_ms != new_cfg.gossip_interval_ms) {
        changed.emplace_back("gossip_interval_ms");
    }
    if (old_cfg.quarantine_interval_ms != new_cfg.quarantine_interval_ms) {
        changed.emplace_back("quarantine_interval_ms");
    }
    if (old_cfg.rpc_timeout_ms != new_cfg.rpc_timeout_ms) {
        changed.emplace_back("rpc_timeout_ms");
    }
    if (old_cfg.anti_entropy_interval_ms != new_cfg.anti_entropy_interval_ms) {
        changed.emplace_back("anti_entropy_interval_ms");
    }
    if (old_cfg.anti_entropy_buckets != new_cfg.anti_entropy_buckets) {
        changed.emplace_back("anti_entropy_buckets");
    }
    if (old_cfg.persistence_enabled != new_cfg.persistence_enabled) {
        changed.emplace_back("persistence_enabled");
    }
    if (old_cfg.data_dir != new_cfg.data_dir) {
        changed.emplace_back("data_dir");
    }
    if (old_cfg.snapshot_interval_s != new_cfg.snapshot_interval_s) {
        changed.emplace_back("snapshot_interval_s");
    }
    if (old_cfg.max_wal_entries != new_cfg.max_wal_entries) {
        changed.emplace_back("max_wal_entries");
    }
    if (old_cfg.tls.enabled != new_cfg.tls.enabled) {
        changed.emplace_back("tls_enabled");
    }
    if (old_cfg.tls.cert_file != new_cfg.tls.cert_file) {
        changed.emplace_back("tls_cert_file");
    }
    if (old_cfg.tls.key_file != new_cfg.tls.key_file) {
        changed.emplace_back("tls_key_file");
    }
    if (old_cfg.tls.ca_file != new_cfg.tls.ca_file) {
        changed.emplace_back("tls_ca_file");
    }
    if (old_cfg.log_level != new_cfg.log_level) {
        changed.emplace_back("log_level");
    }
    if (old_cfg.metrics_port != new_cfg.metrics_port) {
        changed.emplace_back("metrics_port");
    }
    return changed;
}

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
        cfg.metrics_port = server["metrics_port"].as<uint16_t>(cfg.metrics_port);
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
    // Anti-entropy
    if (auto ae = root["anti_entropy"]) {
        cfg.anti_entropy_interval_ms = ae["interval_ms"].as<int>(cfg.anti_entropy_interval_ms);
        cfg.anti_entropy_buckets = ae["buckets"].as<int>(cfg.anti_entropy_buckets);
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
