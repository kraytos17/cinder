#include "cinder/common/config.hpp"

#include <format>
#include <fstream>
#include <ranges>
#include <string>
#include <yaml-cpp/yaml.h>

#include "cinder/node/cache_node_server.hpp"

namespace cinder {
    namespace {

        // Escapes a string for JSON embedding. Returns the escaped CONTENT without
        // surrounding quotes — every call site already supplies those via
        // R"("key":")" ... "\"" framing.
        auto escapeJsonString(std::string_view s) -> std::string {
            std::string out;
            out.reserve(s.size());
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

    auto formatConfigJson(const Config& cfg) -> std::string {
        std::string peers;
        for (size_t i = 0; i < cfg.peers.size(); ++i) {
            if (i > 0) {
                peers += ',';
            }

            peers += std::format(
                R"({{"id":"{}","host":"{}","port":{}}})",
                escapeJsonString(cfg.peers[i].id),
                escapeJsonString(cfg.peers[i].host),
                cfg.peers[i].port
            );
        }
        return std::format(
            R"({{"node_id":"{}","port":{},"capacity":{},"replica_factor":{},)"
            R"("consistency":"{}","peers":[{}],)"
            R"("ping_interval_ms":{},"suspect_timeout_ms":{},)"
            R"("gossip_interval_ms":{},"quarantine_interval_ms":{},)"
            R"("rpc_timeout_ms":{},"max_hint_bytes":{},"tombstone_gc_ms":{},)"
            R"("anti_entropy_interval_ms":{},"anti_entropy_buckets":{},)"
            R"("anti_entropy_fetch_bytes":{},"anti_entropy_full_rehash_interval_ms":{},)"
            R"("persistence_enabled":{},"data_dir":"{}",)"
            R"("snapshot_interval_s":{},"max_wal_entries":{},"max_wal_bytes":{},"wal_fsync":{},)"
            R"("tls_enabled":{},"auth_enabled":{},"client_auth_enabled":{},)"
            R"("metrics_port":{},"grpc_port":{},"log_level":"{}","config_path":"{}"}})",
            escapeJsonString(cfg.node_id),
            cfg.port,
            cfg.capacity,
            cfg.replica_factor,
            escapeJsonString(cfg.consistency),
            peers,
            cfg.ping_interval_ms,
            cfg.suspect_timeout_ms,
            cfg.gossip_interval_ms,
            cfg.quarantine_interval_ms,
            cfg.rpc_timeout_ms,
            cfg.max_hint_bytes,
            cfg.tombstone_gc_ms,
            cfg.anti_entropy_interval_ms,
            cfg.anti_entropy_buckets,
            cfg.anti_entropy_fetch_bytes,
            cfg.anti_entropy_full_rehash_interval_ms,
            cfg.persistence_enabled,
            escapeJsonString(cfg.data_dir),
            cfg.snapshot_interval_s,
            cfg.max_wal_entries,
            cfg.max_wal_bytes,
            cfg.wal_fsync,
            cfg.tls.enabled,
            !cfg.auth.shared_secret.empty(),
            !cfg.auth.client_shared_secret.empty(),
            cfg.metrics_port,
            cfg.grpc_port,
            escapeJsonString(cfg.log_level),
            escapeJsonString(cfg.config_path)
        );
    }

    auto diffConfig(const Config& old_cfg, const Config& new_cfg) -> std::vector<std::string> {
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
            for (auto&& [old_peer, new_peer] : std::views::zip(old_cfg.peers, new_cfg.peers)) {
                if (old_peer.id
                    != new_peer.id
                    || old_peer.host
                    != new_peer.host
                    || old_peer.port
                    != new_peer.port) {
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
        if (old_cfg.max_hint_bytes != new_cfg.max_hint_bytes) {
            changed.emplace_back("max_hint_bytes");
        }
        if (old_cfg.tombstone_gc_ms != new_cfg.tombstone_gc_ms) {
            changed.emplace_back("tombstone_gc_ms");
        }
        if (old_cfg.anti_entropy_interval_ms != new_cfg.anti_entropy_interval_ms) {
            changed.emplace_back("anti_entropy_interval_ms");
        }
        if (old_cfg.anti_entropy_buckets != new_cfg.anti_entropy_buckets) {
            changed.emplace_back("anti_entropy_buckets");
        }
        if (old_cfg.anti_entropy_fetch_bytes != new_cfg.anti_entropy_fetch_bytes) {
            changed.emplace_back("anti_entropy_fetch_bytes");
        }
        if (old_cfg.anti_entropy_full_rehash_interval_ms
            != new_cfg.anti_entropy_full_rehash_interval_ms) {
            changed.emplace_back("anti_entropy_full_rehash_interval_ms");
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
        if (old_cfg.max_wal_bytes != new_cfg.max_wal_bytes) {
            changed.emplace_back("max_wal_bytes");
        }
        if (old_cfg.wal_fsync != new_cfg.wal_fsync) {
            changed.emplace_back("wal_fsync");
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
        if (old_cfg.auth.shared_secret != new_cfg.auth.shared_secret) {
            changed.emplace_back("auth_shared_secret");
        }
        if (old_cfg.auth.client_shared_secret != new_cfg.auth.client_shared_secret) {
            changed.emplace_back("auth_client_shared_secret");
        }
        if (old_cfg.log_level != new_cfg.log_level) {
            changed.emplace_back("log_level");
        }
        if (old_cfg.metrics_port != new_cfg.metrics_port) {
            changed.emplace_back("metrics_port");
        }
        if (old_cfg.grpc_port != new_cfg.grpc_port) {
            changed.emplace_back("grpc_port");
        }
        return changed;
    }

    auto loadConfig(const std::string& path) -> Result<Config> {
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
                Error(Errc::InvalidArgument, std::string("YAML parse error: ") + e.what())
            );
        }

        // Server
        if (auto server = root["server"]) {
            cfg.node_id = server["node_id"].as<std::string>(cfg.node_id);
            cfg.port = server["port"].as<uint16_t>(cfg.port);
            cfg.capacity = server["capacity"].as<size_t>(cfg.capacity);
            cfg.rpc_timeout_ms = server["rpc_timeout_ms"].as<int>(cfg.rpc_timeout_ms);
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
            cfg.anti_entropy_fetch_bytes =
                ae["fetch_bytes"].as<size_t>(cfg.anti_entropy_fetch_bytes);
            cfg.anti_entropy_full_rehash_interval_ms =
                ae["full_rehash_interval_ms"].as<int>(cfg.anti_entropy_full_rehash_interval_ms);
        }
        // Hinted handoff
        if (auto repl = root["replication"]) {
            cfg.max_hint_bytes = repl["max_hint_bytes"].as<size_t>(cfg.max_hint_bytes);
            cfg.tombstone_gc_ms = repl["tombstone_gc_ms"].as<int>(cfg.tombstone_gc_ms);
        }
        // Persistence
        if (auto pers = root["persistence"]) {
            cfg.persistence_enabled = pers["enabled"].as<bool>(cfg.persistence_enabled);
            cfg.data_dir = pers["data_dir"].as<std::string>(cfg.data_dir);
            cfg.snapshot_interval_s = pers["snapshot_interval_s"].as<int>(cfg.snapshot_interval_s);
            cfg.max_wal_entries = pers["max_wal_entries"].as<int>(cfg.max_wal_entries);
            cfg.max_wal_bytes = pers["max_wal_bytes"].as<size_t>(cfg.max_wal_bytes);
            cfg.wal_fsync = pers["wal_fsync"].as<bool>(cfg.wal_fsync);
        }
        // TLS
        if (auto tls = root["tls"]) {
            cfg.tls.enabled = tls["enabled"].as<bool>(cfg.tls.enabled);
            cfg.tls.cert_file = tls["cert_file"].as<std::string>(cfg.tls.cert_file);
            cfg.tls.key_file = tls["key_file"].as<std::string>(cfg.tls.key_file);
            cfg.tls.ca_file = tls["ca_file"].as<std::string>(cfg.tls.ca_file);
        }
        // Auth
        if (auto auth = root["auth"]) {
            cfg.auth.shared_secret = auth["shared_secret"].as<std::string>(cfg.auth.shared_secret);
            cfg.auth.client_shared_secret =
                auth["client_shared_secret"].as<std::string>(cfg.auth.client_shared_secret);
        }
        // Logging
        if (auto log = root["logging"]) {
            cfg.log_level = log["level"].as<std::string>(cfg.log_level);
        }
        // gRPC
        if (auto grpc = root["grpc"]) {
            cfg.grpc_port = grpc["port"].as<uint16_t>(cfg.grpc_port);
        }
        return cfg;
    }

    auto parsePeersString(const std::string& peers, Config& cfg) -> size_t {
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

    auto logLevelFromString(std::string_view str) -> LogLevel {
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

    static auto nodeStateToString(NodeState state) -> const char* {
        switch (state) {
            case NodeState::Alive:
                return "alive";
            case NodeState::Suspect:
                return "suspect";
            case NodeState::Dead:
                return "dead";
        }
        return "unknown";
    }

    auto formatNodeInfoJson(
        const NodeId& node_id,
        const Config& config,
        size_t store_size,
        size_t store_entries
    ) -> std::string {
        std::string peers;
        for (auto&& [index, peer] : std::views::enumerate(config.peers)) {
            if (index != 0) {
                peers += ',';
            }

            peers += std::format(
                R"({{"id":"{}","host":"{}","port":{}}})",
                escapeJsonString(peer.id),
                escapeJsonString(peer.host),
                peer.port
            );
        }
        return std::format(
            R"({{"node_id":"{}","port":{},"capacity_bytes":{},"current_bytes":{},"current_entries":{},)"
            R"("eviction_policy":"lru","replica_factor":{},"consistency":"{}",)"
            R"("persistence_enabled":{},"anti_entropy_interval_ms":{},)"
            R"("anti_entropy_buckets":{},"anti_entropy_fetch_bytes":{},)"
            R"("anti_entropy_full_rehash_interval_ms":{},"tls_enabled":{},)"
            R"("auth_enabled":{},"client_auth_enabled":{},"log_level":"{}","peers":[{}]}})",
            escapeJsonString(node_id),
            config.port,
            config.capacity,
            store_size,
            store_entries,
            config.replica_factor,
            escapeJsonString(config.consistency),
            config.persistence_enabled,
            config.anti_entropy_interval_ms,
            config.anti_entropy_buckets,
            config.anti_entropy_fetch_bytes,
            config.anti_entropy_full_rehash_interval_ms,
            config.tls.enabled,
            !config.auth.shared_secret.empty(),
            !config.auth.client_shared_secret.empty(),
            escapeJsonString(config.log_level),
            peers
        );
    }

    auto formatClusterJson(const std::vector<NodeInfo>& nodes) -> std::string {
        std::string entries;
        for (auto&& [index, node] : std::views::enumerate(nodes)) {
            if (index != 0) {
                entries += ',';
            }

            entries += std::format(
                R"({{"id":"{}","host":"{}","port":{},"state":"{}","incarnation":{}}})",
                escapeJsonString(node.id),
                escapeJsonString(node.host),
                node.port,
                nodeStateToString(node.state),
                node.incarnation
            );
        }
        return std::format("[{}]", entries);
    }

    auto formatRingJson(const NodeId& self) -> std::string {
        return std::format(
            R"({{"self":"{}","vnodes_per_node":150,"note":"ring is an immutable snapshot with 150 vnodes per physical node"}})",
            escapeJsonString(self)
        );
    }
} // namespace cinder
