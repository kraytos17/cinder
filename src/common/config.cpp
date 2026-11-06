#include "cinder/common/config.hpp"

#include <format>
#include <fstream>
#include <ranges>
#include <string>
#include <yaml-cpp/yaml.h>

#include "cinder/node/cache_node_server.hpp"

namespace cinder {
    namespace {
        using std::chrono::milliseconds;

        // Optional YAML key: assign node[key] into field, keeping the current
        // value when the key is absent. Type is deduced from the field, so
        // call sites never repeat it: loadOr(sec, "port", cfg.port).
        template <typename T> void loadOr(const YAML::Node& node, const char* key, T& field) {
            if (const auto child = node[key]) {
                field = child.template as<T>(field);
            }
        }

        // Durations serialize as integer milliseconds in YAML.
        void loadOr(const YAML::Node& node, const char* key, milliseconds& field) {
            int ms = static_cast<int>(field.count());
            loadOr(node, key, ms);
            field = milliseconds{ms};
        }

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
            R"("snapshot_interval_s":{},"max_wal_entries":{},"max_wal_bytes":{},"max_wal_segment_bytes":{},"wal_fsync":{},)"
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
            cfg.anti_entropy.interval.count(),
            cfg.anti_entropy.buckets,
            cfg.anti_entropy.fetch_bytes,
            cfg.anti_entropy.full_rehash_interval.count(),
            cfg.persistence_enabled,
            escapeJsonString(cfg.data_dir),
            cfg.persistence.snapshot_interval_s,
            cfg.persistence.max_wal_entries,
            cfg.persistence.max_wal_bytes,
            cfg.persistence.max_wal_segment_bytes,
            cfg.persistence.wal_fsync,
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
        const auto check = [&](std::string_view name, const auto& old_v, const auto& new_v) {
            if (old_v != new_v) {
                changed.emplace_back(name);
            }
        };

        check("node_id", old_cfg.node_id, new_cfg.node_id);
        check("port", old_cfg.port, new_cfg.port);
        check("capacity", old_cfg.capacity, new_cfg.capacity);
        check("replica_factor", old_cfg.replica_factor, new_cfg.replica_factor);
        check("consistency", old_cfg.consistency, new_cfg.consistency);
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
        check("ping_interval_ms", old_cfg.ping_interval_ms, new_cfg.ping_interval_ms);
        check("suspect_timeout_ms", old_cfg.suspect_timeout_ms, new_cfg.suspect_timeout_ms);
        check("gossip_interval_ms", old_cfg.gossip_interval_ms, new_cfg.gossip_interval_ms);
        check(
            "quarantine_interval_ms",
            old_cfg.quarantine_interval_ms,
            new_cfg.quarantine_interval_ms
        );

        check("rpc_timeout_ms", old_cfg.rpc_timeout_ms, new_cfg.rpc_timeout_ms);
        check("max_hint_bytes", old_cfg.max_hint_bytes, new_cfg.max_hint_bytes);
        check("tombstone_gc_ms", old_cfg.tombstone_gc_ms, new_cfg.tombstone_gc_ms);
        check(
            "anti_entropy_interval_ms",
            old_cfg.anti_entropy.interval,
            new_cfg.anti_entropy.interval
        );

        check("anti_entropy_buckets", old_cfg.anti_entropy.buckets, new_cfg.anti_entropy.buckets);
        check(
            "anti_entropy_fetch_bytes",
            old_cfg.anti_entropy.fetch_bytes,
            new_cfg.anti_entropy.fetch_bytes
        );

        check(
            "anti_entropy_full_rehash_interval_ms",
            old_cfg.anti_entropy.full_rehash_interval,
            new_cfg.anti_entropy.full_rehash_interval
        );

        check("persistence_enabled", old_cfg.persistence_enabled, new_cfg.persistence_enabled);
        check("data_dir", old_cfg.data_dir, new_cfg.data_dir);
        check(
            "snapshot_interval_s",
            old_cfg.persistence.snapshot_interval_s,
            new_cfg.persistence.snapshot_interval_s
        );

        check(
            "max_wal_entries",
            old_cfg.persistence.max_wal_entries,
            new_cfg.persistence.max_wal_entries
        );

        check(
            "max_wal_bytes",
            old_cfg.persistence.max_wal_bytes,
            new_cfg.persistence.max_wal_bytes
        );
        check(
            "max_wal_segment_bytes",
            old_cfg.persistence.max_wal_segment_bytes,
            new_cfg.persistence.max_wal_segment_bytes
        );

        check("wal_fsync", old_cfg.persistence.wal_fsync, new_cfg.persistence.wal_fsync);
        check("tls_enabled", old_cfg.tls.enabled, new_cfg.tls.enabled);
        check("tls_cert_file", old_cfg.tls.cert_file, new_cfg.tls.cert_file);
        check("tls_key_file", old_cfg.tls.key_file, new_cfg.tls.key_file);
        check("tls_ca_file", old_cfg.tls.ca_file, new_cfg.tls.ca_file);
        check("auth_shared_secret", old_cfg.auth.shared_secret, new_cfg.auth.shared_secret);
        check(
            "auth_client_shared_secret",
            old_cfg.auth.client_shared_secret,
            new_cfg.auth.client_shared_secret
        );

        check("log_level", old_cfg.log_level, new_cfg.log_level);
        check("metrics_port", old_cfg.metrics_port, new_cfg.metrics_port);
        check("grpc_port", old_cfg.grpc_port, new_cfg.grpc_port);
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
            loadOr(server, "node_id", cfg.node_id);
            loadOr(server, "port", cfg.port);
            loadOr(server, "capacity", cfg.capacity);
            loadOr(server, "rpc_timeout_ms", cfg.rpc_timeout_ms);
            loadOr(server, "replication_factor", cfg.replica_factor);
            loadOr(server, "consistency", cfg.consistency);
            loadOr(server, "metrics_port", cfg.metrics_port);
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
            loadOr(fd, "ping_interval_ms", cfg.ping_interval_ms);
            loadOr(fd, "suspect_timeout_ms", cfg.suspect_timeout_ms);
            loadOr(fd, "gossip_interval_ms", cfg.gossip_interval_ms);
            loadOr(fd, "quarantine_interval_ms", cfg.quarantine_interval_ms);
        }
        // Anti-entropy
        if (auto ae = root["anti_entropy"]) {
            loadOr(ae, "interval_ms", cfg.anti_entropy.interval);
            loadOr(ae, "buckets", cfg.anti_entropy.buckets);
            loadOr(ae, "fetch_bytes", cfg.anti_entropy.fetch_bytes);
            loadOr(ae, "full_rehash_interval_ms", cfg.anti_entropy.full_rehash_interval);
        }
        // Hinted handoff
        if (auto repl = root["replication"]) {
            loadOr(repl, "max_hint_bytes", cfg.max_hint_bytes);
            loadOr(repl, "tombstone_gc_ms", cfg.tombstone_gc_ms);
        }
        // Persistence
        if (auto pers = root["persistence"]) {
            loadOr(pers, "enabled", cfg.persistence_enabled);
            loadOr(pers, "data_dir", cfg.data_dir);
            loadOr(pers, "snapshot_interval_s", cfg.persistence.snapshot_interval_s);
            loadOr(pers, "max_wal_entries", cfg.persistence.max_wal_entries);
            loadOr(pers, "max_wal_bytes", cfg.persistence.max_wal_bytes);
            loadOr(pers, "max_wal_segment_bytes", cfg.persistence.max_wal_segment_bytes);
            loadOr(pers, "wal_fsync", cfg.persistence.wal_fsync);
        }
        // TLS
        if (auto tls = root["tls"]) {
            loadOr(tls, "enabled", cfg.tls.enabled);
            loadOr(tls, "cert_file", cfg.tls.cert_file);
            loadOr(tls, "key_file", cfg.tls.key_file);
            loadOr(tls, "ca_file", cfg.tls.ca_file);
        }
        // Auth
        if (auto auth = root["auth"]) {
            loadOr(auth, "shared_secret", cfg.auth.shared_secret);
            loadOr(auth, "client_shared_secret", cfg.auth.client_shared_secret);
        }
        // Logging
        if (auto log = root["logging"]) {
            loadOr(log, "level", cfg.log_level);
        }
        // gRPC
        if (auto grpc = root["grpc"]) {
            loadOr(grpc, "port", cfg.grpc_port);
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
            config.anti_entropy.interval.count(),
            config.anti_entropy.buckets,
            config.anti_entropy.fetch_bytes,
            config.anti_entropy.full_rehash_interval.count(),
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
