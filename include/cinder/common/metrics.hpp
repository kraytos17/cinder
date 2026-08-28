#pragma once

#include <atomic>
#include <cstdint>
#include <string>
#include <vector>

namespace cinder {

// Per-opcode operation counters.
struct OpcodeMetrics {
    std::atomic<uint64_t> gets{0};
    std::atomic<uint64_t> sets{0};
    std::atomic<uint64_t> dels{0};
    std::atomic<uint64_t> pings{0};
    std::atomic<uint64_t> replicates{0};
    std::atomic<uint64_t> hints{0};
    std::atomic<uint64_t> gets_versioned{0};
};

// Per-node store metrics.
struct ShardMetrics {
    std::atomic<uint64_t> hits{0};
    std::atomic<uint64_t> misses{0};
    std::atomic<uint64_t> writes{0};
    std::atomic<uint64_t> deletes{0};
    std::atomic<uint64_t> evictions_ttl{0};
    std::atomic<uint64_t> evictions_capacity{0};
    std::atomic<uint64_t> expires_on_read{0};
    std::atomic<size_t> current_bytes{0};
    std::atomic<size_t> current_entries{0};
    std::atomic<size_t> capacity_bytes{0};
};

// Replication metrics.
struct ReplicationMetrics {
    std::atomic<uint64_t> async_writes{0};
    std::atomic<uint64_t> quorum_writes_ok{0};
    std::atomic<uint64_t> quorum_writes_failed{0};
    std::atomic<uint64_t> quorum_reads{0};
    std::atomic<uint64_t> read_repairs{0};
    std::atomic<uint64_t> hints_enqueued{0};
    std::atomic<uint64_t> hints_replayed{0};
    std::atomic<uint64_t> replica_unreachable{0};

    struct ReplicaLag {
        std::string node_id;
        uint64_t last_ack_epoch_ms{0};
    };

    std::vector<ReplicaLag> replica_lags;
};

// Cluster metrics.
struct ClusterMetrics {
    std::atomic<uint64_t> gossip_rounds{0};
    std::atomic<uint64_t> probe_sent{0};
    std::atomic<uint64_t> probe_received{0};
    std::atomic<uint64_t> suspect_marked{0};
    std::atomic<uint64_t> dead_marked{0};
    std::atomic<uint64_t> alive_marked{0};
    std::atomic<uint64_t> refuted_self_rumors{0};
    std::atomic<uint64_t> rebalance_copies{0};
    std::atomic<uint64_t> rebalance_migrations{0};
};

// Connection metrics.
struct ConnectionMetrics {
    std::atomic<uint64_t> connections_opened{0};
    std::atomic<uint64_t> connections_closed{0};
    std::atomic<uint64_t> decode_failures{0};
    std::atomic<uint64_t> redirects{0};
};

class MetricsCollector {
  public:

    MetricsCollector() = default;
    ~MetricsCollector() = default;

    MetricsCollector(const MetricsCollector&) = delete;
    auto operator=(const MetricsCollector&) -> MetricsCollector& = delete;
    MetricsCollector(MetricsCollector&&) = delete;
    auto operator=(MetricsCollector&&) -> MetricsCollector& = delete;

    auto opcodeMetrics() -> OpcodeMetrics& { return opcode_; }

    auto shardMetrics() -> ShardMetrics& { return shard_; }

    auto replicationMetrics() -> ReplicationMetrics& { return repl_; }

    auto clusterMetrics() -> ClusterMetrics& { return cluster_; }

    auto connectionMetrics() -> ConnectionMetrics& { return conn_; }

    auto opcodeMetrics() const -> const OpcodeMetrics& { return opcode_; }

    auto shardMetrics() const -> const ShardMetrics& { return shard_; }

    auto replicationMetrics() const -> const ReplicationMetrics& { return repl_; }

    auto clusterMetrics() const -> const ClusterMetrics& { return cluster_; }

    auto connectionMetrics() const -> const ConnectionMetrics& { return conn_; }

    // Generate Prometheus text exposition format.
    [[nodiscard]] auto formatPrometheus() const -> std::string;

  private:

    OpcodeMetrics opcode_;
    ShardMetrics shard_;
    ReplicationMetrics repl_;
    ClusterMetrics cluster_;
    ConnectionMetrics conn_;
};
} // namespace cinder
