#pragma once

#include <array>
#include <atomic>
#include <bit>
#include <cstdint>
#include <cstring>
#include <string>
#include <vector>

namespace cinder {

// Cache line size for alignment. Using 64 bytes (x86-64 standard).
inline constexpr size_t K_CACHE_LINE = 64;

// Lock-free log2-bucketed latency histogram. Bucket i counts samples with
// duration in [2^i, 2^(i+1)) ns; the last bucket also absorbs everything
// above it. Percentiles are estimated from the cumulative bucket counts.
//
// Buckets are grouped into cache-line-aligned slabs of 8 to reduce false
// sharing between concurrent recorders hitting adjacent buckets.
struct LatencyHistogram {
    static constexpr size_t K_NUM_BUCKETS = 40; // 1 ns .. ~2^40 ns (~18 min)
    static constexpr size_t K_BUCKETS_PER_SLAB = 8;
    static constexpr size_t K_NUM_SLABS = K_NUM_BUCKETS / K_BUCKETS_PER_SLAB; // 5

    // Each slab holds 8 atomic counters on a single cache line.
    struct alignas(K_CACHE_LINE) BucketSlab {
        std::array<std::atomic<uint64_t>, K_BUCKETS_PER_SLAB> buckets{};
    };

    std::array<BucketSlab, K_NUM_SLABS> slabs{};
    alignas(K_CACHE_LINE) std::atomic<uint64_t> count{0};
    alignas(K_CACHE_LINE) std::atomic<uint64_t> total_ns{0};

    void record(uint64_t ns) {
        size_t idx = 0;
        if (ns > 0) {
            idx = static_cast<size_t>(63 - std::countl_zero(ns));
            if (idx >= K_NUM_BUCKETS) {
                idx = K_NUM_BUCKETS - 1;
            }
        }

        slabs[idx / K_BUCKETS_PER_SLAB].buckets[idx % K_BUCKETS_PER_SLAB].fetch_add(
            1, std::memory_order_relaxed);
        count.fetch_add(1, std::memory_order_relaxed);
        total_ns.fetch_add(ns, std::memory_order_relaxed);
    }

    // Estimated p* latency (bucket upper bound in ns), or 0 when empty.
    [[nodiscard]] auto percentile(double q) const -> uint64_t {
        uint64_t c = count.load(std::memory_order_relaxed);
        if (c == 0) {
            return 0;
        }

        uint64_t target = static_cast<uint64_t>(q * static_cast<double>(c)) + 1;
        uint64_t cum = 0;
        for (size_t s = 0; s < K_NUM_SLABS; ++s) {
            for (size_t b = 0; b < K_BUCKETS_PER_SLAB; ++b) {
                cum += slabs[s].buckets[b].load(std::memory_order_relaxed);
                size_t idx = s * K_BUCKETS_PER_SLAB + b;
                if (cum >= target) {
                    return (idx == K_NUM_BUCKETS - 1) ? ~uint64_t{0} : (uint64_t{1} << (idx + 1));
                }
            }
        }
        return ~uint64_t{0};
    }

    // Upper bound of bucket `i` in ns; the last bucket has no finite bound.
    [[nodiscard]] static constexpr auto upperBoundNs(size_t i) -> uint64_t {
        return (i == K_NUM_BUCKETS - 1) ? ~uint64_t{0} : (uint64_t{1} << (i + 1));
    }
};

struct OpcodeMetrics {
    // Client opcodes: GET, SET, DEL, PING — one increment per request.
    struct alignas(K_CACHE_LINE) ClientCounters {
        std::atomic<uint64_t> gets{0};
        std::atomic<uint64_t> sets{0};
        std::atomic<uint64_t> dels{0};
        std::atomic<uint64_t> pings{0};
    };

    // Replication opcodes: REPLICATE, HINT, GET_VERSIONED, ANTI_ENTROPY_*.
    struct alignas(K_CACHE_LINE) ReplicationCounters {
        std::atomic<uint64_t> replicates{0};
        std::atomic<uint64_t> hints{0};
        std::atomic<uint64_t> gets_versioned{0};
        std::atomic<uint64_t> anti_entropy_digest{0};
        std::atomic<uint64_t> anti_entropy_sync{0};
    };

    // Admin opcodes: ADMIN_INFO, ADMIN_CLUSTER, etc. (rare).
    struct alignas(K_CACHE_LINE) AdminCounters {
        std::atomic<uint64_t> info{0};
        std::atomic<uint64_t> cluster{0};
        std::atomic<uint64_t> ring{0};
        std::atomic<uint64_t> compact{0};
        std::atomic<uint64_t> config_reload{0};
        std::atomic<uint64_t> shutdown{0};
    };

    ClientCounters client{};
    ReplicationCounters repl{};
    AdminCounters admin{};
    std::array<LatencyHistogram, 16> latency{};

    void recordLatency(uint8_t raw_opcode, uint64_t ns) {
        size_t idx = static_cast<size_t>(raw_opcode) - 1;
        if (idx < latency.size()) {
            latency[idx].record(ns);
        }
    }
};

struct ShardMetrics {
    // Live counters: hits/misses/writes/deletes/bytes/entries — updated on every request.
    struct alignas(K_CACHE_LINE) LiveCounters {
        std::atomic<uint64_t> hits{0};
        std::atomic<uint64_t> misses{0};
        std::atomic<uint64_t> writes{0};
        std::atomic<uint64_t> deletes{0};
        std::atomic<size_t> current_bytes{0};
        std::atomic<size_t> current_entries{0};
    };

    // Capacity counters: evictions/expires/capacity — rarely written.
    struct alignas(K_CACHE_LINE) CapacityCounters {
        std::atomic<uint64_t> evictions_ttl{0};
        std::atomic<uint64_t> evictions_capacity{0};
        std::atomic<uint64_t> expires_on_read{0};
        std::atomic<size_t> capacity_bytes{0};
    };

    LiveCounters live{};
    CapacityCounters cap{};
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
    std::atomic<uint64_t> anti_entropy_rounds{0};
    std::atomic<uint64_t> anti_entropy_keys_repaired{0};

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
    std::atomic<uint64_t> write_failures{0};
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
