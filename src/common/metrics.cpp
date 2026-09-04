#include "cinder/common/metrics.hpp"

#include <array>
#include <cstddef>
#include <sstream>

namespace cinder {
namespace {

constexpr std::array<const char*, 10> K_OPCODE_NAMES = {"get",
    "set",
    "del",
    "ping",
    "gossip",
    "replicate",
    "hint",
    "get_versioned",
    "anti_entropy_digest",
    "anti_entropy_sync"};

constexpr std::array<const char*, 4> K_LATENCY_QUANTILE_LABELS = {"0.5", "0.95", "0.99", "0.999"};

void
appendCounter(std::ostringstream& os, const char* name, uint64_t value, const char* label = nullptr,
    const char* label_value = nullptr) {
    os << "# HELP cinder_" << name << " Total " << name << "\n";
    os << "# TYPE cinder_" << name << " counter\n";
    os << "cinder_" << name;
    if (label) {
        os << "{" << label << "=\"" << label_value << "\"}";
    }
    os << " " << value << "\n";
}

void
appendGauge(std::ostringstream& os, const char* name, uint64_t value, const char* label = nullptr,
    const char* label_value = nullptr) {
    os << "# HELP cinder_" << name << " " << name << "\n";
    os << "# TYPE cinder_" << name << " gauge\n";
    os << "cinder_" << name;
    if (label) {
        os << "{" << label << "=\"" << label_value << "\"}";
    }
    os << " " << value << "\n";
}
} // namespace

auto
MetricsCollector::formatPrometheus() const -> std::string {
    std::ostringstream os;
    // Store metrics
    appendCounter(os, "cache_hits_total", shard_.hits.load());
    appendCounter(os, "cache_misses_total", shard_.misses.load());
    appendCounter(os, "cache_writes_total", shard_.writes.load());
    appendCounter(os, "cache_deletes_total", shard_.deletes.load());
    appendCounter(os, "cache_evictions_total", shard_.evictions_ttl.load(), "cause", "ttl");
    appendCounter(
        os, "cache_evictions_total", shard_.evictions_capacity.load(), "cause", "capacity");

    appendCounter(os, "cache_expires_on_read_total", shard_.expires_on_read.load());
    appendGauge(os, "store_bytes", shard_.current_bytes.load());
    appendGauge(os, "store_entries", shard_.current_entries.load());
    appendGauge(os, "store_capacity_bytes", shard_.capacity_bytes.load());

    // Operation counters
    appendCounter(os, "operations_total", opcode_.gets.load(), "opcode", "get");
    appendCounter(os, "operations_total", opcode_.sets.load(), "opcode", "set");
    appendCounter(os, "operations_total", opcode_.dels.load(), "opcode", "del");
    appendCounter(os, "operations_total", opcode_.pings.load(), "opcode", "ping");
    appendCounter(os, "operations_total", opcode_.replicates.load(), "opcode", "replicate");
    appendCounter(os, "operations_total", opcode_.hints.load(), "opcode", "hint");
    appendCounter(os, "operations_total", opcode_.gets_versioned.load(), "opcode", "get_versioned");
    appendCounter(os,
        "operations_total",
        opcode_.anti_entropy_digest.load(),
        "opcode",
        "anti_entropy_digest");

    appendCounter(
        os, "operations_total", opcode_.anti_entropy_sync.load(), "opcode", "anti_entropy_sync");

    // Per-opcode latency summaries (p50/p95/p99/p999 + sum + count).
    for (size_t i = 0; i < opcode_.latency.size(); ++i) {
        const auto& hist = opcode_.latency[i];
        uint64_t cnt = hist.count.load();
        if (cnt == 0) {
            continue;
        }

        const char* op = K_OPCODE_NAMES[i];
        os << "# HELP cinder_request_latency_seconds Request handling latency by opcode\n";
        os << "# TYPE cinder_request_latency_seconds summary\n";
        for (size_t q = 0; q < K_LATENCY_QUANTILE_LABELS.size(); ++q) {
            double pct = (q == 0) ? 0.50 : (q == 1) ? 0.95 : (q == 2) ? 0.99 : 0.999;
            auto ns = hist.percentile(pct);
            os << "cinder_request_latency_seconds{opcode=\"" << op << "\",quantile=\""
               << K_LATENCY_QUANTILE_LABELS[q] << "\"} " << (static_cast<double>(ns) / 1e9) << "\n";
        }

        os << "cinder_request_latency_seconds_sum{opcode=\"" << op << "\"} "
           << (static_cast<double>(hist.total_ns.load()) / 1e9) << "\n";
        os << "cinder_request_latency_seconds_count{opcode=\"" << op << "\"} " << cnt << "\n";
    }

    // Replication metrics
    appendCounter(os, "replication_async_writes_total", repl_.async_writes.load());
    appendCounter(os, "replication_quorum_writes_ok_total", repl_.quorum_writes_ok.load());
    appendCounter(os, "replication_quorum_writes_failed_total", repl_.quorum_writes_failed.load());
    appendCounter(os, "replication_quorum_reads_total", repl_.quorum_reads.load());
    appendCounter(os, "replication_read_repairs_total", repl_.read_repairs.load());
    appendCounter(os, "replication_hints_enqueued_total", repl_.hints_enqueued.load());
    appendCounter(os, "replication_hints_replayed_total", repl_.hints_replayed.load());
    appendCounter(os, "replication_replica_unreachable_total", repl_.replica_unreachable.load());
    appendCounter(os, "replication_anti_entropy_rounds_total", repl_.anti_entropy_rounds.load());
    appendCounter(os,
        "replication_anti_entropy_keys_repaired_total",
        repl_.anti_entropy_keys_repaired.load());

    // Per-replica lag
    if (!repl_.replica_lags.empty()) {
        os << "# HELP cinder_replication_lag_ms Replication lag per replica (ms since last ack)\n";
        os << "# TYPE cinder_replication_lag_ms gauge\n";
        auto now_ms = static_cast<uint64_t>(std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::steady_clock::now().time_since_epoch())
                .count());
        for (const auto& lag : repl_.replica_lags) {
            auto delta = now_ms > lag.last_ack_epoch_ms ? now_ms - lag.last_ack_epoch_ms : 0;
            os << "cinder_replication_lag_ms{replica=\"" << lag.node_id << "\"} " << delta << "\n";
        }
    }

    // Cluster metrics
    appendCounter(os, "cluster_gossip_rounds_total", cluster_.gossip_rounds.load());
    appendCounter(os, "cluster_probe_sent_total", cluster_.probe_sent.load());
    appendCounter(os, "cluster_probe_received_total", cluster_.probe_received.load());
    appendCounter(os, "cluster_suspect_marked_total", cluster_.suspect_marked.load());
    appendCounter(os, "cluster_dead_marked_total", cluster_.dead_marked.load());
    appendCounter(os, "cluster_alive_marked_total", cluster_.alive_marked.load());
    appendCounter(os, "cluster_refuted_self_rumors_total", cluster_.refuted_self_rumors.load());
    appendCounter(os, "cluster_rebalance_copies_total", cluster_.rebalance_copies.load());
    appendCounter(os, "cluster_rebalance_migrations_total", cluster_.rebalance_migrations.load());
    // Connection metrics
    appendCounter(os, "connections_opened_total", conn_.connections_opened.load());
    appendCounter(os, "connections_closed_total", conn_.connections_closed.load());
    appendCounter(os, "connections_decode_failures_total", conn_.decode_failures.load());
    appendCounter(os, "connections_redirects_total", conn_.redirects.load());
    appendCounter(os, "connections_write_failures_total", conn_.write_failures.load());
    return os.str();
}
} // namespace cinder
