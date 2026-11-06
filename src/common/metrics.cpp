#include "cinder/common/metrics.hpp"

#include <array>
#include <chrono>
#include <cstddef>
#include <format>
#include <iterator>
#include <string>

using std::chrono::duration_cast;
using std::chrono::milliseconds;
using std::chrono::steady_clock;

namespace cinder {
    namespace {

        constexpr std::array<const char*, 16> K_OPCODE_NAMES = {
            "get",
            "set",
            "del",
            "ping",
            "gossip",
            "replicate",
            "hint",
            "get_versioned",
            "anti_entropy_digest",
            "anti_entropy_sync",
            "admin_info",
            "admin_cluster",
            "admin_ring",
            "admin_compact",
            "admin_config_reload",
            "admin_shutdown",
        };

        constexpr std::array<const char*, 4> K_LATENCY_QUANTILE_LABELS =
            {"0.5", "0.95", "0.99", "0.999"};

        void appendCounter(
            std::string& out,
            const char* name,
            uint64_t value,
            const char* label = nullptr,
            const char* label_value = nullptr
        ) {
            std::format_to(
                std::back_inserter(out),
                "# HELP cinder_{} Total {}\n# TYPE cinder_{} counter\ncinder_{}",
                name,
                name,
                name,
                name
            );
            if (label) {
                std::format_to(std::back_inserter(out), "{{{}=\"{}\"}}", label, label_value);
            }
            std::format_to(std::back_inserter(out), " {}\n", value);
        }

        void appendGauge(
            std::string& out,
            const char* name,
            uint64_t value,
            const char* label = nullptr,
            const char* label_value = nullptr
        ) {
            std::format_to(
                std::back_inserter(out),
                "# HELP cinder_{} {}\n# TYPE cinder_{} gauge\ncinder_{}",
                name,
                name,
                name,
                name
            );
            if (label) {
                std::format_to(std::back_inserter(out), "{{{}=\"{}\"}}", label, label_value);
            }
            std::format_to(std::back_inserter(out), " {}\n", value);
        }
    } // namespace

    auto MetricsCollector::formatPrometheus() const -> std::string {
        std::string out;
        // Store metrics
        appendCounter(out, "cache_hits_total", shard_.live.hits.load());
        appendCounter(out, "cache_misses_total", shard_.live.misses.load());
        appendCounter(out, "cache_writes_total", shard_.live.writes.load());
        appendCounter(out, "cache_deletes_total", shard_.live.deletes.load());
        appendCounter(
            out,
            "cache_evictions_total",
            shard_.cap.evictions_ttl.load(),
            "cause",
            "ttl"
        );
        appendCounter(
            out,
            "cache_evictions_total",
            shard_.cap.evictions_capacity.load(),
            "cause",
            "capacity"
        );

        appendCounter(out, "cache_expires_on_read_total", shard_.cap.expires_on_read.load());
        appendGauge(out, "store_bytes", shard_.live.current_bytes.load());
        appendGauge(out, "store_entries", shard_.live.current_entries.load());
        appendGauge(out, "store_capacity_bytes", shard_.cap.capacity_bytes.load());

        // Operation counters
        appendCounter(out, "operations_total", opcode_.client.gets.load(), "opcode", "get");
        appendCounter(out, "operations_total", opcode_.client.sets.load(), "opcode", "set");
        appendCounter(out, "operations_total", opcode_.client.dels.load(), "opcode", "del");
        appendCounter(out, "operations_total", opcode_.client.pings.load(), "opcode", "ping");
        appendCounter(
            out,
            "operations_total",
            opcode_.repl.replicates.load(),
            "opcode",
            "replicate"
        );

        appendCounter(out, "operations_total", opcode_.repl.hints.load(), "opcode", "hint");
        appendCounter(
            out,
            "operations_total",
            opcode_.repl.gets_versioned.load(),
            "opcode",
            "get_versioned"
        );
        appendCounter(
            out,
            "operations_total",
            opcode_.repl.anti_entropy_digest.load(),
            "opcode",
            "anti_entropy_digest"
        );
        appendCounter(
            out,
            "operations_total",
            opcode_.repl.anti_entropy_sync.load(),
            "opcode",
            "anti_entropy_sync"
        );

        appendCounter(out, "operations_total", opcode_.admin.info.load(), "opcode", "admin_info");
        appendCounter(
            out,
            "operations_total",
            opcode_.admin.cluster.load(),
            "opcode",
            "admin_cluster"
        );

        appendCounter(out, "operations_total", opcode_.admin.ring.load(), "opcode", "admin_ring");
        appendCounter(
            out,
            "operations_total",
            opcode_.admin.compact.load(),
            "opcode",
            "admin_compact"
        );
        appendCounter(
            out,
            "operations_total",
            opcode_.admin.config_reload.load(),
            "opcode",
            "admin_config_reload"
        );
        appendCounter(
            out,
            "operations_total",
            opcode_.admin.shutdown.load(),
            "opcode",
            "admin_shutdown"
        );

        // Per-opcode latency summaries (p50/p95/p99/p999 + sum + count).
        for (size_t i = 0; i < opcode_.latency.size(); ++i) {
            const auto& hist = opcode_.latency[i];
            uint64_t cnt = hist.count.load();
            if (cnt == 0) {
                continue;
            }

            const char* op = K_OPCODE_NAMES[i];
            std::format_to(
                std::back_inserter(out),
                "# HELP cinder_request_latency_seconds Request handling latency by opcode\n"
                "# TYPE cinder_request_latency_seconds summary\n"
            );
            for (size_t q = 0; q < K_LATENCY_QUANTILE_LABELS.size(); ++q) {
                double pct = (q == 0) ? 0.50 : (q == 1) ? 0.95 : (q == 2) ? 0.99 : 0.999;
                auto ns = hist.percentile(pct);
                std::format_to(
                    std::back_inserter(out),
                    "cinder_request_latency_seconds{{opcode=\"{}\",quantile=\"{}\"}} {:.6g}\n",
                    op,
                    K_LATENCY_QUANTILE_LABELS[q],
                    static_cast<double>(ns) / 1e9
                );
            }

            std::format_to(
                std::back_inserter(out),
                "cinder_request_latency_seconds_sum{{opcode=\"{}\"}} {:.6g}\n"
                "cinder_request_latency_seconds_count{{opcode=\"{}\"}} {}\n",
                op,
                static_cast<double>(hist.total_ns.load()) / 1e9,
                op,
                cnt
            );
        }

        // Replication metrics
        appendCounter(out, "replication_async_writes_total", repl_.async_writes.load());
        appendCounter(out, "replication_async_deletes_total", repl_.async_deletes.load());
        appendCounter(out, "replication_quorum_writes_ok_total", repl_.quorum_writes_ok.load());
        appendCounter(
            out,
            "replication_quorum_writes_failed_total",
            repl_.quorum_writes_failed.load()
        );

        appendCounter(out, "replication_quorum_deletes_ok_total", repl_.quorum_deletes_ok.load());
        appendCounter(
            out,
            "replication_quorum_deletes_failed_total",
            repl_.quorum_deletes_failed.load()
        );

        appendCounter(out, "replication_quorum_reads_total", repl_.quorum_reads.load());
        appendCounter(out, "replication_reads_coalesced_total", repl_.reads_coalesced.load());
        appendCounter(out, "replication_read_repairs_total", repl_.read_repairs.load());
        appendCounter(out, "replication_hints_enqueued_total", repl_.hints_enqueued.load());
        appendCounter(out, "replication_hints_replayed_total", repl_.hints_replayed.load());
        appendCounter(out, "replication_hints_dropped_total", repl_.hints_dropped.load());
        appendGauge(out, "replication_hint_queue_entries", repl_.hint_queue_entries.load());
        appendGauge(
            out,
            "replication_hint_queue_estimated_bytes",
            repl_.hint_queue_estimated_bytes.load()
        );
        appendGauge(
            out,
            "replication_hint_queue_estimated_bytes_peak",
            repl_.hint_queue_estimated_bytes_peak.load()
        );
        appendGauge(
            out,
            "replication_hint_replay_snapshot_bytes",
            repl_.hint_replay_snapshot_bytes.load()
        );
        appendGauge(
            out,
            "replication_hint_replay_snapshot_bytes_peak",
            repl_.hint_replay_snapshot_bytes_peak.load()
        );
        appendCounter(
            out,
            "replication_replica_unreachable_total",
            repl_.replica_unreachable.load()
        );
        appendCounter(
            out,
            "replication_anti_entropy_rounds_total",
            repl_.anti_entropy_rounds.load()
        );
        appendCounter(
            out,
            "replication_anti_entropy_keys_repaired_total",
            repl_.anti_entropy_keys_repaired.load()
        );
        appendCounter(
            out,
            "replication_anti_entropy_meta_rows_sent_total",
            repl_.anti_entropy_meta_rows_sent.load()
        );
        appendCounter(
            out,
            "replication_anti_entropy_meta_rows_recv_total",
            repl_.anti_entropy_meta_rows_recv.load()
        );
        appendCounter(
            out,
            "replication_anti_entropy_entries_fetched_total",
            repl_.anti_entropy_entries_fetched.load()
        );
        appendCounter(
            out,
            "replication_anti_entropy_full_rehashes_total",
            repl_.anti_entropy_full_rehashes.load()
        );
        appendCounter(
            out,
            "replication_anti_entropy_rehash_mismatches_total",
            repl_.anti_entropy_rehash_mismatches.load()
        );
        appendCounter(
            out,
            "replication_anti_entropy_group_mismatches_total",
            repl_.anti_entropy_group_mismatches.load()
        );
        appendCounter(
            out,
            "replication_anti_entropy_bucket_mismatches_total",
            repl_.anti_entropy_bucket_mismatches.load()
        );
        appendCounter(
            out,
            "replication_anti_entropy_iblt_successes_total",
            repl_.anti_entropy_iblt_successes.load()
        );
        appendCounter(
            out,
            "replication_anti_entropy_iblt_fallbacks_total",
            repl_.anti_entropy_iblt_fallbacks.load()
        );

        // Per-replica lag
        if (!repl_.replica_lags.empty()) {
            std::format_to(
                std::back_inserter(out),
                "# HELP cinder_replication_lag_ms Replication lag per replica (ms since last ack)\n"
                "# TYPE cinder_replication_lag_ms gauge\n"
            );
            auto now_ms = static_cast<uint64_t>(
                duration_cast<milliseconds>(steady_clock::now().time_since_epoch()).count()
            );
            for (const auto& lag : repl_.replica_lags) {
                auto delta = now_ms > lag.last_ack_epoch_ms ? now_ms - lag.last_ack_epoch_ms : 0;
                std::format_to(
                    std::back_inserter(out),
                    "cinder_replication_lag_ms{{replica=\"{}\"}} {}\n",
                    lag.node_id,
                    delta
                );
            }
        }
        // Cluster metrics
        appendCounter(out, "cluster_gossip_rounds_total", cluster_.gossip_rounds.load());
        appendCounter(out, "cluster_probe_sent_total", cluster_.probe_sent.load());
        appendCounter(out, "cluster_probe_received_total", cluster_.probe_received.load());
        appendCounter(out, "cluster_suspect_marked_total", cluster_.suspect_marked.load());
        appendCounter(out, "cluster_dead_marked_total", cluster_.dead_marked.load());
        appendCounter(out, "cluster_alive_marked_total", cluster_.alive_marked.load());
        appendCounter(
            out,
            "cluster_refuted_self_rumors_total",
            cluster_.refuted_self_rumors.load()
        );

        appendCounter(out, "cluster_rebalance_copies_total", cluster_.rebalance_copies.load());
        appendCounter(
            out,
            "cluster_rebalance_migrations_total",
            cluster_.rebalance_migrations.load()
        );
        appendCounter(
            out,
            "cluster_rebalance_migration_failures_total",
            cluster_.rebalance_migration_failures.load()
        );

        appendCounter(
            out,
            "cluster_rebalance_migration_retries_total",
            cluster_.rebalance_migration_retries.load()
        );
        appendCounter(
            out,
            "cluster_rebalance_dropped_underreplicated_total",
            cluster_.rebalance_dropped_underreplicated.load()
        );
        // Connection metrics
        appendCounter(out, "connections_opened_total", conn_.connections_opened.load());
        appendCounter(out, "connections_closed_total", conn_.connections_closed.load());
        appendCounter(out, "connections_decode_failures_total", conn_.decode_failures.load());
        appendCounter(out, "connections_redirects_total", conn_.redirects.load());
        appendCounter(out, "connections_write_failures_total", conn_.write_failures.load());
        return out;
    }
} // namespace cinder
