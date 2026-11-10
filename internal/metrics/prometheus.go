package metrics

import (
	"fmt"
	"strings"

	"github.com/kraytos17/cinder/internal/net/protocol"
)

// StoreGauges carries live store sizes at scrape time (read from the
// store's own atomics by the scrape handler).
type StoreGauges struct {
	Bytes    uint64
	Entries  uint64
	Capacity uint64
	// Hint queue live depth (set when replication is configured).
	HintEntries uint64
	HintBytes   uint64
}

// quantileLabels are the quantiles exposed for each opcode latency summary.
var quantileLabels = []struct {
	label string
	q     float64
}{
	{"0.5", 0.50},
	{"0.95", 0.95},
	{"0.99", 0.99},
	{"0.999", 0.999},
}

// AppendPrometheus appends the Prometheus text exposition to b and returns
// the resulting slice. Metric names and labels are a stable monitoring API.
func (c *Collector) AppendPrometheus(b []byte, g StoreGauges) []byte {
	var sb strings.Builder
	counter := func(name string, v uint64) {
		fmt.Fprintf(&sb, "# HELP cinder_%s Total %s\n# TYPE cinder_%s counter\ncinder_%s %d\n", name, name, name, name, v)
	}
	counterLbl := func(name string, v uint64, label, val string) {
		fmt.Fprintf(&sb, "# HELP cinder_%s Total %s\n# TYPE cinder_%s counter\ncinder_%s{%s=%q} %d\n", name, name, name, name, label, val, v)
	}
	gauge := func(name string, v uint64) {
		fmt.Fprintf(&sb, "# HELP cinder_%s %s\n# TYPE cinder_%s gauge\ncinder_%s %d\n", name, name, name, name, v)
	}

	counter("cache_hits_total", c.Hits.Load())
	counter("cache_misses_total", c.Misses.Load())
	counter("cache_writes_total", c.Writes.Load())
	counter("cache_deletes_total", c.Deletes.Load())
	counterLbl("cache_evictions_total", c.EvictionsTTL.Load(), "cause", "ttl")
	counterLbl("cache_evictions_total", c.EvictionsCap.Load(), "cause", "capacity")
	counter("cache_expires_on_read_total", c.ExpiresOnRead.Load())
	gauge("store_bytes", g.Bytes)
	gauge("store_entries", g.Entries)
	gauge("store_capacity_bytes", g.Capacity)

	for op := protocol.Opcode(1); op <= maxOpcode; op++ {
		counterLbl("operations_total", c.ops[int(op)].Load(), "opcode", OpName(op))
	}
	for op := protocol.Opcode(1); op <= maxOpcode; op++ {
		h := &c.latency[int(op)]
		cnt := h.Count()
		if cnt == 0 {
			continue
		}
		name := OpName(op)
		sb.WriteString("# HELP cinder_request_latency_seconds Request handling latency by opcode\n# TYPE cinder_request_latency_seconds summary\n")
		for _, ql := range quantileLabels {
			fmt.Fprintf(&sb, "cinder_request_latency_seconds{opcode=%q,quantile=%q} %.6g\n", name, ql.label, float64(h.Percentile(ql.q))/1e9)
		}
		fmt.Fprintf(&sb, "cinder_request_latency_seconds_sum{opcode=%q} %.6g\n", name, float64(h.SumNs())/1e9)
		fmt.Fprintf(&sb, "cinder_request_latency_seconds_count{opcode=%q} %d\n", name, cnt)
	}

	counter("replication_async_writes_total", c.AsyncWrites.Load())
	counter("replication_async_deletes_total", c.AsyncDeletes.Load())
	counter("replication_quorum_writes_ok_total", c.QuorumWritesOK.Load())
	counter("replication_quorum_writes_failed_total", c.QuorumWritesErr.Load())
	counter("replication_quorum_deletes_ok_total", c.QuorumDeletesOK.Load())
	counter("replication_quorum_deletes_failed_total", c.QuorumDeletesEr.Load())
	counter("replication_quorum_reads_total", c.QuorumReads.Load())
	counter("replication_reads_coalesced_total", c.ReadsCoalesced.Load())
	counter("replication_read_repairs_total", c.ReadRepairs.Load())
	counter("replication_hints_enqueued_total", c.HintsEnqueued.Load())
	counter("replication_hints_replayed_total", c.HintsReplayed.Load())
	counter("replication_hints_dropped_total", c.HintsDropped.Load())
	gauge("replication_hint_queue_entries", g.HintEntries)
	gauge("replication_hint_queue_estimated_bytes", g.HintBytes)
	gauge("replication_hint_queue_estimated_bytes_peak", c.HintBytesPeak.Load())
	counter("replication_replica_unreachable_total", c.ReplicaUnreach.Load())
	counter("replication_anti_entropy_rounds_total", c.AERounds.Load())
	counter("replication_anti_entropy_keys_repaired_total", c.AEKeysRepaired.Load())
	counter("replication_anti_entropy_meta_rows_sent_total", c.AEMetaSent.Load())
	counter("replication_anti_entropy_meta_rows_recv_total", c.AEMetaRecv.Load())
	counter("replication_anti_entropy_entries_fetched_total", c.AEFetched.Load())
	counter("replication_anti_entropy_full_rehashes_total", c.AEFullRehashes.Load())
	counter("replication_anti_entropy_rehash_mismatches_total", c.AERehashMismatch.Load())
	counter("replication_anti_entropy_group_mismatches_total", c.AEGroupMismatch.Load())
	counter("replication_anti_entropy_bucket_mismatches_total", c.AEBucketMismatch.Load())
	counter("replication_anti_entropy_iblt_successes_total", c.AEIBLTOk.Load())
	counter("replication_anti_entropy_iblt_fallbacks_total", c.AEIBLTFallback.Load())
	counter("replication_counter_anti_entropy_rounds_total", c.CounterAERounds.Load())
	counter("replication_counter_anti_entropy_keys_repaired_total", c.CounterAERepaired.Load())

	counter("cluster_gossip_rounds_total", c.GossipRounds.Load())
	counter("cluster_probe_sent_total", c.ProbeSent.Load())
	counter("cluster_probe_received_total", c.ProbeReceived.Load())
	counter("cluster_suspect_marked_total", c.SuspectMarked.Load())
	counter("cluster_dead_marked_total", c.DeadMarked.Load())
	counter("cluster_alive_marked_total", c.AliveMarked.Load())
	counter("cluster_refuted_self_rumors_total", c.RefutedRumors.Load())
	counter("cluster_rebalance_copies_total", c.RebalanceCopies.Load())
	counter("cluster_rebalance_migrations_total", c.RebalanceMigrations.Load())
	counter("cluster_rebalance_migration_failures_total", c.RebalanceMigFailures.Load())
	counter("cluster_rebalance_migration_retries_total", c.RebalanceMigRetries.Load())
	counter("cluster_rebalance_dropped_underreplicated_total", c.RebalanceDroppedUnder.Load())

	counter("connections_opened_total", c.ConnsOpened.Load())
	counter("connections_closed_total", c.ConnsClosed.Load())
	counter("connections_decode_failures_total", c.DecodeFailures.Load())
	counter("connections_redirects_total", c.Redirects.Load())
	counter("connections_write_failures_total", c.WriteFailures.Load())
	return append(b, sb.String()...)
}

// FormatPrometheus renders the full exposition.
func (c *Collector) FormatPrometheus(g StoreGauges) []byte {
	return c.AppendPrometheus(nil, g)
}
