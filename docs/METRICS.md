# Metrics catalog

Every counter and gauge the server can emit, with its labels. Metric
names are part of the compatibility contract: **additive additions only —
never rename or remove an existing name**. `internal/metrics/metrics_test.go`
(`TestMetricsDocumented`) renders a zero collector, extracts every emitted
series, and fails when the documented set drifts in either direction (a
renamed metric or an undocumented new one both break the build).

Scrape `GET /metrics` on the metrics port. Unless noted, a metric is a
cumulative counter since process start.

## Cache and store

| Metric | Type | Labels | Description |
|---|---|---|---|
| `cinder_cache_hits_total` | counter | — | Local store hits |
| `cinder_cache_misses_total` | counter | — | Local store misses |
| `cinder_cache_writes_total` | counter | — | Accepted local writes |
| `cinder_cache_deletes_total` | counter | — | Accepted local deletes |
| `cinder_cache_evictions_total` | counter | `cause="ttl"\|"capacity"` | Entries evicted by TTL vs capacity pressure |
| `cinder_cache_expires_on_read_total` | counter | — | Entries found expired on read and reaped |
| `cinder_store_bytes` | gauge | — | Live store byte accounting |
| `cinder_store_entries` | gauge | — | Live store entry count (tombstones included) |
| `cinder_store_capacity_bytes` | gauge | — | Configured store capacity |

## Per-request

| Metric | Type | Labels | Description |
|---|---|---|---|
| `cinder_operations_total` | counter | `opcode="<name>"` | Requests handled per opcode (all 22 opcodes, always emitted). Opcode names: `get`, `set`, `del`, `ping`, `gossip`, `replicate`, `hint`, `get_versioned`, `anti_entropy_digest`, `anti_entropy_sync`, `admin_info`, `admin_cluster`, `admin_ring`, `admin_compact`, `admin_config_reload`, `admin_shutdown`, `admin_drain`, `counter_adjust`, `counter_merge`, `counter_get`, `counter_anti_entropy`, `counter_adjust_result` |
| `cinder_request_latency_seconds` | summary | `opcode`, `quantile` | Request handling latency quantiles per opcode (emitted per opcode with at least one sample) |
| `cinder_request_latency_seconds_sum` | summary | `opcode` | Total latency per opcode |
| `cinder_request_latency_seconds_count` | summary | `opcode` | Sample count per opcode |

## Replication and repair

| Metric | Type | Labels | Description |
|---|---|---|---|
| `cinder_replication_async_writes_total` | counter | — | Best-effort fan-out writes |
| `cinder_replication_async_deletes_total` | counter | — | Best-effort fan-out deletes |
| `cinder_replication_quorum_writes_ok_total` | counter | — | Quorum writes reaching W |
| `cinder_replication_quorum_writes_failed_total` | counter | — | Quorum writes failing fast (`ErrQuorum`) |
| `cinder_replication_quorum_deletes_ok_total` | counter | — | Quorum deletes reaching W |
| `cinder_replication_quorum_deletes_failed_total` | counter | — | Quorum deletes failing fast |
| `cinder_replication_quorum_reads_total` | counter | — | Quorum read fan-outs |
| `cinder_replication_quorum_late_failures_total` | counter | — | Straggler sends failing after the quorum decision (queued as hints) |
| `cinder_replication_reads_coalesced_total` | counter | — | Concurrent reads sharing one in-flight fan-out |
| `cinder_replication_read_repairs_total` | counter | — | Read repairs writing the LWW winner back |
| `cinder_replication_hints_enqueued_total` | counter | — | Hinted-handoff writes queued (cumulative; includes hints that later age out via TTL) |
| `cinder_replication_hints_replayed_total` | counter | — | Hints successfully redelivered |
| `cinder_replication_hints_dropped_total` | counter | — | Hints dropped (oversize or budget eviction) |
| `cinder_replication_hint_queue_entries` | gauge | — | Live hint queue depth |
| `cinder_replication_hint_queue_estimated_bytes` | gauge | — | Live hint queue estimated bytes |
| `cinder_replication_hint_queue_estimated_bytes_peak` | gauge | — | Hint queue byte high-water mark |
| `cinder_replication_replica_unreachable_total` | counter | — | Replica sends failing with transport errors |
| `cinder_replication_anti_entropy_rounds_total` | counter | — | Anti-entropy rounds run |
| `cinder_replication_anti_entropy_keys_repaired_total` | counter | — | Keys repaired by anti-entropy |
| `cinder_replication_anti_entropy_meta_rows_sent_total` | counter | — | AE metadata rows sent |
| `cinder_replication_anti_entropy_meta_rows_recv_total` | counter | — | AE metadata rows received |
| `cinder_replication_anti_entropy_entries_fetched_total` | counter | — | AE entries fetched during repair |
| `cinder_replication_anti_entropy_full_rehashes_total` | counter | — | AE full rehashes |
| `cinder_replication_anti_entropy_rehash_mismatches_total` | counter | — | AE rehash mismatches |
| `cinder_replication_anti_entropy_group_mismatches_total` | counter | — | AE group-digest mismatches |
| `cinder_replication_anti_entropy_bucket_mismatches_total` | counter | — | AE bucket-digest mismatches |
| `cinder_replication_anti_entropy_iblt_successes_total` | counter | — | AE IBLT decodes succeeding |
| `cinder_replication_anti_entropy_iblt_fallbacks_total` | counter | — | AE IBLT decodes falling back to full rows |
| `cinder_replication_counter_anti_entropy_rounds_total` | counter | — | Counter anti-entropy rounds run |
| `cinder_replication_counter_anti_entropy_keys_repaired_total` | counter | — | Counter keys repaired by counter anti-entropy |
| `cinder_tombstone_resurrection_blocked_total` | counter | — | Over-age anti-entropy syncs rejected (see OPERATIONS.md) |

## Membership and rebalance

| Metric | Type | Labels | Description |
|---|---|---|---|
| `cinder_cluster_gossip_rounds_total` | counter | — | Gossip rounds run |
| `cinder_cluster_probe_sent_total` | counter | — | Failure-detector probes sent |
| `cinder_cluster_probe_received_total` | counter | — | Failure-detector probes answered |
| `cinder_cluster_suspect_marked_total` | counter | — | Members transitioned to suspect |
| `cinder_cluster_dead_marked_total` | counter | — | Members transitioned to dead |
| `cinder_cluster_alive_marked_total` | counter | — | Members transitioned to alive |
| `cinder_cluster_refuted_self_rumors_total` | counter | — | Stale self-rumors refuted with a higher incarnation |
| `cinder_cluster_rebalance_copies_total` | counter | — | Rebalance copy pushes sent |
| `cinder_cluster_rebalance_migrations_total` | counter | — | Rebalance key migrations started |
| `cinder_cluster_rebalance_migration_failures_total` | counter | — | Rebalance migrations failing on the primary send |
| `cinder_cluster_rebalance_migration_retries_total` | counter | — | Rebalance passes scheduled after migration failure |
| `cinder_cluster_rebalance_dropped_underreplicated_total` | counter | — | Migrations kept locally because the new set under-acked |

## Connections

| Metric | Type | Labels | Description |
|---|---|---|---|
| `cinder_connections_opened_total` | counter | — | Accepted TCP connections |
| `cinder_connections_closed_total` | counter | — | Closed TCP connections |
| `cinder_connections_rejected_total` | counter | — | Sockets refused at accept by the connection budget |
| `cinder_connections_decode_failures_total` | counter | — | Frames failing to decode |
| `cinder_connections_redirects_total` | counter | — | `moved to` ownership redirects sent |
| `cinder_connections_write_failures_total` | counter | — | Response write failures |
| `cinder_connections_slow_consumer_drops_total` | counter | — | Connections dropped for slow reads |
| `cinder_auth_failures_total` | counter | `reason="client"\|"node"` | `PermissionDenied` responses by opcode class (client/admin vs replication) |
