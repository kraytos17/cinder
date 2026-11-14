# Operations runbook

## Replica offline longer than tombstone grace

Tombstones suppress stale values after a delete. By default, Cinder retains
them for 24 hours (`--tombstone-gc-ms` overrides this). If a replica stays
offline beyond that retention horizon, the rest of the cluster may have
already collected the marker. Reintroducing the replica's old data can then
resurrect a deleted value.

### Required procedure

1. **Do not restart an over-age replica with its old data directory attached.**
2. Stop it and preserve a copy of the directory if forensic recovery is needed.
3. Verify the directory with `cinderd --verify-data-dir <dir>` for diagnostics.
4. Move the old directory out of the configured path; start the node with a
   fresh data directory and the same stable `--node-id`.
5. Wait for membership to mark it alive and let normal rebalance/anti-entropy
   repopulate it from current replicas.

The TCP server rejects authenticated anti-entropy requests (digest, refinement,
and sync) from a peer whose
membership `JoinedAt` is older than this local tombstone grace, and increments
`cinder_tombstone_resurrection_blocked_total`. The comparison is based on
membership age, not durable proof of the peer's outage duration. This is a
safety fence, not an automatic repair: the peer may continue to serve traffic,
but its AE data is not trusted. Wipe/bootstrap it as above before relying on it
as a replica.

`JoinedAt` is the current membership record's transition-to-Alive time, not a
durable per-node last-seen record. It is reset when a node is newly discovered
or marked alive again after suspicion/death. Consequently the fence can reject
an old membership record, but cannot prove the exact duration of every outage;
a reset may make an over-age disk appear newly joined. Keep node IDs stable, do
not manually force a stale node to `Alive`, and use wipe/bootstrap whenever
outage duration is uncertain. Set `--tombstone-gc-ms` consistently across
replicas and above the maximum supported outage when that recovery window is
required. The TCP tombstone fence protects anti-entropy; it does not establish
that every other service path or client request from an old disk is harmless.

Verification evidence: `internal/net/tcp/tombstone_resurrection_test.go`
reproduces resurrection after tombstone GC and checks the anti-entropy fence;
`TestTombstoneGraceDefaultIsTwentyFourHours` pins the default.
