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

The TCP server rejects authenticated anti-entropy syncs from a peer whose
current membership `JoinedAt` exceeds this local tombstone grace, and increments
`cinder_tombstone_resurrection_blocked_total`. This is a safety fence, not an
automatic repair: the peer may continue to serve traffic, but its AE data is
not trusted. Wipe/bootstrap it as above before relying on it as a replica.

`JoinedAt` is membership rejoin time, not a durable per-node last-seen record.
It can be refreshed by membership transitions, so this guard is conservative
but does not prove the exact duration of every network outage. Keep node IDs
stable, do not manually force a stale node to `Alive`, and use wipe/bootstrap
whenever outage duration is uncertain. Set `--tombstone-gc-ms` above the
maximum supported outage if the deployment requires a longer offline recovery
window; remember this value must be consistent across all replicas.
