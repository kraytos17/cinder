# Rebalance & Membership Protocol

## Overview

Cinder uses a SWIM-style membership protocol with gossip dissemination and a
consistent hash ring to distribute keys across nodes. When membership changes
(a node joins, leaves, or fails), rebalance migrates keys to their new owners
and pushes replicas to new replica-set members. A quarantine window prevents
crash-looping nodes from becoming hot migration targets.

This document covers cluster lifecycle, replication, read repair, hinted
handoff, anti-entropy, consistency modes, and persistence. The wire format is
documented in `protocol.md`.

## Membership State Machine

Each node is tracked by a `NodeInfo` record:

| Field | Type | Description |
|-------|------|--------------|
| `state` | enum | `Alive`, `Suspect`, or `Dead` |
| `incarnation` | `uint64` | Monotonically increasing; used for conflict resolution |
| `joined_at` | `time_point` | Timestamp of the last transition to `Alive`; anchors the quarantine window |

```
                    K consecutive probe failures / timeout
      Alive ──────────────────────────────────────────► Suspect
       ▲                                      │
       │                 suspect_timeout expires
       │                                      ▼
       │   gossip: alive (higher incarnation)  Dead
       └──────────────────────────────────────┘
```

- **Alive → Suspect**: the failure detector's ping probe fails
  `K_SUSPECT_THRESHOLD` (2) times consecutively, or a probe times out.
  The detector records `suspect_since` and starts the escalation clock.
- **Suspect → Dead**: `markDead()` fires once `suspect_timeout` elapses
  without recovery. Final under normal operation.
- **Dead → Alive (rejoin)**: a previously-`Dead` node is heard alive again via
  gossip with a strictly higher incarnation. `markAlive()` resets `joined_at`,
  starting a fresh quarantine window.

**Incarnation numbers** provide SWIM-style conflict resolution: `applyRumor()`
only applies a gossip rumor if its incarnation is ≥ the locally known value,
rejecting stale entries. When a node hears a `Suspect` or `Dead` rumor about
itself, it refutes by bumping its own incarnation to `rumor.incarnation + 1`
and forcing itself back to `Alive` (`refuteSelfRumor()`); the higher
incarnation then overrides the stale rumor everywhere it propagates.

## Failure Detection

A SWIM-style probe cycle runs on a configurable interval. All probe and
round-robin state is guarded by `state_mutex_`, since probe callbacks and
timer ticks can fire concurrently from different io-pool threads.

1. **Ping** — each tick, a random peer is selected and sent a `Ping`.
2. **Suspect** — if `K_SUSPECT_THRESHOLD` (2) consecutive `Ping`s fail, the
   peer is marked `Suspect` and a suspicion timer starts. A single transient
   failure is tolerated without flapping the ring.
3. **Dead** — if the peer is still `Suspect` after `suspect_timeout` elapses
   from the initial probe, it is marked `Dead`.
4. **Blackhole detection** — a timed-out probe (no response within
   `suspect_timeout`) also triggers `Suspect`, independent of the async
   callback path.

`MembershipTable` mutations and `sendAsync()` happen outside `state_mutex_`:
the transport may complete callbacks synchronously and re-enter the failure
detector, so the lock serializes only probe state, never network I/O.

## Gossip Dissemination

`GossipManager` periodically (every `gossip_interval`) sends the full
membership view to one randomly selected peer.

**Wire format** — the `GOSSIP` payload (opcode 5) is a semicolon-delimited
text string:

```
id@host:port:state:incarnation;id@host:port:state:incarnation;...
```

e.g. `node1@127.0.0.1:7000:alive:3;node2@127.0.0.1:7001:dead:7`

**Application** — on receipt, `handleMessage()` decodes the view and calls
`applyRumor()` per entry. Integer fields (port, incarnation) are parsed with
`std::from_chars` for zero-allocation, exception-free conversion. The
incarnation guard ensures only newer information is adopted. Any resulting
state or incarnation change fires `fireCallbacks()`, which notifies:

1. `CacheNodeServer` — rebuilds the hash ring and triggers rebalance.
2. `GossipManager` — rebuilds its peer list.
3. `FailureDetector` — rebuilds its peer list.

`GossipManager` and `FailureDetector` each register a `table_.onChange(...)`
callback that rebuilds their own `peers_` vector under their respective
mutex, keeping peer selection current without periodic full scans.

**Graceful leave** — on shutdown, `CacheNodeServer`:

1. Cancels all timers (replay, gossip, probe, evict, quarantine, compact).
2. Calls `gossip_.leave()`: `markDead(self_)` sets this node's state to `Dead`
   and bumps its incarnation, then the full membership view (self included,
   as `Dead`) is sent to every known peer via `sendView()`.
3. Drains pending handlers with `while (io_.poll() > 0) {}`, ensuring the
   leave broadcast is flushed before the transport tears down.

This lets peers learn the node is `Dead` from the explicit broadcast, rather
than waiting for their own probes to time out and suspect-mark it.

## Consistent Hash Ring

`ConsistentHashRing` maps keys to nodes using xxHash3 virtual nodes (150 per
physical node), built as an immutable snapshot:

1. A sorted vector of `(hash, NodeId)` pairs represents the ring.
2. Add/remove builds a new vector and atomically swaps it in via
   `std::atomic<std::shared_ptr<const RingSnapshot>>`.
3. Reads are lock-free, via binary search over the snapshot.
4. Mutators use a CAS retry loop, so concurrent mutations merge rather than
   clobbering each other.

Cluster-scale lookup maps (`MembershipTable::nodes_`,
`ConnectionPool::node_addrs_`, `TcpTransport::addrs_`) use `std::flat_map` for
cache-friendly, sorted-key iteration with lower memory overhead than
`ankerl::unordered_dense::map` at the 3–10 node scale typical of a Cinder cluster.

**Key lookup** — `getNodes(key, replica_factor)` returns the ordered list of
`replica_factor` successors on the ring; the first is the primary, the rest
are replicas. Dead nodes are excluded, so fewer than `replica_factor` results
are returned when not enough alive nodes exist.

## Rebalance Protocol

**Trigger:**

1. Membership change — every `onChange` callback (alive/dead/suspect) calls
   `rebuildRing()`, which updates ring membership and then calls
   `shard_.rebalance()` via a 200 ms debounced timer (`scheduleRebalanceDebounced`).
   Rapid flaps collapse into a single scan over the latest ring view.
2. Quarantine retry — once `quarantine_interval` elapses, a timer retries
   `rebalance()` for keys deferred earlier due to quarantine.
3. Migration-failure retry — each failed async `migrateKey()` send keeps the
   local copy and fires `onMigrationFailed`, which schedules a single 2 s
   delayed re-rebalance through the same debounce gate
   (`scheduleMigrationRetry`). A burst of failures coalesces into one retry,
   giving a briefly-overwhelmed new owner (TCP accept queue, TLS handshake
   backlog) time to recover.

**Two-phase design** avoids re-entrant deadlocks with synchronous transports:

1. *Enumerate under lock* — `store_.forEach()` invokes a callback per live
    entry under a shared lock. For each key, the desired replica set is
    computed via `ring_.getNodes(key, replica_factor_)`, and migrate/push
    actions are collected without sending anything (the callback never sends —
    see phase 2).
2. *Send after unlock* — the store lock is released, then the collected
   requests are sent sequentially.

**Key stays local** (this node is in the desired set):
- The key stays in the local store.
- A replica copy is pushed to every other desired owner via `pushReplica()`,
  repairing gaps left by joins or removals.
- Pushes to quarantined targets are skipped (`deferred = true`).
- Pushes are idempotent (version-gated LWW apply), so duplicates are harmless.

**Key migrates** (this node is not in the desired set):
1. `migrateKey()` sends a `Replicate` request to `desired[0]`. On success, the
   local copy is removed. On failure, the key stays local as a failover copy,
   `cluster_rebalance_migration_failures_total` is incremented, and the
   migration-failed callback schedules a 2 s retry (counted in
   `cluster_rebalance_migration_retries_total`).
2. `pushReplica()` sends copies to `desired[1..N]`, skipping quarantined
   targets.

`makeReplicateRequest()` builds the `Replicate` (opcode 6) request: key,
value, `version`, `writer_node_hash`, and `expires_at` when applicable
(converted to absolute system time for consistent TTL across replicas).

**Liveness check** — before any push or migrate send, the shard manager
checks the membership table; sends to `Suspect` or `Dead` targets are
skipped, avoiding wasted I/O to nodes that won't ack.

## Quarantine Window

A quarantine is a grace period after a node joins (or rejoins) during which
other nodes will not push keys to it — preventing a crash-looping node from
becoming a hot migration target on every restart.

A node is quarantined when all of the following hold:

```
isQuarantined = (quarantine_interval_ms > 0)
             && (state == Alive)
             && (now - joined_at < quarantine_interval_ms)
```

`joined_at` is set on: initial seeding (`seed()`, for all peers), first
discovery via gossip (`applyRumor()`), and recovery from `Suspect`/`Dead`
(`markAlive()`) — so every recovery starts a fresh quarantine window.

**Retry** — if `rebalance()` returns `true` (some keys deferred due to
quarantine), `CacheNodeServer::rebuildRing()` calls `scheduleRebalance()`:

```
scheduleRebalance():
    if quarantine_interval <= 0: return
    timer.expires_after(quarantine_interval)
    timer.async_wait():
        if shard_.rebalance():    // still deferred?
            scheduleRebalance()   // retry again after interval
```

This self-retries every `quarantine_interval` until all deferred keys have
been migrated, even against a flapping node that keeps resetting `joined_at`.

## Read Repair

On a quorum read (`R > 1`), `ReplicationManager::readAsync()` compares
responses from the local store and all replicas using LWW (version +
`writer_node_hash`). A replica with a newer version than the current best
flags the read as `needs_repair`.

**Repair:**
1. *Local self-heal* — if the local store is behind, it is updated via
   `putVersioned()` (LWW, so a stale write is a no-op).
2. *Fan-out repair* — `sendRepairFanOut()` sends a `Replicate` request
   carrying the winning entry to **all** replicas, not just the stale one.
   Idempotent, so pushing to an already-current replica is harmless.

**Triggers**: a quorum read (fan-out `GetVersioned`, compare versions, repair
if needed), and a read miss on one replica while the key exists elsewhere
(the miss implies staleness).

Read repair handles transient inconsistency, e.g. a replica missing one write
due to a blip. Anti-entropy (below) handles deeper drift.

## Hinted Handoff

When a write (async or quorum) fails to reach a replica, a hint is enqueued
in the local `HintQueue` for later replay, preventing data loss while the
replica is unreachable.

- **Queue**: bounded FIFO, capacity 1,024 entries **and** a configurable
  estimated-bytes budget (`replication.max_hint_bytes`, default 64 MiB,
  `--max-hint-bytes`, hot-reloadable). When either bound would be exceeded,
  the oldest hints are evicted first; a single hint larger than the whole
  budget is rejected. Evictions and rejections count toward
  `cinder_replication_hints_dropped_total`, with live/peak gauges at
  `cinder_replication_hint_queue_estimated_bytes[_peak]`.
- **Coalescing**: at most one live hint per (replica, key). A newer write
  for an already-queued key replaces it (TTL refreshed, moved to the tail
  as fresh data); an older-or-equal write is ignored. LWW order decides:
  higher version wins, ties break toward the larger writer hash, and an
  identical stamp counts as newer so retries refresh the TTL. Replacement
  bumps the slot generation, so a stale replay completion cannot delete
  the newer hint. Coalescing preserves budget for distinct keys during an
  outage instead of spending it on superseded versions.
- **TTL**: 30 s (`K_HINT_TTL`); expired hints are dropped during replay.
- **Thread safety**: all operations take an internal mutex.

```cpp
struct Hint {
    NodeId target;                         // unreachable replica node
    net::Request req;                      // the original Replicate request
    steady_clock::time_point expires_at;   // hint TTL
};
```

`ReplicationManager::replayHints()` snapshots live hints under the lock, then
sends each to its target. A single-consumer guard (`replaying_`, atomic)
prevents overlapping replays from concurrent timer ticks or pool threads.
Successful replays remove the hint; failed or expired hints are dropped
silently.

Hints are enqueued whenever a replica is unreachable during an async or
quorum write.

## Hint Durability

With persistence enabled, hints are journaled to
`<data_dir>/hints.journal` so they survive a node crash or restart (a plain
in-memory queue would lose them). Recovery replays the journal through the
same push/remove path, so coalescing, LWW ordering and both bounds come out
identically; only successfully replayed hints need tombstones, and expired
records are skipped.

- **Crash window**: appends are page-cache writes; only compaction and
  shutdown fsync. A torn tail record is truncated on open.
- **Compaction**: past 4,096 records the journal is rewritten from the live
  snapshot, keeping long outages from growing the file without bound.
- **Recovery order**: hint recovery runs before serving, so replayed hints
  join the normal ~1 s replay cycle once the replica returns.

## Anti-Entropy

Periodic background repair between replica partners, using range-hash
bucketing, run by the primary every `anti_entropy_interval`. It catches drift
that read repair and hinted handoff cannot — e.g. a replica down longer than
the hint TTL.

**Three-phase exchange with metadata-first sync and hierarchical digests:**
1. *Digest exchange* — the initiator computes one xxHash per group of 16
   consecutive buckets (136 bytes for the default 256 buckets) and sends it
   to a replica partner (`ANTI_ENTROPY_DIGEST`, opcode 9). The partner
   answers with its own group hashes. Converged rounds end here.
2. *Bucket refinement* — for each divergent group, the initiator requests
   that group's 16 bucket hashes (same opcode, `key` set to
   `"bucket-refinement-v3"`). The responder returns the intersection of
   requested buckets whose hashes differ, plus compact **metadata rows**
   `(bucket, key_hash, version, writer)` — 28 bytes each — for the buckets
   its byte budget covers, and an explicit covered-bucket set. Values never
   cross the wire in this phase; uncovered buckets are deferred, never
   treated as missing.
3. *Targeted fetch* — both sides diff the metadata against their local store
   (remote wins iff newer version, or equal version with greater writer).
   The initiator pushes full entries only where it wins and sends a
   **needlist** (key hashes) where it loses; the partner serves the needlist
   with full entries (`ANTI_ENTROPY_SYNC`, opcode 10). Values cross only for
   true deltas, and each direction is byte-budgeted (`anti_entropy_fetch_bytes`,
   default 1 MiB) with cursor-free geometric convergence: the next round
   re-diffs the remainder.

**Group digests**: keys are partitioned into N hash buckets (default 256),
folded 16-to-1 into group hashes. Each bucket hash is the XOR of its entries'
hashes over `(key, version_and_ttl, writer_node_hash)` — order-independent,
so no per-bucket sort. Two nodes with identical data produce identical group
hashes, so metadata is only exchanged for buckets inside divergent groups,
and only true deltas exchange values.

**IBLT refinement**: for divergent buckets with at least 32 metadata rows,
the responder sends a 64-cell invertible Bloom lookup table rather than all
rows. Cells encode the signed count and XORs of `(key_hash, version, writer,
tombstone)` plus an item checksum keyed over the same tuple, so a live write
and its delete never cancel during peeling. The initiator subtracts its
local sketch and peels pure cells to recover the exact symmetric difference
of LWW identities (tombstones included — a delete heals through the sketch
path like any write).
Small buckets continue using metadata rows; if any sketch fails to peel, that
bucket is retried through the existing explicit-metadata path. This bounds
the extra work and wire bytes while retaining as the correctness fallback.

**Incremental digests**: stores maintain the per-bucket XOR on the write path
(observed mutations), so converged rounds cost a 2 KiB copy instead of a full
scan. A wall-clock backstop (`anti_entropy_full_rehash_interval_ms`, default
5 min) re-verifies from a full scan with an exact divert-while-scanning
protocol, bounding any silent-divergence window.

**Entry format** (sync payload entries):

```
[key_len(4)][key][version(8)][writer_hash(8)][entry_flags(1)][expires_at_ms?(8)][val_len(4)][value]
```

All entries are applied via LWW (`putVersioned`), so the protocol is
idempotent and commutative — replaying the same sync blob is always safe.

## Deletes

Deletes are versioned tombstones, not erasures. A `DEL` mints a fresh Lamport
version, plants a marker (empty value + tombstone bit + GC deadline), and
fans out through the same async/quorum path as writes — including hints, so
an unreachable replica heals on return. The tombstone wins or loses LWW
exactly like a put: a stale delete loses to a newer write, a concurrent
delete and write at identical `(version, writer)` resolves tombstone-wins
(this tie is only reachable across standalone writers sharing writer 0;
per-node writers are unique and versions monotonic, so replicas always agree
on the order).

The marker flows everywhere values flow: digests hash it (bit-packed into
`version_and_ttl`), metadata rows and IBLT sketches carry it, sync entries
apply it, quorum reads count tombstone votes (a tombstone quorum reads as
`NotFound` to clients), and snapshots/WAL/hints persist it. Migration moves
tombstones with their range; only source-side cleanup after handoff uses a
hard erase.

Tombstones are exempt from capacity eviction (evicting a live marker would
resurrect the key on the next anti-entropy round) and are reaped by the TTL
wheel at their GC deadline, which rides the existing expiry machinery: the
primary's absolute deadline converges across replicas like a TTL. The grace
period (`tombstone_gc_ms`, default 5 min) must exceed the worst-case
partition healing time — hint TTL plus several anti-entropy rounds. Premature
GC resurrects the key from any replica that missed the delete.

Dotted version vectors were considered for delete-vs-concurrent-write
causality and deferred: LWW over `(version, writer)` already orders delete
vs write the same way it orders write vs write, which is this system's
consistency contract. Revisit only if measurements demand finer causality.

## Consistency Modes

Set via `--consistency`:

**Async (default)** — the local write commits immediately; fan-out to
replicas is best-effort, and the client sees success right after the local
commit. Unreachable replicas get hints queued for later replay.
- Pros: lowest latency, always available.
- Cons: replicas may briefly lag the primary.

**Quorum** — the local write commits, then fans out to replicas. The client
sees success only after `W = total / 2 + 1` acknowledgements, where
`total = replica_factor + 1` (including local). Fewer than `W` acks fails the
write with `NotReady`. E.g. with 2 replicas (`total = 3`), `W = 2`
(local + 1 replica).
- Pros: stronger consistency — reads with `R > 1` always see the latest write.
- Cons: higher latency; writes fail if too many replicas are down.

## Persistence

Optional disk persistence via snapshot + write-ahead log (WAL).

**Snapshot format** — magic `0x43534E50` ("CSNP"), format version 2. All
integers are **native byte order** (little-endian on x86).

| Field              | Size | Description |
|--------------------|------|--------------|
| `magic`             | 4    | `0x43534E50` ("CSNP") |
| `format_version`    | 4    | `1` (`uint32`) |
| `next_version`      | 8    | monotonic version counter, `uint64` |
| `entry_count`       | 4    | number of entries, `uint32` |
| — per entry — | | |
| `key_len`           | 4    | `uint32` |
| `key`               | N    | |
| `val_len`           | 4    | `uint32` |
| `value`             | M    | |
| `version`           | 8    | LWW version, `uint64` |
| `writer_node_hash`  | 8    | `uint64` |
| `expires_at_ms`     | 8    | `uint64`, `0` = no TTL |
| `has_ttl`           | 1    | `0` or `1` |
| `freq`              | 8    | access frequency for LFU, `uint64` (`0` for LRU) |
| `tombstone`         | 1    | `0` or `1` (v2: delete markers survive restarts) |

Writes are atomic: data is written to `path.tmp`, fsync'd, then renamed to
`path`, followed by a directory fsync.

**WAL format** — magic `0x57414C30` ("WAL0"), format version 2. All integers
are **native byte order** (little-endian on x86). Each entry records a `Set` or
`Del` with version metadata, a trailing tombstone byte (v2), and an 8-byte
XXH3-64 checksum of the entry payload for integrity verification. A batch of
entries is encoded identically to N sequential appends and landed with a
single write (group commit). The active log is `wal.log`; once it passes
`max_wal_segment_bytes` it is sealed into `wal_<seq>.log` (zero-padded, so
lexicographic order is chronological) and a fresh active log starts.

**Recovery** (`PersistenceManager::recover()`, on startup):
1. Find the latest snapshot in `data_dir` (via the `CURRENT` pointer file,
   falling back to a lexicographic scan).
2. Stream it into the store (entries + version counter + LFU frequencies).
3. Replay sealed segments in sequence order, then the active log, applying
   each entry via LWW; a checksum mismatch aborts with `CorruptData`, while a
   torn tail (crash mid-append) is truncated so later appends cannot poison
   the framing. The log is kept in append mode, so a crash before the next
   snapshot loses nothing already replayed.

The snapshot gives a consistent base; the WAL captures writes since.

**Compaction** — `PersistenceManager::compact()` snapshots current store
state, deletes the now-covered sealed segments, and rotates to a fresh active
WAL. Triggered by `snapshot_interval_s`, or out-of-cycle once the WAL exceeds
`max_wal_entries` or `max_wal_bytes` (sealed + active combined), or once
`max_sealed_segments` sealed segments accumulate (backstop for configs that
disable both size triggers).

## Edge Cases

**Concurrent rebalance** — several `onChange` callbacks can fire in quick
succession (e.g. multiple gossip updates arriving close together), each
triggering `rebuildRing()` → `rebalance()`. This is safe: pushes are
idempotent (version-gated LWW) and the two-phase design avoids re-entrant
deadlocks with synchronous transports.

**Node dies mid-migration** — if the target dies after receiving a
`migrateKey()` request but before the success callback fires, the transport
returns an error and `store_.remove()` is not called; the key stays local as
a failover copy and is re-evaluated on the next rebalance.

**Flapping node** — repeated join/leave keeps resetting `joined_at` via
`markAlive()`. The `scheduleRebalance()` retry loop (see
[Quarantine Window](#quarantine-window)) keeps re-deferring and retrying
until the node stabilizes.

**Crash-looping node** — the primary motivation for quarantine: without it,
a crash-looping node would attract key migrations on every restart, only to
crash again before it can serve them.

**Split brain** — incarnation-based refutation (see
[Membership State Machine](#membership-state-machine)) handles false death
detection from a network partition: the falsely-`Dead` node hears the rumor
about itself, bumps its incarnation, forces itself back to `Alive`, and the
higher incarnation overrides the stale rumor everywhere. `markAlive()` also
resets `joined_at`, so it re-enters quarantine. If a true partition leaves a
minority still serving, `isDegraded()` (visible alive count below majority of
expected cluster size) lets external clients detect it; writes accepted by
both sides of the partition are reconciled by LWW.

## Configuration Reference

| Flag | Default | Description |
|------|---------|--------------|
| `--quarantine-interval` | `10000` | Quarantine window after a node joins (ms). `0` disables it |
| `--suspect-timeout` | `3000` | Time a `Suspect` persists before being marked `Dead` (ms) |
| `--gossip-interval` | `1000` | Membership gossip dissemination interval (ms) |
| `--ping-interval` | `1000` | Failure-detector ping interval (ms) |
| `--replication-factor` | `1` | Copies per key (`1` = no replication) |
| `--consistency` | `async` | Write consistency: `async` or `quorum` |
| `--rpc-timeout` | `5000` | Per-RPC deadline for peer sends (ms). `0` = no timeout |
| `--max-hint-bytes` | `67108864` | Hinted-handoff byte budget (drop-oldest past it) |
| `--tombstone-gc-ms` | `300000` | Delete-marker GC grace (ms). Must exceed worst-case partition healing time |
| `--anti-entropy-interval` | `30000` | Anti-entropy round interval (ms). `0` disables it |
| `--anti-entropy-buckets` | `256` | Number of hash buckets for anti-entropy digest |
| `--anti-entropy-fetch-bytes` | `1048576` | Per-round anti-entropy repair fetch budget (bytes) |
| `--anti-entropy-full-rehash-interval` | `300000` | Anti-entropy full-rehash backstop interval (ms) |
| `--persistence-enabled` | `false` | Enable disk persistence (snapshot + WAL) |
| `--data-dir` | (empty) | Directory for snapshot and WAL files |
| `--snapshot-interval-s` | `60` | Seconds between automatic snapshots |
| `--max-wal-entries` | `10000` | Max WAL entries before forced compaction |
| `--max-wal-bytes` | `67108864` | Max WAL bytes (sealed + active) before forced compaction (`0` = disabled) |
| `--max-wal-segment-bytes` | `16777216` | Seal the active WAL into `wal_<seq>.log` past this size (`0` = disabled) |
| `--max-sealed-segments` | `32` | Force compaction past this many sealed segments (`0` = disabled) |
| `--wal-fsync` | `false` | fdatasync on every WAL flush (survives OS crashes, costs throughput) |

All parameters can be set via CLI flags or the YAML config file; an
explicitly passed CLI flag wins, otherwise the YAML value (or its default)
applies.

```yaml
server:
  node_id: node1
  port: 7000
  capacity: 67108864        # 64 MiB
  replication_factor: 2
  consistency: quorum
  metrics_port: 9090
  rpc_timeout_ms: 5000

cluster:
  peers:
    - id: node1
      host: 127.0.0.1
      port: 7000
    - id: node2
      host: 127.0.0.1
      port: 7001
    - id: node3
      host: 127.0.0.1
      port: 7002

failure_detector:
  ping_interval_ms: 1000
  suspect_timeout_ms: 3000
  gossip_interval_ms: 1000
  quarantine_interval_ms: 10000

anti_entropy:
  interval_ms: 30000
  buckets: 256
  fetch_bytes: 1048576
  full_rehash_interval_ms: 300000

persistence:
  enabled: true
  data_dir: /var/lib/cinder/data
  snapshot_interval_s: 60
  max_wal_entries: 10000
  max_wal_bytes: 67108864
  max_wal_segment_bytes: 16777216
  max_sealed_segments: 32
  wal_fsync: false

tls:
  enabled: false
  cert_file: /path/to/cert.pem
  key_file: /path/to/key.pem
  ca_file: /path/to/ca.pem

logging:
  level: info
```
