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
                   probe fails / timeout
     Alive ──────────────────────────────► Suspect
       ▲                                      │
       │                 suspect_timeout expires
       │                                      ▼
       │   gossip: alive (higher incarnation)  Dead
       └──────────────────────────────────────┘
```

- **Alive → Suspect**: the failure detector's ping probe fails or times out.
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
2. **Suspect** — if the `Ping` doesn't return within `suspect_timeout`, the
   peer is marked `Suspect` and a suspicion timer starts.
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
`std::unordered_map` at the 3–10 node scale typical of a Cinder cluster.

**Key lookup** — `getNodes(key, replica_factor)` returns the ordered list of
`replica_factor` successors on the ring; the first is the primary, the rest
are replicas. Dead nodes are excluded, so fewer than `replica_factor` results
are returned when not enough alive nodes exist.

## Rebalance Protocol

**Trigger:**

1. Membership change — every `onChange` callback (alive/dead/suspect) calls
   `rebuildRing()`, which updates ring membership and then calls
   `shard_.rebalance()`.
2. Quarantine retry — once `quarantine_interval` elapses, a timer retries
   `rebalance()` for keys deferred earlier due to quarantine.

**Two-phase design** avoids re-entrant deadlocks with synchronous transports:

1. *Enumerate under lock* — `store_.liveEntries()` (a `std::generator`)
   snapshots live entries under a shared lock and yields them outside it. For
   each key, the desired replica set is computed via
   `ring_.getNodes(key, replica_factor_)`, and migrate/push actions are
   collected without sending anything.
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
   retried on the next rebalance.
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

- **Queue**: bounded FIFO, capacity 1,024; the oldest hint is dropped when
  full.
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

## Anti-Entropy

Periodic background repair between replica partners, using range-hash
bucketing, run by the primary every `anti_entropy_interval`. It catches drift
that read repair and hinted handoff cannot — e.g. a replica down longer than
the hint TTL.

**Two-phase exchange:**
1. *Digest exchange* — the initiator computes a digest (N bucket hashes via
   xxHash3) and sends it to a replica partner (`ANTI_ENTROPY_DIGEST`, opcode
   9). The partner diffs digests, identifies divergent buckets, and responds
   with **its own digest** plus its entries for those buckets.
2. *Entry sync* — the initiator applies the received entries via LWW
   (`putVersioned`), derives the divergent set by comparing both digests, then
   sends its own entries for the same divergent buckets back to the partner
   (`ANTI_ENTROPY_SYNC`, opcode 10).

**Bucket digest**: keys are partitioned into N hash buckets (default 256).
Within a bucket, keys are sorted lexicographically, then hashed via xxHash3
over `key + version + writer_node_hash`; the digest is the resulting vector
of N `uint64` hashes. Two nodes with identical data produce identical
digests, so only divergent buckets need a full entry exchange.

**Entry format** (sync payload):

```
[key_len(4)][key][version(8)][writer_hash(8)][has_ttl(1)][expires_at_ms?(8)][val_len(4)][value]
```

All entries are applied via LWW (`putVersioned`), so the protocol is
idempotent and commutative — replaying the same sync blob is always safe.

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

**Snapshot format** — magic `0x43534E50` ("CSNP"), format version 1. All
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

Writes are atomic: data is written to `path.tmp`, then renamed to `path` on
success.

**WAL format** — magic `0x57414C30` ("WAL0"), format version 1. All integers
are **native byte order** (little-endian on x86). Each entry records a `Set` or
`Del` with version metadata, followed by an 8-byte XXH3-64 checksum of the
entry payload for integrity verification.

**Recovery** (`PersistenceManager::recover()`, on startup):
1. Find the latest snapshot in `data_dir`.
2. Load it into the store (entries + version counter).
3. Replay the WAL from after the snapshot timestamp, applying each entry via
   LWW.

The snapshot gives a consistent base; the WAL captures writes since.

**Compaction** — `PersistenceManager::compact()` snapshots current store
state and truncates the WAL, triggered by `snapshot_interval_s` or once the
WAL exceeds `max_wal_entries`.

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
| `--anti-entropy-interval` | `30000` | Anti-entropy round interval (ms). `0` disables it |
| `--anti-entropy-buckets` | `256` | Number of hash buckets for anti-entropy digest |
| `--persistence-enabled` | `false` | Enable disk persistence (snapshot + WAL) |
| `--data-dir` | (empty) | Directory for snapshot and WAL files |
| `--snapshot-interval-s` | `60` | Seconds between automatic snapshots |
| `--max-wal-entries` | `10000` | Max WAL entries before forced compaction |

All parameters can be set via CLI flags or the YAML config file; CLI flags
take precedence.

```yaml
server:
  node_id: node1
  port: 7000
  capacity: 67108864        # 64 MiB
  replication_factor: 2
  consistency: quorum
  metrics_port: 9090

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

persistence:
  enabled: true
  data_dir: /var/lib/cinder/data
  snapshot_interval_s: 60
  max_wal_entries: 10000

tls:
  enabled: false
  cert_file: /path/to/cert.pem
  key_file: /path/to/key.pem
  ca_file: /path/to/ca.pem

logging:
  level: info
```
