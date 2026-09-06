# Rebalance & Membership Protocol

## Overview

Cinder uses a SWIM-style membership protocol with gossip dissemination and a
consistent hash ring to distribute keys across nodes. When membership changes
(nodes join, leave, or fail), a rebalance phase migrates keys to their new
owners and pushes replicas to new replica-set members. A quarantine window
prevents crash-looping nodes from becoming hot migration targets.

This document covers the cluster lifecycle, replication, read repair, hinted
handoff, anti-entropy, consistency modes, and persistence. The wire protocol
is documented in `protocol.md`.

## Membership State Machine

Every node in the cluster is tracked by a `NodeInfo` record with three fields
that drive the state machine:

| Field | Type | Description |
|-------|------|-------------|
| `state` | enum | `Alive`, `Suspect`, or `Dead` |
| `incarnation` | uint64 | Monotonically increasing; used for conflict resolution |
| `joined_at` | time_point | Timestamp of last transition to Alive; anchors the quarantine window |

### Transitions

```
                   probe fails / timeout
     Alive ──────────────────────────────► Suspect
       ▲                                      │
       │                 spect_timeout expires│
       │                                      ▼
       │   gossip: alive (higher incarnation)  Dead
       └──────────────────────────────────────┘
```

- **Alive → Suspect**: The failure detector's Ping probe fails or times out.
  The detector records `suspect_since` and starts the escalation clock.
- **Suspect → Dead**: After `suspect_timeout` elapses without recovery, the
  detector calls `markDead()`. This is final under normal operation.
- **Dead → Alive (rejoin)**: A previously-Dead node is heard alive again via
  gossip with a strictly higher incarnation. `markAlive()` resets `joined_at`,
  starting a fresh quarantine window.

### Incarnation Numbers

Incarnation numbers provide SWIM-style conflict resolution. A gossip rumor is
only applied if its incarnation is >= the locally known value (`applyRumor()`
rejects stale entries). When a node hears a Suspect or Dead rumor about itself,
it **refutes** by bumping its incarnation to `rumor.incarnation + 1` and forcing
itself back to Alive (`refuteSelfRumor()`). The higher incarnation overrides the
stale rumor in all peers.

## Failure Detection

The failure detector runs a SWIM-style probe cycle on a configurable interval.
All probe and round-robin state is guarded by `state_mutex_` to handle
concurrent probe callbacks and timer ticks across io-pool threads.

1. **Ping**: Each tick, the detector selects a random peer and sends a `Ping`.
2. **Suspect**: If the Ping does not return within `suspect_timeout`, the peer
   is marked `Suspect` and a suspicion timer starts.
3. **Dead**: If the peer is still Suspect after `suspect_timeout` elapses (from
   the initial probe), it is marked `Dead`.
4. **Blackhole detection**: A timed-out probe (no response within
   `suspect_timeout`) also triggers Suspect marking, separate from the async
   callback path.

The `MembershipTable` mutations and `sendAsync()` happen outside the lock — the
transport may complete callbacks synchronously and re-enter the failure detector,
so the lock only serializes probe state, not network I/O.

## Gossip Dissemination

Membership state is disseminated via the `GossipManager`, which periodically
(every `gossip_interval`) sends the full membership view to a randomly selected
peer.

### Wire Format

The GOSSIP payload (opcode 5) carries a semicolon-delimited text string:

```
id@host:port:state:incarnation;id@host:port:state:incarnation;...
```

Example:

```
node1@127.0.0.1:7000:alive:3;node2@127.0.0.1:7001:dead:7
```

### Application

On receipt, `handleMessage()` decodes the view and calls `applyRumor()` for
each entry. Parsing uses `std::from_chars` for zero-allocation,
exception-free integer conversion (port numbers and incarnation counters).
The incarnation guard ensures only newer information is adopted.
When `applyRumor()` results in a state or incarnation change, it fires
`fireCallbacks()`, which notifies:

1. `CacheNodeServer` — rebuilds the hash ring and triggers rebalance.
2. `GossipManager` — rebuilds its peer list.
3. `FailureDetector` — rebuilds its peer list.

### Dynamic Peer Lists

`GossipManager` and `FailureDetector` register `table_.onChange(...)` callbacks
that rebuild their internal `peers_` vectors under their respective mutexes.
This ensures peer selection reflects the latest membership without requiring
periodic full scans.

### Graceful Leave

When a `CacheNodeServer` shuts down:

1. **Timers are cancelled** (replay, gossip, probe, evict, quarantine, compact).
2. **`gossip_.leave()`** is called:
   - `markDead(self_)` sets this node's state to Dead and bumps its incarnation.
   - The full membership view (including self as Dead) is sent to **every known
     peer** via `sendView()`.
3. **Pending handlers are drained**: `while (io_.poll() > 0) {}` ensures the
   leave gossip messages are flushed before the transport is torn down.

This prevents the failure detector on peer nodes from suspect-marking this node
during the shutdown window — they learn it is Dead via the explicit leave
broadcast before any probe would time out.

## Consistent Hash Ring

The `ConsistentHashRing` maps keys to nodes using xxHash3 virtual nodes (150
per physical node). The ring uses an immutable-snapshot design:

1. A sorted vector of `(hash, NodeId)` pairs represents the ring.
2. Updates (add/remove) create a new vector and atomically swap it in via
   `std::atomic<std::shared_ptr<const RingSnapshot>>`.
3. Reads lock-free using binary search over the snapshot.
4. Mutators use a CAS retry loop, so concurrent mutators merge instead of
   losing updates.

Cluster-scale lookup maps (`MembershipTable::nodes_`,
`ConnectionPool::node_addrs_`, `TcpTransport::addrs_`) use `std::flat_map`
for cache-friendly, sorted-key iteration with lower memory overhead than
`std::unordered_map` at the 3–10 node scale typical of a Cinder cluster.

### Key Lookup

`getNodes(key, replica_factor)` returns the ordered list of `replica_factor`
successors on the ring. The first entry is the primary; the rest are replicas.
Dead nodes are excluded — if fewer than `replica_factor` alive nodes exist,
only the available ones are returned.

## Rebalance Protocol

### Trigger

Rebalance is triggered by:

1. **Membership change**: Every `onChange` callback (node alive/dead/suspect)
   calls `rebuildRing()`, which adds/removes nodes from the ring and then calls
   `shard_.rebalance()`.
2. **Quarantine retry**: After `quarantine_interval` elapses, a timer retries
   `rebalance()` for any keys that were deferred due to quarantine.

### Two-Phase Design

`rebalance()` operates in two phases to avoid re-entrant deadlocks with
synchronous transports:

1. **Phase 1 — Enumerate under lock**: `store_.liveEntries()` (a
   `std::generator`) snapshots live entries under a shared lock, then yields
   them outside the lock. For each key, the desired replica set is computed via
   `ring_.getNodes(key, replica_factor_)`. Actions (migrate or push) are
   collected into vectors without sending.
2. **Phase 2 — Send after unlock**: The store lock is released, then the
   collected migrate/push requests are sent sequentially.

### Key Stays Local (node is in desired set)

If this node is one of the `replica_factor` desired owners:

- The key stays in the local store.
- A replica copy is pushed to every **other** desired owner via `pushReplica()`.
  This repairs gaps left by joins or removals.
- Pushes to quarantined targets are skipped (`deferred = true`).
- Pushes are idempotent (version-gated LWW apply), so duplicate pushes are
  harmless.

### Key Migrates (node is NOT in desired set)

If this node is not in the desired replica set:

1. **Migrate to new primary**: `migrateKey()` sends a `Replicate` request to
   `desired[0]`. On success, the local copy is removed. On failure, the key
   stays local as a failover copy (retried on next rebalance).
2. **Push replicas**: `pushReplica()` sends copies to `desired[1..N]`, skipping
   quarantined targets.

### Replicate Request

`makeReplicateRequest()` constructs a `Replicate` (opcode 6) request carrying
key, value, version, writer_node_hash, and optionally `expires_at` (converted to
system absolute time for consistent TTL across replicas).

## Quarantine Window

### Purpose

A quarantine is a grace period after a node joins (or re-joins) during which
other nodes will **not** push keys to it. This prevents a crash-looping node
from becoming a hot migration target — each restart would otherwise immediately
attract key transfers, only to crash again.

### How It Works

The quarantine is derived from `NodeInfo::joined_at`:

```
isQuarantined = (quarantine_interval_ms > 0)
             && (state == Alive)
             && (now - joined_at < quarantine_interval_ms)
```

A node is quarantined if:
1. `quarantine_interval_ms > 0` (default 10,000 ms).
2. The node is `Alive` (suspect/dead nodes cannot receive keys anyway).
3. The time since `joined_at` is less than the quarantine interval.

### When `joined_at` Is Set

- **Initial seed**: `seed()` sets `joined_at = now()` for all peers.
- **New node discovered via gossip**: `applyRumor()` sets `joined_at = now()`.
- **Recovery from Suspect/Dead**: `markAlive()` resets `joined_at = now()`.
  Every recovery starts a fresh quarantine window.

### Retry Mechanism

If `rebalance()` returns `true` (some keys were deferred due to quarantine),
`CacheNodeServer::rebuildRing()` calls `scheduleRebalance()`:

```
scheduleRebalance():
    if quarantine_interval <= 0: return
    timer.expires_after(quarantine_interval)
    timer.async_wait():
        if shard_.rebalance():    // still deferred?
            scheduleRebalance()   // retry again after interval
```

This creates a self-retrying loop: after `quarantine_interval` milliseconds, it
tries again. If a flapping node keeps resetting `joined_at`, the loop continues
until all deferred keys have been migrated.

### Configuration

| Parameter | Default | Description |
|-----------|---------|-------------|
| `--quarantine-interval` | `10000` ms | Duration of the quarantine window after a node joins. `0` disables quarantine entirely. |

## Edge Cases

### Concurrent Rebalance

Multiple `onChange` callbacks can fire in rapid succession (e.g., several gossip
updates arriving close together). Each triggers `rebuildRing()` → `rebalance()`.
The two-phase design and idempotent pushes make this safe: duplicate pushes are
harmless (version-gated LWW), and the two-phase design prevents re-entrant
deadlocks with synchronous transports.

### Node Dies During Migration

If the target node dies after receiving a `migrateKey()` request but before the
success callback fires, the transport returns an error and `store_.remove()` is
**not** called. The key stays local as a failover copy. On the next rebalance
(triggered by the membership change), the key is re-evaluated against the new
ring topology.

### Flapping Node

A node that repeatedly joins and leaves keeps resetting `joined_at` (via
`markAlive()` after recovery from Dead/Suspect). The `scheduleRebalance()` retry
loop handles this: if `rebalance()` still returns `deferred`, it re-schedules
for another `quarantine_interval` later. The loop continues until the node
stabilizes.

### Crash-Looping Node

This is the primary motivation for the quarantine window. Without quarantine, a
crash-looping node would repeatedly attract key migrations, only to crash again.
During quarantine, no keys are pushed to it, allowing it to stabilize first.

### Split Brain

The SWIM protocol with incarnation-based refutation handles false death
detection. If a node is falsely marked Dead (e.g., due to a network partition),
when it comes back it will:
1. Hear the Dead rumor about itself via gossip.
2. Bump its incarnation and force itself Alive (`refuteSelfRumor()`).
3. The higher incarnation overrides the stale Dead rumor in all peers.
4. `markAlive()` resets `joined_at`, starting a fresh quarantine window.

If a true partition occurs where a minority partition continues to serve, the
`isDegraded()` check (visible alive count < majority of expected cluster size)
can help external clients detect the partition. Writes to both partitions are
resolved by LWW (last-writer-wins via version + writer_node_hash).

## Liveness Checks in Rebalance

During `rebalance()`, the shard manager checks the membership table before
sending any push or migrate request. If the target node is `Suspect` or `Dead`,
the send is skipped (the key stays local or the push is deferred). This prevents
wasted network I/O and avoids sending data to nodes that will not ack.

## Read Repair

When a quorum read (`R > 1`) is performed, `ReplicationManager::readAsync()`
compares responses from the local store and all replicas using LWW (last-writer-wins
via version + writer_node_hash). If any replica has a newer version than the current
best, the read is flagged as `needs_repair`.

### Repair Mechanism

1. **Local self-heal**: If the local store is behind a replica, it is updated
   via `putVersioned()` (LWW — stale writes are no-ops).
2. **Fan-out repair**: `sendRepairFanOut()` sends a `Replicate` request carrying
   the winning entry (value, version, writer_node_hash, expires_at) to **all**
   replica nodes — not just the stale one. This is idempotent (version-gated LWW),
   so pushing to an already-current replica is harmless.

### When Repair Is Triggered

- **Quorum read** (`R > 1`): fan-out `GetVersioned` to all replicas, compare
  versions, repair if needed.
- **Read miss on replica**: If a replica returns `NotFound` but the key exists
  elsewhere, the replica is stale and repair is triggered.

Read repair handles transient inconsistencies (e.g., a replica missed a write
due to a transient failure). Anti-entropy handles deeper drift.

## Hinted Handoff

When a write (async or quorum) fails to reach a replica node, a **hint** is
enqueued in the local `HintQueue` for later replay. This prevents data loss when
a replica is temporarily unreachable.

### Hint Queue

- **Bounded FIFO**: capacity = 1,024 hints. When full, the oldest hint is dropped.
- **TTL**: hints expire after 30 seconds (`K_HINT_TTL`). Expired hints are
  dropped during replay.
- **Thread-safe**: all operations take an internal mutex.

### Hint Structure

```
struct Hint {
    NodeId target;        // unreachable replica node
    net::Request req;     // the original Replicate request
    steady_clock::time_point expires_at;  // hint TTL
};
```

### Replay

`ReplicationManager::replayHints()` snapshots live hints under the lock, then
sends each hint to its target node. A single-consumer guard (`replaying_` atomic)
prevents overlapping replays from concurrent timer ticks or pool threads.

Successful replays call `remove()` to drop the hint from the queue. Failed or
expired hints are silently dropped.

### When Hints Are Enqueued

- **Async write**: replica unreachable → `enqueueHint(node, req)`
- **Quorum write**: replica unreachable → `enqueueHint(node, req)`
- **Replica failover test**: verifies hints are replayed when the replica returns

## Anti-Entropy

Periodic background repair between replica partners using range-hash bucketing.
This handles deeper drift that read repair and hinted handoff cannot catch
(e.g., a replica was down for longer than the hint TTL).

### Protocol

Two-phase exchange (initiated by primary every `anti_entropy_interval`):

1. **Phase 1 — Digest exchange**: Initiator computes a digest (N bucket hashes
   via xxHash3) and sends it to a replica partner (`ANTI_ENTROPY_DIGEST`, opcode 9).
   The partner compares digests, identifies divergent buckets, and responds with
   the divergent bucket IDs + its entries for those buckets.
2. **Phase 2 — Entry sync**: Initiator applies received entries via LWW
   (`putVersioned`), then sends its entries for the same divergent buckets back
   to the partner (`ANTI_ENTROPY_SYNC`, opcode 10).

### Bucket Digest

Keys are partitioned into N hash buckets (default 256). Within each bucket, keys
are sorted lexicographically, then hashed via xxHash3 over key + version +
writer_node_hash. The resulting digest is a vector of N uint64 hashes.

Two nodes with identical data produce identical digests. Divergent bucket hashes
indicate differing entries — only those buckets trigger full entry exchange.

### Entry Format

Each entry in the sync payload is serialized as:

```
[key_len(4)][key][version(8)][writer_hash(8)][has_ttl(1)][expires_at_ms?(8)][val_len(4)][value]
```

All entries are applied via LWW (`putVersioned`), making the protocol idempotent
and commutative. Replaying the same sync blob is always safe.

### Configuration

| Parameter | Default | Description |
|-----------|---------|-------------|
| `--anti-entropy-interval` | `30000` ms | Interval between anti-entropy rounds. `0` = disabled |
| `--anti-entropy-buckets` | `256` | Number of hash buckets for digest computation |

## Consistency Modes

Cinder supports two write consistency modes, configured via `--consistency`:

### Async (default)

Local write commits immediately, then fan-out to replicas is best-effort. The
client receives success after the local commit. Unreachable replicas get hints
queued for later replay.

- **Pros**: lowest latency, always available.
- **Cons**: replicas may be briefly behind the primary.

### Quorum

Local write commits, then fan-out to replicas. The client receives success only
after `W = R/2 + 1` acknowledgements (including the local write). If fewer than
W replicas ack, the write fails with `NotReady`.

- **Pros**: stronger consistency — reads with `R > 1` always see the latest write.
- **Cons**: higher latency, writes fail if too many replicas are down.

The quorum formula: with `replica_factor` replicas, `total = replica_factor + 1`
(including local), `W = total / 2 + 1`. For example, with 2 replicas (3 total),
W = 2 (local + 1 replica).

## Persistence

Cinder supports optional disk persistence via snapshot + WAL (write-ahead log).

### Snapshot Format

Binary format with magic `0x43534E50` ("CSNP") and format version 1:

```
Field              Size  Description
────────────────   ────  ─────────────────────────────────
magic              4     0x43534E50 ("CSNP")
format_version     4     1 (uint32)
next_version       8     Monotonic version counter (uint64)
entry_count        4     Number of entries (uint32)
── per entry ─────────────────────────────────────────────
key_len            4     Key length (uint32)
key                N     Key bytes
val_len            4     Value length (uint32)
value              M     Value bytes
version            8     LWW version (uint64)
writer_node_hash   8     Writer node hash (uint64)
expires_at_ms      8     Absolute expiry ms (uint64, 0 = no TTL)
has_ttl            1     0 or 1 (uint8)
freq               8     Access frequency for LFU (uint64, 0 for LRU)
```

Atomic write: data is written to `path.tmp`, then renamed to `path` on success.

### WAL Format

Binary format with magic `0x57414C30` ("WAL0") and format version 1. Each entry
records a Set or Del operation with version metadata for crash recovery.

### Recovery

On startup, `PersistenceManager::recover()`:

1. Finds the latest snapshot file in `data_dir`.
2. Loads the snapshot into the store (restores all entries + version counter).
3. Replays the WAL from after the snapshot timestamp, applying each entry via LWW.

This ensures crash consistency: the snapshot provides a consistent base, and the
WAL captures writes since the last snapshot.

### Compaction

`PersistenceManager::compact()` creates a new snapshot from the current store
state and truncates the WAL. This is triggered periodically based on
`snapshot_interval_s` or when the WAL exceeds `max_wal_entries`.

### Configuration

| Parameter | Default | Description |
|-----------|---------|-------------|
| `--persistence-enabled` | `false` | Enable disk persistence |
| `--data-dir` | (empty) | Directory for snapshot and WAL files |
| `--snapshot-interval-s` | `60` | Seconds between automatic snapshots |
| `--max-wal-entries` | `10000` | Max WAL entries before forced compaction |

## Configuration Reference

| Flag | Default | Description |
|------|---------|-------------|
| `--quarantine-interval` | `10000` | Quarantine window after a node joins (ms). `0` = off |
| `--suspect-timeout` | `3000` | Time a suspect persists before being marked dead (ms) |
| `--gossip-interval` | `1000` | Membership gossip dissemination interval (ms) |
| `--ping-interval` | `1000` | Failure-detector ping interval (ms) |
| `--replication-factor` | `1` | Number of copies per key (1 = no replication) |
| `--consistency` | `async` | Write consistency: `async` or `quorum` |
| `--rpc-timeout` | `5000` | Per-RPC deadline for peer sends (ms). `0` = no timeout |
| `--anti-entropy-interval` | `30000` | Anti-entropy round interval (ms). `0` = disabled |
| `--anti-entropy-buckets` | `256` | Number of hash buckets for anti-entropy digest |
| `--persistence-enabled` | `false` | Enable disk persistence (snapshot + WAL) |
| `--data-dir` | (empty) | Directory for snapshot and WAL files |
| `--snapshot-interval-s` | `60` | Seconds between automatic snapshots |
| `--max-wal-entries` | `10000` | Max WAL entries before forced compaction |

All parameters can be set via CLI flags or the YAML configuration file. CLI
flags override config file values.

## YAML Configuration Example

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
