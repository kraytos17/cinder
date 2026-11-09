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

> Implementation names below are the Go ones (`internal/cluster` for
> membership/gossip/failure detection, `internal/shard` for rebalance,
> `internal/repl` for replication and hints, `internal/ae` and
> `internal/persist` for anti-entropy and durability, `internal/store`
> for tombstones and TTL). Behavior matches the retired C++ tree; where
> the Go port deliberately diverges (full-scan digests, uncompressed
> snapshots only, no metrics collector), it is called out inline.

## Membership State Machine

Each node is tracked by a `NodeInfo` record:

| Field | Type | Description |
|-------|------|--------------|
| `state` | enum | `Alive`, `Suspect`, or `Dead` |
| `incarnation` | `uint64` | Monotonically increasing; used for conflict resolution |
| `joined_at` | `time.Time` | Timestamp of the last transition to `Alive`; anchors the quarantine window |

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
  `SuspectThreshold` (2) times consecutively, or a probe times out.
  The detector records the suspicion start and starts the escalation clock.
- **Suspect → Dead**: `MarkDead()` fires once `suspect_timeout` elapses
  without recovery. Final under normal operation.
- **Dead → Alive (rejoin)**: a previously-`Dead` node is heard alive again via
  gossip with a strictly higher incarnation. `MarkAlive()` resets `joined_at`,
  starting a fresh quarantine window.

**Incarnation numbers** provide SWIM-style conflict resolution: `ApplyRumor()`
only applies a gossip rumor if its incarnation is ≥ the locally known value,
rejecting stale entries. When a node hears a `Suspect` or `Dead` rumor about
itself, it refutes by bumping its own incarnation to `rumor.incarnation + 1`
and forcing itself back to `Alive`; the higher
incarnation then overrides the stale rumor everywhere it propagates.

## Failure Detection

A SWIM-style probe cycle runs on a configurable interval. Probe and
round-robin state live behind the detector's mutex: timer ticks and probe
callbacks may fire concurrently from different goroutines, so the lock
serializes probe state while sends (synchronous pool RPCs with deadlines)
happen outside it.

1. **Ping** — each tick, a random peer is selected and sent a `Ping`.
2. **Suspect** — if `K_SUSPECT_THRESHOLD` (2) consecutive `Ping`s fail, the
   peer is marked `Suspect` and a suspicion timer starts. A single transient
   failure is tolerated without flapping the ring.
3. **Dead** — if the peer is still `Suspect` after `suspect_timeout` elapses
   from the initial probe, it is marked `Dead`.
4. **Blackhole detection** — a timed-out probe (no response within
   `suspect_timeout`) also triggers `Suspect`, independent of the async
   callback path.

`MembershipTable` mutations never run while holding the table lock across
network I/O: callbacks fire without the lock, so observers (ring rebuild,
peer-list refresh) may safely re-enter the table.

## Gossip Dissemination

`GossipManager` periodically (every `gossip_interval`) sends the full
membership view to one randomly selected peer.

**Wire format** — the `GOSSIP` payload (opcode 5) is a semicolon-delimited
text string:

```
id@host:port:state:incarnation;id@host:port:state:incarnation;...
```

e.g. `node1@127.0.0.1:7000:alive:3;node2@127.0.0.1:7001:dead:7`

**Application** — on receipt, `HandleMessage()` decodes the view and calls
`ApplyRumor()` per entry. Integer fields (port, incarnation) parse without
allocations. The
incarnation guard ensures only newer information is adopted. Any resulting
state or incarnation change fires the table's change callbacks, which notify:

1. `tcp.Server` — rebuilds the hash ring (and its pool addresses) and triggers rebalance.
2. `GossipManager` — rebuilds its peer list.
3. `FailureDetector` — rebuilds its peer list.

`GossipManager` and `FailureDetector` each register a `table.OnChange(...)`
callback that rebuilds their own peer slice, keeping peer selection current
without periodic full scans.

**Graceful leave** — on shutdown, the server:

1. Broadcasts Dead first: `gossip.Leave()` marks self `Dead` (bumping the
   incarnation so the rumor overrides stale Alive entries) and sends the
   full membership view (self included, as `Dead`) to every known peer.
2. Stops timers (gossip, probe, rebalance, replay, eviction, quarantine, compact)
   and closes the listener and connections.

This lets peers learn the node is `Dead` from the explicit broadcast, rather
than waiting for their own probes to time out and suspect-mark it.

## Consistent Hash Ring

`hashring.Ring` maps keys to nodes using xxHash3 virtual nodes (150 per
physical node) behind a mutex:

1. A sorted slice of `(hash, nodeID)` pairs represents the ring.
2. `SetNodes` rebuilds that slice in place from the alive set; lookups
   binary-search it.
3. Rebuilds are cheap and rare (one per membership change), so no
   lock-free snapshot machinery: the mutex serializes writers and readers.

**Key lookup** — `GetNodes(key, replica_factor)` returns the ordered list of
`replica_factor` successors on the ring; the first is the primary, the rest
are replicas. Nodes not in the alive set are never members, so fewer than
`replica_factor` results are returned when not enough alive nodes exist.

## Rebalance Protocol

**Trigger:**

1. Membership change — every `OnChange` callback rebuilds the ring, which
   updates ring membership and pool addresses, then calls `Rebalance()`
   via a 200 ms debounced timer (`scheduleRebalanceDebounced`).
   Rapid flaps collapse into a single scan over the latest ring view.
2. Quarantine retry — once `quarantine_interval` elapses, a timer retries
   `Rebalance()` for keys deferred earlier due to quarantine.
3. Migration-failure retry — each failed async `migrateKey()` send keeps the
   local copy and fires `onMigrationFailed`, which schedules a single 2 s
   delayed re-rebalance through the same debounce gate
   (`scheduleMigrationRetry`). A burst of failures coalesces into one retry,
   giving a briefly-overwhelmed new owner (TCP accept queue, TLS handshake
   backlog) time to recover.

**Two-phase design** avoids re-entrant deadlocks with synchronous transports:

1. *Enumerate a snapshot* — `LiveEntries()` returns a point-in-time copy of
   all live entries (tombstones included, expired entries skipped). For each
   key, the desired replica set is computed via `ring.GetNodes(key, rf)`,
   and migrate/push actions are collected without sending anything.
2. *Send after enumeration* — the collected requests are sent (replica
   pushes concurrently); the store is freely mutable throughout, since no
   store state is held across the two phases.

**Key stays local** (this node is in the desired set):
- The key stays in the local store.
- A replica copy is pushed to every other desired owner via `pushReplica()`,
  repairing gaps left by joins or removals.
- Pushes to quarantined targets are skipped (`deferred = true`).
- Pushes are idempotent (version-gated LWW apply), so duplicates are harmless.

**Key migrates** (this node is not in the desired set):
1. `migrateKey()` sends a `Replicate` request to the whole new set
   (primary plus every other member, concurrently). Only once **every**
   new holder has acked is the local copy dropped via a hard erase —
   retention is always safe (an extra copy is redundant, never split-brain)
   while destruction on a stale ack is not. On primary failure the key
   stays local as a failover copy and the migration-failed callback
   schedules a 2 s retry; a primary ack with unconfirmed replicas likewise
   retains the copy for the next round (the next rebalance, hint replay,
   or anti-entropy covers the gap independently).
2. Skipped targets (quarantined or not `Alive`) are deferred or skipped at
   send time (see below).

`makeReplicateRequest()` builds the `Replicate` (opcode 6) request: key,
value, `version`, `writer_node_hash`, and `expires_at` when applicable
(converted to absolute system time for consistent TTL across replicas).

**Liveness check** — before any push or migrate send, the shard manager
checks the membership table; sends to `Suspect` or `Dead` targets are
skipped, avoiding wasted I/O to nodes that won't ack. (There is no metrics
collector: failures and retries are visible only as behavior — retained
copies and timer-driven re-runs.)

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

`joined_at` is set on: initial seeding (`Seed()`, for all peers), first
discovery via gossip (`ApplyRumor()`), and recovery from `Suspect`/`Dead`
(`MarkAlive()`) — so every recovery starts a fresh quarantine window.

**Retry** — if `Rebalance()` returns `true` (some keys deferred due to
quarantine), the server calls `scheduleQuarantineRetry()`:

```
scheduleQuarantineRetry():
    if quarantine_interval <= 0: return
    after(quarantine_interval):
        if Rebalance():    // still deferred?
            scheduleQuarantineRetry()   // retry again after interval
```

This self-retries every `quarantine_interval` until all deferred keys have
been migrated, even against a flapping node that keeps resetting `joined_at`.

## Read Repair

On a quorum read (`R > 1`), the replication manager compares
responses from the local store and all replicas using LWW (version +
`writer_node_hash`). A replica with a newer version than the current best
flags the read as `needs_repair`.

**Repair:**
1. *Local self-heal* — if the local store is behind, it is updated via
   `PutVersioned()` (LWW, so a stale write is a no-op).
2. *Fan-out repair* — a `Replicate` request carrying the winning entry is
   sent to **all** replicas, not just the stale one.
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
  estimated-bytes budget (default 64 MiB, adjustable via `SetMaxHintBytes`).
  When either bound would be exceeded,
  the oldest hints are evicted first; a single hint larger than the whole
  budget is rejected. (No metrics: evictions and rejections are silent.)
- **Coalescing**: at most one live hint per (replica, key) — the queue is
  keyed by `(target, key)` handle, so a newer write
  for an already-queued key replaces it (TTL refreshed); an older-or-equal
  write is ignored. LWW order decides:
  higher version wins, ties break toward the larger writer hash, and an
  identical stamp counts as newer so retries refresh the TTL. A successful
  replay removes exactly its own handle, so a superseding newer hint can
  never be deleted by a stale completion. Coalescing preserves budget for
  distinct keys during an outage instead of spending it on superseded versions.
- **TTL**: 30 s; expired hints are dropped during replay.
- **Thread safety**: all operations take an internal mutex.

```go
type Hint struct {
    ID        uint64 // stable handle for replay removal
    Target    string // unreachable replica node
    Req       protocol.Request // the original Replicate request
    ExpiresAt time.Time // hint TTL
}
```

`Manager.ReplayHints()` snapshots live hints under the lock, then
sends each to its target. A single-consumer guard (atomic bool)
prevents overlapping replays from concurrent timer ticks or pool threads.
Successful replays remove the hint; failed or expired hints are dropped
silently.

Hints are enqueued whenever a replica is unreachable during an async or
quorum write.

## Hint Durability

With persistence enabled, hints are journaled to
`<data_dir>/hints.log` so they survive a node crash or restart (a plain
in-memory queue would lose them). Recovery replays the journal through the
same push/remove path, so coalescing, LWW ordering and both bounds come out
identically; only successfully replayed hints are deleted (a `DEL` record),
and expired records are skipped.

- **Crash window**: appends are page-cache writes; only `Sync`/`Close`
  flush. A torn tail record is truncated on open.
- **Compaction**: past 4,096 records the journal is rewritten from the live
  queue snapshot (drops and expired hints fall out), keeping long outages
  from growing the file without bound. Atomic via tmp file + rename; the
  replication manager triggers it inline on enqueue, so retries apply.
- **Recovery order**: hint recovery runs before serving. A background
  replay tick (1 s, `SetReplayInterval` for tests) drains queued hints
  once their target is reachable again; ticks that fire while it is still
  down fail silently and the next tick retries.

## Anti-Entropy

Periodic background repair between replica partners, using range-hash
bucketing. Every node runs rounds against its partners each
`anti-entropy-interval`; it catches drift
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
   `(bucket, key_hash, version, writer, tombstone)` — 29 bytes each — for the buckets
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

**Full-scan digests**: every round rehashes the whole store (a plain scan
XOR-folding each entry into its bucket), so converged rounds cost one scan
instead of a 2 KiB copy. The retired C++ tree kept per-bucket XORs on the
write path with a wall-clock full-rehash backstop; the Go port deliberately
replaces that with a full rehash per round — behavior-identical, no
divert-while-scanning machinery, no `full-rehash-interval` knob. The cost
is ~1 alloc/entry (see `just bench`: `BenchmarkComputeDigest`), which is
the standing invitation to reintroduce incremental digests if a profile
ever demands it.

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
integers are **native byte order** (little-endian on x86). Versions parse
into a format enum; readers accept v1/v2 and writers always emit v2, so a
future v3 would add an enumerator instead of new branches at every use
site.

| Field              | Size | Description |
|--------------------|------|--------------|
| `magic`             | 4    | `0x43534E50` ("CSNP") |
| `format_version`    | 4    | `2` (`uint32`) |
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
| `freq`              | 8    | access frequency (`0`: the Go store is LRU-only, so this field is always written `0` and ignored on read) |
| `tombstone`         | 1    | `0` or `1` (v2: delete markers survive restarts) |

Writes are atomic: data is written to `path.tmp`, fsync'd, then renamed to
`path`, followed by a directory fsync.

**WAL format** — magic `0x57414C30` ("WAL0"), format version 2. All integers
are **native byte order** (little-endian on x86). Versions are parsed once
into `WalFormat` (`Legacy` = pre-magic format with no checksums); readers
accept every known version and writers always emit current. Each entry records a `Set` or
`Del` with version metadata, a trailing tombstone byte (v2), and an 8-byte
XXH3-64 checksum of the entry payload for integrity verification. A batch of
entries is encoded identically to N sequential appends and landed with a
single write (group commit). The active log is `wal.log`; sealed
`wal_<seq>.log` segments (zero-padded, so lexicographic order is chronological)
are honored on recovery -- e.g. from a previously C++-managed data dir -- but
the Go writer never seals by size: `Compact` snapshots, deletes sealed
segments, and truncates to a fresh active log.

**Recovery** (on startup):
1. Find the latest snapshot in `data_dir` (via the `CURRENT` pointer file,
   falling back to a lexicographic scan).
2. Stream it into the store (entries + version counter; the LFU frequency
   field is ignored).
3. Replay sealed segments in sequence order, then the active log, applying
   each entry via LWW; a checksum mismatch aborts, while a
   torn tail (crash mid-append) is truncated so later appends cannot poison
   the framing. The log is kept in append mode, so a crash before the next
   snapshot loses nothing already replayed.

The snapshot gives a consistent base; the WAL captures writes since.

**Compaction** — `Compact()` snapshots current store
state, deletes the now-covered sealed segments, and truncates to a fresh
active WAL. It runs on demand (`cinder-admin compact`), on a snapshot
interval (default 60s, `--snapshot-interval-s`), and out-of-cycle once the
active WAL passes `--max-wal-bytes` (default 64 MiB, `0` disables that
trigger).

**Snapshot format** — snapshots are versioned (`CSNP` magic + u32 version).
v1/v2 store entries raw inline (v2 adds the trailing tombstone byte);
writers emit v2 uncompressed, readers accept v1/v2. v3 (zstd-compressed
256 KiB blocks in the retired C++ tree) is rejected outright — enabling
it mid-cluster is not applicable here.

## Edge Cases

**Concurrent rebalance** — several `OnChange` callbacks can fire in quick
succession (e.g. multiple gossip updates arriving close together), each
triggering `rebuildRingFromTable()` → `Rebalance()`. This is safe: pushes are
idempotent (version-gated LWW) and the two-phase design (snapshot first,
sends after) never holds store state across a send.

**Node dies mid-migration** — if the target dies after receiving a
`migrateKey()` request but before all sends complete, the transport
returns errors and `Remove()` is not called; the key stays local as a
failover copy and is re-evaluated on the next rebalance.

**Flapping node** — repeated join/leave keeps resetting `joined_at` via
`MarkAlive()`. The `scheduleQuarantineRetry()` loop (see
[Quarantine Window](#quarantine-window)) keeps re-deferring and retrying
until the node stabilizes.

**Crash-looping node** — the primary motivation for quarantine: without it,
a crash-looping node would attract key migrations on every restart, only to
crash again before it can serve them.

**Split brain** — incarnation-based refutation (see
[Membership State Machine](#membership-state-machine)) handles false death
detection from a network partition: the falsely-`Dead` node hears the rumor
about itself, bumps its incarnation, forces itself back to `Alive`, and the
higher incarnation overrides the stale rumor everywhere. `MarkAlive()` also
resets `joined_at`, so it re-enters quarantine. If a true partition leaves a
minority still serving, `IsDegraded()` (visible alive count below majority of
expected cluster size) lets external clients detect it; writes accepted by
both sides of the partition are reconciled by LWW.

## Configuration Reference

Flags only (`cinderd --help` is authoritative); there is no config file and
no hot-reload. `config-reload` is accepted and answers OK.

| Flag | Default | Description |
|------|---------|--------------|
| `--port` | `7000` | TCP listen port |
| `--node-id` | `node1` | Stable node identifier |
| `--capacity` | `67108864` | Per-node store capacity in bytes |
| `--peers` | (empty) | Cluster members, comma-separated `id@host:port` |
| `--replication-factor` | `1` | Copies per key (`1` = none) |
| `--consistency` | `async` | Write consistency: `async` or `quorum` |
| `--gossip-interval-ms` | `1000` | Membership gossip dissemination interval |
| `--ping-interval-ms` | `1000` | Failure-detector ping interval |
| `--suspect-timeout-ms` | `3000` | Time a `Suspect` persists before being marked `Dead` |
| `--quarantine-interval-ms` | `10000` | Rejoin warming window (`0` disables it) |
| `--anti-entropy-interval-ms` | `30000` | Anti-entropy round interval (`0` disables it) |
| `--anti-entropy-buckets` | `256` | Digest buckets |
| `--grpc-port` | `0` | gRPC gateway port (`0` disables it) |
| `--tls` | `false` | Enable TLS encryption (requires `--tls-cert` + `--tls-key`) |
| `--tls-cert` / `--tls-key` | (empty) | PEM certificate chain / private key |
| `--tls-ca` | (empty) | CA for peer verification (chain-only, like asio `verify_peer`) |
| `--shared-secret` | (empty) | Node auth secret (or `CINDER_SHARED_SECRET`); replication opcodes require a token minted with it |
| `--client-shared-secret` | (empty) | Client auth secret (or `CINDER_CLIENT_SECRET`); client/admin opcodes and the gRPC bearer require it; needs `--tls` |
| `--enable-persistence` | `false` | Enable WAL + snapshots |
| `--data-dir` | (empty) | Directory for WAL, snapshots, and `hints.log` |
| `--snapshot-interval-s` | `60` | Seconds between automatic snapshots (`0` disables it) |
| `--max-wal-bytes` | `67108864` | Active WAL bytes before out-of-cycle compact (`0` = disabled) |

Fixed (not flags): pool RPC timeout 5 s; hint TTL 30 s with 1,024-entry /
64 MiB budgets; tombstone GC grace 5 min; AE fetch budget 1 MiB per round;
snapshots v2 uncompressed only (v3 rejected); WAL v2 only (Legacy/V1/V2
accepted on read); compaction on demand via `cinder-admin compact` only.
The gRPC gateway serves TLS exactly when a client secret is configured
(same certificate as `--tls-cert`/`--tls-key`); otherwise plaintext,
mirroring C++.

```bash
# Three-node cluster in a minute (terminals 1–3):
cinderd --port 7000 --node-id node1 --peers "node2@127.0.0.1:7001,node3@127.0.0.1:7002" \
  --replication-factor 2 --consistency quorum --enable-persistence --data-dir /tmp/cinder1
cinderd --port 7001 --node-id node2 --peers "node1@127.0.0.1:7000,node3@127.0.0.1:7002" \
  --replication-factor 2 --consistency quorum --enable-persistence --data-dir /tmp/cinder2
cinderd --port 7002 --node-id node3 --peers "node1@127.0.0.1:7000,node2@127.0.0.1:7001" \
  --replication-factor 2 --consistency quorum --enable-persistence --data-dir /tmp/cinder3

cinder-cli --port 7000 set mykey myvalue   # quorum write
cinder-cli --port 7001 get mykey           # via replica / redirect
```
