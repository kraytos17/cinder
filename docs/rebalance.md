# Rebalance & Membership Protocol

## Overview

Cinder uses a SWIM-style membership protocol with gossip dissemination and a
consistent hash ring to distribute keys across nodes. When membership changes
(nodes join, leave, or fail), a rebalance phase migrates keys to their new
owners and pushes replicas to new replica-set members. A quarantine window
prevents crash-looping nodes from becoming hot migration targets.

This document covers the cluster lifecycle. The wire protocol is documented in
`protocol.md`.

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
       │                 suspect_timeout expires│
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

The failure detector runs a SWIM-style probe cycle on a configurable interval:

1. **Ping**: Each tick, the detector selects a random peer and sends a `Ping`.
2. **Suspect**: If the Ping does not return within `suspect_timeout`, the peer
   is marked `Suspect` and a suspicion timer starts.
3. **Dead**: If the peer is still Suspect after `suspect_timeout` elapses (from
   the initial probe), it is marked `Dead`.
4. **Blackhole detection**: A timed-out probe (no response within
   `suspect_timeout`) also triggers Suspect marking, separate from the async
   callback path.

The `state_mutex_` in the failure detector serializes concurrent probe callbacks
and state transitions, preventing races between the probe timeout sweep and the
async response path.

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

## Consistent Hash Ring

The `ConsistentHashRing` maps keys to nodes using xxHash3 virtual nodes (150
per physical node). The ring uses an immutable-snapshot design:

1. A sorted vector of `(hash, NodeId)` pairs represents the ring.
2. Updates (add/remove) create a new vector and atomically swap it in via
   `std::atomic`.
3. Reads lock-free using binary search over the snapshot.

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

1. **Phase 1 — Enumerate under lock**: `store_.forEach()` iterates every key
   in the local store. For each key, the desired replica set is computed via
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

## Graceful Leave

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

## Configuration Reference

| Flag | Default | Description |
|------|---------|-------------|
| `--quarantine-interval` | `10000` | Quarantine window after a node joins (ms). `0` = off |
| `--suspect-timeout` | `3000` | Time a suspect persists before being marked dead (ms) |
| `--gossip-interval` | `1000` | Membership gossip dissemination interval (ms) |
| `--ping-interval` | `1000` | Failure-detector ping interval (ms) |
| `--replication-factor` | `1` | Number of copies per key (1 = no replication) |
| `--consistency` | `async` | Write consistency: `async` or `quorum` |

All parameters can be set via CLI flags or the YAML configuration file. CLI
flags override config file values.
