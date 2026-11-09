# Cinder Architecture

How the pieces fit together. The wire format lives in `protocol.md`;
membership, failure detection, and rebalancing in `rebalance.md`.

## Process model

A `cinderd` process hosts one `tcp.Server`, which assembles the store, hash
ring, replication, transport, and cluster subsystems. Three tools ship
alongside it (all flags, no config file):

- `cinderd` — the server (`--port`, `--node-id`, `--peers`, `--replication-factor`,
  `--consistency`, `--tls*`, `--grpc-port`, `--enable-persistence --data-dir`, …).
- `cinder-cli` — `get`/`set`/`del`/`ping`/`get-versioned`/`mget`/`mset` plus
  `info`/`cluster`/`ring` over the binary protocol.
- `cinder-admin` — operational tooling (`info`, `cluster`, `ring`, `compact`,
  `config-reload`, `shutdown`).

`client.Cluster` + `client.Pool` offer the same access in-process, with
ring routing, redirect following (up to 4 hops, visited-set bounded), and
quorum fan-out with read repair.

## Request path

```
client ──TCP frame (v6)──▶ tcp.Server ──▶ dispatch ──▶ store / repl.Manager
          ▲                      │                │ ring: who owns this key?
          │                      │                ├─ owned → local store (+ replicate)
          └── redirect/error ────┘                └─ not owned → `moved to <node>@<host>:<port>` redirect
```

Frames are length-prefixed with a validated 7-byte header
(big-endian fields, 20 opcodes, 64 MiB max). The server handles one
request-response exchange at a time per connection (strictly sequential,
mutex-guarded on the client); the pool holds one persistent connection per
node, dialed lazily with per-RPC deadlines, and drops the connection on
transport failure so the next send redials. Inter-node traffic (gossip,
probes, replication, anti-entropy, rebalance, hints) rides the same pool,
flipped to TLS as a whole when the server is.

CRDT counter opcodes (17–20) decode but answer `NotSupported`: there is no
counter store yet. Everything else dispatches identically over TCP and the
gRPC gateway (`internal/grpcgw` maps each RPC onto the same handlers, with
`Delete` bypassing the ownership check and `GetVersioned` reading the local
store directly).

## Storage

- **LRU store** — sharded (`LRUStore`, one shard per CPU-friendly slice of
  capacity), LRU lists with O(1) touch/evict, per-entry versions. No LFU:
  only the LRU policy was ported.
- **TTL wheel** — 256-slot wheel for near-term expiries plus a min-heap for
  long TTLs; tombstone GC rides the same machinery.
- **Reads/writes** go through `Get`/`Put` (live views) and
  `GetVersioned`/`PutVersioned` (LWW entries carrying version, writer hash,
  tombstone bit, and absolute expiry). Every mutation mints a Lamport
  version; `PutVersioned` applies idempotently (a strictly losing LWW stamp
  is a no-op success).

## Distribution

- **Consistent hash ring** — xxHash3 with 150 virtual nodes per physical
  node, mutex-guarded and rebuilt in place from the alive set on every
  membership change (no lock-free snapshot: rebuilds are cheap and rare).
- **Replication** — primary-driven, `async` (acknowledge after local write,
  fan out best-effort) or `quorum` (`W = total/2 + 1`, fails closed as
  `NotReady`). Versions are monotonic Lamport stamps; ties break on
  `writer_node_hash` (LWW). Replicas serve `GetVersioned` locally when the
  primary is gone.
- **Read repair** — quorum reads compare replica versions and write the
  winner back to stale replicas (plus a local self-heal).
- **Hinted handoff** — writes to unreachable replicas queue bounded, TTL
  (30s) hints (1024 entries and 64 MiB estimated bytes, drop-oldest,
  LWW-coalesced per target+key); replayed FIFO when the replica returns, and
  journaled to `<data_dir>/hints.log` when persistence is on (torn tail
  truncated on open).
- **Membership** — SWIM-style Ping → Suspect → Dead with incarnation-guarded
  gossip and automatic ring rebuild. `Leave()` broadcasts Dead first to avoid
  false suspicion.
- **Rebalance** — two-phase (enumerate a `LiveEntries` snapshot, then send —
  nothing is ever sent while holding store state) with a quarantine window
  for crash-looping nodes; 200 ms debounce on ring rebuild, self-retrying
  quarantine timer, 2 s migration-failure retry. See `rebalance.md`.
- **Anti-entropy** — background repair between replica partners: full-scan
  group digests every round (no incremental accumulators — a deliberate
  simplification, behavior-identical), then targeted sync of divergent
  buckets only; large divergent buckets go through IBLT sketches with a
  metadata-row fallback; see `rebalance.md`.

## Durability and operations

- **Persistence** — append-only WAL (`WAL0` magic, XXH3-64 per-record
  checksums) plus periodic snapshots (`CSNP` magic, format v2, atomic
  write-to-temp + rename). Readers accept WAL Legacy/V1/V2 and snapshots
  V1/V2; writers emit WAL v2 and snapshots v2 uncompressed (v3 zstd blocks
  are rejected). Recovery loads the latest snapshot (via `CURRENT`, with
  scan fallback) and replays sealed + active segments, truncating torn
  tails; `Compact` snapshots and rotates to a fresh WAL.
- **Observability** — none yet: no metrics collector, no tracing substrate,
  no profiling endpoint. Admin JSON (`info`/`cluster`/`ring`, same payloads
  over TCP and gRPC) is the introspection surface; benchmarks are the
  performance story (`just bench`, `-benchmem` always on).
- **Configuration** — flags only (`cinderd --help` is authoritative);
  `config-reload` is accepted and answers OK. No YAML, no hot-reload.
- **TLS** — opt-in (`--tls --tls-cert --tls-key [--tls-ca]`, TLS 1.2+);
  peer verification is chain-only like asio `verify_peer`.
- **gRPC gateway** — opt-in (`--grpc-port`); plaintext by default, TLS with
  the server certificate when a client secret is configured (refuses to
  serve authenticated plaintext, like C++).
  same handler semantics as TCP (see above).
