# Cinder Architecture

How the pieces fit together. The wire format lives in `protocol.md`;
membership, failure detection, and rebalancing in `rebalance.md`.

## Process model

A `cinderd` process hosts one `tcp.Server`, which assembles the store, hash
ring, replication, transport, and cluster subsystems. Three tools ship
alongside it:

- `cinderd` — the server (flags or a strict JSON `--config` file; flags
  override file values). `cinderd --help` lists the current flags.
- `cinder-cli` — `get`/`set`/`del`/`ping`/`get-versioned`/`mget`/`mset` plus
  `info`/`cluster`/`ring` over the binary protocol.
- `cinder-admin` — operational tooling (`info`, `cluster`, `ring`, `compact`,
  `config-reload`, `shutdown`).

`client.Cluster` + `client.Pool` offer client-side ring routing, redirect
following (up to 4 hops, visited-set bounded), and retries. Quorum replication
and read repair are performed by server-side `repl.Manager`, not by
`client.Cluster`.

## Request path

```
client ──TCP frame (v6)──▶ tcp.Server ──▶ dispatch ──▶ store / repl.Manager
          ▲                      │                │ ring: who owns this key?
          │                      │                ├─ owned → local store (+ replicate)
          └── redirect/error ────┘                └─ not owned → `moved to <node>@<host>:<port>` redirect
```

Frames are length-prefixed with a 7-byte header (big-endian fields, protocol
v6, 20 request opcodes, 64 MiB maximum frame size). The server handles one
request-response exchange at a time per connection; clients serialize access
to each pooled connection. The pool holds one persistent connection per node,
dials lazily with per-RPC deadlines, and drops the connection on transport
failure so the next send redials. Inter-node traffic (gossip, probes,
replication, anti-entropy, rebalance, and hints) uses the peer pool, configured
with TLS when server TLS is enabled.

Counter opcodes (17–20) are implemented: `internal/counter` stores sharded
PN-counter state, replication merges complete states, and anti-entropy sends
bounded counter batches. Counters are persisted in WAL v3 records and snapshot
v4 bundles when persistence is enabled. The gRPC gateway exposes cache,
counter, and admin RPCs, but some bulk/versioned methods use direct local-store
operations rather than the TCP handler path. In particular, gRPC `Delete`
uses `DeleteNoRedirect`, `GetVersioned` reads the local store, and `MultiGet`
and `MultiSet` run per-item locally; they do not provide the same routing or
replication semantics as the single-key TCP methods.

## Storage

- **Eviction store** — sharded (`store.Store`, one shard per 64 KiB of
  configured capacity, clamped to 16), per-entry versions, TTL wheel and
  tombstones. `--eviction-policy=lru` uses O(1) recency lists;
  `--eviction-policy=lfu` uses per-shard frequency buckets (read/write
  counts, minimum-frequency victim, least-recently-incremented tie-break).
  LFU charges the larger 136-byte node in capacity accounting and snapshots
  preserve frequencies. WAL records do not carry frequency state, so accesses
  since the last snapshot are not reconstructed by WAL recovery.
- **TTL wheel** — 256-slot wheel for near-term expiries plus a min-heap for
  long TTLs; tombstone GC rides the same machinery.
- **Reads/writes** go through `Get`/`Put` (live views) and
  `GetVersioned`/`PutVersioned` (LWW entries carrying version, writer hash,
  tombstone bit, and absolute expiry). Every mutation mints a Lamport
  version; `PutVersioned` applies idempotently (a strictly losing LWW stamp
  is a no-op success).

## Distribution

- **Consistent hash ring** — xxHash3 with 150 virtual nodes per physical
  node by default. Writers serialize membership changes and publish immutable
  snapshots through an atomic pointer; lookups binary-search the snapshot
  without taking the writer mutex. `GetNodes` walks clockwise for distinct
  replica owners. The bounded-load helper and load counters are not used by
  production routing. See `rebalance.md` for the membership and placement path.
- **Replication** — primary-driven, `async` (acknowledge after local write,
  fan out best-effort) or `quorum` (`W = total/2 + 1`, fails closed as
  `NotReady`). Versions are monotonic Lamport stamps; ties break on
  `writer_node_hash` (LWW). Replicas serve `GetVersioned` locally when the
  primary is gone.
- **Read repair** — quorum reads compare replica versions and write the
  winner back to stale replicas (plus a local self-heal).
- **Hinted handoff** — writes to unreachable replicas queue bounded, TTL
  (30s) hints (1,024 entries and 64 MiB estimated bytes by default,
  drop-oldest, LWW-coalesced per target+key); replayed FIFO when the replica
  returns, and
  journaled to `<data_dir>/hints.log` when persistence is on (torn tail
  truncated on open).
- **Membership** — SWIM-style Ping → Suspect → Dead with incarnation-guarded
  gossip and automatic ring rebuild. `Leave()` broadcasts Dead first to avoid
  false suspicion. See `rebalance.md` for the state machine and failure timing.
- **Rebalance** — two-phase (enumerate a `LiveEntries` snapshot, then send —
  nothing is ever sent while holding store state) with a quarantine window
  for crash-looping nodes; 200 ms debounce on ring rebuild, self-retrying
  quarantine timer, 2 s migration-failure retry. See `rebalance.md`.
- **Anti-entropy** — background repair between replica partners: incremental
  per-bucket XOR digests fed by the store mutation sink (2 KiB copy per
  round, full-scan backstop every 5 min with mismatch counting), then
  targeted sync of divergent buckets only; large divergent buckets go
  through IBLT sketches with a metadata-row fallback; see `rebalance.md`.

## Durability and operations

- **Persistence** — append-only WAL (`WAL0` magic, XXH3-64 per-record
  checksums) plus periodic snapshots (`CSNP` magic, format v2 for KV-only
  state and v4 with counters; snapshot publication uses temp + rename but is
  not power-loss durable). Readers accept WAL Legacy/V1/V2/V3 and snapshots
  V1/V2/V4; writers emit WAL v3 and snapshots v2 or v4 (v3 zstd blocks are
  rejected). Recovery loads the latest snapshot (via `CURRENT`, with
  scan fallback) and replays sealed + active segments, truncating torn
  tails; `Compact` snapshots and truncates to a fresh WAL — on demand
  (`cinder-admin compact`), every `--snapshot-interval-s` (default 60s), or
  when active WAL byte/record limits are reached (64 MiB / 10,000 records by
  default).
- **Observability** — metrics + logs, no tracing: `internal/metrics`
  records per-opcode counters with log2-bucketed latency histograms plus
  store/replication/AE/cluster/rebalance/connection counters, exposed as
  Prometheus text at `GET /metrics` on `--metrics-port` (`0` = disabled).
  Operational logs are zerolog JSON on stderr (`--log-level`, default
  `info`); every request line carries the wire `trace_id`/`span_id`
  (forwarded on fan-out, so replica-side lines correlate), and external
  aggregation reconstructs cross-node paths — no log shipping is built.
  Metric names match the retired C++ exposition (plus the four counter
  opcodes, which postdate it). Admin JSON (`info`/`cluster`/`ring`, same
  payloads over TCP and gRPC) stays the introspection surface; benchmarks
  are the performance story (`just bench`, `-benchmem` always on).
- **Configuration** — flags or strict JSON `--config` (flags override file
  values); secrets can fall back to `CINDER_SHARED_SECRET` and
  `CINDER_CLIENT_SECRET`. SIGHUP re-reads the startup JSON file and applies its
  reloadable subset; the admin `config-reload` opcode applies an explicit
  reload payload. Neither
  path reloads every startup setting; listener addresses, peers, node ID,
  replication factor, persistence directory, and other fixed settings require
  restart.
- **TLS** — opt-in (`--tls --tls-cert --tls-key [--tls-ca]`, TLS 1.2+);
  peer verification is chain-only like asio `verify_peer`.
- **gRPC gateway** — opt-in (`--grpc-port`). It uses TLS when `--tls` is
  enabled; client-secret authentication requires TLS. Without TLS or a client
  secret it can serve plaintext. Its RPC surface is not uniformly equivalent
  to TCP routing; see the request-path caveat above.
