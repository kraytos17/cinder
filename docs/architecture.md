# Cinder Architecture

How the pieces fit together. The wire format lives in `protocol.md`;
membership, failure detection, and rebalancing in `rebalance.md`.

## Process model

A `cinderd` process hosts one `CacheNodeServer`, which assembles the store,
hash ring, replication, transport, and cluster subsystems. Three tools ship
alongside it:

- `cinderd` — the server (CLI11 flags, YAML config, TLS setup).
- `cinder-cli` — `get`/`set`/`del`/`ping` over the binary protocol.
- `cinder-admin` — operational tooling (`info`, `cluster`, `ring`, `compact`,
  `config-reload`, `shutdown`).

`CacheClient` + `ConnectionPool` offer the same access in-process, with
ring routing, one redirect retry, and pipelined per-owner `multiGet`.

## Request path

```
client ──TCP frame (v5)──▶ TcpServer ──▶ CacheNodeServer ──▶ CacheStore
         ▲                      │                │ ring: who owns this key?
         │                      │                ├─ owned → local store (+ replicate)
         └── redirect/error ────┘                └─ not owned → `moved to <node>` redirect
```

Frames are length-prefixed with a compile-time-validated 7-byte header
(`consteval` + `static_assert`), big-endian fields, 16 opcodes, 64 MiB max.
`TcpServer` serializes each connection on its own strand; `TcpTransport` and
`ConnectionPool` run request-response as `asio::co_spawn` coroutines with
configurable RPC deadlines. The event loop is a thread pool (`--io-threads`,
auto `min(4, hardware_concurrency)`).

## Storage

- **Eviction stores** — LRU and LFU behind a policy-templated CRTP base
  (`EvictionStoreBase`), so the strategies share all plumbing. LFU keeps O(1)
  swap-and-pop frequency buckets with `freq_index` back-pointers (no linear
  scan on hits).
- **TTL wheel** — a 256-slot intrusive wheel (`WheelNode` base,
  `WheelNodeLike` concept) for near-term expiries plus a min-heap for long
  TTLs, so expiry never reinserts repeatedly.
- **Slab allocator** — `SlabAllocator` serves store lists from 256-slot slabs
  over a lock-free CAS free-list, using `std::start_lifetime_as` for
  well-defined type-punning. Allocation failure throws `bad_alloc`.
- **Reads/writes** go through `CacheStore` (`forEach`, `mintVersion`); every
  mutation is versioned for conflict resolution (below).

## Distribution

- **Consistent hash ring** — xxHash3 with 150 virtual nodes per physical node,
  stored as a flat `uint64_t` vector (SoA) for cache-dense binary search.
  Readers hold an immutable snapshot swapped via `std::atomic<shared_ptr>`,
  so lookups are lock-free; `string_view` hashing avoids allocations, and
  cluster maps use `std::flat_map` for the same reason.
- **Replication** — primary-driven, `async` (acknowledge after local write,
  fan out best-effort) or `quorum` (`W = R/2 + 1`, fails closed as `NotReady`).
  Versions are monotonic Lamport stamps; ties break on `writer_node_hash`
  (LWW). Replicas serve `GetVersioned` locally when the primary is gone.
- **Read repair** — quorum reads compare replica versions and asynchronously
  write the winner back to stale replicas.
- **Hinted handoff** — writes to unreachable replicas queue bounded, TTL
  (30s) hints (max 1024), replayed on return.
- **Membership** — SWIM-style Ping → Suspect → Dead with incarnation-guarded
  gossip and automatic ring
  rebuild. `leave()` broadcasts Dead first to avoid false suspicion.
- **Rebalance** — two-phase (enumerate under lock via `forEach`, send after
  unlock) with a quarantine window for crash-looping nodes; retries until all
  deferred keys migrate. See `rebalance.md`.
- **Anti-entropy** — background replica repair on a timer: exchange xxHash3
  range-bucket digests, then sync only divergent buckets (configurable
  interval and bucket count).

## Durability and operations

- **Persistence** — append-only WAL (`WAL0` header, per-entry XXH3-64) plus
  periodic snapshots (`CSNP` header, atomic write-to-temp + rename). Crash
  recovery loads the latest snapshot and replays the tail. See `rebalance.md`.
- **Observability** — Prometheus `/metrics` (counters, gauges, per-opcode
  latency summaries with p50–p999) and `/config` (live config as JSON);
  spdlog-backed structured `Event`/`Span` tracing with wire trace-id
  propagation. Errors chain provenance via `Error::wrap()` with
  `std::source_location` (Rule-of-Five deep-copied cause chains).
- **Configuration** — YAML file with CLI overrides and live hot-reload of log
  level, gossip/suspect intervals, and capacity.
- **TLS** — compile-time opt-in, TLS 1.2; plaintext and TLS paths share the
  transport, selected by a null/non-null `ssl_ctx`.
- **gRPC gateway** — compile-time opt-in protobuf façade (10 RPCs) on its own
  port/thread pool, sharing the store, ring, and replication internals.
  Internal opcodes never leave the cluster.
