# Cinder

Distributed in-memory cache in Go: consistent-hash sharding, quorum
replication (LWW versions), SWIM gossip membership, two-phase rebalance,
periodic anti-entropy, checksummed WAL + atomic snapshots, small binary TCP
protocol (v6) with gRPC gateway and TLS 1.2.

Module: `github.com/kraytos17/cinder` (toolchain pinned to go1.27.3 in
`go.mod`). The C++ tree was retired at M6; `docs/` retains the design
reference and `cinder/v1/cache.proto` remains the gRPC source of truth.

## Rules

- **Wire compatibility is law.** `internal/net/protocol` stays byte-identical
  with the frozen v6 spec (`docs/protocol.md`); `internal/persist` files
  (WAL0/snapshot v2) keep their magic + XXH3 checksums byte-identical.
- **Hot path stays pointer-light.** Flat `[]byte` slabs, integer handles,
  no pointer graphs in the store — the GC stays out of the way.
- **Port tests first.** Every behavior mirrors a unit test case-for-case.

## Status

- M0: skeleton, CI, lint — done.
- M1: protocol v6 codec (`internal/net/protocol`) with byte-exact vectors
  and fuzz targets — done.
- M2: in-memory LRU store (`internal/store`: sharded LRU, TTL wheel,
  LWW versions, tombstones, Lamport clock, mutation sink) — done.
- M3: TCP server + client (`internal/net/tcp`, `internal/client`) — done.
- M4a: ring (`internal/hashring`, XXH3-verified), cluster-aware server with
  redirects, cluster client with redirect-following — done.
- M4b: quorum replication, hints + replay, read repair with singleflight
  (`internal/repl`), wired into server + `cinderd --replication-factor /
  --consistency` — done.
- M4c: membership, gossip, failure detection (`internal/cluster`),
  ring-follows-alive wiring, graceful leave vs crash distinction — done.
- M4d-1: WAL + snapshots + recovery (`internal/persist`), store hooks,
  server restart/compact tests — done.
- M4d-2: anti-entropy (`internal/ae`): digests, IBLT peel with
  tombstone identity and P2 fallback, hint journal, full round driver with
  server dispatch (opcodes 9/10) and tick loop — done.
- M4d-3: two-phase rebalance (`internal/shard`): enumerate-then-send,
  stay-and-push vs migrate-everywhere-and-drop-on-full-ack, quarantine
  deferral with window retry, 200ms debounce on ring rebuild — done.
- M5: TLS (`internal/tlscfg`, `--tls*` flags, client-auth), gRPC gateway
  (`internal/grpcgw`, `--grpc-port`), `cinder-cli` + `cinder-admin`, and
  benchmarks (`*_bench_test.go`, `just bench`) — done.
- M6: 100% replacement — C++ tree retired, `go/` promoted to repo root,
  CI cut over — done.

## Recipes

From the repo root: `just build`, `just test`, `just vet`, `just fuzz`,
`just bench`. Or `go ...` directly. `just verify` is the pre-push gate.
