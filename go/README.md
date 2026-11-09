# Cinder (Go port)

Incremental Go 1.27 reimplementation of Cinder. The C++ tree is frozen
except for protocol-compat fixes; this subtree will eventually replace it
100%.

Module: `github.com/kraytos17/cinder/go` (toolchain pinned to go1.27.3 in
`go.mod`).

## Rules

- **Wire compatibility is law.** `internal/net/protocol` must stay
  byte-identical with the C++ `src/net/protocol.cpp` (protocol v6,
  spec in `../docs/protocol.md`). The C++ side wins disagreements.
- **Hot path stays pointer-light.** Flat `[]byte` slabs, integer handles,
  no pointer graphs in the store — the GC stays out of the way.
- **Port tests first.** Every behavior mirrors a C++ unit test case-for-case
  before the Go implementation lands.

## Status

- M0: skeleton, CI, lint — done.
- M1: protocol v6 codec (`internal/net/protocol`) with byte-exact vectors
  and fuzz targets — done.
- M2: in-memory LRU store (`internal/store`: sharded LRU, TTL wheel,
  LWW versions, tombstones, Lamport clock, mutation sink) with C++-mirrored
  tests, race-clean — done.
- M3: TCP server + client (`internal/net/tcp`, `internal/client`),
  interop-tested against C++ `cinderd` — done.
- M4a: ring (`internal/hashring`, XXH3-verified vs C++), cluster-aware
  server with redirects, cluster client with redirect-following, 2-node
  interop both directions — done.
- M4b: quorum replication, hints + replay, read repair with singleflight
  (`internal/repl`), wired into server + `cinderd --replication-factor /
  --consistency`; 3-node Go quorum/hint/repair tests + C++ quorum interop —
  done. Hint journal file goes with WAL in M4d.
- M4c: membership, gossip, failure detection (`internal/cluster`),
  ring-follows-alive wiring, graceful leave vs crash distinction; Go gossip
  interop with C++ in both directions, FD kill test — done. Rebalance and
  anti-entropy remain — next (M4d).
- M4d-1: WAL + snapshots + recovery (`internal/persist`), byte-identical
  C++ files verified both directions, store hooks, server restart/compact
  tests — done. V3 compressed snapshots rejected (zstd deferred); hint
  journal goes with anti-entropy.
- M4d-2: anti-entropy (`internal/ae`): AEM3/AEB5/AER6/AEMS/AEI4 codecs
  byte-identical vs C++ oracle vectors, IBLT peel with tombstone identity
  and P2 fallback, hint journal (add/del, torn-tail truncation, recovery),
  full round driver (group digest → refinement → sync) with server dispatch
  (opcodes 9/10), tick loop, and `--anti-entropy-*` flags; Go-only restart
  convergence (incl. tombstones) and live Go↔C++ AE interop both directions
  — done. PGO-style incremental accumulators replaced by full rehash per
  round (behavior-identical).
- M4d-3: two-phase rebalance (`internal/shard`, mirroring `ShardManager`):
  enumerate-then-send, stay-and-push vs migrate-everywhere-and-drop-on-full-
  ack, quarantine deferral with window retry, dead-target skip, 2s
  migration-retry gate, 200ms debounce on every ring rebuild; wired into the
  server (`ConfigureRebalance`, `--quarantine-interval-ms`) with timer
  cancel on shutdown; pool learns joiner addresses on rebuild (C++ `addAddr`
  parity — also fixes AE/replication fan-out to joiners). RF=1/RF=2 join
  tests, quarantine unit tests, plus a Go-joins-C++-pair live interop test.
  No metrics counters (no Go collector yet). — done.
- M5: TLS, gRPC gateway, admin/CLI, benchmark parity — in progress.
  - CLI done: `cinder-cli` (get/set/del/ping/info/cluster/ring/mget/mset/
    get-versioned, byte-identical outputs incl. `(not found)` status words
    and `expires_at` epoch seconds) and `cinder-admin` (info/cluster/ring/
    compact/config-reload/shutdown) with C++-mirroring test suites; Go CLI
    verified against live C++ `cinderd`.
  - TLS done: `internal/tlscfg` (TLS 1.2+, chain-only peer verification like
    asio verify_peer), server `ConfigureTLS` (+ inter-node pool + repl-pool
    coverage, client-cert presentation), client `DialTLS`/pool/cluster
    support, `--tls*` flags on `cinderd` and both CLIs; Go↔Go incl.
    client-auth, Go↔C++ both directions against `build/debug-tls` (rebuilt:
    the old binaries predated protocol v6).
  - Next: gRPC gateway, benchmarks.

## Recipes

From the repo root: `just go-build`, `just go-test`, `just go-vet`,
`just go-fuzz`. Or `cd go && go ...` directly.
