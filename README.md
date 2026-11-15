# Cinder

Distributed in-memory cache in Go: consistent-hash sharding, quorum
replication (LWW versions), SWIM gossip membership, two-phase rebalance,
periodic anti-entropy, checksummed WAL + snapshots, small binary TCP
protocol (v6) with gRPC gateway and TLS 1.2.

Module: `github.com/kraytos17/cinder` (Go 1.27; toolchain pinned to
go1.27.4 in `go.mod`). The implementation is Go. `docs/` describes the
architecture and wire/storage contracts; `cinder/v1/cache.proto` is the
source for the gRPC API.

## Rules

- **Wire compatibility is law.** `internal/net/protocol` stays byte-identical
  with the frozen v6 spec (`docs/protocol.md`); `internal/persist` files
  keep their magic + XXH3 checksums byte-identical. Writers emit WAL v3 and
  snapshots v2 (KV-only) or v4 (with counters); readers accept WAL
  Legacy/V1/V2/V3 and snapshots V1/V2/V4 (compressed v3 is rejected).
- **Hot path stays pointer-light.** Flat `[]byte` slabs, integer handles,
  no pointer graphs in the store — the GC stays out of the way.
  Single-copy puts and shared replicated-write buffers keep per-write
  allocations at their minimum.
- **Test behavior at its boundary.** Protocol, persistence, state-machine,
  and CLI contracts have focused unit, integration, golden-vector, and fuzz
  tests. Preserve documented wire and file-format invariants when changing
  implementation details. Multi-node convergence lives in
  [`internal/sim`](docs/sim.md): deterministic manager-level clusters over
  a virtual network, with fault injection and crash-recovery tests.

## Status

- **Wire protocol:** v6 TCP framing, status/opcode mapping, per-class auth
  tokens, optional trace fields, TTL range-guarded to the u32 wire field,
  and CRDT counter operations (17–20). See
  [`docs/protocol.md`](docs/protocol.md).
- **Storage:** sharded LRU/LFU cache, expiry wheel, LWW versions and
  tombstones, plus WAL/snapshot recovery (v2/v4 snapshots, v3 WAL). See
  [`docs/architecture.md`](docs/architecture.md) and
  [`docs/rebalance.md`](docs/rebalance.md).
- **Cluster:** immutable-snapshot consistent hash ring, quorum replication,
  gossip membership, failure detection, anti-entropy repair, hinted handoff,
  ownership rebalance, and truthful admin introspection.
- **Interfaces:** `cinderd`, `cinder-cli`, `cinder-admin`, and the gRPC
  gateway. The proto source is `cinder/v1/cache.proto`. `cinderd` takes
  flags or a strict JSON `--config` (flags override file values); SIGHUP
  and `config-reload` apply the documented reloadable subset only.

## Recipes

From the repo root: `just build`, `just test`, `just test-race`, `just vet`,
`just lint`, `just lint-fix`, `just vuln`, `just fuzz`, `just proto-check`,
and `just bench`. `just verify` is the local pre-push gate (format, vet,
lint, race tests). GitHub Actions additionally checks vulnerability
findings, module exactness, generated proto freshness, and runs the seventeen
20-second fuzz targets concurrently. The Go build/module caches are managed
by `actions/setup-go`.
