# Cinder

Distributed in-memory cache in Go: consistent-hash sharding, quorum
replication (LWW versions), SWIM gossip membership, two-phase rebalance,
periodic anti-entropy, checksummed WAL + atomic snapshots, small binary TCP
protocol (v6) with gRPC gateway and TLS 1.2.

Module: `github.com/kraytos17/cinder` (Go 1.27; toolchain pinned to
go1.27.3 in `go.mod`). The implementation is Go. `docs/` describes the
architecture and wire/storage contracts; `cinder/v1/cache.proto` is the
source for the gRPC API.

## Rules

- **Wire compatibility is law.** `internal/net/protocol` stays byte-identical
  with the frozen v6 spec (`docs/protocol.md`); `internal/persist` files
  (WAL0/snapshot v2) keep their magic + XXH3 checksums byte-identical.
- **Hot path stays pointer-light.** Flat `[]byte` slabs, integer handles,
  no pointer graphs in the store — the GC stays out of the way.
- **Test behavior at its boundary.** Protocol, persistence, state-machine,
  and CLI contracts have focused unit, integration, golden-vector, and fuzz
  tests. Preserve documented wire and file-format invariants when changing
  implementation details.

## Status

- **Wire protocol:** v6 TCP framing, status/opcode mapping, auth tokens,
  optional trace fields, and CRDT counter operations (17–20). See
  [`docs/protocol.md`](docs/protocol.md).
- **Storage:** sharded LRU/LFU cache, expiry wheel, LWW versions and
  tombstones, plus WAL/snapshot recovery. See
  [`docs/architecture.md`](docs/architecture.md) and
  [`docs/rebalance.md`](docs/rebalance.md).
- **Cluster:** consistent hashing, quorum replication, gossip membership,
  failure detection, anti-entropy repair, hinted handoff, and ownership
  rebalance.
- **Interfaces:** `cinderd`, `cinder-cli`, `cinder-admin`, and the gRPC
  gateway. The proto source is `cinder/v1/cache.proto`.

## Recipes

From the repo root: `just build`, `just test`, `just test-race`, `just vet`,
`just lint`, `just vuln`, `just fuzz`, `just proto-check`, and `just bench`.
`just verify` is the local pre-push gate (format, vet, lint, race tests).
GitHub Actions additionally checks vulnerability findings, module exactness,
generated proto freshness, and runs the eight 20-second fuzz targets
concurrently. The Go build/module caches are managed by `actions/setup-go`.
