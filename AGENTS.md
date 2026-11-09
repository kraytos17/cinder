# AGENTS.md — Cinder

## What this repo is

Distributed in-memory cache in Go: consistent-hash sharding (150 vnodes/node),
quorum replication (LWW versions), SWIM gossip membership, two-phase rebalance,
periodic anti-entropy, checksummed WAL + atomic snapshots, small binary TCP
protocol (v6) with gRPC gateway and TLS 1.2.

* One `cinderd` process hosts one `tcp.Server` (store + ring + replication +
  transport + cluster subsystems). Tools: `cinderd`, `cinder-cli`, `cinder-admin`.
  `client.Cluster` + `client.Pool` offer the same access in-process.
* Details live in `docs/`: `architecture.md` (process model, request path, storage),
  `protocol.md` (wire format — the compatibility law), `rebalance.md`
  (membership, rebalance, persistence), `profiling.md` (benchmarks workflow).
  Do not duplicate them here; link them. Implementation file references in
  `docs/` predate the Go port; the design still applies.

## Go layout

* Module `github.com/kraytos17/cinder`, toolchain pinned to go1.27.3 in
  `go.mod`. Packages: `internal/{store,hashring,net,cluster,repl,shard,ae,
  persist,tlscfg,grpcgw,client}`, commands in `cmd/{cinderd,cinder-cli,cinder-admin}`.
  Recipes: `just build`, `just test`, `just vet`, `just fuzz`, `just bench`;
  CI job in `.github/workflows/go.yml`.
* **Wire compatibility is law**: `internal/net/protocol` is byte-identical
  with the frozen v6 spec in `docs/protocol.md`; `internal/persist` files
  (WAL0/snapshot v2) keep their magic + XXH3 checksums byte-identical.
* **Hot path stays pointer-light**: flat `[]byte` slabs, integer handles, no
  pointer graphs in the store. Tune with `GOGC`/`GOMEMLIMIT` + `pprof`.
* Port tests first: every behavior mirrors a C++ unit test case-for-case
  (the C++ tree is retired but its test suite was the spec).

## Daily loop (Justfile)

* `just build` / `just test` / `just vet` (gofmt + vet gate).
  `just lint` is the golangci-lint suite (`.golangci.yml`, pinned via
  `go tool` — no installs); `just verify` is the fast pre-push gate
  (fmt + vet + lint + `go test -race ./...`, goleak mains included).
* `just vuln` scans the module graph (govulncheck, also pinned).
  `just tidy` keeps go.mod/go.sum exact; `just fix` applies `go fix`
  rewrites (usually a no-op, never in CI).
* Coverage: `just cover` (func summary + cover.out), `just cover-html`.
* Benchmarks: `just bench` (`-benchmem` always on — allocation counts are the
  GC-tuning signal); pass `-benchtime` for publishable numbers.
  Profiles: `just bench-cpu` / `just bench-mem` (`-alloc_space` +
  `-inuse_space` tops) / `just bench-trace` (see docs/profiling.md).
* Fuzz: `just fuzz` (protocol codec fixed points, 20s each).
* gRPC stubs: `just proto-generate` (buf via `go tool`, pinned plugins; see
  `buf.gen.yaml`). `buf lint` gates the proto; CI also checks the stubs
  are current.

## Tests (`go test ./...` is source of truth)

* Packages: `store`, `hashring`, `net/protocol`, `net/tcp`, `cluster`,
  `repl`, `shard`, `ae`, `persist`, `tlscfg`, `grpcgw`, `client`, plus
  `cmd/cinder-cli` and `cmd/cinder-admin` CLI suites.
* Cluster tests run real in-process nodes (`testCluster`); AE/rebalance
  convergence tests drive manual rounds for determinism (the background
  tick loops stay at their slow defaults). Timing-sensitive suites run
  `-race`; on failure re-run isolated before treating as real.
* Interop suites (Go↔C++ wire proof) need prebuilt binaries under `build/`
  (`debug`, `debug-tls`, `tls-grpc`) and skip when absent — set
  `CINDER_TEST_CINDERD_PATH` / `CINDER_TEST_TLS_*` / `CINDER_TEST_CPP_ADDR`
  to override. Note: `build/debug-grpc` is stale (undefined abseil symbols);
  use `build/tls-grpc` until it is rebuilt.

## Lint gate (this is what CI enforces)

* `gofmt -l .` empty, `go vet ./...` clean, `go tool golangci-lint run ./...`
  clean (see `.golangci.yml`; exclusions carry their reasons), `go build ./...`,
  `go test -race ./...` (goleak mains fail on leaked goroutines), plus
  `go tool govulncheck ./...` and the eight 20s protocol fuzz targets.
* Pre-commit hook (catches the cheap half before push):
  `just hooks-install` sets repo-local `core.hooksPath=.githooks`
  (`just hooks-run` runs the same checks manually). The hook checks staged
  files only: whitespace errors (`git diff --check`, `*.md` excluded so
  hard-breaks pass), gofmt on staged Go blobs, YAML syntax on staged
  workflows, Justfile formatting. Slow checks (vet, lint, race tests, vuln,
  fuzz) stay in `just verify` and CI. Bypass with `git commit --no-verify`.

## Gotchas for agents

* Init order matters in `tcp.Server`: members must construct after their
  dependencies; keep any new field's init after what it reads.
* Redirect budget semantics in `client.Cluster.tryFollowRedirect`: follows reuse
  the attempt budget and are bounded by hops + visited set — preserve both
  when touching retry logic.
* `WAL0`/`CSNP` magic + XXH3 checksums: `persist` writer layout and reader
  checksum recompute must stay byte-identical (fuzz: `wal`, `snapshot`,
  `store_put` via the seed corpus).
* AE uses wall-clock `ExpiresAt` directly (no steady-clock conversion);
  `newTestCluster` freezes FD via long suspect timeout for replication tests.
* The pool learns joiner addresses on every ring rebuild (`rebuildRingFromTable`
  → `AddAddr`) — replication, AE, and rebalance all depend on it; don't cache
  the peer set anywhere else.
* Counts in `cinderd --help` output and admin JSON are part of the CLI test
  contract (`cmd/cinder-cli`, `cmd/cinder-admin` assert exact strings).
* Prefer `rg`/`grep` over graph for string literals, error messages, config keys.

## Verification checklist (every change)

1. `go build ./...` clean.
2. `go test ./...` (full suite); affected timing-sensitive suites isolated
   (`-run`, sometimes `-count=5` for join/convergence tests); `-race` for
   concurrency-adjacent changes.
3. `gofmt -l .` empty + `go vet ./...` clean; `just verify` before pushing.
4. `git diff --stat` + `git status` review.
