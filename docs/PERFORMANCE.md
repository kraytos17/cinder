# Performance targets and regression guard

Benchmarks are comparative, not portable claims. Numbers are only
comparable on the same host, toolchain, power/CPU conditions, flags, and
workload; see `bench/README.md` and `docs/profiling.md`. Targets below
are ratios vs `bench/baseline.txt`, not absolute guesses.

## Standard workloads

| Workload | Shape | What it measures |
|---|---|---|
| read-heavy 95/5 | `load.Run` 95% Get / 5% Set, 10k keys, 256 B values | p99 serving latency, hit path |
| mixed 50/50 | `load.Run` 50/50, 10k keys, 256 B values | write + read-repair pressure |
| write-heavy | `load.Run` 20/80, 10k keys, 1 KiB values | WAL, replication fan-out |
| large values | store 64 KiB values, overwrite loop | staging-buffer reuse, GC pressure |
| hot-key | zipfian 80% to 100 keys of 10k | lock contention, LRU promotion |
| counter-heavy | repeat `Increment` one key/one actor | CRDT hot path (must stay 0 allocs) |

## Reproduction

```sh
# Smoke (allocations always on; rules out cliffs, publishes nothing):
just bench

# Publishable (pin CPU, quiet machine, same flags both sides):
just bench -benchtime=1s

# One path:
go test -bench='BenchmarkStoreOverwrite' -benchtime=1s -benchmem ./internal/store/

# System p99 (not a bench loop; this is the tail signal):
go test -run TestLoad -count=1 ./internal/load/
```

Compare with `benchstat`, never by eye:

```sh
just bench-compare
# or: go test ./... -run='^$' -bench=. -benchmem -count=5 > bench/current.txt
#      go tool benchstat bench/baseline.txt bench/current.txt
```

## Targets (vs baseline, same host)

- Hot paths flat or better: store overwrite 0 allocs, counter hot actor
  0 allocs, ring `GetNode` 0 allocs, client `preferredNode` 0 allocs.
- Small SET encode ≤ 1 alloc, decode ≤ 2 allocs (see `alloc_guard_test.go`
  in each package). The guard tests fail the build on regression; they
  are the CI gate because allocs are deterministic.
- `ns/op` via `benchstat` is advisory (±10% budget on selected
  benchmarks; investigate beyond it, do not gate merges on it — CI
  machines are noisy). The guard that blocks merges is the alloc tests
  above plus `just verify`.

## Measured (dev host, go1.27.4)

- `BenchmarkStoreOverwrite/none`: ~50–54 ns/op, 0 allocs (hot update path; down from ~64–83 ns after removing `defer Unlock` from the write path).
- `BenchmarkStoreGetHit`: ~134–145 ns/op, 1 alloc (detached value copy);
  `GetMiss`: ~19 ns/op, 0 allocs. Non-TTL reads skip the clock entirely.
- Parallel `GetVersioned` (8-way, RWMutex fast path): ~50 ns/op, 1 alloc
  (entry clone); parallel `Get` (LRU promotion under write lock): ~132
  ns/op. Versioned reads bypass LRU promotion, so quorum/AE voter paths
  share reads while plain `Get` still serializes per shard for correct
  recency. pprof top on the store bench is GC + map lookup + lock, not
  application logic; `runtimeNow` is gone from the GetHit profile after
  the expiry fast path.
- `BenchmarkEncodeRequest`: ~60–140 ns/op, 1 alloc; `DecodeRequest`:
  ~70–100 ns/op, 2 allocs (key + value detach). pprof top is memmove +
  GC; further gains need wire-incompatible zero-copy framing, out of
  scope.
- Samply note: the toolchain pins `go1.27.4-X:nodwarf5`, so samply
  cannot resolve Go symbols (empty func tables). pprof (`-cpuprofile` /
  `-memprofile` / `-mutexprofile`) is the working profiler here; samply
  records run but only prove system-wide time, not function breakdown.
- Shard rule stays 1 per 64 KiB capped at 16: raising the cap only adds
  lock overhead below ~16 cores and reshuffles local placement, so it is
  not a free win on this host. Repl hint-queue contention (`HintQueue.Push`
  + `procyield`) shows only in failure-path contended benches, not
  steady-state quorum writes (rf1 ~187 ns, rf2 ~362 ns).

## Memory accounting

Store byte accounting covers key, value, entry overhead, tombstones, and
the separate counter store (`capacity/4` default). Expect total process
memory ≈ `capacity * 1.25 + overhead`; verify with `runtime.MemStats`
delta on a filled store (see `profiling.md`). Documented factor, not a
byte-exact promise.

## GC tuning

Tune only against a `load`-measured p99: `GOGC` (heap-growth headroom)
and `GOMEMLIMIT` (heap cap). Change one, re-profile (`just bench-cpu` /
`bench-mem`), keep the number that moved the p99 you measured.
