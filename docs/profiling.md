# Profiling and Performance Measurement

How to measure Cinder: repeatable benchmarks, CPU profiles, and allocation
accounting. The rule for all of it: **measure → explain → change →
re-measure**, one variable at a time, same machine, same build, same flags.

## Benchmarks (`*_bench_test.go`)

Benchmarks live next to the code they measure (`store`, `hashring`,
`net/protocol`, `persist`, `repl`, `ae`, `net/tcp`) and run under the
standard test binary — no separate bench harness:

```bash
# Quick smoke (100 iterations each, allocations always on):
just bench

# Publishable numbers:
just bench -benchtime=1s

# One package, one benchmark:
go test -bench='BenchmarkStorePut' -benchtime=1s -benchmem ./internal/store/
```

`-benchmem` is always on: **allocation counts are the GC-tuning signal**.
A benchmark that is fast but allocates per operation is a latency
time-bomb under heap pressure — treat `allocs/op` as a first-class result
next to `ns/op`, and `B/op` as the size of the bomb. Past wins live here
as regression guards: key canonicalization via `unique.Make` cost ~9% of
Put CPU plus a weak-backed table per resident key (94% of Put live heap)
while sharing with nothing, and was removed (Put 2386→685ns, 9→2
allocs); per-bucket AE digests moved to write-time accumulators
(scan 1.8ms → 0.5µs copy).

Current families: store put/get/TTL/miss/eviction, ring lookup and
replica sets, codec encode/decode with MB/s, WAL sequential/batch appends,
hint push/snapshot, AE full-scan digest + IBLT build/peel, and a loopback
TCP set/get round-trip (+ parallel variant) in `net/tcp`.

### Getting trustworthy numbers

- **Pin the CPU** (`taskset -c 2 ...`) and keep the machine quiet.
  Numbers are only comparable to reruns on the same box under the same
  conditions.
- **Wall time + iters**: `-benchtime=1s` (or higher) for publishable
  numbers; the default smoke run rules out cliffs, it does not publish
  anything. For tail behavior, drive the system with a load client and a
  latency histogram — benchmark loops don't do tails.
- **Silence the background**: benchmarks that rebuild state (hint backlogs,
  large stores) should not share the process with gossip ticks or AE
  rounds. The bench helpers construct stores/managers directly, never a
  full server, except `BenchmarkRPC*` which owns its loopback server and
  nothing else.
- **Know what the loopback bench proves**: `BenchmarkRPCSetGet` measures
  sequential Set+Get round-trips over loopback TCP — framing, dispatch,
  and store. Not network I/O, delayed completion, or contention. Keep it
  for cheap A/B comparisons; confirm anything load-bearing with a real
  load test before generalizing (see below).

## Load testing (`internal/load`)

Benchmarks measure single paths in-process. `internal/load` measures the
system: it drives a live cluster over real TCP with a mixed get/set/del
workload (configurable mix, keyspace, hot-key fraction, workers, op
budget or deadline, seed) and reports client-observed latency histograms
(p50/p99/max per op) plus throughput and transport-error counts.

```go
res, _ := load.Run(ctx, load.Config{
    Nodes: addrs, Workers: 8, TotalOps: 100_000,
    KeySpace: 10000, HotKeys: 100, HotFraction: 0.8, ValueSize: 256,
})
res.Sets.Quantile(0.99) // p99 set latency, nanoseconds
res.Throughput()        // ops/sec
```

Rules: fix a seed for comparable runs; size KeySpace so the working set
fits the stores (spilling into eviction measures a different system);
read p99, not the mean — the mean hides the tail this tool exists to
show. Tune `GOGC`/`GOMEMLIMIT` only against a p99 measured here.

## CPU profiling with pprof

```bash
# Record while running the benchmark you care about:
go test -bench='BenchmarkStorePut' -benchtime=5s -cpuprofile=/tmp/cpu.out ./internal/store/

# Same via just (profiles/ is gitignored scratch, prints top15):
just bench-cpu -bench='BenchmarkStorePut' ./internal/store/

# Interactive exploration:
go tool pprof /tmp/cpu.out
#   (pprof) top10 -cum
#   (pprof) list PutVersioned
#   (pprof) weblist expensiveFunc   # annotated source, needs graphviz for web

# Compare two runs (before/after a change):
go test -bench=. -benchtime=5s -cpuprofile=/tmp/after.out ./internal/store/
go tool pprof -base /tmp/before.out /tmp/after.out
```

`GOGC` and `GOMEMLIMIT` are the tuning knobs the profile informs:
`GOGC=50` halves the heap-growth headroom (more frequent, shorter GCs) for
latency-sensitive services at a throughput cost; `GOMEMLIMIT` caps total
heap with the same trade. Change one, re-profile, keep the number that
moved the p99 you actually measured — never tune blind.

`perf stat` works on Go binaries like any other (`perf stat -r 3 -d --
taskset -c 2 ./my.test -test.bench=. -test.benchtime=5s`); use it the same
way: IPC for stall-vs-retire, cache-miss rate as a pointer to memory-access
work, not a verdict. `perf record` + `go tool pprof` can read perf.data
too, if you prefer system-wide sampling over cooperative profiling.

## Memory profiling

```bash
go test -bench='BenchmarkStoreEvictPressure' -benchtime=5s -memprofile=/tmp/mem.out ./internal/store/
go tool pprof -alloc_space /tmp/mem.out   # where it was allocated
go tool pprof -inuse_space /tmp/mem.out  # what is still live
```

`-alloc_space` (default `top` view is in-use) is usually the view you want
for benchmark tuning: it shows allocation rate, which is what `-benchmem`
summarizes. The store's hot path goal is flat `[]byte` slabs and integer
handles — any pointer graph in a profile is a defect to remove, not a
tuning knob.

## Anti-patterns

- Optimizing a pprof total-time frame without checking self time (you
  will "fix" a caller for a callee's cost).
- Trusting benchmark loops for tail latency (they don't do tails).
- Comparing numbers across machines, GOGC settings, or benchtimes.
- Keeping a profile after changing code or workload and calling the
  result "optimized".
- Tuning `GOGC`/`GOMEMLIMIT` without a measured p99 to move.
