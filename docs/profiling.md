# Profiling and Performance Measurement

How to measure Cinder: repeatable benchmarks, CPU profiles, hardware
counters, cache-contention analysis, and profile-guided optimization.
The rule for all of it: **measure → explain → change → re-measure**,
one variable at a time, same machine, same build, same flags.

## Benchmarks (`benchmarks/`)

All benchmarks live in one binary, `cinder_throughput_bench`, built by any
preset with `CINDER_BUILD_BENCHMARKS=ON` (`release`, `release-clang`,
`release-unity`, `profile`, `ci`, `ci-bench` — see AGENTS.md for the full
matrix). Benchmark sources are covered by the format gate and by
clang-tidy/cppcheck in `just lint`, like everything else.

```bash
# Build and run everything (release by default):
just bench

# Release build, then run a subset:
just bench_preset=release bench -- --benchmark_filter='Quorum|ReplayBatch'

# Profile build (RelWithDebInfo + frame pointers) for sampling profilers:
just bench_preset=profile bench -- --benchmark_filter='WriteQuorumSync'

# Replication fan-out benches with the reference flags (11 reps):
just bench-repl
# ...plus CSV output for percentile math:
just bench-repl -- --benchmark_out=/tmp/repl.csv --benchmark_out_format=csv
```

All `CINDER_*` tuning knobs (`CINDER_BENCH_PRESET`, `CINDER_REPL_FILTER`,
`CINDER_BENCH_REPETITIONS`, … — see the `ENVIRONMENT` block at the top of
`Justfile`, or copy `.env.example` to `.env`) also work from the
environment or as `just VAR=value` overrides.

Current families in `benchmarks/`: store put/get (`lrU`/`lfu`), ring
lookup, slab allocator, tracing overhead, anti-entropy, WAL/snapshot,
hint-queue snapshot/remove, and `replication_bench.cpp` (quorum
write/read across replica counts, hint replay at 64/256/1024 backlogs).

### Getting trustworthy numbers

- **Pin the CPU** (`taskset -c 2 ...`) and keep the machine quiet.
  The 8×4.5 GHz dev box numbers below are only comparable to reruns on
  the same box under the same conditions.
- **Repetitions + min time**: `--benchmark_repetitions=11
  --benchmark_min_time=0.05` for quick iteration,
  `--benchmark_min_time=1` (or higher) for publishable numbers.
- **CSV/JSON out** for percentile math:
  `--benchmark_out=/tmp/x.csv --benchmark_out_format=csv`. Note the CSV
  reporter only emits aggregate rows when
  `--benchmark_report_aggregates_only` is set — leave it off when you
  need per-repetition p50/p95/p99.
- **Silence logging in benches**: per-hint `CINDER_WARN`/`INFO` spam can
  dominate wall clock without touching timed results. Benches that rebuild
  large hint backlogs call `cinder::setLogLevel(cinder::LogLevel::Error)`
  once at startup (`silenceBenchLogging()` in `replication_bench.cpp`).
- **Know what the fake transport proves**: the replication benches use
  synchronous inline transports. They isolate coordinator cost (local
  commit, channel fan-out, LWW reduce, spans) — not network I/O, delayed
  completion, or contention. Keep them for cheap A/B comparisons; confirm
  anything load-bearing with a realistic load test before generalizing.
- **p50/p95 from 11 reps is coarse**; it rules out cliffs, it does not
  characterize tail latency. True p99 needs a load test with a latency
  histogram, not GBM repetitions.

### Reference numbers (2026-11-05, release, sync-inline transport)

| op | p50 | scaling note |
|---|---|---|
| write quorum @1–5 | 694 → 1048ns | +~90ns/replica |
| read miss @1–4 | 781 → 1249ns | linear |
| read repair @1–4 | 1174 → 1630ns | +~400ns over miss (self-heal + repair fan-out) |
| replay/call | 8.3 / 32.1 / 34.9µs @64/256/1024 | ~135ns/hint; 1024 costs one 256-batch |

Full percentile tables live in `.opencode/plan/repl_improv.md` (Phase 6).
The 1024-backlog result confirms the `K_REPLAY_BATCH = 256` bound: one
call replays one batch, the remainder drains on later ticks.

## CPU profiling with samply

`just samply-repl` builds the bench for `bench_preset` (default
`release`) and records the replication benches, saving
`build/<preset>/repl_profile.json.gz` and exiting (`--save-only` —
without it `samply record` starts a local web server and waits):

```bash
just samply-repl
# custom filter / longer sampling:
just samply-repl -- --benchmark_filter='WriteQuorumSync/5' --benchmark_min_time=2
# symbols: record the profile-preset binary instead of release:
just bench_preset=profile samply-repl
```

Requirements: `perf_event_paranoid <= 1`
(`echo 1 | sudo tee /proc/sys/kernel/perf_event_paranoid`) **and** a
runtime that allows `perf_event_open` (no seccomp denial — plain
containers often block it even for self-profiling, in which case no
sampler works and you are limited to GBM + `perf stat` if available).

For release builds the profile has no symbols in the JSON; use the
`profile` preset binary and resolve offline:

```bash
cmake --preset profile
cmake --build --preset profile -j$(nproc) --target cinder_throughput_bench
samply record --save-only -o /tmp/p.json.gz -- \
  ./build/profile/bin/cinder_throughput_bench \
  --benchmark_filter='WriteQuorumSync/5|ReadQuorumRepair/4' \
  --benchmark_min_time=0.3
# then: walk each thread's stack table, count function hits
# (total vs self), and addr2line the main-binary offsets:
addr2line -e build/profile/bin/cinder_throughput_bench -f -C < addrs.txt
```

`--save-only` matters: without it `samply record` starts a local web
server and waits. Past runs used a small throwaway Python parser over
the Firefox-profile JSON (stack-table walk → per-function total/self
counts → `addr2line` for names); keep that script next to the profile,
not in the repo, unless it graduates into `benchmarks/` tooling.

Read the profile as **self time = where CPU burns, total time = expensive
call trees**. The last full analysis is a worked example:

- ~35% of wall samples in `Event::dispatchEvent` / `formatRecordInto`:
  the no-subscriber direct path formatted every log record and *then*
  dropped it in spdlog level filtering. Fix (`src/common/tracing/spdlog.cpp`):
  consult `should_log` before formatting. Behavior-preserving (dropped
  stays dropped); write quorum 1.19 → 0.69µs.
- Post-fix top: `writeAsync`/`fanOutWrite`/transport invoke,
  `EvictionStoreBase::putVersioned` ~3.7% self, key `memcmp` ~4.8%,
  `shared_ptr` refcounting ~1–2% each of inc/dec, per-replica `Span`
  construction ~6% self (`beginSpan`: splitmix id + thread-local
  installs per span per completion).

## Hardware counters with `perf stat`

When `perf_event_open` works at all, `perf stat` is the cheapest way to
classify a workload (CPU-bound vs cache/branch-bound). Recipe form:

```bash
just perf-stat-repl
# explicit events instead of -d groups:
just perf-stat-repl -- -e cycles,instructions,branches,branch-misses,cache-references,cache-misses
```

which runs `perf stat -r 3 -d` pinned via `taskset -c 2` around the
replication benches (`--benchmark_min_time=2`; extend via args).

`-d` adds L1/LLC groups (`-d -d` adds TLB, `-d -d -d` prefetch).
Explicit events when you want a fixed set:

```bash
perf stat -r 10 \
  -e cycles,instructions,branches,branch-misses,cache-references,cache-misses \
  -- taskset -c 2 ./build/release/bin/cinder_throughput_bench \
  --benchmark_filter='WriteQuorumSync/5' --benchmark_min_time=5
```

What to look for: IPC (stalled vs retiring), branch-miss rate (PGO or
code-shape candidate), cache-miss rate (pointer to memory-access work,
not a verdict). Event names and counter multiplexing vary by CPU — check
`perf list` first, and run long enough that startup is negligible.
Man page: `man perf-stat`.

## Cache-line contention with `perf c2c`

`perf stat` cache-misses cannot distinguish a hot line from a *shared*
line. For true/false sharing — lines bouncing between cores (HITM),
which is the only kind that layout changes fix — use `perf c2c` on a
**multithreaded** workload (single-threaded runs cannot show it):

```bash
perf c2c record -- \
  ./build/profile/bin/cinder_throughput_bench \
  --benchmark_filter='YOUR_MULTITHREADED_CASE' --benchmark_min_time=5
perf c2c report --stdio
```

Read the Shared Data Cache Line Table (highest HITM first), then the
per-offset breakdown (code address → symbol → source:line). Map offsets
to members (`pahole -C TypeName <binary>` with DWARF) and change layout
only for lines on the measured hot path: separate hot read-mostly from
hotly written members, group same-time writes, keep read-only together.
Padding everything preemptively costs memory and locality — the kernel
false-sharing guide's warning applies here too. References:
`man perf-c2c`, [kernel false-sharing guide](https://docs.kernel.org/kernel-hacking/false-sharing.html),
[Intel VTune false-sharing cookbook](https://www.intel.com/content/www/us/en/docs/vtune-profiler/cookbook/2023-1/false-sharing.html).

Cinder candidates *to investigate, not to assume*: packed replication
metrics (`Metrics`), shared atomics on completion paths, concurrent
hint-queue/transport state. Evidence first.

## Profile-guided optimization (PGO)

Repo support (GCC only, matching the release toolchain):

```bash
just pgo-train   # instrumented build + full bench as training workload
just pgo-use     # rebuild consuming build/pgo-data
./build/pgo/bin/cinder_throughput_bench --benchmark_filter='Quorum|ReplayBatch' ...
```

Details: `CINDER_PGO={Generate,Use}` + `CINDER_PGO_DIR` in
`CMakeLists.txt`; `pgo-generate`/`pgo-use` configure presets share
`build/pgo` so profile paths match exactly. Same workflow as Rust/Clang
LLVM PGO (`-fprofile-generate` → workload → `-fprofile-use`).

Measured outcome on the fan-out benches (back-to-back A/B): write +3%,
replay +11%, read-miss −10% — mixed sign, within noise for ~1µs ops.
PGO tunes branch layout for the training workload; our path is
straight-line code plus virtual transport dispatch, and bench-trained
profiles don't match production transports/concurrency. **Not adopted
as default** (doubles build cost, stale-profile risk); infra retained
for future use. Retrain after meaningful code/workload changes, and
retain only on a dependable measured win — same rule as any
optimization here.

## Anti-patterns

- Optimizing a samply total-time frame without checking self time (you
  will "fix" a caller for a callee's cost).
- Changing layout for a `perf stat` cache-miss rate without `perf c2c`
  evidence (hot ≠ shared).
- Trusting GBM repetitions as tail latency (they aren't; see above).
- Comparing numbers across machines, presets, or flag sets — including
  pre/post log-gate numbers measured under different logging levels.
- Keeping a PGO profile after changing code or workload and calling the
  result "PGO-optimized".
- Running benchmarks from `build/release` while iterating on `fast` —
  confirm which binary your numbers came from (`--benchmark_list_tests`
  lists what it contains).
