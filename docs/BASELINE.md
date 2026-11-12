# Cinder baseline

Captured 2026-11-12 before starting Phase 2 changes. This records the
repository and machine baseline; benchmark numbers are comparative, not
portable performance claims.

## Repository and toolchain

- Module: `github.com/kraytos17/cinder`
- Go directive: 1.27; pinned toolchain: Go 1.27.3
- Host: Linux/amd64, 11th Gen Intel Core i5-11320H @ 3.20 GHz, 8 logical
  CPUs, 15.4 GiB RAM
- Full per-file and per-package Go inventory: [`_filemap.txt`](./_filemap.txt)
- Benchmark command: `go test ./... -run='^$' -bench=. -benchmem -count=5`
- Raw benchmark output: [`../bench/baseline.txt`](../bench/baseline.txt)
- Benchmark host notes: [`../bench/README.md`](../bench/README.md)

## Baseline gates

| Gate | Result | Notes |
|---|---|---|
| `go build ./...` | PASS | All packages build. |
| `go vet ./...` | PASS | No diagnostics. |
| `go test ./... -count=1` | PASS | All packages pass; interop tests follow their normal skip behavior when external C++ binaries are absent. |
| `go test ./... -race -count=1` | PASS | All packages pass. |
| `go tool golangci-lint run ./...` | PASS | 0 issues. |
| 8 fuzz targets, 60 seconds each | PASS | All passed with no crashers. Combined: 13,333,706 executions (per-target results below); existing corpus seeds loaded at start. |
| Benchmarks, `-benchmem -count=5` | PASS | Completed; see raw output. Results are not pinned and have ordinary host variance. |

## Selected benchmark observations

Five-run samples from the raw output (ns/op; B/op; allocs/op):

| Benchmark | Observed range |
|---|---|
| `BenchmarkStorePut` | 589–592 ns; 572 B; 2 allocs |
| `BenchmarkStoreGetMiss` | 24–28 ns; 0 B; 0 allocs |
| `BenchmarkCounterIncrementHotActor` | 100–104 ns; 0 B; 0 allocs |
| `BenchmarkDigestForRound` | 547–613 ns; 2048 B; 1 alloc |
| `BenchmarkIBLTBuildPeel` | 28.5–29.5 µs; 43,136 B; 21 allocs |

These are initial reference samples only. The benchmark suite contains multiple
workloads; compare future changes with `benchstat` on the same machine and
under the same Go version and flags.

## Fuzz results

Command shape: `GOMAXPROCS=1 go test -run=^$ -fuzz=<target> -fuzztime=60s
./<package>/`.

| Target | Package | Executions | New interesting inputs | Result |
|---|---|---:|---:|---|
| `FuzzDecodeRequest` | `internal/net/protocol` | 3,031,664 | 0 | PASS |
| `FuzzDecodeResponse` | `internal/net/protocol` | 3,138,830 | 0 | PASS |
| `FuzzGossipView` | `internal/cluster` | 1,295,189 | 4 | PASS |
| `FuzzWALReader` | `internal/persist` | 581,621 | 0 | PASS |
| `FuzzSnapshot` | `internal/persist` | 71,323 | 1 | PASS |
| `FuzzAECodecs` | `internal/ae` | 12,097 | 0 | PASS |
| `FuzzStoreSequence` | `internal/store` | 1,540,213 | 10 | PASS |
| `FuzzCounterCodec` | `internal/counter` | 3,662,769 | 1 | PASS |

Total executions: 13,333,706. No failing inputs or crashers were reported.
The WAL reader, snapshot, and AE codec fuzzers reported zero executions for
part of their allotted wall time while still exiting successfully; this is
recorded as observed, not interpreted as evidence of a failure. Interesting
inputs were retained by Go's fuzz cache/corpus mechanisms; none reproduced as
failures.

## Known interpretation limits

- The race and unit suites establish a green starting point; they do not prove
  the distributed invariants listed in [`INVARIANTS.md`](./INVARIANTS.md).
- The benchmark host is a developer machine, not designated reference hardware.
