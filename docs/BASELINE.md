# Cinder baseline

Captured 2026-11-14 from the current repository state. This is a developer
machine baseline; benchmark numbers are comparative, not portable performance
claims.

## Repository and toolchain

- Module: `github.com/kraytos17/cinder`
- Go directive: 1.27; pinned toolchain and runtime: Go 1.27.4
- Host: Linux/amd64, kernel `7.3.3-1-cachyos`, 11th Gen Intel Core
  i5-11320H @ 3.20 GHz, 8 logical CPUs, 15.4 GiB RAM
- Benchmark command: `go test ./... -run='^$' -bench=. -benchmem -count=5`
- Raw benchmark output: [`../bench/baseline.txt`](../bench/baseline.txt)
- Benchmark host notes: [`../bench/README.md`](../bench/README.md)

## Baseline gates

| Gate | Result | Notes |
|---|---|---|
| `go build ./...` | PASS | All packages build. |
| `go vet ./...` | PASS | No diagnostics. |
| `go test ./... -count=1` | PASS | All packages pass; interop tests follow their normal skip behavior when external C++ binaries are absent. |
| `go test -race ./... -count=1` | PASS | All packages pass. |
| `go tool golangci-lint run ./...` | PASS | 0 issues. |
| 8 fuzz targets, 60 seconds each | NOT RUN | This refresh did not rerun the fuzz campaigns; see prior capture history in Git if needed. |
| Benchmarks, `-benchmem -count=5` | PASS | All benchmark packages completed; raw output is in `../bench/baseline.txt`. |

## Selected benchmark observations

Current full five-run samples (ns/op; B/op; allocs/op):

| Benchmark | Observed range |
|---|---|
| `BenchmarkStorePut` | 552.9–658.5 ns; 571–578 B; 2 allocs |
| `BenchmarkStoreGetMiss` | 20.28–20.70 ns; 0 B; 0 allocs |
| `BenchmarkCounterIncrementHotActor` | 132.7–136.3 ns; 0 B; 0 allocs |
| `BenchmarkDigestForRound` | 472.5–503.7 ns; 2048 B; 1 alloc |
| `BenchmarkIBLTBuildPeel` | 23.4–30.0 µs; 43,136 B; 21 allocs |

The benchmark suite contains multiple workloads; compare future changes with
`benchstat` on the same machine and under the same Go version and flags.

## Fuzz results

Fuzz campaigns were not run as part of this 2026-11-14 refresh. The previous
2026-11-12 capture recorded 8 targets × 60 seconds and 13,333,706 executions;
that historical result is not a current gate result. Run the repository's
`just fuzz` recipe (20 seconds per target) or the CI workflow for a fresh
fuzz status.

## Known interpretation limits

- The race and unit suites establish a green starting point; they do not prove
  the distributed invariants listed in [`INVARIANTS.md`](./INVARIANTS.md).
- The benchmark host is a developer machine, not designated reference hardware.
- Benchmark results are from this developer host, not designated reference
  hardware; ordinary CPU-frequency, thermal, and background-load variation
  applies. The prior capture's non-default GOGC/GOMEMLIMIT observations are not
  repeated in this baseline.
