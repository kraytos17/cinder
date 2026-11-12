# Benchmark baseline

Initial baseline captured 2026-11-12 with:

```sh
go test ./... -run='^$' -bench=. -benchmem -count=5
```

Host: Linux/amd64; 11th Gen Intel Core i5-11320H @ 3.20 GHz; 8 logical CPUs;
15.4 GiB RAM. Go: `go1.27.3-X:nodwarf5 linux/amd64`.

Raw output is in [`baseline.txt`](baseline.txt). These measurements are a
developer-host reference, not a release performance claim. For comparisons,
use the same host, toolchain, power/CPU conditions, flags, and workload; use
`benchstat` rather than comparing one sample by eye. See
[`docs/profiling.md`](../docs/profiling.md) for the profiling workflow.
