# Cinder

Distributed in-memory cache in C++23 — minimal, fast, no external dependencies at runtime.

## Features

- **In-memory cache store** with LRU / LFU eviction + TTL expiry
- **Binary wire protocol** over TCP — length-prefixed frames with magic/version validation
- **Async TCP server** using Asio, with per-connection backpressure
- **Smart client library** (`CacheClient`) with consistent hashing
- **Consistent hash ring** with virtual nodes and immutable-snapshot swapping
- **Replication** with per-key versioning and quorum writes
- **Cluster membership** via SWIM-style gossip with incarnation numbers

## Build

Requires CMake 4.4+ and a C++23 compiler (GCC 16+ or Clang 19+).

```bash
cmake --preset fast          # Debug build with unity + lld
cmake --build --preset fast  # Build
```

Or use the Makefile wrapper:

```bash
make fast       # configure + build (Ninja + lld + unity)
make test-unit  # run unit tests
make test       # run all tests (unit + integration + sim)
```

Available presets: `debug`, `release`, `sanitized`, `ci`, `fast`.

## Quick start

```bash
# Start a server on port 7000
make run ARGS="--port 7000"

# In another terminal:
cinder-cli set mykey myvalue
cinder-cli get mykey       # → myvalue
cinder-cli del mykey
cinder-cli ping
```

## Project structure

```
cinder/
├── CMakeLists.txt               # Build system
├── CMakePresets.json            # Build presets (debug/release/ci/fast)
├── Makefile                     # Convenience wrapper
├── .clang-format                # Code formatting config
├── .clang-tidy                  # Static analysis config
├── .clangd                      # LSP config
├── .editorconfig                # Editor settings
├── .gitattributes               # Git line endings
├── .gitignore
├── include/cinder/
│   ├── common/                  # Core types, Result<T>, logger
│   ├── store/                   # CacheStore, LruStore, LfuStore, TtlWheel
│   ├── hashing/                 # ConsistentHashRing
│   ├── node/                    # ShardManager, ReplicationManager
│   ├── cluster/                 # Membership, gossip, failure detection
│   ├── net/                     # Wire protocol, TCP server/connection
│   └── client/                  # CacheClient, ConnectionPool
├── src/                         # Implementations
├── tools/
│   ├── cinderd_main.cpp         # Server entry point
│   └── cinder_cli.cpp           # CLI client
├── tests/
│   ├── unit/                    # Unit tests (GoogleTest)
│   ├── integration/             # Integration tests
│   └── sim/                     # Deterministic simulation tests
└── benchmarks/                  # Throughput/latency benchmarks
```

## Tooling

```bash
make format        # Format all source files (clang-format)
make check-format  # Check formatting (CI use)
```

## Dependencies

- [Asio](https://think-async.com/Asio/) — networking (standalone, no Boost)
- [xxHash](https://xxhash.com/) — fast hashing for consistent hash ring
- [spdlog](https://github.com/gabime/spdlog) — structured logging
- [CLI11](https://github.com/CLIUtils/CLI11) — command-line parsing
- [mimalloc](https://github.com/microsoft/mimalloc) — allocator baseline
- [GoogleTest](https://github.com/google/googletest) — unit/integration tests
- [GoogleBenchmark](https://github.com/google/benchmark) — microbenchmarks
