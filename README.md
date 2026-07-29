# Cinder

Distributed in-memory cache in C++23 — minimal, fast, no external dependencies at runtime.

## Features

- **In-memory cache store** with LRU / LFU eviction + TTL expiry
- **Binary wire protocol** over TCP — length-prefixed frames with magic/version validation
- **Async TCP server** using Asio, with per-connection backpressure
- **Cluster-aware routing** via consistent hash ring with virtual nodes (immutable-snapshot, lock-free reads)
- **Consistent hash ring** with xxHash3, binary search over sorted vector, atomic snapshot swap

## Build

Requires CMake 4.4+ and a C++23 compiler (GCC 16+ or Clang 19+).

```bash
cmake --preset fast          # Debug build with unity + lld
cmake --build --preset fast  # Build
```

Or use the Makefile wrapper:

```bash
make fast       # configure + build (Ninja + lld + unity)
make test-unit  # run unit tests (49 tests)
make test       # run all tests (unit + integration + sim)
```

Available presets: `debug`, `release`, `sanitized`, `ci`, `fast`.

## Quick start

```bash
# Start a server on port 7000
make run ARGS="--port 7000"

# In another terminal:
cinder-cli set mykey myvalue   # → OK
cinder-cli get mykey           # → myvalue
cinder-cli del mykey           # → OK
cinder-cli get missing         # → (not found)
cinder-cli ping                # → pong
```

### Multi-node (cluster-aware)

```bash
# Node 1
cinderd --port 7000 --node-id node1 --peers "node2@127.0.0.1:7001"

# Node 2
cinderd --port 7001 --node-id node2 --peers "node1@127.0.0.1:7000"
```

Each node routes requests to the correct owner via the hash ring. Non-owned keys return a redirect.

## Tests

```
49 unit tests:  Result, LruStore, LfuStore, TtlWheel, Protocol, ConsistentHashRing
 1 integration: ClusterSmokeTest.SetGetDelPing (end-to-end: fork server, SET/GET/DEL/PING)
 2 skipped:     ReplicaFailover, RebalanceOnJoin (need multi-node setup)
```

```bash
make test-unit         # fast — 49 tests
make test-integration  # slow — forks real server process, 1 test
```

## Project structure

```
cinder/
├── CMakeLists.txt               # Build system
├── CMakePresets.json            # Build presets (debug/release/ci/fast)
├── Makefile                     # Convenience wrapper
├── .clang-format                # Code formatting config
├── .clang-tidy                  # Static analysis (bugprone, modernize, performance)
├── .clangd                      # LSP config (points to build/fast/)
├── .editorconfig                # Editor settings
├── .gitattributes               # Git line endings
├── .gitignore
├── include/cinder/
│   ├── common/                  # Core types, Result<T>, logger
│   ├── store/                   # CacheStore, LruStore, LfuStore, TtlWheel
│   ├── hashing/                 # ConsistentHashRing (xxHash3, immutable snapshots)
│   ├── net/                     # Wire protocol, TCP server/connection
│   ├── node/                    # ShardManager, ReplicationManager (stubs)
│   ├── cluster/                 # Membership, gossip (stubs)
│   └── client/                  # CacheClient, ConnectionPool (stubs)
├── src/                         # Implementations
├── tools/
│   ├── cinderd_main.cpp         # Server entry point (--port, --node-id, --peers)
│   └── cinder_cli.cpp           # CLI client (get/set/del/ping)
├── tests/
│   ├── unit/                    # 49 unit tests (GoogleTest)
│   ├── integration/             # 1 integration test
│   └── sim/                     # Deterministic simulation tests (stubs)
└── benchmarks/                  # Throughput/latency benchmarks (stubs)
```

## Tooling

```bash
make format        # Format all source files (clang-format)
make check-format  # Check formatting (CI use)
cmake --preset ci  # CI build — runs clang-tidy inline during compilation
```

## Server options

```
cinderd --port 7000 --capacity 67108864 --node-id node1 --peers "node2@..."
```

| Flag | Default | Description |
|---|---|---|
| `-p,--port` | `7000` | Listen port |
| `-c,--capacity` | `64MB` | Per-node cache size in bytes |
| `-n,--node-id` | `"node1"` | This node's identifier |
| `--peers` | `""` | Comma-separated peer node IDs |

## Dependencies

- [Asio](https://think-async.com/Asio/) — networking (standalone, no Boost)
- [xxHash](https://xxhash.com/) — fast hashing for consistent hash ring
- [spdlog](https://github.com/gabime/spdlog) — structured logging
- [CLI11](https://github.com/CLIUtils/CLI11) — command-line parsing
- [mimalloc](https://github.com/microsoft/mimalloc) — allocator baseline
- [GoogleTest](https://github.com/google/googletest) — unit/integration tests
- [GoogleBenchmark](https://github.com/google/benchmark) — microbenchmarks
