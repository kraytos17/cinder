# Cinder

Distributed in-memory cache in C++23 — minimal, fast, no external dependencies at runtime.

## Features

- **In-memory cache store** with LRU / LFU eviction + TTL expiry
- **Binary wire protocol** over TCP — length-prefixed frames with magic/version validation
- **Async TCP server** using Asio, with per-connection backpressure
- **Consistent hash ring** — xxHash3 virtual nodes, immutable-snapshot atomic swap, lock-free reads, binary search over sorted vector
- **Cluster-aware routing** — each node owns a hash-ring range; non-owned keys return a redirect
- **Smart client library** (`CacheClient` + `ConnectionPool`) — routes via the ring, maintains persistent per-node connections

## Build

Requires **CMake 4.4+** and a **C++23 compiler** (GCC 16+ or Clang 19+).

```bash
make fast           # GCC + Ninja + lld + unity (one-time full builds)
make debug          # GCC + Ninja, per-file (editor + incremental dev loop)
make test-unit      # run unit tests
make test           # run all tests
```

Or directly with CMake:

```bash
cmake --preset fast && cmake --build --preset fast
```

### Presets

| Preset | Compiler | Config | Use |
|---|---|---|---|
| `debug` | GCC | Debug, per-file | Editor indexing + incremental dev |
| `fast` | GCC | Debug, **unity** | One-time full builds |
| `release` | GCC | Release + LTO | Production |
| `release-clang` | Clang | Release + LTO | Production (Clang) |
| `debug-clang` | Clang | Debug, per-file | Clang dev |
| `ci` | Clang | RelWithDebInfo + **clang-tidy** | Lint-gated CI |
| `ci-gcc` | GCC | RelWithDebInfo + ASan/UBSan | Sanitizer CI |
| `asan` / `asan-clang` | GCC / Clang | Debug + ASan/UBSan | Memory checking |
| `tsan` | GCC | Debug + ThreadSanitizer | Race detection |
| `ubsan` | GCC | Debug + UBSan | Undefined behavior |
| `msan` | Clang | Debug + MemorySanitizer | Uninitialized reads |

### Sanitizer test runs

```bash
make asan-test        # ASan + leak detection + init-order checks
make tsan-test        # TSan with deadlock detection
make ubsan-test       # UBSan with stacktrace on halt
make asan-clang-test  # Clang ASan + use-after-scope/return
```

Suppressions live in `suppressions/` (`asan.supp`, `lsan.supp`, `tsan.supp`, `ubsan.supp`).

## Quick start

```bash
# Terminal 1 — start a server
make run ARGS="--port 7000"

# Terminal 2 — CLI
build/debug/bin/cinder-cli set mykey myvalue   # → OK
build/debug/bin/cinder-cli get mykey           # → myvalue
build/debug/bin/cinder-cli del mykey           # → OK
build/debug/bin/cinder-cli get missing         # → (not found)
build/debug/bin/cinder-cli ping                # → pong
```

### Multi-node (cluster-aware)

```bash
# Node 1
cinderd --port 7000 --node-id node1 --peers "node2@127.0.0.1:7001"

# Node 2
cinderd --port 7001 --node-id node2 --peers "node1@127.0.0.1:7000"
```

Each node routes requests to the correct owner via the hash ring. Non-owned keys return a redirect (`moved to <node>`).

### Smart client (in-process)

```cpp
#include "cinder/client/cache_client.hpp"

cinder::ClusterConfig config;
config.nodes = {{"node1", "127.0.0.1", 7000},
                {"node2", "127.0.0.1", 7001}};

cinder::CacheClient client(config);
client.set("key", "value", std::chrono::milliseconds(5000));
auto v = client.get("key");       // std::optional<std::string>
client.remove("key");             // bool
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

## Tests

```
 52 unit tests (7 suites): Result, LruStore, LfuStore, TtlWheel,
                           Protocol, ConsistentHashRing, CacheClient routing
  5 cli tests:             Ping, SetGet, GetNotFound, ConnectRefused, Del
  4 integration tests:     SetGetDelPing, TTLExpiry, CapacityEviction, LargeValue
  2 skipped:               ReplicaFailover, RebalanceOnJoin (need multi-node)
```

```bash
make test-unit         # 52 fast in-process tests
make test-integration  # 4 tests, forks real cinderd processes
make test-cli          # 5 tests, forks cinderd + cinder-cli
make test              # everything via ctest
```

## Project structure

```
cinder/
├── CMakeLists.txt               # Build system (dual-compiler, clang-tidy hooks)
├── CMakePresets.json            # 13 presets (GCC/Clang, sanitizers, CI)
├── Makefile                     # Wrapper: build/run/test/bench/format/clean
├── suppressions/                # Sanitizer suppressions
├── .clang-format                # Code formatting config
├── .clang-tidy                  # Static analysis (bugprone, modernize, performance)
├── .clangd                      # LSP config (points to build/debug/)
├── .editorconfig                # Editor settings
├── .gitattributes               # Git line endings
├── .gitignore
├── include/cinder/
│   ├── common/                  # Core types, Result<T>, logger
│   ├── store/                   # CacheStore, LruStore, LfuStore, TtlWheel
│   ├── hashing/                 # ConsistentHashRing (xxHash3, immutable snapshots)
│   ├── net/                     # Wire protocol, TCP server/connection
│   ├── client/                  # CacheClient, ConnectionPool, ClusterConfig
│   ├── node/                    # Replication, sharding (stubs)
│   └── cluster/                 # Membership, gossip, failure detection (stubs)
├── src/                         # Implementations
├── tools/
│   ├── cinderd_main.cpp         # Server entry point
│   └── cinder_cli.cpp           # CLI client (get/set/del/ping)
├── tests/
│   ├── unit/                    # 52 unit tests (GoogleTest)
│   ├── integration/             # 4 integration + 5 CLI tests
│   └── sim/                     # Deterministic simulation (stubs)
└── benchmarks/                  # throughput_bench (LRU/LFU) + allocator/ring (stubs)
```

## Tooling

```bash
make format          # clang-format all sources
make check-format    # verify formatting (CI)
make ci              # Clang + clang-tidy inline — catches lint at build time
```

- **clang-tidy** runs inline during `ci` builds; `.clang-tidy` excludes noisy checks (`#pragma once`, snake_case naming) for our style.
- **clangd** reads `build/debug/compile_commands.json` (per-file). Use `make debug` for editor indexing; `fast`/unity builds hide per-file commands from clangd.
- **ccache** auto-detected; capped at 25 GB by the Makefile.
- **mold** auto-selected as the linker (falls back to lld, then GNU ld) — disable with `-DCINDER_USE_FAST_LINKER=OFF`.
- **split-dwarf** (`-gsplit-dwarf` + `--gdb-index`) enabled in Debug/RelWithDebInfo for faster linking — disable with `-DCINDER_USE_SPLIT_DWARF=OFF`.

## Dependencies (all fetched by CMake)

- [Asio](https://think-async.com/Asio/) — networking (standalone, no Boost)
- [xxHash](https://xxhash.com/) — fast hashing for the consistent hash ring
- [spdlog](https://github.com/gabime/spdlog) — structured logging
- [CLI11](https://github.com/CLIUtils/CLI11) — command-line parsing
- [mimalloc](https://github.com/microsoft/mimalloc) — allocator baseline
- [GoogleTest](https://github.com/google/googletest) — unit/integration/CLI tests
- [GoogleBenchmark](https://github.com/google/benchmark) — microbenchmarks
