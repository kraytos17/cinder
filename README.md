# Cinder

Distributed in-memory cache in C++23 — minimal, fast, no external dependencies at runtime.

## Features

- **In-memory cache store** with LRU / LFU eviction + TTL expiry
- **Binary wire protocol** over TCP — length-prefixed frames with magic/version/opcode validation, big-endian fields
- **Async TCP server** using Asio, with per-connection backpressure and reusable write buffers
- **Consistent hash ring** — xxHash3 virtual nodes, immutable-snapshot atomic swap, lock-free reads, binary search over sorted vector
- **Cluster-aware routing** — each node owns a hash-ring range; non-owned keys return a redirect
- **Primary-driven replication** — async or quorum (`W = R/2+1`) writes with versioned LWW conflict resolution and hinted handoff for down replicas
- **Failover reads** — a replica serves local reads, so data stays available if the primary dies
- **SWIM-style membership** — failure detection (Ping → suspect → dead), incarnation-guarded gossip, and automatic ring rebuild on membership change
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

### Replication

```bash
# Node 1 — replicate every write to 1 replica (total factor 2), quorum-consistent
cinderd --port 7000 --node-id node1 --replication-factor 2 --consistency quorum \
        --peers "node2@127.0.0.1:7001"

# Node 2
cinderd --port 7001 --node-id node2 --replication-factor 2 --consistency quorum \
        --peers "node1@127.0.0.1:7000"
```

- `--replication-factor R` — each key is written to the primary + `R-1` successors on the ring.
- `--consistency async` (default) — the primary acknowledges after its local write and fans out best-effort.
- `--consistency quorum` — the write succeeds only if `W = R/2 + 1` acknowledgements (including the local write) are observed; otherwise it fails closed (`NotReady`).
- Replicas that are unreachable during a write get a **hint** queued and replayed periodically (hinted handoff), so they catch up when they return.

### Failure detection & gossip

Membership runs automatically when `--peers` is set:

```bash
# Tune the detection cadence (defaults shown)
cinderd --port 7000 --node-id node1 --peers "node2@127.0.0.1:7001,node3@127.0.0.1:7002" \
        --ping-interval 1000 --suspect-timeout 3000 --gossip-interval 1000
```

- Nodes probe peers with `Ping`; a failed/missing reply marks a node **suspect**, then **dead** after `--suspect-timeout`.
- Membership views are disseminated over `Gossip` with **incarnation numbers**, so a node that recovers refutes stale dead rumors.
- The hash ring is rebuilt automatically as membership changes (dead nodes are removed, alive nodes re-added).

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
cinderd --port 7000 --capacity 67108864 --node-id node1 \
        --peers "node2@127.0.0.1:7001" --replication-factor 2 --consistency quorum
```

| Flag | Default | Description |
|---|---|---|
| `-p,--port` | `7000` | Listen port |
| `-c,--capacity` | `64MB` | Per-node cache size in bytes |
| `-n,--node-id` | `"node1"` | This node's identifier |
| `--peers` | `""` | Comma-separated peers (`id@host:port`) |
| `-r,--replication-factor` | `1` | Replication factor (1 = no replication) |
| `--consistency` | `async` | Write consistency: `async` \| `quorum` |
| `--ping-interval` | `1000` | Failure-detector ping interval (ms) |
| `--suspect-timeout` | `3000` | Time a suspect persists before being marked dead (ms) |
| `--gossip-interval` | `1000` | Membership gossip dissemination interval (ms) |

## Tests

```
 81 unit tests (11 suites):   Result, LruStore, LfuStore, TtlWheel, Protocol,
                              ConsistentHashRing, CacheClient routing,
                              VersionedStore, CacheNodeServer parsePeer, Membership
 16 sim tests (2 suites):     replication (async/quorum/hinted-handoff),
                              gossip partition (suspect/dead/incarnation/degraded)
 11 integration tests:        SetGetDelPing, TTLExpiry, CapacityEviction, LargeValue,
                              replica failover (fanout, failover read, quorum,
                              hinted handoff, 3-node fanout, TTL-over-wire)
  5 cli tests:                Ping, SetGet, GetNotFound, ConnectRefused, Del
  1 skipped:                  RebalanceOnJoin (needs Phase 7 rebalancing)
```

```bash
make test-unit         # 81 fast in-process tests
make test-sim          # 16 deterministic simulation tests (SimClock/SimBus)
make test-integration  # forks real cinderd processes (RUN_SERIAL)
make test-cli          # forks cinderd + cinder-cli
make test-all          # run every test binary against the current preset
make test              # everything via ctest (113 entries)
```

The simulation harness (`tests/sim/`) drives replication and membership logic against a
deterministic clock and a fault-injecting message bus (delay, loss, reorder, node down),
so partition behavior is reproducible without real sockets.

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
│   ├── common/                  # Core types, Result<T> (std::expected), to_string(Errc)
│   ├── store/                   # CacheStore, LruStore, LfuStore, TtlWheel
│   ├── hashing/                 # ConsistentHashRing (xxHash3, immutable snapshots)
│   ├── net/                     # Wire protocol, TCP server/connection, async transport
│   ├── client/                  # CacheClient, ConnectionPool, ClusterConfig
│   ├── node/                    # CacheNodeServer, ReplicationManager
│   └── cluster/                 # MembershipTable, FailureDetector, Gossip, Clock, Transport
├── src/                         # Implementations
├── tools/
│   ├── cinderd_main.cpp         # Server entry point
│   └── cinder_cli.cpp           # CLI client (get/set/del/ping)
├── tests/
│   ├── unit/                    # 81 unit tests (GoogleTest)
│   ├── integration/             # 11 integration + 5 CLI tests (fork real cinderd)
│   └── sim/                     # 16 deterministic simulation tests (SimClock/SimBus)
└── benchmarks/                  # throughput_bench (LRU/LFU) + allocator/ring (stubs)
```

## Tooling

```bash
make format          # clang-format all sources
make check-format    # verify formatting (CI)
make ci              # Clang + clang-tidy inline — catches lint at build time
make info            # show resolved PRESET/build paths
make help            # all targets
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
