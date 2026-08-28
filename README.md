# Cinder

Distributed in-memory cache in C++23 — minimal, fast, no external dependencies at runtime.

## Features

- **Eviction store** with LRU and LFU policies, backed by a policy-templated CRTP base (`EvictionStoreBase`) that eliminates duplication across eviction strategies; a 256-slot `TtlWheel` with a min-heap for long TTLs (>256 ticks) reaps expiries without repeated wheel reinsertion
- **Binary wire protocol** (v3) over TCP — length-prefixed frames with compile-time-validated header layout (`consteval` + `static_assert`), big-endian fields, 8 opcodes, max 64 MiB messages
- **Async TCP server** using Asio, per-connection strand serialization, 1 MiB read buffers, write-queue backpressure (max 64 queued writes)
- **Consistent hash ring** — xxHash3 virtual nodes (150/physical node), immutable-snapshot atomic swap via `std::atomic<shared_ptr>`, lock-free reads with binary search; cluster-scale maps (`MembershipTable`, `ConnectionPool`, `TcpTransport`) use `std::flat_map` for cache-friendly lookups
- **Cluster-aware routing** — each node owns a hash-ring range; non-owned keys return a `moved to <node>` redirect
- **Primary-driven replication** — async or quorum (`W = R/2+1`) writes with monotonic versioned LWW conflict resolution; `writer_node_hash` breaks version ties
- **Failover reads** — a replica serves `GetVersioned` locally, so data stays available if the primary dies
- **Read repair** — quorum reads compare replica versions (LWW) and asynchronously write back the winning value to stale replicas
- **Hinted handoff** — unreachable replicas receive bounded, TTL-expiring hints (max 1024, 30s TTL) that are replayed when they return
- **SWIM-style membership** — failure detection (Ping → Suspect → Dead), incarnation-guarded gossip, automatic ring rebuild on membership change; `std::from_chars` for zero-allocation gossip parsing
- **Graceful leave** — `leave()` broadcasts Dead to all peers before shutdown, preventing false suspect-marking during teardown
- **Automatic rebalancing** — two-phase design (enumerate under lock, send after unlock); quarantine window prevents crash-loop nodes from becoming migration targets; self-retrying loop until all deferred keys migrate
- **Smart client** (`CacheClient` + `ConnectionPool`) — routes via the ring, follows redirects with one retry, pipelined `multiGet` per owner node
- **Transport coroutines** — `TcpTransport` and `ConnectionPool` use `asio::co_spawn` + `asio::awaitable` for clean async request-response; per-node strand serialization, configurable RPC deadlines
- **Thread-pool event loop** — `--io-threads` for N-worker `io_context`; auto-detects `min(4, hardware_concurrency)` when unset
- **Slab allocator** — `SlabAllocator<Node>` for store lists; 256-slot slabs, lock-free CAS free-list, `std::start_lifetime_as` for well-defined type-punning on the free-list overlay
- **Structured logging** — spdlog-backed `Logger` with `Stdout`/`Stderr` sink, `std::format`-based API, subsystem-level logging across TCP, replication, membership, failure detection, gossip, and shard management
- **YAML configuration** — `cinderd.yaml` config file with CLI flag override (`--config`, `--log-level`, `--verbose`)
- **Persistence** — append-only WAL + periodic snapshot; WAL entries carry per-entry XXH3-64 checksums (8-byte `WAL0` header + format version); atomic snapshot via write-to-temp + rename; crash recovery replays WAL from last snapshot; backward-compatible with older headerless WAL files
- **Error provenance** — `Error::wrap()` chains error origins across call layers with `std::source_location`; full Rule of Five (deep copy of `cause_` chain, move, assignment)
- **Fuzz harnesses** — 4 libFuzzer targets (protocol decode, gossip parse, store put, snapshot read) with 58+ seed corpus files and a protocol dictionary
- **TLS encryption** — compile-time opt-in (`CINDER_ENABLE_TLS`) with TLS 1.2; self-signed certs for testing, CA verification for production

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
| `debug-tls` | GCC | Debug + **TLS** | TLS development and testing |
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
| `fuzz` | Clang | Debug + libFuzzer + ASan/UBSan | Fuzz testing |

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

### Configuration

```bash
# Using a YAML config file
cinderd --config cinderd.yaml

# CLI flags override config file values
cinderd --config cinderd.yaml --port 7001 --log-level debug

# Verbose CLI logging (stderr)
cinder-cli -v set mykey myvalue
```

Example `cinderd.yaml`:

```yaml
server:
  port: 7000
  node_id: node1
  capacity: 67108864
  replication_factor: 1
  consistency: async

cluster:
  peers: []

failure_detector:
  ping_interval_ms: 1000
  suspect_timeout_ms: 3000
  gossip_interval_ms: 1000
  quarantine_interval_ms: 10000

persistence:
  enabled: false
  data_dir: /var/lib/cinder
  snapshot_interval_s: 60
  max_wal_entries: 10000

tls:
  enabled: false
  cert_file: /etc/cinder/cert.pem
  key_file: /etc/cinder/key.pem
  ca_file: /etc/cinder/ca.pem

logging:
  level: info
```

### Multi-node (cluster-aware)

```bash
# Node 1
cinderd --port 7000 --node-id node1 --peers "node2@127.0.0.1:7001"

# Node 2
cinderd --port 7001 --node-id node2 --peers "node1@127.0.0.1:7000"
```

Each node routes requests to the correct owner via the hash ring. Non-owned keys return a redirect (`moved to <node>`).

### TLS

```bash
# Build with TLS support
cmake --preset debug-tls && cmake --build --preset debug-tls

# Start a TLS-enabled server (uses test certs in tests/fixtures/)
build/debug-tls/bin/cinderd --port 7000 --tls \
    --tls-cert tests/fixtures/server.pem \
    --tls-key tests/fixtures/server-key.pem \
    --tls-ca tests/fixtures/ca.pem

# Connect with the CLI
build/debug-tls/bin/cinder-cli --port 7000 --tls \
    --tls-ca tests/fixtures/ca.pem set mykey myvalue
```

- Compile-time opt-in via `CINDER_ENABLE_TLS` (the `debug-tls` preset enables it).
- TLS 1.2 only; self-signed certs in `tests/fixtures/` for development.
- Server with `--tls` rejects plaintext connections.
- Client/server both support `--tls`, `--tls-cert`, `--tls-key`, `--tls-ca`.
- Non-TLS code paths are unaffected — `ConnectionPool` and `TcpTransport` gracefully fall back to plain TCP when `ssl_ctx` is null.

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
- Replicas that are unreachable during a write get a **hint** queued (bounded FIFO, max 1024 entries, 30s TTL) and replayed periodically.
- Quorum reads fan out `GetVersioned` to replicas, pick the highest version (LWW), and asynchronously repair stale replicas.

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
- Gossip uses `std::from_chars` for zero-allocation, exception-free integer parsing.

### Persistence

```bash
# Enable WAL + snapshot persistence
cinderd --port 7000 --node-id node1 \
        --enable-persistence --data-dir /var/lib/cinder

# Or via YAML config
cinderd --config cinderd.yaml
```

- **WAL** (write-ahead log): every `SET`/`DEL` is appended to `wal.log` before the store mutation is visible. Each entry carries an XXH3-64 checksum over its serialized fields; the WAL file begins with a magic header (`WAL0`, format version 1). Old-format headerless WAL files are replayed transparently.
- **Snapshot**: periodic compaction (default every 60s) serializes the full store to `snapshot_<epoch>.dat` with a `CSNP` magic header, format version 1, and per-entry XXH3-64 checksums; then truncates the WAL. Entries include frequency metadata for LFU eviction.
- **Recovery**: on startup, loads the latest snapshot and replays any WAL entries written after it.
- **Crash safety**: ungraceful shutdown loses only WAL entries not yet flushed; the next startup replays from the last snapshot.
- **Snapshot validation**: `SnapshotReader::readAll()` validates `entry_count * 41 <= remaining_file_bytes` before `reserve()` to prevent OOM on crafted snapshots.

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

// Batch fetch — one round trip per owner node
auto results = client.multiGet({"key1", "key2", "key3"});
```

## Server options

```
cinderd --port 7000 --capacity 67108864 --node-id node1 \
        --peers "node2@127.0.0.1:7001" --replication-factor 2 --consistency quorum
```

| Flag | Default | Description |
|---|---|---|
| `-C,--config` | `""` | Path to YAML config file |
| `-p,--port` | `7000` | Listen port |
| `-c,--capacity` | `64MB` | Per-node cache size in bytes |
| `-n,--node-id` | `"node1"` | This node's identifier |
| `--peers` | `""` | Comma-separated peers (`id@host:port`) |
| `-r,--replication-factor` | `1` | Replication factor (1 = no replication) |
| `--consistency` | `async` | Write consistency: `async` \| `quorum` |
| `--ping-interval` | `1000` | Failure-detector ping interval (ms) |
| `--suspect-timeout` | `3000` | Time a suspect persists before being marked dead (ms) |
| `--gossip-interval` | `1000` | Membership gossip dissemination interval (ms) |
| `--quarantine-interval` | `10000` | Re-join quarantine before a node receives migrated keys (ms, `0` = off) |
| `--io-threads` | `0` | Worker threads running the event loop (`0` = auto: min(4, hw)) |
| `--eviction-policy` | `lru` | Eviction policy: `lru` \| `lfu` |
| `--rpc-timeout` | `5000` | Per-RPC deadline in milliseconds (`0` = no timeout) |
| `--log-level` | `info` | Log level: `trace` \| `debug` \| `info` \| `warn` \| `error` |
| `--enable-persistence` | off | Enable WAL + snapshot persistence |
| `--data-dir` | `""` | Directory for WAL and snapshot files |
| `--snapshot-interval` | `60` | Snapshot compaction interval (seconds) |
| `--tls` | off | Enable TLS encryption (requires `CINDER_ENABLE_TLS` build) |
| `--tls-cert` | `""` | Path to TLS certificate chain (PEM) |
| `--tls-key` | `""` | Path to TLS private key (PEM) |
| `--tls-ca` | `""` | Path to CA certificate for peer verification (PEM) |

## Tests

```
 198 unit tests (16 suites):    Result, LruStore, LfuStore, LfuConcurrent,
                                TtlWheel, Protocol, ConsistentHashRing,
                                CacheClient routing + redirects,
                                VersionedStore (LRU/LFU), parsePeer,
                                Membership, Config, Persistence, TcpServer,
                                RpcTimeout, Gossip, ErrorProvenance
  35 sim tests (4 suites):      replication (async/quorum/hinted-handoff/read-repair),
                                gossip partition (suspect/dead/incarnation/degraded/
                                graceful-leave, late-joiner), rebalancing (keys migrate
                                on join, quarantine, replicas spread to all new owners,
                                concurrent rapid join/leave)
  22 integration tests:         SetGetDelPing, TTLExpiry, CapacityEviction, LargeValue,
                                replica failover (fanout, failover read, quorum,
                                hinted handoff, 3-node fanout, TTL-over-wire),
                                rebalance on join, rebalance RF=2, read repair, multi-get
  10 CLI tests:                 Ping, SetGet, GetNotFound, ConnectRefused, Del
                                + TLS (SetGetOverTls, PingOverTls, PlaintextRejected)
```

```bash
make test-unit         # fast in-process tests
make test-sim          # deterministic simulation tests (SimClock/SimBus)
make test-integration  # forks real cinderd processes (RUN_SERIAL)
make test-cli          # forks cinderd + cinder-cli
make test-all          # run every test binary against the current preset
make test              # everything via ctest
```

### Fuzz testing

```bash
make fuzz-test         # run 4 fuzz targets for 10s each (requires fuzz preset)
```

Four libFuzzer harnesses with ASan + UBSan:

| Target | Entry point | What it fuzzes |
|---|---|---|
| `protocol_decode_fuzz` | `net::decode()` / `decodeResponse()` | Malformed wire frames, truncated payloads, integer overflow in `expires_at` |
| `gossip_parse_fuzz` | `gossip::parseEntry()` | Gossip text format edge cases (empty, malformed, huge incarnations) |
| `store_put_fuzz` | `LruStore::put()` / `get()` / `remove()` | Arbitrary key/value lengths, capacity edge cases |
| `snapshot_fuzz` | `SnapshotReader::readAll()` | Crafted snapshot files, oversized `entry_count`, truncated data |

Corpus seeds (58+ files) are generated by `tests/fuzz/generate_corpus.py`.

The simulation harness (`tests/sim/`) drives replication and membership logic against a
deterministic clock and a fault-injecting message bus (delay, loss, reorder, node down),
so partition behavior is reproducible without real sockets.

## Project structure

```
cinder/
├── CMakeLists.txt               # Build system (dual-compiler, clang-tidy hooks)
├── CMakePresets.json            # 15 presets (GCC/Clang, sanitizers, CI, TLS, fuzz)
├── Makefile                     # Wrapper: build/run/test/bench/format/fuzz/clean
├── cinderd.yaml                 # Example YAML configuration
├── suppressions/                # Sanitizer suppressions
├── .clang-format                # Code formatting config
├── .clang-tidy                  # Static analysis (bugprone, modernize, performance)
├── .clangd                      # LSP config (points to build/debug/)
├── .editorconfig                # Editor settings
├── .gitattributes               # Git line endings
├── .gitignore
├── include/cinder/
│   ├── common/                  # Core types, Result<T>, Error (Rule of Five + wrap),
│   │   │                        #   Logger, Config, SlabAllocator (start_lifetime_as)
│   │   ├── cluster_config.hpp   # ClusterConfig (NodeConfig)
│   │   ├── config.hpp           # Config struct + YAML loader
│   │   ├── logger.hpp           # spdlog-backed Logger (std::format API)
│   │   ├── slab_allocator.hpp   # SlabAllocator with lock-free CAS free-list
│   │   ├── status.hpp           # Error (wrap/cause, Rule of Five), Result<T>
│   │   └── types.hpp            # VersionedEntry, ConsistencyMode, Bytes, NodeId
│   ├── store/
│   │   ├── cache_store.hpp      # CacheStore abstract base (liveEntries, forEach, mintVersion)
│   │   ├── lru_store.hpp        # LruStore (inherits EvictionStoreBase)
│   │   ├── lfu_store.hpp        # LfuStore (inherits EvictionStoreBase)
│   │   ├── ttl_wheel.hpp        # TtlWheel (256-slot wheel + min-heap for long TTLs)
│   │   ├── persistence.hpp      # PersistenceManager (WAL drain-queue, snapshot compaction)
│   │   ├── wal.hpp              # WalWriter/WalReader (XXH3 checksums, WAL0 header)
│   │   ├── snapshot.hpp         # SnapshotWriter/Reader (CSNP header, format v1)
│   │   └── detail/
│   │       ├── eviction_store_base.hpp  # CRTP policy base (shared store skeleton)
│   │       └── io_utils.hpp     # Binary I/O helpers (readU32, readU64, readString)
│   ├── hashing/
│   │   └── consistent_hash_ring.hpp  # xxHash3 virtual nodes, atomic snapshot, binary search
│   ├── net/
│   │   ├── protocol.hpp         # Wire protocol v3 (encode/decode, consteval frame size)
│   │   ├── tcp_server.hpp       # TcpServer (strand, acceptor, TLS opt-in)
│   │   ├── tcp_connection.hpp   # TcpConnection (strand, 1MB read buffer, write queue)
│   │   └── tcp_transport.hpp    # TcpTransport (per-node coroutines, RPC deadline, flat_map)
│   ├── client/
│   │   ├── cache_client.hpp     # CacheClient (ring routing, redirect retry, multiGet)
│   │   └── connection_pool.hpp  # ConnectionPool (coroutine send, batch pipeline, TLS)
│   ├── node/
│   │   ├── cache_node_server.hpp # CacheNodeServer (assembles store + ring + repl + gossip)
│   │   ├── shard_manager.hpp    # ShardManager (two-phase rebalance, quarantine-aware)
│   │   ├── replication_manager.hpp # ReplicationManager (async/quorum writes, read repair)
│   │   └── hint_queue.hpp       # HintQueue (bounded FIFO, 1024 capacity, 30s TTL)
│   └── cluster/
│       ├── clock.hpp            # Clock/RealClock, steady↔system conversions
│       ├── membership.hpp       # MembershipTable (flat_map, incarnation guard, onChange)
│       ├── failure_detector.hpp # FailureDetector (SWIM probe, state_mutex_, suspect timeout)
│       ├── gossip.hpp           # GossipManager (view dissemination, leave broadcast)
│       └── transport.hpp        # Transport abstract base (sendAsync, sendRequestAsync)
├── src/                         # Implementations
├── tools/
│   ├── cinderd_main.cpp         # Server entry point (CLI11, YAML config, TLS setup)
│   └── cinder_cli.cpp           # CLI client (get/set/del/ping, TLS support)
├── tests/
│   ├── unit/                    # 198 unit tests (GoogleTest)
│   ├── integration/             # 22 integration + 10 CLI tests (fork real cinderd)
│   ├── sim/                     # 35 simulation tests (SimClock/SimBus)
│   ├── fuzz/                    # 4 libFuzzer harnesses + corpus + generate_corpus.py
│   │   ├── protocol_decode_fuzz.cpp
│   │   ├── gossip_parse_fuzz.cpp
│   │   ├── store_put_fuzz.cpp
│   │   ├── snapshot_fuzz.cpp
│   │   ├── generate_corpus.py
│   │   └── corpus/              # 58+ seed files across 4 directories
│   └── fixtures/                # Test certificates (ca.pem, server.pem, server-key.pem)
├── benchmarks/                  # throughput, allocator, and ring benchmarks
└── docs/
    ├── protocol.md              # Wire protocol v3 (opcodes, payload format, hex dump)
    └── rebalance.md             # Membership, failure detection, rebalance, quarantine
```

## Tooling

```bash
make format          # clang-format all sources
make check-format    # verify formatting (CI)
make ci              # Clang + clang-tidy inline — catches lint at build time
make info            # show resolved PRESET/build paths
make kill-stale      # kill leftover cinderd daemons holding test ports
make help            # all targets
```

- **clang-tidy** runs inline during `ci` builds; `.clang-tidy` excludes noisy checks for the project's coding style.
- **clangd** reads `build/debug/compile_commands.json` (per-file). Use `make debug` for editor indexing; `fast`/unity builds hide per-file commands from clangd.
- **ccache** auto-detected; capped at 25 GB by the Makefile.
- **mold** auto-selected as the linker (falls back to lld, then GNU ld) — disable with `-DCINDER_USE_FAST_LINKER=OFF`.
- **split-dwarf** (`-gsplit-dwarf` + `--gdb-index`) enabled in Debug/RelWithDebInfo for faster linking — disable with `-DCINDER_USE_SPLIT_DWARF=OFF`.

## Dependencies

- [Asio](https://think-async.com/Asio/) — networking (standalone, no Boost)
- [xxHash](https://xxhash.com/) — fast hashing for the consistent hash ring, WAL checksums, and snapshot checksums
- [spdlog](https://github.com/gabime/spdlog) — structured logging
- [yaml-cpp](https://github.com/jbeder/yaml-cpp) — YAML configuration file parsing
- [CLI11](https://github.com/CLIUtils/CLI11) — command-line parsing
- [mimalloc](https://github.com/microsoft/mimalloc) — allocator baseline
- [GoogleTest](https://github.com/google/googletest) — unit/integration/CLI tests
- [GoogleBenchmark](https://github.com/google/benchmark) — microbenchmarks
