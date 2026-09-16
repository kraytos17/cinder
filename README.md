# Cinder

Distributed in-memory cache in C++23 — consistent-hash sharding, quorum
replication, gossip membership, and persistence, speaking a small binary
protocol over TCP with an opt-in gRPC gateway.

```bash
just preset=fast build
just run args="--port 7000"

# another terminal
build/debug/bin/cinder-cli set mykey myvalue
build/debug/bin/cinder-cli get mykey
```

## A three-node cluster in a minute

```bash
# Terminals 1–3
cinderd --port 7000 --node-id node1 --peers "node2@127.0.0.1:7001,node3@127.0.0.1:7002" \
        --replication-factor 2 --consistency quorum
cinderd --port 7001 --node-id node2 --peers "node1@127.0.0.1:7000,node3@127.0.0.1:7002" \
        --replication-factor 2 --consistency quorum
cinderd --port 7002 --node-id node3 --peers "node1@127.0.0.1:7000,node2@127.0.0.1:7001" \
        --replication-factor 2 --consistency quorum

build/debug/bin/cinder-cli --port 7000 set mykey myvalue   # quorum write
build/debug/bin/cinder-cli --port 7001 get mykey           # via replica / redirect

# Kill node1 then:
build/debug/bin/cinder-cli --port 7001 get mykey           # still served (failover read)
```

Nodes discover each other over gossip, rebalance keys on join, and quarantine
crash-looping peers before trusting them with data again.

## How it fits together

One `cinderd` process hosts a `CacheNodeServer`: a TCP server in front of an
eviction store, with a consistent-hash ring deciding ownership, primary-driven
replication for durability, and SWIM-style membership keeping every node
informed. `cinder-cli` / `cinder-admin` talk the same binary protocol;
`CacheClient` embeds it in-process. Details: [`docs/architecture.md`](docs/architecture.md),
wire format in [`docs/protocol.md`](docs/protocol.md), membership and
rebalance in [`docs/rebalance.md`](docs/rebalance.md).

## Features

- **Eviction stores** — LRU and LFU with O(1) hits, intrusive TTL wheel, slab allocator
- **Binary wire protocol (v5)** — length-prefixed TCP frames, 16 opcodes, 64 MiB max
- **Cluster-aware routing** — hash-ring ownership with `moved to <node>@<host>:<port>` redirects
- **Replication** — async or quorum (`W = R/2+1`), versioned LWW, failover reads, read repair
- **Membership** — SWIM failure detection, incarnation-guarded gossip, graceful leave
- **Rebalance & repair** — two-phase migration with quarantine, periodic anti-entropy
- **Persistence** — checksummed WAL + atomic snapshots, crash recovery
- **Observability** — Prometheus `/metrics` + `/config`, structured tracing with wire trace-ids
- **Security & ops** — opt-in TLS 1.2, opt-in gRPC gateway, `cinder-admin` tooling, live config hot-reload
- **Correctness tooling** — 313 unit + 35 sim + 22 integration + 5 CLI tests, 7 libFuzzer harnesses, ASan/TSan/UBSan/MSan CI

## Build

Requires **CMake 4.4+** and a **C++23 compiler** (GCC 16+ or Clang 22+).

```bash
just preset=fast build
just build
just test-unit
just test
```

Or directly with CMake: `cmake --preset fast && cmake --build --preset fast`.

### Presets

| Preset | Compiler | Config | Use |
|---|---|---|---|
| `debug` | GCC | Debug, per-file | Editor indexing + incremental dev |
| `debug-tls` / `debug-tls-clang` | GCC / Clang | Debug + **TLS** | TLS development and testing |
| `debug-grpc` | GCC | Debug + **gRPC gateway** | gRPC gateway development |
| `fast` | GCC | Debug, **unity** | One-time full builds |
| `release` | GCC | Release + LTO | Production |
| `release-clang` | Clang | Release + LTO | Production (Clang) |
| `debug-clang` | Clang | Debug, per-file | Clang dev |
| `ci` | Clang | RelWithDebInfo + **clang-tidy** | Lint-gated CI |
| `ci-gcc` | GCC | RelWithDebInfo + ASan/UBSan | Sanitizer CI |
| `asan` / `asan-clang` | GCC / Clang | Debug + ASan/UBSan | Memory checking |
| `tsan` / `tsan-clang` | GCC / Clang | Debug + ThreadSanitizer | Race detection |
| `tls-grpc` / `tls-grpc-clang` | GCC / Clang | Release + **TLS** + **gRPC** | TLS + gRPC gateway CI |
| `ubsan` | GCC | Debug + UBSan | Undefined behavior |
| `msan` | Clang | Debug + MemorySanitizer | Uninitialized reads |
| `release-unity` | GCC | Release + **unity** | Fast perf-iteration rebuilds |
| `profile` | GCC | RelWithDebInfo + frame pointers | Flamegraphs / profiling |
| `analyze` | GCC | Debug + `-fanalyzer` | Static analysis |
| `iwyu` | Clang | Debug + include-what-you-use | Include hygiene |
| `fuzz` | Clang | Debug + libFuzzer + ASan/UBSan | Fuzz testing |
| `fuzz-coverage` | Clang | Coverage instrumentation | Corpus minimization |

```bash
just asan-test        # ASan + leak detection + init-order checks
just tsan-test        # TSan with deadlock detection
just ubsan-test       # UBSan with stacktrace on halt
just asan-clang-test  # Clang ASan + use-after-scope/return
```

Suppressions live in `suppressions/` (`asan.supp`, `lsan.supp`, `tsan.supp`, `ubsan.supp`, plus `asan_ignorelist.txt` and `cppcheck.supp`).

## Configuration

YAML file with CLI overrides and **live hot-reload** (log level, gossip/suspect
intervals, capacity apply without restart):

```bash
cinderd --config cinderd.yaml
cinderd --config cinderd.yaml --port 7001 --log-level debug
```

```yaml
server:
  port: 7000
  node_id: node1
  capacity: 67108864
  replication_factor: 1
  consistency: async        # async | quorum
  metrics_port: 9100        # Prometheus /metrics + /config (0 = disabled)
cluster:
  peers: []                 # ["node2@127.0.0.1:7001", ...]
persistence:
  enabled: false
  data_dir: /var/lib/cinder
tls:
  enabled: false            # + cert_file / key_file / ca_file
```

The full reference is `cinderd.yaml`; every flag is documented in `cinderd --help`.
Key tunables: `--ping-interval`, `--suspect-timeout`, `--gossip-interval`,
`--quarantine-interval`, `--io-threads` (0 = auto), `--eviction-policy`
(lru|lfu), `--rpc-timeout`, `--enable-persistence`, `--anti-entropy-interval`,
`--grpc-port` (0 = disabled, needs a `CINDER_ENABLE_GRPC` build).

## Tests

313 unit + 35 sim + 22 integration + 5 CLI tests (plus 3 TLS and 10 gRPC tests
under those builds):

```bash
just test-unit
just test-sim
just test-integration
just test-cli
just test-all
just test
```

The simulation harness (`tests/sim/`) drives replication and membership logic
against a deterministic clock and a fault-injecting message bus (delay, loss,
reorder, node down), so partition behavior is reproducible without real sockets.

### Fuzz testing

```bash
just fuzz-test
just fuzz-long
```

Seven libFuzzer harnesses with ASan + UBSan:

| Target | Entry point | What it fuzzes |
|---|---|---|
| `protocol_decode_fuzz` | `net::decode()` / `decodeResponse()` | Malformed wire frames, truncated payloads, integer overflow in `expires_at`; protocol dictionary with opcodes 1–16 |
| `gossip_parse_fuzz` | `gossip::parseEntry()` + `handleMessage()` (full `decodeView` path) | Gossip text format edge cases, semicolon-delimited multi-entry parsing |
| `store_put_fuzz` | `LruStore` / `LfuStore` — put, get, remove, putVersioned, evictExpired, plus op-dispatch (Set/Get/Del/Ping) | Arbitrary key/value lengths, versioned writes, capacity edge cases |
| `snapshot_fuzz` | `SnapshotReader::readAll()` | Crafted snapshot files, oversized `entry_count`, truncated data |
| `wal_fuzz` | `WalReader::next()` + `PersistenceManager::recover()` | WAL checksummed/legacy formats, truncated entries, replay through persistence |
| `anti_entropy_fuzz` | `decodeDigest()`, `decodeBucketIds()`, `applyEntries()`, `onSyncRequest()`, `onDigestRequest()` | Anti-entropy binary blobs, malformed digests, truncated entry streams |
| `http_parse_fuzz` | `parseHttpRequest()` | Malformed HTTP requests, bad methods, truncated headers |

Corpus seeds (90+ files) are generated by `tests/fuzz/generate_corpus.py`.
Per-target `-max_len` and `-timeout` are configured in the Justfile.

## Project structure

```
cinder/
├── CMakeLists.txt               # Build system (dual-compiler, clang-tidy hooks)
├── CMakePresets.json            # 25 presets (24 configurable + `base` helper)
├── Justfile                     # Build/run/test/bench/format/fuzz/clean runner
├── cinderd.yaml                 # Example YAML configuration
├── include/cinder/              # Headers: common/, store/, hashing/, net/,
│                                #   client/, node/, cluster/
├── src/                         # Implementations
├── tools/                       # cinderd, cinder-cli, cinder-admin entry points
├── tests/                       # unit/ (313) · integration/ (22+5 CLI) · sim/ (35) ·
│                                #   fuzz/ (7 harnesses + corpus) · fixtures/ (test certs)
├── benchmarks/                  # throughput, allocator, and ring benchmarks
├── cinder/v1/cache.proto        # gRPC service definition
├── suppressions/                # Sanitizer + cppcheck suppressions
└── docs/
    ├── architecture.md          # Component map and design notes
    ├── protocol.md              # Wire protocol v5 (opcodes, payload format, hex dump)
    └── rebalance.md             # Membership, failure detection, rebalance, quarantine
```

## Tooling

```bash
just format
just check-format
just preset=ci build
just proto-lint
just proto-generate
just proto-breaking
just info
just kill-stale
just --list
```

- **clang-tidy** runs inline during `ci` builds; `.clang-tidy` excludes noisy checks for the project's coding style.
- **clangd** reads `build/debug/compile_commands.json` (per-file). Use `just build` for editor indexing; `fast`/unity builds hide per-file commands from clangd.
- **sccache** auto-detected via CMake; falls back to ccache.
- **mold** auto-selected as the linker (falls back to lld, then GNU ld; skipped for GCC+LTO, whose slim bytecode neither handles) — disable with `-DCINDER_USE_FAST_LINKER=OFF`.
- **split-dwarf** (`-gsplit-dwarf` + `--gdb-index`) enabled in Debug/RelWithDebInfo for faster linking — disable with `-DCINDER_USE_SPLIT_DWARF=OFF`.

## CI

Single workflow (`.github/workflows/ci.yml`): a `build-image` job builds and pushes
the Arch-based CI image first (GCC 16, Clang 22, lld, cmake, ninja, cppcheck —
libstdc++ only), then the matrix runs inside it, pinned to the exact built digest
(no `:latest` race). The image ships SLSA provenance + SBOM attestations. Required
check: `CI Success`.

| Job | What |
|---|---|
| Build CI image | Toolchain image build/push |
| build-test (×7) | GCC/Clang debug, release, TLS, ASan+UBSan — unit/sim/cli |
| Feature Flags (×2) | Release + TLS + gRPC, GCC and Clang |
| Sanitizers (GCC) | ASan+UBSan (`ci-gcc`) — unit/sim/cli |
| TSan (×2) | ThreadSanitizer, GCC and Clang (non-PIE binaries + relaxed container seccomp) |
| Lint | Clang + inline clang-tidy, `check-format`, cppcheck |
| CodeQL (×2) | Release and TLS+gRPC |

- Same-repo PRs run the full matrix against their own image; fork PRs validate the Dockerfile only (no registry push) and skip the matrix.
- Docs-only changes skip the matrix via `ci-skip.yaml`, which still reports `CI Success`.
- Caches: per-family FetchContent archives + sccache; a new `CMakeLists.txt` hash cold-starts them.

## Dependencies

- [Asio](https://think-async.com/Asio/) — networking (standalone, no Boost)
- [xxHash](https://xxhash.com/) — fast hashing for the consistent hash ring, WAL checksums, and snapshot checksums
- [spdlog](https://github.com/gabime/spdlog) — structured logging
- [yaml-cpp](https://github.com/jbeder/yaml-cpp) — YAML configuration file parsing
- [CLI11](https://github.com/CLIUtils/CLI11) — command-line parsing
- [mimalloc](https://github.com/microsoft/mimalloc) — allocator baseline
- [GoogleTest](https://github.com/google/googletest) — unit/integration/CLI tests
- [GoogleBenchmark](https://github.com/google/benchmark) — microbenchmarks
- [gRPC](https://github.com/grpc/grpc) — optional, for multi-language gRPC gateway (`CINDER_ENABLE_GRPC`)

## License

MIT — see [LICENSE](LICENSE).
