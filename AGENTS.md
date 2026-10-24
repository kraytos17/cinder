# AGENTS.md — Cinder

## What this repo is

Distributed in-memory cache in C++23: consistent-hash sharding (150 vnodes/node),
quorum replication (LWW versions), SWIM gossip membership, two-phase rebalance,
periodic anti-entropy, checksummed WAL + atomic snapshots, small binary TCP
protocol (v5, 16 opcodes) with opt-in gRPC gateway and opt-in TLS 1.2.

* One `cinderd` process hosts one `CacheNodeServer` (store + ring + replication +
  transport + cluster subsystems). Tools: `cinderd`, `cinder-cli`, `cinder-admin`.
  `CacheClient` + `ConnectionPool` offer the same access in-process.
* Details live in `docs/`: `architecture.md` (process model, request path, storage),
  `protocol.md` (wire format), `rebalance.md` (membership, rebalance, persistence).
  Do not duplicate them here; link them.

## Architecture cheat sheet

* Languages: C++ 78 files, YAML 5, Python 3. Packages: `unit` 357, `cinder` 333,
  `sim` 89, `integration` 76, `net` 67, `node` 55, `common` 50, … Entry points:
  `tools/cinderd_main.cpp`, `tools/cinder_cli.cpp`, `tools/cinder_admin.cpp`
  (gRPC gateway adds 10 routes: Get/Set/Delete/GetVersioned/Ping/MultiGet/
  MultiSet/Info/ClusterInfo/RingInfo).
* Layers: `cinder` is core (fan-in 352); every feature package is
  `entry: only outbound calls`. Heaviest boundaries: `node→cinder` 81,
  `net→cinder` 78, `store→cinder` 57.
* Hotspots by fan-in (treat `size` counts as resolver noise — the graph merges
  same-named methods; e.g. a `HintQueue::size` caller query returned
  `ByteReader`/`SlabAllocator` false positives): `EvictionStoreBase::size` 120,
  `err` 35, `CacheStore::now` 34, `ok` 26, `Error::message` 26,
  `TcpConnection::close` 24, `ConsistentHashRing::addNode` 21, `getNode` 16.
* Fan-out refactor radar: `CacheNodeServer::CacheNodeServer` (~100, now
  decomposed into `seedCluster/wireMetrics/installAdminCallbacks/installAddrResolver`
  + `NodeTimers` bundle + `partitionConfigChanges/applyHotField` split),
  `CinderCacheServiceImpl::Get/Set` (18/19, grpc-gated),
  `WalWriter::append` (20), `applyHotField` (20, log-macro inflation),
  `Span::Span` (in 48/out 19), `AntiEntropyManager::applyEntries` (now split into
  `parseSingleEntry/isNewer`), `CacheClient::sendToOwner` (now split into
  `tryFollowRedirect/refreshAfterFailure`).
* Cycles (5) and dispositions: `TtlWheel::insert↔remove` (intentional
  re-insert idempotency; unlink logic since deduped into `unlink/linkBefore` +
  `expireSlot/expireHeap`); `io_utils writeU*↔SnapshotWriter::write` (**false
  positive** — stream vs buffer overloads share a name; sources never recurse);
  `TcpConnection::close↔closeConnection` + 8-node read state machine
  (intentional state machine); `sendToOwner↔get↔ConnectionPool::send`
  (redirect-following call chain).
* Key clusters: #3 `CacheNodeServer;run;applyConfig`, #28
  `size;applyEntries;forEach`, #40 `sendToOwner;addNode;getNode;multiGet;Set`.

## Build presets (CMakePresets.json + Justfile)

* Daily loop: `just preset=fast build` (GCC unity — fastest edit-compile-test,
  **not** perf-representative) or `just build` (`debug`: per-file, best for
  indexing/incremental work). Direct CMake equivalent:
  `cmake --preset fast && cmake --build --preset fast`.
* Build parallelism: `just jobs=N <recipe>` (default `nproc`). Unity TUs for the
  test/core targets peak ~700 MB compiler RSS each, so on a 16 GB / 8-core box
  the default 8 jobs is near the ceiling; drop to `jobs=4` if the OOM killer
  or heavy swapping appears. `CINDER_UNITY_BATCH_SIZE` (default 8, matching
  CMake's own default) is the other memory knob — raise it only on a
  big-memory machine, since batch size dominates peak compiler RSS.
* Benchmarks are **OFF by default** (`CINDER_BUILD_BENCHMARKS`); only
  `release`, `release-clang`, `release-unity` and `profile` enable them.
  `just bench` re-configures with them ON, so it works from any preset.
* What to use when:
  `debug` dev · `fast` quick loop · `debug-tls[-clang]` TLS ·
  `debug-grpc` gRPC gateway (fetches gRPC+protobuf+abseil) ·
  `release[-clang]` + `release-unity` production/perf ·
  `ci` Clang RelWithDebInfo + inline clang-tidy (lint gate) ·
  `ci-gcc` GCC + ASan/UBSan · `asan[-clang]` / `tsan[-clang]` / `ubsan` /
  `msan` (MSan needs an MSan-instrumented stdlib) · `tls-grpc[-clang]` feature
  CI · `fuzz` / `fuzz-coverage` libFuzzer · `profile` flamegraphs ·
  `analyze` `-fanalyzer` · `iwyu` include hygiene.
* Toolchains: `toolchains/gcc16.cmake`, `toolchains/clang22.cmake`.
  Requires CMake 4.4+, GCC 16+ or Clang 22+.
* Sanitizer env comes from test presets (`ASAN_OPTIONS`+`LSAN_OPTIONS`,
  `TSAN_OPTIONS`, `UBSAN_OPTIONS`); suppressions/ignorelist in `suppressions/`
  (`asan.supp`, `asan_ignorelist.txt`, `lsan.supp`, `tsan.supp`, `ubsan.supp`,
  `cppcheck.supp`).

## Tests (ctest -L labels are source of truth)

* `just test-unit` → `-L unit` (22 files under `tests/unit`, compiled as four
  binaries split by area: `cinder_unit_{store,ring,net,node}_tests`, so an edit
  to one area no longer rebuilds all unit tests),
  `just test-sim` → `-L sim` (8 files), `just test-cli` → `-L cli`,
  `just test-integration` (12 files, kills stale `cinderd` first) → 24 tests,
  `just test-all` runs every binary with `--gtest_also_run_disabled_tests`,
  `just grpc-test` for `debug-grpc` builds.
  `just verify` is the fast pre-push gate (format + cppcheck + unit + sim
  + cli, no integration).
  Integration tests `fork`+`execl` real `cinderd` processes via `spawnNode`
  (up to 3 per test), but an idle daemon is only ~2 MB RSS, so parallel
  integration runs are bounded by fd/cpu contention, not memory. Every
  integration test has a 300s ctest `TIMEOUT`, so a hung test fails fast
  instead of burning the CI job.
* Fuzz: `just fuzz-test` (10s per target: protocol_decode gossip_parse wal
  snapshot anti_entropy store_put http_parse redirect_parse),
  `just fuzz-long` (5min), coverage/minimize/report recipes; corpus under
  `tests/fuzz/corpus*` (git-ignored).
* Benchmarks: `just bench` (`cinder_throughput_bench`); `buf lint/generate/breaking`
  for `cinder/v1/cache.proto`.
* Flake protocol: run integration `-j1`. `ClientRedirectTest.
  MultiGetFollowsRedirectsAfterJoin` takes ~60s even when healthy (collects
  migrated+redirected keys for up to 120s) and has flaked once under parallel
  `release-clang -j$(nproc)` (64.59s fail) while passing isolated (59.34s).
  On failure: re-run isolated (`-R ClientRedirectTest -j1`) before treating as real.
  Parallel unit runs can also flake on shared tmp files — confirm with `-j1`.

## Lint gate (this is what CI enforces)

* `just preset=ci configure && just preset=ci build` — clang-tidy runs **inline**
  during compilation. `.clang-tidy` is `Checks: *` minus ~40 exclusions with
  `WarningsAsErrors: bugprone cert clang-analyzer concurrency misc performance
  portability`. Consequence: **`readability-*` are warnings-only; only the
  listed families fail the build.** `HeaderFilterRegex` limits tidy to
  `(src|include)/cinder/.*`.
* Past failures (fixed Oct 2026 — do not reintroduce):
  single-char `+=` (`out += ","` → `','` in `src/common/config.cpp`,
  `rendered += ";"` → `';'` in `tests/unit/tracing_test.cpp`),
  redundant `std::string("short")` → `"short"` in `tests/unit/auth_test.cpp`,
  `reason->data()` on a `string_view` → pass `*reason` in
  `src/node/anti_entropy.cpp`.
* `just preset=ci check-format` (clang-format, LLVM base, 100 col) and
  `cmake --build build/ci-gcc --target cppcheck` (see `suppressions/cppcheck.supp`).
* Repo `lint` target = `check-format` + `cppcheck` only. There is **no cpplint**
  target or binary in this repo — do not invent one.
* Pre-commit hook (catches the cheap half of the lint gate before push):
  `just hooks-install` sets repo-local `core.hooksPath=.githooks`
  (`just hooks-run` runs the same checks manually). The hook checks staged
  files only: whitespace errors (`git diff --check`, `*.md` excluded so
  hard-breaks pass), clang-format `--dry-run` on C++ blobs, YAML syntax on
  staged workflows, and the ci.yml/ci-skip.yaml trigger-paths sync. Slow checks
  (tidy build, cppcheck, actionlint, tests) stay in `just verify` and CI — the
  hook is local-only by design (nothing is staged in a fresh checkout).
  Bypass with `git commit --no-verify`; CI (toolchain image's clang-format) is
  the source of truth on version skew.
* CI jobs (`.github/workflows/ci.yml`): `build-image` → `fetch-deps` (×2: union
  dep seed per family — the **sole writer** per `fetchcontent-<family>-*` key) +
  `build-test` (7 presets: debug, debug-tls, release, debug-clang,
  debug-tls-clang, release-clang, asan-clang; each runs unit+sim+cli+integration
  with `-j$(nproc)`) → `feature-flags` (tls-grpc*) → `sanitize` (ci-gcc) →
  `tsan` → `lint` → `codeql` → `ci-success` gate. Build jobs restore the
  FetchContent cache **read-only** with a family-prefix `restore-keys` fallback
  (a new `CMakeLists.txt` hash reuses previous deps); parallel jobs sharing one
  save key raced (`Unable to reserve cache`), hence the single writer.
  `SCCACHE_GHA_ENABLED=true` is set workflow-wide — without it the installed
  sccache writes to ephemeral disk and every job compiles cold. Writes are
  main-only (`SCCACHE_GHA_RW_MODE` is `READ_WRITE` on main, `READ_ONLY`
  elsewhere): PR-branch sccache entries are unreadable by other branches, so
  PR writes would be pure quota churn. Markdown/docs-only
  changes skip CI (`paths-ignore`).

## Gotchas for agents

* Unity (`fast`) merges TUs — per-file warnings can hide; confirm suspect files
  with a per-file preset (`debug`/`ci`) when chasing warnings.
* `CINDER_ENABLE_TLS` / `CINDER_ENABLE_GRPC` gate code (`cache_service_impl.cpp`
  is invisible in `fast`): verify grpc changes with `debug-grpc` or `tls-grpc*`.
* Init order matters: `CacheNodeServer` members were reordered so
  `io_ → ring_/table_ → transport_ → gossip_/detector_/repl_ → shard_/
  anti_entropy_ → server_` construct after their dependencies; keep init list
  in declaration order (`-Wreorder`).
* Redirect budget semantics in `CacheClient::sendToOwner`: follows reuse the
  attempt budget (`--attempt`) and are bounded by hops + visited set — preserve
  both when touching retry logic.
* `WAL0`/`CSNP` magic + XXH3 checksums: `WalWriter` buffer-layout and
  `WalReader` checksum recompute must stay byte-identical (fuzz: `wal`,
  `snapshot`, `store_put`).
* Prefer `rg`/`grep` over graph for string literals, error messages, config keys,
  Docker/shell/config files.

## Verification checklist (every change)

1. `cmake --build --preset fast -j$(nproc)` clean.
2. `ctest --preset fast -L unit -j1` (343 across the four unit binaries)
   + `-L sim -j1` (36); run affected `-L integration` isolated `-j1`
   (full suite = 24).
3. `cmake --build --preset fast --target check-format`, `cppcheck`, `lint` —
   all exit 0; for tidy-sensitive changes also build `ci` preset.
4. Sanitizers for the affected area (`asan`, `tsan`, `ubsan` recipes).
5. `git diff --stat` + `git status` review; re-index the graph when `src/` or
  `include/` changed before making further structural claims.
