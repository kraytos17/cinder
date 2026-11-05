set shell := ["bash", "-euo", "pipefail", "-c"]
set quiet := true

presets := `jq -r '.configurePresets[] | select(.hidden != true) | .name' CMakePresets.json 2>/dev/null | tr '\n' ' ' || echo "debug debug-tls debug-tls-clang debug-clang release release-clang asan asan-clang msan tsan tsan-clang ubsan ci ci-gcc tls-grpc tls-grpc-clang fast release-unity profile analyze iwyu fuzz fuzz-coverage debug-grpc"`

preset := "debug"
# Preset used by `just bench` (must have CINDER_BUILD_BENCHMARKS=ON).
bench_preset := "release"
jobs := `nproc`
args := ""

fuzz_targets := "protocol_decode gossip_parse store_put snapshot wal anti_entropy http_parse redirect_parse byte_reader ttl_wheel hint_queue ring config"
fuzz_opts_protocol_decode := "-max_len=8192 -dict=tests/fuzz/corpus/protocol_decode/protocol.dict"
fuzz_opts_gossip_parse := "-max_len=1024 -dict=tests/fuzz/corpus/gossip_parse/gossip.dict"
fuzz_opts_wal := "-max_len=4096 -rss_limit_mb=512 -dict=tests/fuzz/corpus/wal/wal.dict"
fuzz_opts_snapshot := "-max_len=4096 -dict=tests/fuzz/corpus/snapshot/snapshot.dict"
fuzz_opts_anti_entropy := "-max_len=8192"
fuzz_opts_store_put := "-max_len=1024"
fuzz_opts_http_parse := "-max_len=2048 -dict=tests/fuzz/corpus/http_parse/http.dict"
fuzz_opts_redirect_parse := "-max_len=256 -dict=tests/fuzz/corpus/redirect_parse/redirect.dict"
fuzz_opts_byte_reader := "-max_len=512"
fuzz_opts_ttl_wheel := "-max_len=2048"
fuzz_opts_hint_queue := "-max_len=2048 -rss_limit_mb=512"
fuzz_opts_ring := "-max_len=1024"
fuzz_opts_config := "-max_len=1024"
fuzz_opts_default := "-timeout=5"

# Show all available recipes
default:
    @just --list

[group('build')]
[doc('Configure the build dir for the current preset.')]
configure:
    cmake --preset {{ preset }}

[group('build')]
[doc('Build the current preset.')]
build:
    cmake --preset {{ preset }}
    cmake --build --preset {{ preset }} -j{{ jobs }}

[group('test')]
[doc('Full suite for the current preset (unit+sim+cli+integration).')]
test: build
    ctest --preset {{ preset }} --output-on-failure -j{{ jobs }} -- {{ args }}

# Fast pre-push gate: format, static analysis, and every suite except
# integration (integration is slow, spawns daemons, and parallel-flaky —
# run it separately via test-integration before pushing).
# Note: dependencies share this invocation's preset/jobs/args overrides.
[group('test')]
[doc('Fast pre-push gate: format, cppcheck, unit, sim, cli (no integration).')]
verify: check-format cppcheck test-unit test-sim test-cli
    @echo "VERIFY PASSED ({{ preset }}): format, cppcheck, unit, sim, cli"

[group('test')]
[doc('Run one label’s suite for the current preset (label = unit|sim|cli|integration).')]
test-suite label: build
    ctest --preset {{ preset }} -L {{ label }} --output-on-failure -- {{ args }}

[group('test')]
[doc('Unit tests for the current preset.')]
test-unit: (test-suite "unit")

[group('test')]
[doc('Sim tests for the current preset.')]
test-sim: (test-suite "sim")

[group('test')]
[doc('CLI tests for the current preset.')]
test-cli: (test-suite "cli")

[group('test')]
[doc('Integration tests (spawns real cinderd processes; kills stales first).')]
test-integration: build kill-stale
    ctest --preset {{ preset }} -L integration --output-on-failure -j{{ jobs }} -- {{ args }}

[group('test')]
[doc('Run every test binary directly (also runs --gtest_also_run_disabled_tests).')]
test-all: build kill-stale
    @just _run-bins "cinder_unit_store_tests cinder_unit_ring_tests cinder_unit_net_tests cinder_unit_node_tests cinder_sim_tests cinder_cli_tests cinder_integration_tests" {{ preset }}
    @echo ""
    @echo "════════════════════════════════════════"
    @echo "All test suites passed ({{ preset }})."

[group('test')]
[doc('Run unit + integration binaries against a gRPC-enabled build.')]
grpc-test: build kill-stale
    @just _run-bins "cinder_unit_store_tests cinder_unit_ring_tests cinder_unit_net_tests cinder_unit_node_tests cinder_integration_tests" debug-grpc
    @echo ""
    @echo "════════════════════════════════════════"
    @echo "gRPC test suites passed."

# Shared loop for running test binaries by name. Keeps test-all/grpc-test
# bin lists in one shape; when tests/CMakeLists.txt gains a target, add it
# to the caller lists above.
[private]
_run-bins bins dir:
    #!/usr/bin/env bash
    set -euo pipefail
    for bin in {{ bins }}; do
        echo ""
        echo "==> $bin..."
        echo "────────────────────────────────────────"
        build/{{ dir }}/tests/"$bin" --gtest_also_run_disabled_tests {{ args }} \
            || { echo ""; echo "FAILED: $bin ({{ dir }})"; exit 1; }
        echo "PASSED: $bin"
    done

[group('sanitizers')]
[doc('Full suite under ASan+UBSan (GCC).')]
asan-test:
    @just preset=asan test

[group('sanitizers')]
[doc('Full suite under TSan (GCC).')]
tsan-test:
    @just preset=tsan test

[group('sanitizers')]
[doc('Full suite under TSan (Clang).')]
tsan-clang-test:
    @just preset=tsan-clang test

[group('sanitizers')]
[doc('Full suite under UBSan only.')]
ubsan-test:
    @just preset=ubsan test

[group('sanitizers')]
[doc('Full suite under ASan+UBSan (Clang).')]
asan-clang-test:
    @just preset=asan-clang test

[group('run')]
[doc('Build and run cinderd for the current preset.')]
run: build
    ./build/{{ preset }}/bin/cinderd {{ args }}

[group('run')]
[doc('Build and run cinder-cli for the current preset.')]
run-cli: build
    ./build/{{ preset }}/bin/cinder-cli {{ args }}

[group('run')]
[doc('Build and run cinder-admin for the current preset.')]
run-admin: build
    ./build/{{ preset }}/bin/cinder-admin {{ args }}

[group('run')]
[doc('Kill leftover cinderd processes from integration runs.')]
kill-stale:
    pkill -x cinderd || echo "no stale cinderd processes"

[group('presets')]
[doc('Build with the debug-tls preset.')]
debug-tls:
    just preset=debug-tls build

[group('presets')]
[doc('Build with the fast (unity) preset.')]
fast:
    just preset=fast build

[group('presets')]
[doc('Run the full CI lint gate locally (inline clang-tidy + check-format + cppcheck).')]
ci: lint
    @echo "CI LINT PASSED: benchmark-enabled clang-tidy, check-format, cppcheck"

[group('presets')]
[doc('Configure every preset (reports failures at the end, exits nonzero).')]
configure-all:
    #!/usr/bin/env bash
    set -euo pipefail
    failed=""
    for p in {{ presets }}; do
        [ "$p" = "base" ] && continue   # hidden inheritance stub, not configurable
        echo "==> configuring $p..."
        if ! cmake --preset "$p"; then
            echo "FAILED: $p"
            failed="$failed $p"
        fi
    done
    if [ -n "$failed" ]; then
        echo ""
        echo "Presets that failed to configure:$failed"
        exit 1
    fi
    echo "All presets configured."

[private]
fuzz-build:
    cmake --preset fuzz
    cmake --build --preset fuzz -j{{ jobs }}

[private]
_fuzz-run duration label: fuzz-build
    #!/usr/bin/env bash
    set -euo pipefail
    # Seed the corpus (and the tracked dictionaries) if this checkout has never
    # generated them — on a fresh clone the git-ignored corpus dir is empty and
    # libFuzzer would otherwise run with no seeds.
    if [ ! -d tests/fuzz/corpus/protocol_decode ]; then
        echo "Generating fuzz corpus + dictionaries..."
        python3 tests/fuzz/generate_corpus.py
    fi
    for t in {{ fuzz_targets }}; do
        echo "==> $t ({{ label }})..."
        echo "────────────────────────────────────────"
        case "$t" in
            protocol_decode)  opts="{{ fuzz_opts_protocol_decode }}" ;;
            gossip_parse)     opts="{{ fuzz_opts_gossip_parse }}" ;;
            wal)              opts="{{ fuzz_opts_wal }}" ;;
            snapshot)         opts="{{ fuzz_opts_snapshot }}" ;;
            anti_entropy)     opts="{{ fuzz_opts_anti_entropy }}" ;;
            store_put)        opts="{{ fuzz_opts_store_put }}" ;;
            http_parse)       opts="{{ fuzz_opts_http_parse }}" ;;
            redirect_parse)   opts="{{ fuzz_opts_redirect_parse }}" ;;
            byte_reader)      opts="{{ fuzz_opts_byte_reader }}" ;;
            ttl_wheel)        opts="{{ fuzz_opts_ttl_wheel }}" ;;
            hint_queue)       opts="{{ fuzz_opts_hint_queue }}" ;;
            ring)             opts="{{ fuzz_opts_ring }}" ;;
            config)           opts="{{ fuzz_opts_config }}" ;;
            *)                opts="" ;;
        esac
        # Drop a -dict= flag whose file is absent: libFuzzer aborts the entire
        # run ("ParseDictionaryFile: file does not exist or is empty") rather
        # than skipping it, so a missing dictionary must not be fatal.
        dict_opt=$(printf '%s' "$opts" | tr ' ' '\n' | grep -m1 '^-dict=' || true)
        if [ -n "$dict_opt" ] && [ ! -f "${dict_opt#-dict=}" ]; then
            echo "note: ${dict_opt#-dict=} missing — running without a dictionary"
            opts=$(printf '%s' "$opts" | tr ' ' '\n' | grep -v '^-dict=' | tr '\n' ' ')
        fi
        mkdir -p tests/fuzz/corpus/"$t"
        build/fuzz/tests/"${t}"_fuzz \
            -max_total_time={{ duration }} -print_final_stats=1 \
            {{ fuzz_opts_default }} $opts \
            tests/fuzz/corpus/"$t" {{ args }} \
            || { echo "FAILED: $t"; exit 1; }
        echo "PASSED: $t"
    done
    echo "════════════════════════════════════════"
    echo "All fuzz targets passed ({{ label }})."

[group('fuzz')]
[doc('Run every fuzz target for 10s each.')]
fuzz-test: (_fuzz-run "10" "10s")

[group('fuzz')]
[doc('Run every fuzz target for 5 minutes each.')]
fuzz-long: (_fuzz-run "300" "5min")

# Regenerate seed corpora + the tracked libFuzzer dictionaries
# (protocol_decode/protocol.dict, gossip_parse/gossip.dict). The .dict files
# ARE committed even though the rest of tests/fuzz/corpus is git-ignored —
# re-run this after changing any wire format with a dictionary, and commit
# the updated dicts.
[group('fuzz')]
[doc('Regenerate fuzz seed corpora and the tracked libFuzzer dictionaries.')]
fuzz-dicts:
    python3 tests/fuzz/generate_corpus.py
    @echo ""
    @echo "Dictionaries (tracked, commit if changed):"
    @git status --short tests/fuzz/corpus --untracked-files=all | grep -E '\.dict$' || echo "  (unchanged)"

[private]
fuzz-coverage-build:
    cmake --preset fuzz-coverage
    cmake --build --preset fuzz-coverage -j{{ jobs }}

[group('fuzz')]
fuzz-coverage-collect target: fuzz-coverage-build
    python3 tests/fuzz/collect_coverage.py \
        --fuzzer build/fuzz-coverage/tests/{{ target }}_cov \
        --corpus tests/fuzz/corpus/{{ target }} \
        --output tests/fuzz/coverage/{{ target }}

[group('fuzz')]
fuzz-coverage-minimize target:
    python3 tests/fuzz/minimize_corpus.py \
        --fuzzer build/fuzz-coverage/tests/{{ target }}_cov \
        --corpus tests/fuzz/corpus/{{ target }} \
        --profdata tests/fuzz/coverage/{{ target }}/merged.profdata \
        --output tests/fuzz/corpus_minimal/{{ target }}

[group('fuzz')]
[doc('Distill a corpus with libFuzzer -merge (fast path: feature coverage, no coverage build). Merges the live corpus into corpus_minimal/<target>. Prefer this for routine distillation; keep fuzz-coverage-minimize for line-coverage reports.')]
fuzz-merge target: fuzz-build
    #!/usr/bin/env bash
    set -euo pipefail
    mkdir -p tests/fuzz/corpus_minimal/{{ target }}
    build/fuzz/tests/{{ target }}_fuzz -merge=1 \
        tests/fuzz/corpus_minimal/{{ target }} \
        tests/fuzz/corpus/{{ target }}

[group('fuzz')]
fuzz-coverage-report target: fuzz-coverage-build
    mkdir -p tests/fuzz/coverage/{{ target }}/html
    llvm-cov show build/fuzz-coverage/tests/{{ target }}_cov \
        --instr-profile=tests/fuzz/coverage/{{ target }}/merged.profdata \
        --ignore-filename-regex='_deps/' \
        --format=html \
        --output-dir=tests/fuzz/coverage/{{ target }}/html
    echo "Report: tests/fuzz/coverage/{{ target }}/html/index.html"

[group('fuzz')]
[doc('Collect coverage + minimize corpus for every fuzz target.')]
fuzz-coverage-all: fuzz-coverage-build
    #!/usr/bin/env bash
    set -euo pipefail
    for t in {{ fuzz_targets }}; do
        echo ""
        echo "==> $t coverage..."
        echo "────────────────────────────────────────"
        python3 tests/fuzz/collect_coverage.py \
            --fuzzer build/fuzz-coverage/tests/"$t"_cov \
            --corpus tests/fuzz/corpus/"$t" \
            --output tests/fuzz/coverage/"$t"
        python3 tests/fuzz/minimize_corpus.py \
            --fuzzer build/fuzz-coverage/tests/"$t"_cov \
            --corpus tests/fuzz/corpus/"$t" \
            --profdata tests/fuzz/coverage/"$t"/merged.profdata \
            --output tests/fuzz/corpus_minimal/"$t"
    done
    echo ""
    echo "════════════════════════════════════════"
    echo "Coverage minimization complete."

[group('lint')]
[doc('Reformat all project C++ sources (include, src, tools, tests, and benchmarks).')]
format: configure
    cmake --build build/{{ preset }} --target format -j{{ jobs }}

[group('lint')]
[doc('Fail if any project C++ source needs formatting, including benchmark sources.')]
check-format: configure
    cmake --build build/{{ preset }} --target check-format -j{{ jobs }}

[group('lint')]
[doc('Run cppcheck with the CI GCC compile database, including benchmarks.')]
cppcheck:
    cmake --preset ci-gcc-bench
    cmake --build build/ci-gcc-bench --target cppcheck -j{{ jobs }}

[group('lint')]
[doc('Strict lint: benchmark-enabled clang-tidy build, check-format, and benchmark-enabled cppcheck.')]
lint:
    #!/usr/bin/env bash
    set -euo pipefail
    cmake --preset ci-bench
    cmake --build --preset ci-bench -j{{ jobs }}
    cmake --build build/ci-bench --target check-format -j{{ jobs }}
    cmake --preset ci-gcc-bench
    cmake --build build/ci-gcc-bench --target cppcheck -j{{ jobs }}
    echo "CI LINT PASSED: benchmark-enabled clang-tidy, check-format, cppcheck"

# Repo-local pre-commit hook (.githooks/pre-commit): clang-format --dry-run on
# staged C++ files + YAML syntax on staged workflows. Catches the cheap half of
# the CI lint gate before push; the slow half (tidy build, cppcheck, tests)
# stays in `just verify`. Stored in core.hooksPath (absolute, per-clone local
# config) so every checkout needs one `just hooks-install`.
[group('lint')]
[doc('Install the pre-commit hook (staged whitespace/format/YAML/sync checks).')]
hooks-install:
    git config core.hooksPath "$(git rev-parse --show-toplevel)/.githooks"
    chmod +x .githooks/pre-commit
    @echo "pre-commit hook installed (core.hooksPath=.githooks)"

[group('lint')]
[doc('Remove the pre-commit hook.')]
hooks-uninstall:
    git config --unset core.hooksPath || echo "core.hooksPath was not set"
    @echo "pre-commit hook removed"

[group('lint')]
[doc('Run the pre-commit checks manually (same as the git hook).')]
hooks-run:
    ./.githooks/pre-commit

[group('proto')]
[doc('Lint the protobuf schema.')]
proto-lint:
    buf lint

[group('proto')]
[doc('Regenerate protobuf sources.')]
proto-generate:
    buf generate

[group('proto')]
[doc('Check protobuf schema against main for breaking changes.')]
proto-breaking:
    buf breaking --against '.git#branch=main'

# Benchmarks use a preset that already enables them (release by default), so no
# preset's CMake cache is ever mutated with -D. Override with
# `just bench_preset=profile bench`.
[group('misc')]
[doc('Run the throughput benchmark (bench_preset, default: release).')]
bench:
    cmake --preset {{ bench_preset }}
    cmake --build --preset {{ bench_preset }} -j{{ jobs }}
    ./build/{{ bench_preset }}/bin/cinder_throughput_bench {{ args }}

[group('misc')]
[doc('Profile the replication fan-out benchmarks with samply (needs perf_event_paranoid<=1 and a runtime that allows perf_event_open).')]
samply-repl:
    cmake --preset {{ bench_preset }}
    cmake --build --preset {{ bench_preset }} -j{{ jobs }} --target cinder_throughput_bench
    samply record ./build/{{ bench_preset }}/bin/cinder_throughput_bench --benchmark_filter='Quorum|ReplayBatch' --benchmark_min_time=0.5 {{ args }}

[group('misc')]
[doc('Install the current preset to the CMake prefix.')]
install: build
    cmake --install build/{{ preset }}

[group('misc')]
[doc('Delete the current preset’s build dir.')]
clean:
    rm -rf build/{{ preset }}

[group('misc')]
[doc('Delete all build dirs (keeps the shared dependency cache; see clean-deps).')]
clean-all:
    rm -rf build/

# The dependency source cache lives OUTSIDE the repo (default ~/.cache/cinder/deps,
# override with CINDER_DEPS_CACHE=<dir>) so it survives clean-all, is shared by
# every preset/compiler/worktree, and is keyed by <dep>-<pinned tag>.
# Configure populates missing entries automatically; these recipes are for
# priming it ahead of time (CI images, offline work) and for housekeeping.
[group('deps')]
[doc('Prime the shared dependency source cache (all pinned deps except gRPC).')]
deps:
    cmake -P cmake/FetchDeps.cmake

[group('deps')]
[doc('Prime the shared dependency source cache including gRPC (large).')]
deps-grpc:
    cmake -DCINDER_DEPS_WITH_GRPC=ON -P cmake/FetchDeps.cmake

[group('deps')]
[doc('Print the shared dependency source cache directory.')]
deps-dir:
    cmake -DCINDER_PRINT_DIR=ON -P cmake/FetchDeps.cmake

[group('deps')]
[doc('Delete the shared dependency source cache (next configure refetches).')]
clean-deps:
    #!/usr/bin/env bash
    set -euo pipefail
    dir="$(cmake -DCINDER_PRINT_DIR=ON -P cmake/FetchDeps.cmake)"
    [ -n "$dir" ] && [ "$dir" != "/" ] && rm -rf "$dir" && echo "removed $dir"

# Dep bumps (cmake/CinderDepVersions.cmake) self-heal on the next configure:
# cinder_use_cached_dep drops stale auto-pinned source dirs. This recipe is
# the manual override — it forgets every pinned source dir so the next
# configure re-derives them from the current tags (also drops a deliberate
# per-preset FETCHCONTENT_SOURCE_DIR_<DEP> pin; use an exported env var for
# a persistent override).
[group('deps')]
[doc('Forget pinned FetchContent source dirs; next configure re-derives them from current tags.')]
deps-refresh:
    #!/usr/bin/env bash
    set -euo pipefail
    for p in {{ presets }}; do
        [ "$p" = "base" ] && continue   # hidden inheritance stub, not configurable
        [ -f build/"$p"/CMakeCache.txt ] || continue   # never configured: nothing to forget
        echo "==> refreshing $p..."
        cmake --preset "$p" -U 'FETCHCONTENT_SOURCE_DIR_*' > /dev/null
    done
    echo "done — every configured preset re-derived its source dirs from the current tags"

[group('deps')]
[doc('Show compiler-cache (ccache) statistics.')]
ccache-stats:
    ccache -s

[group('deps')]
[doc('Reset compiler-cache (ccache) statistics before a timing run.')]
ccache-zero:
    ccache -z

[group('misc')]
[doc('Print the effective preset, build dir, jobs, and preset list.')]
info:
    echo "preset={{ preset }}"
    echo "build_dir=build/{{ preset }}"
    echo "jobs={{ jobs }}"
    echo "deps_cache=$(cmake -DCINDER_PRINT_DIR=ON -P cmake/FetchDeps.cmake)"
    echo "presets={{ presets }}"
