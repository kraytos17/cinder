set shell := ["bash", "-euo", "pipefail", "-c"]
set quiet := true

presets := `jq -r '.configurePresets[].name' CMakePresets.json 2>/dev/null | tr '\n' ' ' || echo "debug debug-tls debug-tls-clang debug-clang release release-clang asan asan-clang msan tsan tsan-clang ubsan ci ci-gcc tls-grpc tls-grpc-clang fast release-unity profile analyze iwyu fuzz fuzz-coverage debug-grpc"`

preset := "debug"
jobs := `nproc`
args := ""

fuzz_targets := "protocol_decode gossip_parse store_put snapshot wal anti_entropy http_parse redirect_parse"
fuzz_opts_protocol_decode := "-max_len=8192 -dict=tests/fuzz/corpus/protocol_decode/protocol.dict"
fuzz_opts_gossip_parse := "-max_len=1024 -dict=tests/fuzz/corpus/gossip_parse/gossip.dict"
fuzz_opts_wal := "-max_len=4096 -rss_limit_mb=512"
fuzz_opts_snapshot := "-max_len=4096"
fuzz_opts_anti_entropy := "-max_len=8192"
fuzz_opts_store_put := "-max_len=1024"
fuzz_opts_http_parse := "-max_len=2048"
fuzz_opts_redirect_parse := "-max_len=256"
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
ci:
    cmake --preset ci
    cmake --build --preset ci -j{{ jobs }}
    @just preset=ci check-format
    cmake --preset ci-gcc
    cmake --build build/ci-gcc --target cppcheck -j{{ jobs }}
    @echo "CI LINT PASSED: ci tidy build, check-format, cppcheck"

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
            *)                opts="" ;;
        esac
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
[doc('Reformat all source files in place.')]
format:
    cmake --build build/{{ preset }} --target format -j{{ jobs }}

[group('lint')]
[doc('Fail if any source file needs reformatting (CI lint gate uses this).')]
check-format:
    cmake --build build/{{ preset }} --target check-format -j{{ jobs }}

[group('lint')]
[doc('Run cppcheck on the current preset.')]
cppcheck: build
    cmake --build build/{{ preset }} --target cppcheck -j{{ jobs }}

[group('lint')]
[doc('check-format + cppcheck for the current preset.')]
lint: build
    cmake --build build/{{ preset }} --target lint -j{{ jobs }}

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

# Benchmarks are OFF by default, so this recipe turns them on for the
# configured preset regardless of which preset is selected. Requires the
# preset to be configured already (see `just preset=<name> configure`).
[group('misc')]
[doc('Run the throughput benchmark (enables benchmarks for the preset)')]
bench:
    cmake -S . -B build/{{ preset }} -DCINDER_BUILD_BENCHMARKS=ON
    cmake --build --preset {{ preset }} -j{{ jobs }}
    ./build/{{ preset }}/bin/cinder_throughput_bench {{ args }}

[group('misc')]
[doc('Install the current preset to the CMake prefix.')]
install: build
    cmake --install build/{{ preset }}

[group('misc')]
[doc('Delete the current preset’s build dir.')]
clean:
    rm -rf build/{{ preset }}

[group('misc')]
[doc('Delete all build dirs and the FetchContent cache.')]
clean-all:
    rm -rf build/ .cache/

[group('misc')]
[doc('Print the effective preset, build dir, jobs, and preset list.')]
info:
    echo "preset={{ preset }}"
    echo "build_dir=build/{{ preset }}"
    echo "jobs={{ jobs }}"
    echo "presets={{ presets }}"
