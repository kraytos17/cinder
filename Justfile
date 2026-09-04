set shell := ["bash", "-euo", "pipefail", "-c"]
set quiet

presets := `jq -r '.configurePresets[].name' CMakePresets.json 2>/dev/null | tr '\n' ' ' || echo "debug debug-tls debug-clang release release-clang sanitized asan asan-clang msan tsan ubsan ci ci-gcc fast fuzz fuzz-coverage"`
preset := "debug"
jobs := `nproc`
args := ""

fuzz_targets := "protocol_decode gossip_parse store_put snapshot wal anti_entropy http_parse"
fuzz_opts_protocol_decode := "-dict=tests/fuzz/corpus/protocol_decode/protocol.dict"
fuzz_opts_wal := "-max_len=4096 -rss_limit_mb=512"
fuzz_opts_snapshot := "-max_len=4096"
fuzz_opts_anti_entropy := "-max_len=8192"
fuzz_opts_store_put := "-max_len=1024"
fuzz_opts_default := "-timeout=5"

default:
    @just --list

[group('build')]
configure preset=preset:
    cmake --preset {{ preset }}

[group('build')]
build preset=preset:
    cmake --build --preset {{ preset }} -j{{ jobs }}

[group('test')]
test preset=preset: (build preset)
    ctest --preset {{ preset }} --output-on-failure -j{{ jobs }} -- {{ args }}

[group('test')]
test-suite label preset=preset: (build preset)
    ctest --preset {{ preset }} -L {{ label }} --output-on-failure -- {{ args }}

[group('test')]
test-unit preset=preset: (test-suite "unit" preset)

[group('test')]
test-sim preset=preset: (test-suite "sim" preset)

[group('test')]
test-cli preset=preset: (test-suite "cli" preset)

[group('test')]
test-integration preset=preset: (build preset) kill-stale
    ctest --preset {{ preset }} -L integration --output-on-failure -- {{ args }}

[group('test')]
test-all preset=preset: (build preset) kill-stale
    #!/usr/bin/env bash
    set -euo pipefail
    for bin in cinder_unit_tests cinder_sim_tests cinder_cli_tests cinder_integration_tests; do
        echo ""
        echo "==> $bin..."
        echo "────────────────────────────────────────"
        build/{{ preset }}/tests/"$bin" --gtest_also_run_disabled_tests {{ args }} \
            || { echo ""; echo "FAILED: $bin ({{ preset }})"; exit 1; }
        echo "PASSED: $bin"
    done
    echo ""
    echo "════════════════════════════════════════"
    echo "All test suites passed ({{ preset }})."

[group('sanitizers')]
asan-test: (test "asan")

[group('sanitizers')]
tsan-test: (test "tsan")

[group('sanitizers')]
ubsan-test: (test "ubsan")

[group('sanitizers')]
asan-clang-test: (test "asan-clang")

[group('run')]
run preset=preset: (build preset)
    ./build/{{ preset }}/bin/cinderd {{ args }}

[group('run')]
run-cli preset=preset: (build preset)
    ./build/{{ preset }}/bin/cinder-cli {{ args }}

[group('run')]
kill-stale:
    pkill -x cinderd || echo "no stale cinderd processes"

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
            wal)              opts="{{ fuzz_opts_wal }}" ;;
            snapshot)         opts="{{ fuzz_opts_snapshot }}" ;;
            anti_entropy)     opts="{{ fuzz_opts_anti_entropy }}" ;;
            store_put)        opts="{{ fuzz_opts_store_put }}" ;;
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
fuzz-test: (_fuzz-run "10" "10s")

[group('fuzz')]
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

[group('misc')]
bench preset=preset: (build preset)
    ./build/{{ preset }}/bin/cinder_throughput_bench {{ args }}

[group('misc')]
format preset=preset:
    cmake --build build/{{ preset }} --target format -j{{ jobs }}

[group('misc')]
check-format preset=preset:
    cmake --build build/{{ preset }} --target check-format -j{{ jobs }}

[group('misc')]
install preset=preset: (build preset)
    cmake --install build/{{ preset }}

[group('misc')]
clean preset=preset:
    rm -rf build/{{ preset }}

[group('misc')]
clean-all:
    rm -rf build/ .cache/

[group('misc')]
info:
    echo "preset={{ preset }}"
    echo "build_dir=build/{{ preset }}"
    echo "jobs={{ jobs }}"
    echo "presets={{ presets }}"
