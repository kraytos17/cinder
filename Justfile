# ══════════════════════════════════════════════════════════════════════════════
# Cinder — developer task runner
# ══════════════════════════════════════════════════════════════════════════════
#
# This file is a thin, discoverable front-end over CMakePresets.json. It never
# passes -D flags: every build variant (compiler, sanitizer, LTO, PGO, unity,
# gRPC, TLS, …) is a CMake preset, so `just`, CI and IDEs always build the same
# thing in the same directory (build/<preset>; pgo-generate/pgo-use share
# build/pgo so their profile paths match).
#
# Requires just >= 1.43 (uses [default]); developed against 1.59.0.
#
# QUICK START
#   just                       list every recipe, grouped
#   just build                 configure + build the current preset
#   just test                  build + run the whole ctest suite
#   just verify                the fast pre-push gate
#   just doctor                check that your toolchain is complete
#   just presets               every preset, whether it can run tests, its build dir
#
# CHOOSING A PRESET (default: debug) — any of these work, last one wins:
#   just preset=asan test
#   just --set preset asan test
#   CINDER_PRESET=asan just test          (handy in .envrc / CI)
#
# PASSING ARGUMENTS — everything after the recipe name goes to the underlying
# tool verbatim, quoting preserved:
#   just test -R 'Ring|Store'             ctest regex filter
#   just test-unit --repeat until-fail:5
#   just build cinderd cinder-cli         only those targets
#   just run --config cinder.yaml         flags for cinderd
#   just bench --benchmark_filter=Quorum
#
# ENVIRONMENT (each is also a plain just variable: `just jobs=4 build`;
# or put them in a git-ignored `.env` file — see .env.example)
#   CINDER_PRESET              preset for build/test/run/format/clean     [debug]
#   CINDER_BENCH_PRESET        preset for bench/perf recipes             [release]
#   CINDER_JOBS                parallel build/test jobs                  [#cpus]
#   CINDER_BUILD_JOBS          CMake compile/link parallelism [= min(jobs,4)]
#   CINDER_TEST_JOBS           ctest unit/sim/cli parallelism [= jobs]
#   CINDER_INTEGRATION_JOBS    ctest integration parallelism [= test_jobs]
#   CINDER_REPL_FILTER         replication benchmark regex [Quorum|ReplayBatch]
#   CINDER_BENCH_REPETITIONS   reps for bench-repl             [11]
#   CINDER_BENCH_MIN_TIME      seconds per bench-repl repetition [0.05]
#   CINDER_SAMPLY_MIN_TIME     seconds per samply benchmark     [0.5]
#   CINDER_SAMPLY_OUTPUT       Samply profile output path [<bench-dir>/repl_profile.json.gz]
#   CINDER_PERF_REPETITIONS    perf stat repetitions            [3]
#   CINDER_PERF_MIN_TIME       seconds per perf benchmark       [2]
#   CINDER_PERF_CPU            CPU pinned by perf-stat-repl     [2]
#   CINDER_PGO_TRAIN_MIN_TIME  seconds per PGO training bench  [0.1]
#   CINDER_DEPS_CACHE          shared dependency source cache (read by CMake)
#
# Destructive recipes ask for confirmation; skip with `just --yes <recipe>`.

set shell := ['bash', '-euo', 'pipefail', '-c']
set script-interpreter := ['bash', '-euo', 'pipefail']

# Load ./.env if present (see .env.example). Lets `CINDER_*` overrides live in
# a file instead of the environment; a missing .env is fine. Plain `NAME=value`
# lines only — no `export` needed.
set dotenv-load

# ── Configuration ─────────────────────────────────────────────────────────────
preset := env('CINDER_PRESET', 'debug')
# Must be a preset with CINDER_BUILD_BENCHMARKS=ON: release, release-clang,
# release-unity, profile, pgo-generate, pgo-use (ci-bench / ci-gcc-bench too).
bench_preset := env('CINDER_BENCH_PRESET', 'release')
jobs := env('CINDER_JOBS', num_cpus())
# Unity/sanitizer debug translation units are memory-heavy; use a conservative
# default and allow explicit machine-specific overrides.
build_jobs := env('CINDER_BUILD_JOBS', '4')
test_jobs := env('CINDER_TEST_JOBS', jobs)
# All ctest labels use parallel workers by default. Override integration
# separately only when you need to tune socket/process pressure on one host.
integration_jobs := env('CINDER_INTEGRATION_JOBS', test_jobs)
# Resolve effective binaryDir (including inherited CMake preset settings) from
# CMakePresets.json; avoid maintaining a parallel build-dir/PGO special case.
build_dir := shell('python3 scripts/presets.py "$1" --build-dir', preset)
bench_dir := shell('python3 scripts/presets.py "$1" --build-dir', bench_preset)
# This justfile, for the few recipes that re-invoke it with another preset.
just_self := quote(just_executable()) + ' --justfile ' + quote(justfile())
repl_filter := env('CINDER_REPL_FILTER', 'Quorum|ReplayBatch')
bench_repetitions := env('CINDER_BENCH_REPETITIONS', '11')
bench_min_time := env('CINDER_BENCH_MIN_TIME', '0.05')
samply_min_time := env('CINDER_SAMPLY_MIN_TIME', '0.5')
samply_output := env('CINDER_SAMPLY_OUTPUT', bench_dir / 'repl_profile.json.gz')
perf_repetitions := env('CINDER_PERF_REPETITIONS', '3')
perf_min_time := env('CINDER_PERF_MIN_TIME', '2')
perf_cpu := env('CINDER_PERF_CPU', '2')
pgo_train_min_time := env('CINDER_PGO_TRAIN_MIN_TIME', '0.1')

alias b := build
alias t := test
alias r := run
alias ci := lint

# ── Help ──────────────────────────────────────────────────────────────────────

[default]
[private]
help:
    @echo "preset={{ preset }}   (change: just preset=asan <recipe>  |  CINDER_PRESET=asan just <recipe>)"
    @{{ just_self }} --list --unsorted

# ── Build ─────────────────────────────────────────────────────────────────────

# Configure the build tree for the current preset.
[group('build')]
configure: (_configure preset)

# Configure + build the current preset; name targets to build only those.
[group('build')]
build *targets: (_build preset targets)

# Build the debug-tls preset (Debug + TLS/SSL, GCC).
[group('build')]
[positional-arguments]
debug-tls *targets:
    {{ just_self }} --set preset debug-tls build "$@"

# Build the fast preset (Debug + unity build + fast linker; edit-compile-test loop).
[group('build')]
[positional-arguments]
fast *targets:
    {{ just_self }} --set preset fast build "$@"

# ── Test ──────────────────────────────────────────────────────────────────────
# `ctest --preset` applies the test preset's environment (ASAN_OPTIONS,
# TSAN_OPTIONS, suppressions, …) and output-on-failure. Presets without a test
# preset (analyze, iwyu, pgo-*, ci-bench, ci-gcc-bench, fuzz-coverage) are
# build/lint-only and get a clear error instead of an obscure ctest failure.

# Full suite (every ctest label) for the current preset.
[group('test')]
[positional-arguments]
test *args: (_need-preset 'test' preset) (_build preset)
    ctest --preset {{ quote(preset) }} --parallel {{ test_jobs }} --no-tests=error "$@"

# Integration is excluded: it spawns daemons and is slow, so run `just
# test-integration` separately before pushing. Dependencies share this
# invocation's preset/jobs overrides.

# Fast pre-push gate: format, cppcheck, workflow script tests, unit, sim,
# cli (no integration).
[group('test')]
verify: check-format cppcheck test-scripts test-unit test-sim test-cli
    @echo "VERIFY PASSED ({{ preset }}): format, cppcheck, scripts, unit, sim, cli"

# Workflow script unit tests (stdlib unittest, no build needed).
[group('test')]
test-scripts:
    python3 scripts/tests/test_workflows.py

# Run one ctest label: unit | sim | cli | integration.
[arg('label', pattern='unit|sim|cli|integration')]
[group('test')]
[positional-arguments]
test-suite label *args: (_need-preset 'test' preset) (_build preset)
    : {{ quote(args) }}
    ctest --preset {{ quote(preset) }} --parallel {{ test_jobs }} --no-tests=error -L "$1" "${@:2}"

# Unit tests.
[group('test')]
[positional-arguments]
test-unit *args: (_need-preset 'test' preset) (_build preset)
    ctest --preset {{ quote(preset) }} --parallel {{ test_jobs }} --no-tests=error -L unit "$@"

# Simulation tests.
[group('test')]
[positional-arguments]
test-sim *args: (_need-preset 'test' preset) (_build preset)
    ctest --preset {{ quote(preset) }} --parallel {{ test_jobs }} --no-tests=error -L sim "$@"

# CLI tests.
[group('test')]
[positional-arguments]
test-cli *args: (_need-preset 'test' preset) (_build preset)
    ctest --preset {{ quote(preset) }} --parallel {{ test_jobs }} --no-tests=error -L cli "$@"

# Integration tests (spawn real cinderd processes; kills strays first).
[group('test')]
[positional-arguments]
test-integration *args: (_need-preset 'test' preset) (_build preset) kill-stale
    ctest --preset {{ quote(preset) }} --parallel {{ integration_jobs }} --no-tests=error -L integration "$@"

# Parallel test set for CI presets, with integration kept serial unless the
# caller explicitly opts in to safe parallelism on its machine.
[group('test')]
test-ci *args: (_need-preset 'test' preset) (_build preset) kill-stale
    : {{ quote(args) }}
    ctest --preset {{ quote(preset) }} --parallel {{ test_jobs }} --no-tests=error -L "unit|sim|cli" "$@"
    ctest --preset {{ quote(preset) }} --parallel {{ integration_jobs }} --no-tests=error -L integration "$@"

# Run every test binary directly, including DISABLED_ tests (current preset).
[group('test')]
[positional-arguments]
test-all *args: (_build preset) kill-stale
    python3 scripts/run_gtests.py {{ quote(build_dir) }} {{ quote(preset) }} all "$@"

# Unit + integration binaries against the debug-grpc preset (builds it first).
[group('test')]
[positional-arguments]
grpc-test *args: (_build 'debug-grpc') kill-stale
    python3 scripts/run_gtests.py build/debug-grpc debug-grpc grpc "$@"

# ── Sanitizers ────────────────────────────────────────────────────────────────
# Each shortcut is `just --set preset <p> test`, so `-R …` etc. work as usual.
# Any other preset works the same way: `just preset=msan test`. (MSan needs an
# MSan-instrumented C++ standard library; see its preset description.)

# Full suite under ASan+UBSan (GCC).
[group('sanitizers')]
[positional-arguments]
asan-test *args:
    {{ just_self }} --set preset asan test "$@"

# Full suite under ASan+UBSan (Clang; adds use-after-scope/return + ignorelist).
[group('sanitizers')]
[positional-arguments]
asan-clang-test *args:
    {{ just_self }} --set preset asan-clang test "$@"

# Full suite under TSan (GCC).
[group('sanitizers')]
[positional-arguments]
tsan-test *args:
    {{ just_self }} --set preset tsan test "$@"

# Full suite under TSan (Clang).
[group('sanitizers')]
[positional-arguments]
tsan-clang-test *args:
    {{ just_self }} --set preset tsan-clang test "$@"

# Full suite under UBSan only (GCC).
[group('sanitizers')]
[positional-arguments]
ubsan-test *args:
    {{ just_self }} --set preset ubsan test "$@"

# ── Run ───────────────────────────────────────────────────────────────────────

# Build and run cinderd (the daemon); arguments go to cinderd.
[group('run')]
[positional-arguments]
run *args: (_build preset 'cinderd')
    ./{{ build_dir }}/bin/cinderd "$@"

# Build and run cinder-cli; arguments go to cinder-cli.
[group('run')]
[positional-arguments]
run-cli *args: (_build preset 'cinder-cli')
    ./{{ build_dir }}/bin/cinder-cli "$@"

# Build and run cinder-admin (full build: its target name lives in tools/CMakeLists.txt).
[group('run')]
[positional-arguments]
run-admin *args: (_build preset)
    ./{{ build_dir }}/bin/cinder-admin "$@"

# Kill leftover cinderd processes (any cinderd on this machine) from integration runs.
[group('run')]
kill-stale:
    pkill -x cinderd || echo "no stale cinderd processes"

# ── Lint & format ─────────────────────────────────────────────────────────────

# Reformat all project C++ sources (clang-format, via the CMake `format` target).
[group('lint')]
format: (_need 'clang-format') (_build preset 'format')

# Fail if any project C++ source needs formatting (includes benchmark sources).
[group('lint')]
check-format: (_need 'clang-format') (_build preset 'check-format')

# Run cppcheck on the ci-gcc-bench compile database (includes benchmarks).
[group('lint')]
cppcheck: (_need 'cppcheck') (_build 'ci-gcc-bench' 'cppcheck')

# Strict CI lint, locally (also `just ci`): clang-tidy build + check-format + cppcheck.
[group('lint')]
lint: (_need 'clang-tidy' 'clang-format' 'cppcheck') (_build 'ci-bench') (_build 'ci-bench' 'check-format') cppcheck
    @echo "CI LINT PASSED: benchmark-enabled clang-tidy, check-format, cppcheck"

# Install the pre-commit hook (staged-file format/whitespace/YAML checks). CMake
# also does this on first configure unless -DCINDER_AUTO_HOOKS=OFF.

# Install the pre-commit hook (core.hooksPath=.githooks).
[group('lint')]
hooks-install:
    git config core.hooksPath {{ quote(justfile_directory() / '.githooks') }}
    chmod +x .githooks/pre-commit
    @echo "pre-commit hook installed (core.hooksPath=.githooks)"

# NB: CMake re-installs the hook on the next configure whenever core.hooksPath is
# unset (see the end of CMakeLists.txt), so pass -DCINDER_AUTO_HOOKS=OFF to
# opt out for good.

# Remove the pre-commit hook (core.hooksPath).
[group('lint')]
hooks-uninstall:
    @if git config --unset core.hooksPath; then echo "pre-commit hook removed (a later configure will re-install it unless -DCINDER_AUTO_HOOKS=OFF)"; else echo "core.hooksPath was not set"; fi

# Run the pre-commit checks by hand (same as the git hook).
[group('lint')]
hooks-run:
    ./.githooks/pre-commit

# ── Fuzzing (Clang + libFuzzer; presets: fuzz, fuzz-coverage) ─────────────────

# Smoke-fuzz every target for 10 s each; extra args go to libFuzzer.
[group('fuzz')]
[positional-arguments]
fuzz-test *args: (_build 'fuzz')
    python3 scripts/fuzz.py run 10 all "$@"

# Fuzz every target for 5 minutes each; extra args go to libFuzzer.
[group('fuzz')]
[positional-arguments]
fuzz-long *args: (_build 'fuzz')
    python3 scripts/fuzz.py run 300 all "$@"

# Fuzz ONE target: `just fuzz-one ring`, `just fuzz-one wal 120 -runs=100000`.
[arg('seconds', pattern='[0-9]+')]
[arg('target', pattern='[a-z0-9_]+')]
[group('fuzz')]
[positional-arguments]
fuzz-one target seconds='60' *args: (_build 'fuzz')
    : {{ quote(args) }}
    python3 scripts/fuzz.py run {{ quote(seconds) }} {{ quote(target) }} "${@:3}"

# Regenerate fuzz seed corpora and the tracked libFuzzer dictionaries.
[group('fuzz')]
fuzz-dicts:
    python3 tests/fuzz/generate_corpus.py
    @echo ""
    @echo "Dictionaries (tracked, commit if changed):"
    @git status --short tests/fuzz/corpus --untracked-files=all | grep -E '\.dict$' || echo "  (unchanged)"

# Fast path: feature coverage, no coverage build. Prefer it for routine
# distillation; keep fuzz-coverage-minimize for line-coverage reports.

# Distill a corpus with libFuzzer -merge into corpus_minimal/<target>.
[arg('target', pattern='[a-z0-9_]+')]
[group('fuzz')]
fuzz-merge target: (_build 'fuzz')
    python3 scripts/fuzz.py merge {{ quote(target) }}

# Collect line coverage for one target's corpus (fuzz-coverage preset).
[arg('target', pattern='[a-z0-9_]+')]
[group('fuzz')]
fuzz-coverage-collect target: (_build 'fuzz-coverage')
    python3 scripts/fuzz.py coverage-collect {{ quote(target) }}

# Minimise one target's corpus by line coverage (run fuzz-coverage-collect first).
[arg('target', pattern='[a-z0-9_]+')]
[group('fuzz')]
fuzz-coverage-minimize target: (_build 'fuzz-coverage')
    python3 scripts/fuzz.py coverage-minimize {{ quote(target) }}

# Render an HTML coverage report for one target (needs merged.profdata).
[arg('target', pattern='[a-z0-9_]+')]
[group('fuzz')]
fuzz-coverage-report target: (_need 'llvm-cov') (_build 'fuzz-coverage')
    python3 scripts/fuzz.py coverage-report {{ quote(target) }}

# Collect coverage + minimise the corpus for every fuzz target.
[group('fuzz')]
[script]
fuzz-coverage-all: (_build 'fuzz-coverage')
    python3 scripts/fuzz.py coverage-all

# ── Performance ───────────────────────────────────────────────────────────────
# Workflow from docs/profiling.md. Benchmarks use a preset that already enables
# them (bench_preset), so no preset's CMake cache is ever mutated with -D:
#   just bench_preset=profile bench        # RelWithDebInfo + frame pointers

# Run the throughput benchmark (bench_preset, default: release).
[group('perf')]
[positional-arguments]
bench *args: (_build bench_preset 'cinder_throughput_bench')
    ./{{ bench_dir }}/bin/cinder_throughput_bench "$@"

# Replication fan-out benches with reference flags (11 reps; add --benchmark_format=csv via args).
[group('perf')]
[positional-arguments]
bench-repl *args: (_build bench_preset 'cinder_throughput_bench')
    ./{{ bench_dir }}/bin/cinder_throughput_bench --benchmark_filter={{ quote(repl_filter) }} --benchmark_repetitions={{ bench_repetitions }} --benchmark_min_time={{ bench_min_time }} "$@"

# Needs perf_event_paranoid<=1 and a runtime that allows perf_event_open; use
# bench_preset=profile for symbols.

# Profile the replication benches with samply -> <bench build dir>/repl_profile.json.gz.
[group('perf')]
[positional-arguments]
samply-repl *args: (_need 'samply') (_build bench_preset 'cinder_throughput_bench')
    samply record --save-only -o {{ quote(samply_output) }} -- ./{{ bench_dir }}/bin/cinder_throughput_bench --benchmark_filter={{ quote(repl_filter) }} --benchmark_min_time={{ samply_min_time }} "$@"

# Hardware counters (cycles, IPC, branches, cache) around the replication benches.
[group('perf')]
[linux]
[positional-arguments]
perf-stat-repl *args: (_need 'perf' 'taskset') (_build bench_preset 'cinder_throughput_bench')
    perf stat -r {{ perf_repetitions }} -d -- taskset -c {{ perf_cpu }} ./{{ bench_dir }}/bin/cinder_throughput_bench --benchmark_filter={{ quote(repl_filter) }} --benchmark_min_time={{ perf_min_time }} "$@"

# PGO, both steps: instrumented build + training run, then the optimised rebuild.
[group('perf')]
pgo: pgo-train pgo-use

# PGO step 1: wipe build/pgo + build/pgo-data, build pgo-generate, train on the full bench.
[group('perf')]
[positional-arguments]
pgo-train *args: _pgo-clean (_build 'pgo-generate' 'cinder_throughput_bench')
    ./build/pgo/bin/cinder_throughput_bench --benchmark_min_time={{ pgo_train_min_time }} "$@"
    @echo "PGO profiles in build/pgo-data — now run: just pgo-use"

# PGO step 2: rebuild consuming build/pgo-data (same build dir), then measure the bench.
[group('perf')]
pgo-use: _pgo-require-data (_build 'pgo-use' 'cinder_throughput_bench')
    @echo "PGO-optimized bench at build/pgo/bin/cinder_throughput_bench"

# ── Protobuf (buf) ────────────────────────────────────────────────────────────

# Lint the protobuf schema.
[group('proto')]
proto-lint: (_need 'buf')
    buf lint

# Regenerate protobuf sources.
[group('proto')]
proto-generate: (_need 'buf')
    buf generate

# Check the protobuf schema against main for breaking changes.
[group('proto')]
proto-breaking: (_need 'buf')
    buf breaking --against '.git#branch=main'

# ── Dependencies & caches ─────────────────────────────────────────────────────
# Dependency sources live OUTSIDE the repo (default ~/.cache/cinder/deps, or
# $CINDER_DEPS_CACHE), keyed by <dep>-<pinned tag>: they survive `clean-all` and
# are shared by every preset, compiler and git worktree. Configure fills missing
# entries automatically; these recipes prime it ahead of time (CI images,
# offline work) and do housekeeping. Pins: cmake/CinderDepVersions.cmake.

# Prime the shared dependency source cache (all pinned deps except gRPC).
[group('deps')]
deps:
    cmake -P cmake/FetchDeps.cmake

# Prime the shared dependency source cache including gRPC (large).
[group('deps')]
deps-grpc:
    cmake -DCINDER_DEPS_WITH_GRPC=ON -P cmake/FetchDeps.cmake

# Print the shared dependency source cache directory.
[group('deps')]
deps-dir:
    @cmake -DCINDER_PRINT_DIR=ON -P cmake/FetchDeps.cmake

# Delete the shared dependency source cache (the next configure refetches).
[confirm('Delete the shared dependency source cache?')]
[group('deps')]
[script]
clean-deps:
    python3 scripts/deps.py clean

# Dep bumps already self-heal (cinder_use_cached_dep drops stale auto-pinned
# dirs); this is the manual override. It also drops a deliberate per-build-dir
# FETCHCONTENT_SOURCE_DIR_<DEP> pin: use an exported env var for a persistent one.

# Forget pinned FetchContent source dirs in every configured build dir.
[group('deps')]
[script]
deps-refresh:
    python3 scripts/deps.py refresh

# Show compiler-cache (ccache) statistics.
[group('deps')]
ccache-stats: (_need 'ccache')
    ccache -s

# Reset compiler-cache (ccache) statistics before a timing run.
[group('deps')]
ccache-zero: (_need 'ccache')
    ccache -z

# ── Project housekeeping & diagnostics ────────────────────────────────────────

# List every configure preset: tests available?, build dir, description. `*` = current.
[group('project')]
[script]
presets:
    python3 scripts/presets.py {{ quote(preset) }}

# Print the effective settings.
[group('project')]
[script]
info:
    echo "preset           = {{ preset }}"
    echo "build_dir        = {{ build_dir }}"
    echo "bench_preset     = {{ bench_preset }}"
    echo "bench_dir        = {{ bench_dir }}"
    echo "jobs             = {{ jobs }}"
    echo "integration_jobs = {{ integration_jobs }}"
    echo "just             = $({{ just_self }} --version)"
    echo "cmake            = $(cmake --version | sed -n 1p)"
    echo "deps_cache       = $(cmake -DCINDER_PRINT_DIR=ON -P cmake/FetchDeps.cmake 2>/dev/null || echo '(unavailable)')"

# Check the toolchain: required tools, CMake >= 4.4, toolchain files, optional tools.
[group('project')]
[script]
doctor:
    python3 scripts/doctor.py

# Install the current preset into the CMake prefix, or `just install /opt/cinder`.
[group('project')]
install prefix='': (_build preset)
    cmake --install {{ quote(build_dir) }}{{ if prefix == '' { '' } else { ' --prefix ' + quote(prefix) } }}

# Delete the current preset's build dir. (pgo-* share build/pgo; pgo-data is kept.)
[group('project')]
clean: (_need-preset 'configure' preset)
    rm -rf -- {{ quote(build_dir) }}
    @echo "removed {{ build_dir }}"

# Delete ALL build dirs (keeps the shared dependency cache; see clean-deps).
[confirm('Delete every build directory under build/ ?')]
[group('project')]
clean-all:
    rm -rf -- build/

# ── Private helpers (names start with _ so `just --list` hides them) ──────────

# Configure `p`. Always re-run: it is cheap and picks up CMakePresets.json edits,
# which CMake's own regeneration does not track.
_configure p: (_need-preset 'configure' p)
    cmake --preset {{ quote(p) }}

# Configure + build `p`, optionally only the given targets.
_build p *targets: (_configure p)
    cmake --build --preset {{ quote(p) }} --parallel {{ build_jobs }}{{ if targets == '' { '' } else { ' --target ' + targets } }}

# Fail with a helpful list unless `name` is a <kind> preset (configure | test).
[positional-arguments]
[script]
_need-preset kind name:
    python3 scripts/check_preset.py "$1" "$2"

# Fail unless every named tool is on PATH.
[positional-arguments]
[script]
_need *tools:
    python3 scripts/check_tools.py "$@"

# PGO profiles are keyed by build path; pgo-train deliberately starts fresh.
_pgo-clean:
    python3 scripts/pgo.py clean

# Refuse to "optimise" without profile data (-Wno-missing-profile would hide it).
_pgo-require-data:
    python3 scripts/pgo.py check
