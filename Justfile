# ══════════════════════════════════════════════════════════════════════════════
# Cinder — developer task runner
# ══════════════════════════════════════════════════════════════════════════════
#
# Requires just >= 1.43. Recipes take trailing args verbatim:
#   just test -run TestRing ./internal/hashring/
#
# Toolchain policy: every tool is pinned, none is installed. Linters and
# scanners run via `go tool` from the go.mod tool directives
# (golangci-lint, govulncheck), so `just lint` works on a fresh checkout
# with only the Go toolchain present. Bump pins with:
#   go get -tool github.com/golangci/golangci-lint/v2/cmd/golangci-lint@latest
#   go get -tool golang.org/x/vuln/cmd/govulncheck@latest
#
# QUICK START
#   just            list every recipe
#   just build      build all packages
#   just test       full test suite
#   just verify     pre-push gate (fmt + vet + lint + race tests)
#   just bench      benchmark smoke test (-benchmem always on)

# List every recipe.
help:
    @just --list --unsorted

# Build all packages.
build:
    go build ./...

# Run the full test suite, including Go fuzz seed corpora and goleak checks.
[positional-arguments]
test *args:
    go test ./... "$@"

# Full test suite with the race detector (what CI enforces). A separate
# recipe because `test` appends args after ./... where -race is invalid.
test-race:
    go test -race ./...

# Run exactly one fuzz target (for parallel CI matrix legs).
[positional-arguments]
fuzz-one target package time="20s":
    go test -run=NONE -fuzz={{ target }} -fuzztime={{ time }} ./{{ package }}/

# vet + gofmt gate (mirrors the CI job). Fast; no network.
vet:
    go vet ./... && test -z "$(gofmt -l .)"

# Lint gate: the full .golangci.yml suite (formatters, staticcheck,
# errcheck-free style, modernize, misspell, gosec...).
lint:
    go tool golangci-lint run ./...

# Auto-fix what the linters can fix safely.
lint-fix:
    go tool golangci-lint run --fix ./... || true
    gofmt -w internal/ cmd/

# Pre-push gate: formatting, vet, lint, and race-enabled tests.
verify: vet lint
    go test -race ./...

# Apply `go fix` rewrites.
fix:
    go fix ./...

# Normalize go.mod/go.sum.
tidy:
    go mod tidy

# Vulnerability scan of the module graph (stdlib + deps).
vuln:
    go tool govulncheck ./...

# Coverage: profile the suite, print the func summary, keep cover.out.
cover:
    go test -coverprofile=cover.out ./... > /dev/null
    @go tool cover -func=cover.out | tail -5
    @echo "full report: go tool cover -html=cover.out"

# Coverage as an HTML heat map (opens a browser).
cover-html: cover
    go tool cover -html=cover.out

# Run all fuzz targets sequentially for local use; CI runs them concurrently
# via fuzz-one so each target keeps the same time budget with less wall time.
[positional-arguments]
fuzz *args:
    go test -run=NONE -fuzz=FuzzDecodeRequest -fuzztime=20s ./internal/net/protocol/ "$@" && go test -run=NONE -fuzz=FuzzDecodeResponse -fuzztime=20s ./internal/net/protocol/ "$@" && go test -run=NONE -fuzz=FuzzGossipView -fuzztime=20s ./internal/cluster/ "$@" && go test -run=NONE -fuzz=FuzzWALReader -fuzztime=20s ./internal/persist/ "$@" && go test -run=NONE -fuzz=FuzzSnapshot -fuzztime=20s ./internal/persist/ "$@" && go test -run=NONE -fuzz=FuzzAECodecs -fuzztime=20s ./internal/ae/ "$@" && go test -run=NONE -fuzz=FuzzStoreSequence -fuzztime=20s ./internal/store/ "$@" && go test -run=NONE -fuzz=FuzzCounterCodec -fuzztime=20s ./internal/counter/ "$@"

# Benchmarks (quick smoke by default; pass -benchtime for publishable
# numbers, e.g. `just bench -benchtime=1s`). -benchmem always on:
# allocation pressure is the GC-tuning signal.
[positional-arguments]
bench *args:
    go test -run=NONE -bench=. -benchmem "$@"

# CPU profile of the benchmarks, with a text top10. Profiles land in
# ./profiles/ (gitignored scratch); open the interactive explorer with
# `go tool pprof profiles/cpu.out`. See docs/profiling.md for the workflow
# (pin the CPU, compare with -base, GOGC/GOMEMLIMIT knobs).
[positional-arguments]
bench-cpu *args:
    @mkdir -p profiles
    go test -run=NONE -bench=. -benchmem -benchtime=5s -cpuprofile=profiles/cpu.out "$@"
    go tool pprof -top -nodecount=15 profiles/cpu.out

# Heap profiles of the benchmarks: alloc_space (rate, what -benchmem
# summarizes) and inuse_space (what is still live). Same ./profiles/ deal.
[positional-arguments]
bench-mem *args:
    @mkdir -p profiles
    go test -run=NONE -bench=. -benchmem -benchtime=5s -memprofile=profiles/mem.out "$@"
    @echo "--- alloc_space (rate) ---"
    go tool pprof -top -nodecount=15 -alloc_space profiles/mem.out
    @echo "--- inuse_space (live) ---"
    go tool pprof -top -nodecount=15 -inuse_space profiles/mem.out

# Execution trace of the benchmarks (goroutine/blocking view). Opens the
# trace viewer: `go tool trace profiles/trace.out` (needs a browser).
[positional-arguments]
bench-trace *args:
    @mkdir -p profiles
    go test -run=NONE -bench=. -benchtime=5s -trace=profiles/trace.out "$@"
    @echo "view with: go tool trace profiles/trace.out"

# Regenerate the gRPC stubs from the shared proto (see buf.gen.yaml).
# buf compiles the proto itself — no protoc binary anywhere. Plugins run
# through `go tool`, so the go.mod-pinned versions do the generation on a
# fresh checkout. The `module=` option emits flat files directly into
# gen/ (no cinder/v1 nesting, no mv step). `just proto-check` verifies
# generation matches the committed stubs.
proto-generate:
    go tool buf lint
    go tool buf generate

# Verify generated stubs match the checked-in flat output.
proto-check: proto-generate
    git diff --exit-code -- internal/grpcgw/gen/

# Run a local node (flags pass through to cinderd).
[positional-arguments]
run *args:
    go run ./cmd/cinderd "$@"

# Drop the Go build cache.
clean:
    go clean

# Install the repo-local pre-commit hook.
hooks-install:
    git config core.hooksPath {{ quote(justfile_directory() / '.githooks') }}
    chmod +x .githooks/pre-commit
    @echo "pre-commit hook installed (core.hooksPath=.githooks)"

# Remove the repo-local hook config.
hooks-uninstall:
    git config --unset core.hooksPath
    @echo "pre-commit hook uninstalled"

# Run the pre-commit checks manually.
hooks-run:
    ./.githooks/pre-commit
