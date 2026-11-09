# ══════════════════════════════════════════════════════════════════════════════
# Cinder — developer task runner (Go implementation)
# ══════════════════════════════════════════════════════════════════════════════
#
# Requires just >= 1.43. Recipes take trailing args verbatim:
#   just test -run TestRing ./internal/hashring/
#
# QUICK START
#   just            list every recipe
#   just build      build all packages
#   just test       full test suite
#   just verify     pre-push gate (fmt + vet + race tests)
#   just bench      benchmarks smoke test (-benchmem always on)

# List every recipe.
help:
    @just --list --unsorted

# Build all packages.
build:
    go build ./...

# Run the full test suite (unit + fuzz seed corpus).
[positional-arguments]
test *args:
    go test ./... "$@"

# vet + gofmt gate (mirrors the CI job).
vet:
    go vet ./... && test -z "$(gofmt -l .)"

# Pre-push gate: formatting, vet, and the race-detector suite.
verify: vet
    go test -race ./...

# Short fuzz of the protocol codec fixed-point properties.
[positional-arguments]
fuzz *args:
    go test -run=NONE -fuzz=FuzzDecodeRequest -fuzztime=20s ./internal/net/protocol/ "$@" && go test -run=NONE -fuzz=FuzzDecodeResponse -fuzztime=20s ./internal/net/protocol/ "$@"

# Benchmarks (quick smoke by default; pass -benchtime for publishable
# numbers, e.g. `just bench -benchtime=1s`). -benchmem always on:
# allocation pressure is the GC-tuning signal.
[positional-arguments]
bench *args:
    go test -run=NONE -bench=. -benchmem "$@"

# Regenerate the gRPC stubs from the shared proto (see internal/grpcgw/gen/doc.go).
proto-generate:
    protoc -I. \
        --plugin=protoc-gen-go=$(which protoc-gen-go) \
        --plugin=protoc-gen-grpc=$(which protoc-gen-grpc) \
        --go_out=internal/grpcgw/gen \
        '--go_opt=paths=source_relative,Mcinder/v1/cache.proto=github.com/kraytos17/cinder/internal/grpcgw/gen' \
        --go-grpc_out=internal/grpcgw/gen \
        '--go-grpc_opt=paths=source_relative,Mcinder/v1/cache.proto=github.com/kraytos17/cinder/internal/grpcgw/gen' \
        cinder/v1/cache.proto
    mv internal/grpcgw/gen/cinder/v1/*.pb.go internal/grpcgw/gen/
    rm -rf internal/grpcgw/gen/cinder

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
