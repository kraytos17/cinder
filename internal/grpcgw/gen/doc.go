// Package gen holds the buf-generated CinderCacheService stubs,
// compiled from the shared cinder/v1/cache.proto (wire law lives there;
// this package is a build artifact, never hand-edited).
//
// Regenerate from the repo root (Go toolchain only — buf and both plugins
// run via `go tool` from pinned go.mod directives):
//
//	just proto-generate
//
// (Runs `go tool buf lint` then `go tool buf generate`; the `module=`
// option in buf.gen.yaml emits flat files directly into gen/. Config
// lives in buf.yaml / buf.gen.yaml.)
// Verify with go build ./... && go test ./internal/grpcgw/.
package gen
