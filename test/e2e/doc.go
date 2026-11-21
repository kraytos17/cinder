// Package e2e launches real cinderd processes and drives chaos scenarios
// against them. Scenarios run only when CINDER_E2E=1 is set; otherwise they
// skip instantly so the default unit run stays process-free:
//
//	CINDER_E2E=1 go test ./test/e2e/ -count=1 -timeout 10m
//
// or via `just test-e2e`. Gating at runtime (rather than a build tag) keeps
// these files in every default build, so editors, `go vet ./...`, and lint
// always see them and bitrot is caught by the normal gates.
package e2e
