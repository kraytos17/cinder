// Package gen holds the protoc-generated CinderCacheService stubs,
// compiled from the shared cinder/v1/cache.proto (wire law lives there;
// this package is a build artifact, never hand-edited).
//
// Regenerate from the repo root (protoc-gen-go and protoc-gen-go-grpc must
// be on PATH, e.g. ~/go/bin):
//
//	protoc -I. \
//	  --plugin=protoc-gen-go=$(which protoc-gen-go) \
//	  --plugin=protoc-gen-grpc=$(which protoc-gen-go-grpc) \
//	  --go_out=go/internal/grpcgw/gen \
//	  '--go_opt=paths=source_relative,Mcinder/v1/cache.proto=github.com/kraytos17/cinder/go/internal/grpcgw/gen' \
//	  --go-grpc_out=go/internal/grpcgw/gen \
//	  '--go-grpc_opt=paths=source_relative,Mcinder/v1/cache.proto=github.com/kraytos17/cinder/go/internal/grpcgw/gen' \
//	  cinder/v1/cache.proto
//	mv go/internal/grpcgw/gen/cinder/v1/*.pb.go go/internal/grpcgw/gen/
//	rm -rf go/internal/grpcgw/gen/cinder
//
// The mv flatten keeps the import path short; there is only one proto, so
// no collisions. Verify with go build ./... && go test ./internal/grpcgw/.
package gen
