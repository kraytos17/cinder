package grpcgw

import (
	"context"
	"crypto/subtle"
	"encoding/hex"
	"strings"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	"github.com/kraytos17/cinder/internal/auth"
)

// bearerAuth is one verified credential in both of its forms: the full
// "Bearer <hex>" header value compared (constant-time) against inbound
// metadata, and the decoded raw token forwarded to DispatchWithAuth so
// server-side policy applies uniformly to gateway calls.
type bearerAuth struct {
	header string
	raw    []byte
}

func (s *service) setClientSecret(secret string) {
	s.clientSecret.Store(secret)
	if secret == "" {
		s.bearer.Store(nil)
		return
	}

	raw := auth.ClientToken(secret)
	s.bearer.Store(&bearerAuth{header: "Bearer " + hex.EncodeToString(raw), raw: raw})
}

// checkAuth requires exactly one matching authorization value when a
// client secret is configured. Missing or invalid credentials return
// UNAUTHENTICATED. On success it returns the raw token for
// DispatchWithAuth; when auth is unconfigured it returns nil and no error.
func (s *service) checkAuth(ctx context.Context) ([]byte, error) {
	b := s.bearer.Load()
	if b == nil {
		return nil, nil
	}

	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return nil, status.Error(codes.Unauthenticated, "missing credentials")
	}

	vals := md.Get("authorization")
	// Length gate first: the constant-time compare below only ever runs
	// on equal-length inputs.
	if len(vals) != 1 || len(vals[0]) != len(b.header) || !strings.HasPrefix(vals[0], "Bearer ") {
		return nil, status.Error(codes.Unauthenticated, "invalid credentials")
	}
	if subtle.ConstantTimeCompare([]byte(vals[0]), []byte(b.header)) != 1 {
		return nil, status.Error(codes.Unauthenticated, "invalid credentials")
	}
	return b.raw, nil
}
