package grpcgw

// Bearer authentication: the precomputed "Bearer <hex>" credential,
// constant-time header comparison, and raw-token forwarding so the TCP
// server enforces its own policy on gateway calls exactly as for TCP
// clients.

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

// toProtoStatus mirrors the C++ toProtoStatus: protocol OK is the
// UNSPECIFIED zero value; every other status maps 1:1 by meaning.

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

// checkAuth mirrors the C++ gateway's checkAuth: with a client secret
// configured, every RPC must present "authorization: Bearer <hex>" where
// hex is the hex-encoded static client token; anything else is
// UNAUTHENTICATED. Open when unconfigured. On success it returns the
// decoded raw token for DispatchWithAuth (nil when open); the server then
// enforces its own policy exactly as for a TCP client carrying that
// credential.
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
