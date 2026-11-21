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

// setAdminSecret installs the admin-role bearer. Data and read-only admin
// RPCs accept either bearer; the TCP layer enforces the admin role for
// mutating admin opcodes.
func (s *service) setAdminSecret(secret string) {
	s.adminSecret.Store(secret)
	if secret == "" {
		s.adminBearer.Store(nil)
		return
	}

	raw := auth.ClientToken(secret)
	s.adminBearer.Store(&bearerAuth{header: "Bearer " + hex.EncodeToString(raw), raw: raw})
}

// setPreviousSecrets installs the previous-rotation bearers. Data RPCs
// accept the data, admin, or either previous bearer; the TCP layer
// enforces the admin role for mutating admin opcodes.
func (s *service) setPreviousSecrets(clientPrev, adminPrev string) {
	s.clientSecretPrev.Store(clientPrev)
	if clientPrev == "" {
		s.clientPrevBearer.Store(nil)
	} else {
		raw := auth.ClientToken(clientPrev)
		s.clientPrevBearer.Store(&bearerAuth{header: "Bearer " + hex.EncodeToString(raw), raw: raw})
	}

	s.adminSecretPrev.Store(adminPrev)
	if adminPrev == "" {
		s.adminPrevBearer.Store(nil)
	} else {
		raw := auth.ClientToken(adminPrev)
		s.adminPrevBearer.Store(&bearerAuth{header: "Bearer " + hex.EncodeToString(raw), raw: raw})
	}
}

// checkAuth requires exactly one matching authorization value when any
// secret is configured, accepting the data or the admin bearer. Missing
// or invalid credentials return UNAUTHENTICATED. On success it returns
// the raw token for DispatchWithAuth; when auth is unconfigured it
// returns nil and no error.
func (s *service) checkAuth(ctx context.Context) ([]byte, error) {
	if tok, ok := s.matchAnyBearer(ctx); ok {
		return tok, nil
	}
	if s.bearer.Load() == nil && s.adminBearer.Load() == nil {
		return nil, nil
	}
	return nil, status.Error(codes.Unauthenticated, "invalid credentials")
}

// matchAnyBearer compares the inbound header against each configured
// bearer in constant time on equal lengths, returning the matching raw
// token. Missing metadata or a malformed value matches nothing.
func (s *service) matchAnyBearer(ctx context.Context) ([]byte, bool) {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return nil, false
	}

	vals := md.Get("authorization")
	if len(vals) != 1 || !strings.HasPrefix(vals[0], "Bearer ") {
		return nil, false
	}
	for _, b := range []*bearerAuth{s.bearer.Load(), s.adminBearer.Load(), s.clientPrevBearer.Load(), s.adminPrevBearer.Load()} {
		if b == nil || len(vals[0]) != len(b.header) {
			continue
		}
		if subtle.ConstantTimeCompare([]byte(vals[0]), []byte(b.header)) == 1 {
			return b.raw, true
		}
	}
	return nil, false
}
