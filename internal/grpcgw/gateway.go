// Package grpcgw exposes the cache over gRPC (Go port of the C++ gateway).
//
// It mirrors CinderCacheServiceImpl RPC-for-RPC: OK maps to
// STATUS_CODE_UNSPECIFIED (the proto uses 0 for success), Get/Set/Delete go
// through the same TCP dispatch (ownership, replication), as do counter
// adjust (owner-routed) and counter get (local-hit, redirect on miss).
// GetVersioned reads the local store directly (tombstones and misses both
// report NOT_FOUND, expiry always 0), MultiSet owner-checks then puts
// locally without replication fan-out, and the admin RPCs return the same
// JSON as the admin opcodes.
//
// Deliberate simplification: no trace-metadata propagation (no Go tracing
// infra); every RPC returns gRPC OK with the outcome in the status field,
// exactly like the C++ handlers on the success path. CounterMerge has no
// RPC: it is inter-node replication traffic, like Replicate/Hint. Client auth is
// enforced when configured (SetClientSecret): the C++
// "authorization: Bearer <hex>" contract, UNAUTHENTICATED otherwise.
// A secret refuses plaintext: pair it with SetTLS (same PEM chain + key
// as the TCP server), mirroring the C++ gateway.
package grpcgw

import (
	"context"
	"crypto/tls"
	"errors"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"

	"github.com/kraytos17/cinder/internal/counter"
	gen "github.com/kraytos17/cinder/internal/grpcgw/gen"
	"github.com/kraytos17/cinder/internal/net/protocol"
	"github.com/kraytos17/cinder/internal/net/tcp"
)

// toProtoStatus mirrors the C++ toProtoStatus: protocol OK is the
// UNSPECIFIED zero value; every other status maps 1:1 by meaning.
func toProtoStatus(s protocol.Status) gen.StatusCode {
	switch s {
	case protocol.StatusOK:
		return gen.StatusCode_STATUS_CODE_UNSPECIFIED
	case protocol.StatusNotFound:
		return gen.StatusCode_STATUS_CODE_NOT_FOUND
	case protocol.StatusCapacityExceeded:
		return gen.StatusCode_STATUS_CODE_CAPACITY_EXCEEDED
	case protocol.StatusInvalidArgument:
		return gen.StatusCode_STATUS_CODE_INVALID_ARGUMENT
	case protocol.StatusTTLExpired:
		return gen.StatusCode_STATUS_CODE_TTL_EXPIRED
	case protocol.StatusNotSupported:
		return gen.StatusCode_STATUS_CODE_NOT_SUPPORTED
	case protocol.StatusInternalError:
		return gen.StatusCode_STATUS_CODE_INTERNAL_ERROR
	case protocol.StatusTimeout:
		return gen.StatusCode_STATUS_CODE_TIMEOUT
	case protocol.StatusNotReady:
		return gen.StatusCode_STATUS_CODE_NOT_READY
	case protocol.StatusPermissionDenied:
		return gen.StatusCode_STATUS_CODE_PERMISSION_DENIED
	case protocol.StatusCorruptData:
		return gen.StatusCode_STATUS_CODE_CORRUPT_DATA
	default:
		return gen.StatusCode_STATUS_CODE_INTERNAL_ERROR
	}
}

// service implements CinderCacheService over one TCP server node.
type service struct {
	gen.UnimplementedCinderCacheServiceServer

	node         *tcp.Server
	clientSecret atomic.Value // string; set via SetClientSecret before Serve
	// bearer is the precomputed bearer credential, refreshed with the
	// secret. The header comparison and the raw token forwarded to
	// DispatchWithAuth both come from here, so the per-RPC HMAC + hex
	// encoding happens once per secret, not once per request.
	bearer atomic.Pointer[bearerAuth]
}

func (s *service) Get(ctx context.Context, req *gen.GetRequest) (*gen.GetResponse, error) {
	tok, err := s.checkAuth(ctx)
	if err != nil {
		return &gen.GetResponse{}, err
	}
	res := s.node.DispatchWithAuth(protocol.Request{Opcode: protocol.OpGet, Key: []byte(req.GetKey())}, tok)
	out := &gen.GetResponse{Status: toProtoStatus(res.Status)}
	if res.HasValue {
		out.Value = new(string(res.Value))
	}
	return out, nil
}

func (s *service) Set(ctx context.Context, req *gen.SetRequest) (*gen.SetResponse, error) {
	tok, err := s.checkAuth(ctx)
	if err != nil {
		return &gen.SetResponse{}, err
	}
	r := protocol.Request{Opcode: protocol.OpSet, Key: []byte(req.GetKey()), Value: []byte(req.GetValue())}
	if req.GetTtlMs() > 0 {
		r.TTL = time.Duration(req.GetTtlMs()) * time.Millisecond
	}
	res := s.node.DispatchWithAuth(r, tok)
	return &gen.SetResponse{Status: toProtoStatus(res.Status)}, nil
}

func (s *service) Delete(ctx context.Context, req *gen.DeleteRequest) (*gen.DeleteResponse, error) {
	_, err := s.checkAuth(ctx)
	if err != nil {
		return &gen.DeleteResponse{}, err
	}
	res := s.node.DeleteNoRedirect(req.GetKey())
	return &gen.DeleteResponse{Status: toProtoStatus(res.Status)}, nil
}

func (s *service) GetVersioned(ctx context.Context, req *gen.GetVersionedRequest) (*gen.GetVersionedResponse, error) {
	_, err := s.checkAuth(ctx)
	if err != nil {
		return &gen.GetVersionedResponse{}, err
	}
	out := &gen.GetVersionedResponse{}
	e, ok := s.node.Store().GetVersioned(req.GetKey())
	if !ok || e.Tombstone {
		// Absent or deleted: same client-visible NOT_FOUND. The tombstone
		// marker stays an internal concern, never exposed here.
		out.Status = gen.StatusCode_STATUS_CODE_NOT_FOUND
		return out, nil
	}
	out.Status = gen.StatusCode_STATUS_CODE_UNSPECIFIED
	out.Value = new(string(e.Value))
	out.Version = e.Version
	out.WriterNodeHash = e.WriterNodeHash
	// Expiry intentionally always 0, mirroring the C++ handler.
	return out, nil
}

func (s *service) Ping(ctx context.Context, _ *gen.PingRequest) (*gen.PingResponse, error) {
	tok, err := s.checkAuth(ctx)
	if err != nil {
		return &gen.PingResponse{}, err
	}
	res := s.node.DispatchWithAuth(protocol.Request{Opcode: protocol.OpPing}, tok)
	return &gen.PingResponse{Status: toProtoStatus(res.Status)}, nil
}

func (s *service) CounterAdjust(ctx context.Context, req *gen.CounterAdjustRequest) (*gen.CounterAdjustResponse, error) {
	tok, err := s.checkAuth(ctx)
	if err != nil {
		return &gen.CounterAdjustResponse{}, err
	}
	res := s.node.DispatchWithAuth(protocol.Request{
		Opcode: protocol.OpCounterAdjust, Key: []byte(req.GetKey()),
		CounterDelta: req.GetDelta(), CounterDecrement: req.GetDecrement(),
	}, tok)
	return &gen.CounterAdjustResponse{Status: toProtoStatus(res.Status)}, nil
}

func (s *service) CounterGet(ctx context.Context, req *gen.CounterGetRequest) (*gen.CounterGetResponse, error) {
	tok, err := s.checkAuth(ctx)
	if err != nil {
		return &gen.CounterGetResponse{}, err
	}
	res := s.node.DispatchWithAuth(protocol.Request{Opcode: protocol.OpCounterGet, Key: []byte(req.GetKey())}, tok)
	out := &gen.CounterGetResponse{Status: toProtoStatus(res.Status)}
	if res.Status == protocol.StatusOK && res.HasValue {
		if c, err := counter.DecodePNCounter(res.Value); err == nil {
			v := c.Value()
			out.Value = &v
		} else {
			// Unreachable in practice: the store only holds states this
			// codec produced. Reported, not hidden, via CORRUPT_DATA.
			out.Status = gen.StatusCode_STATUS_CODE_CORRUPT_DATA
		}
	}
	return out, nil
}

func (s *service) MultiGet(ctx context.Context, req *gen.MultiGetRequest) (*gen.MultiGetResponse, error) {
	_, err := s.checkAuth(ctx)
	if err != nil {
		return &gen.MultiGetResponse{}, err
	}
	out := &gen.MultiGetResponse{}
	for _, key := range req.GetKeys() {
		r := &gen.GetResponse{}
		if val, ok := s.node.Store().Get(key); ok {
			r.Status = gen.StatusCode_STATUS_CODE_UNSPECIFIED
			r.Value = new(string(val))
		} else {
			r.Status = gen.StatusCode_STATUS_CODE_NOT_FOUND
		}
		out.Responses = append(out.Responses, r)
	}
	return out, nil
}

func (s *service) MultiSet(ctx context.Context, req *gen.MultiSetRequest) (*gen.MultiSetResponse, error) {
	_, err := s.checkAuth(ctx)
	if err != nil {
		return &gen.MultiSetResponse{}, err
	}
	out := &gen.MultiSetResponse{}
	self := s.node.ID()
	for _, setReq := range req.GetRequests() {
		r := &gen.SetResponse{}
		if s.node.OwnerOf(setReq.GetKey()) != self {
			r.Status = gen.StatusCode_STATUS_CODE_NOT_READY
		} else {
			var err error
			if setReq.GetTtlMs() > 0 {
				err = s.node.Store().PutWithTTL(setReq.GetKey(), []byte(setReq.GetValue()), time.Duration(setReq.GetTtlMs())*time.Millisecond)
			} else {
				err = s.node.Store().Put(setReq.GetKey(), []byte(setReq.GetValue()))
			}
			r.Status = toProtoStatus(tcp.StatusOf(err))
		}
		out.Responses = append(out.Responses, r)
	}
	return out, nil
}

func (s *service) Info(ctx context.Context, _ *gen.InfoRequest) (*gen.InfoResponse, error) {
	tok, err := s.checkAuth(ctx)
	if err != nil {
		return &gen.InfoResponse{}, err
	}
	res := s.node.DispatchWithAuth(protocol.Request{Opcode: protocol.OpAdminInfo}, tok)
	return &gen.InfoResponse{Json: string(res.Value)}, nil
}

func (s *service) ClusterInfo(ctx context.Context, _ *gen.ClusterInfoRequest) (*gen.ClusterInfoResponse, error) {
	tok, err := s.checkAuth(ctx)
	if err != nil {
		return &gen.ClusterInfoResponse{}, err
	}
	res := s.node.DispatchWithAuth(protocol.Request{Opcode: protocol.OpAdminCluster}, tok)
	return &gen.ClusterInfoResponse{Json: string(res.Value)}, nil
}

func (s *service) RingInfo(ctx context.Context, _ *gen.RingInfoRequest) (*gen.RingInfoResponse, error) {
	tok, err := s.checkAuth(ctx)
	if err != nil {
		return &gen.RingInfoResponse{}, err
	}
	res := s.node.DispatchWithAuth(protocol.Request{Opcode: protocol.OpAdminRing}, tok)
	return &gen.RingInfoResponse{Json: string(res.Value)}, nil
}

// Gateway is a gRPC front for one cache node. Create it after the TCP
// server is configured and Serve it on its own listener; Stop halts it
// (cinderd calls both around the TCP server lifecycle).
type Gateway struct {
	mu        sync.Mutex
	srv       *grpc.Server
	svc       *service
	tlsConfig *tls.Config
	serving   bool
}

// New registers the cache service over node. The grpc.Server is built
// lazily in Serve so SetTLS/SetClientSecret can configure it first.
func New(node *tcp.Server) *Gateway {
	return &Gateway{svc: &service{node: node}}
}

// SetClientSecret enables bearer auth on every RPC (see checkAuth).
// Call before Serve.
func (g *Gateway) SetClientSecret(secret string) {
	g.svc.setClientSecret(secret)
}

// SetTLS serves gRPC over TLS with the given PEM chain + key, the same
// files as the TCP server. Like C++, a configured client secret refuses
// to serve over plaintext: call this whenever a secret is set. Call
// before Serve.
func (g *Gateway) SetTLS(certFile, keyFile string) error {
	cert, err := tls.LoadX509KeyPair(certFile, keyFile)
	if err != nil {
		return err
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.serving {
		return errors.New("grpcgw: SetTLS after Serve has no effect")
	}
	g.tlsConfig = &tls.Config{Certificates: []tls.Certificate{cert}, MinVersion: tls.VersionTLS12}
	return nil
}

// Serve blocks serving the gateway. Refuses to serve an authenticated
// gateway over plaintext, mirroring the C++ startup check.
func (g *Gateway) Serve(lis net.Listener) error {
	g.mu.Lock()
	if g.serving {
		g.mu.Unlock()
		return errors.New("grpcgw: already serving")
	}
	g.serving = true
	tlsCfg := g.tlsConfig
	g.mu.Unlock()

	secret, _ := g.svc.clientSecret.Load().(string)
	var srv *grpc.Server
	if tlsCfg != nil {
		srv = grpc.NewServer(grpc.Creds(credentials.NewTLS(tlsCfg)))
	} else {
		if secret != "" {
			g.mu.Lock()
			g.serving = false
			g.mu.Unlock()
			return errors.New("grpcgw: client auth requires TLS certificate and key")
		}
		srv = grpc.NewServer()
	}
	gen.RegisterCinderCacheServiceServer(srv, g.svc)
	g.mu.Lock()
	g.srv = srv
	g.mu.Unlock()
	return srv.Serve(lis)
}

// Stop halts the gateway immediately (in-flight RPCs are canceled).
// No-op before Serve.
func (g *Gateway) Stop() {
	g.mu.Lock()
	srv := g.srv
	g.mu.Unlock()
	if srv != nil {
		srv.Stop()
	}
}
