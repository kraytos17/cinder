// Package grpcgw exposes the cache over gRPC (Go port of the C++ gateway).
//
// It mirrors CinderCacheServiceImpl RPC-for-RPC: OK maps to
// STATUS_CODE_UNSPECIFIED (the proto uses 0 for success), Get/Set/Delete go
// through the same TCP dispatch (ownership, replication), GetVersioned
// reads the local store directly (tombstones and misses both report
// NOT_FOUND, expiry always 0), MultiSet owner-checks then puts locally
// without replication fan-out, and the admin RPCs return the same JSON as
// the admin opcodes.
//
// Deliberate simplification: no trace-metadata propagation (no Go tracing
// infra); every RPC returns gRPC OK with the outcome in the status field,
// exactly like the C++ handlers on the success path. Client auth is
// enforced when configured (SetClientSecret): the C++
// "authorization: Bearer <hex>" contract, UNAUTHENTICATED otherwise.
package grpcgw

import (
	"context"
	"crypto/subtle"
	"encoding/hex"
	"net"
	"strings"
	"sync/atomic"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	"github.com/kraytos17/cinder/internal/auth"
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
}

func (s *service) setClientSecret(secret string) {
	s.clientSecret.Store(secret)
}

// checkAuth mirrors the C++ gateway's checkAuth: with a client secret
// configured, every RPC must present "authorization: Bearer <hex>" where
// hex is the hex-encoded static client token; anything else is
// UNAUTHENTICATED. Open when unconfigured.
func (s *service) checkAuth(ctx context.Context) error {
	want, _ := s.clientSecret.Load().(string)
	if want == "" {
		return nil
	}
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return status.Error(codes.Unauthenticated, "missing credentials")
	}
	vals := md.Get("authorization")
	if len(vals) != 1 || !strings.HasPrefix(vals[0], "Bearer ") {
		return status.Error(codes.Unauthenticated, "invalid credentials")
	}
	if subtle.ConstantTimeCompare([]byte(vals[0][len("Bearer "):]), []byte(hex.EncodeToString(auth.ClientToken(want)))) != 1 {
		return status.Error(codes.Unauthenticated, "invalid credentials")
	}
	return nil
}

func (s *service) Get(ctx context.Context, req *gen.GetRequest) (*gen.GetResponse, error) {
	if err := s.checkAuth(ctx); err != nil {
		return &gen.GetResponse{}, err
	}
	res := s.node.Dispatch(protocol.Request{Opcode: protocol.OpGet, Key: []byte(req.GetKey())})
	out := &gen.GetResponse{Status: toProtoStatus(res.Status)}
	if res.HasValue {
		out.Value = new(string(res.Value))
	}
	return out, nil
}

func (s *service) Set(ctx context.Context, req *gen.SetRequest) (*gen.SetResponse, error) {
	if err := s.checkAuth(ctx); err != nil {
		return &gen.SetResponse{}, err
	}
	r := protocol.Request{Opcode: protocol.OpSet, Key: []byte(req.GetKey()), Value: []byte(req.GetValue())}
	if req.GetTtlMs() > 0 {
		r.HasTTL = true
		r.TTL = time.Duration(req.GetTtlMs()) * time.Millisecond
	}
	res := s.node.Dispatch(r)
	return &gen.SetResponse{Status: toProtoStatus(res.Status)}, nil
}

func (s *service) Delete(ctx context.Context, req *gen.DeleteRequest) (*gen.DeleteResponse, error) {
	if err := s.checkAuth(ctx); err != nil {
		return &gen.DeleteResponse{}, err
	}
	res := s.node.DeleteNoRedirect(string(req.GetKey()))
	return &gen.DeleteResponse{Status: toProtoStatus(res.Status)}, nil
}

func (s *service) GetVersioned(ctx context.Context, req *gen.GetVersionedRequest) (*gen.GetVersionedResponse, error) {
	if err := s.checkAuth(ctx); err != nil {
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

func (s *service) Ping(ctx context.Context, req *gen.PingRequest) (*gen.PingResponse, error) {
	if err := s.checkAuth(ctx); err != nil {
		return &gen.PingResponse{}, err
	}
	res := s.node.Dispatch(protocol.Request{Opcode: protocol.OpPing})
	return &gen.PingResponse{Status: toProtoStatus(res.Status)}, nil
}

func (s *service) MultiGet(ctx context.Context, req *gen.MultiGetRequest) (*gen.MultiGetResponse, error) {
	if err := s.checkAuth(ctx); err != nil {
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
	if err := s.checkAuth(ctx); err != nil {
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

func (s *service) Info(ctx context.Context, req *gen.InfoRequest) (*gen.InfoResponse, error) {
	if err := s.checkAuth(ctx); err != nil {
		return &gen.InfoResponse{}, err
	}
	res := s.node.Dispatch(protocol.Request{Opcode: protocol.OpAdminInfo})
	return &gen.InfoResponse{Json: string(res.Value)}, nil
}

func (s *service) ClusterInfo(ctx context.Context, req *gen.ClusterInfoRequest) (*gen.ClusterInfoResponse, error) {
	if err := s.checkAuth(ctx); err != nil {
		return &gen.ClusterInfoResponse{}, err
	}
	res := s.node.Dispatch(protocol.Request{Opcode: protocol.OpAdminCluster})
	return &gen.ClusterInfoResponse{Json: string(res.Value)}, nil
}

func (s *service) RingInfo(ctx context.Context, req *gen.RingInfoRequest) (*gen.RingInfoResponse, error) {
	if err := s.checkAuth(ctx); err != nil {
		return &gen.RingInfoResponse{}, err
	}
	res := s.node.Dispatch(protocol.Request{Opcode: protocol.OpAdminRing})
	return &gen.RingInfoResponse{Json: string(res.Value)}, nil
}

// Gateway is a gRPC front for one cache node. Create it after the TCP
// server is configured and Serve it on its own listener; Stop halts it
// (cinderd calls both around the TCP server lifecycle).
type Gateway struct {
	srv *grpc.Server
	svc *service
}

// New registers the cache service over node.
func New(node *tcp.Server) *Gateway {
	svc := &service{node: node}
	srv := grpc.NewServer()
	gen.RegisterCinderCacheServiceServer(srv, svc)
	return &Gateway{srv: srv, svc: svc}
}

// SetClientSecret enables bearer auth on every RPC (see checkAuth).
// Call before Serve.
func (g *Gateway) SetClientSecret(secret string) {
	g.svc.setClientSecret(secret)
}

// Serve blocks serving the gateway.
func (g *Gateway) Serve(lis net.Listener) error {
	return g.srv.Serve(lis)
}

// Stop halts the gateway immediately (in-flight RPCs are cancelled).
func (g *Gateway) Stop() {
	g.srv.Stop()
}
