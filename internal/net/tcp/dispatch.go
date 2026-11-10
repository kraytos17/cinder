package tcp

// Request routing: opcode classification, auth gating, ownership
// redirects, trace minting, and the connection read loop.

import (
	"bufio"
	"encoding/binary"
	"io"
	"net"
	"strings"
	"time"

	"github.com/kraytos17/cinder/internal/auth"
	"github.com/kraytos17/cinder/internal/cluster"
	"github.com/kraytos17/cinder/internal/log"
	"github.com/kraytos17/cinder/internal/metrics"
	"github.com/kraytos17/cinder/internal/net/protocol"
)

func (s *Server) Dispatch(req protocol.Request) protocol.Response {
	return s.dispatch(req, nil)
}

// DispatchWithAuth routes like Dispatch but enforces the given credential
// instead of an absent one. The gRPC gateway authenticates the bearer
// itself (mapping failures to UNAUTHENTICATED), then forwards the decoded
// token so server-side policy — auth, ownership, replication fan-out,
// quorum — applies to gateway calls exactly as it does to TCP clients.
// Without this, any server-side client secret would deny every gateway
// call that routes through Dispatch.

func (s *Server) DispatchWithAuth(req protocol.Request, token []byte) protocol.Response {
	return s.dispatch(req, token)
}

// opcodeClass mirrors TcpConnection::classifyOpcode: client opcodes are
// ring-routed, replication and admin opcodes address this node directly.

type opcodeClass int

const (
	classClient opcodeClass = iota
	classReplication
	classAdmin
)

func classifyOpcode(op protocol.Opcode) opcodeClass {
	switch op {
	case protocol.OpReplicate, protocol.OpHint, protocol.OpGossip,
		protocol.OpAntiEntropyDigest, protocol.OpAntiEntropySync,
		protocol.OpCounterAntiEntropy, protocol.OpCounterMerge:
		// CounterMerge rides here, not with the other counter opcodes:
		// it is internal replication traffic (full-state fan-out), so it
		// takes the node token and skips the ownership redirect. C++
		// classifyOpcode leaves it in Client class, which would redirect
		// every merge sent to a non-owner replica and silently break
		// counter replication; the C++ TLS test ("Counter state merge
		// is internal replication traffic ... requires the node token")
		// states the intent this follows.
		return classReplication
	case protocol.OpAdminInfo, protocol.OpAdminCluster, protocol.OpAdminRing,
		protocol.OpAdminCompact, protocol.OpAdminConfigReload, protocol.OpAdminShutdown:
		return classAdmin
	default:
		return classClient
	}
}

// checkAuth mirrors TcpConnection::checkAuth: node tokens guard internal
// replication opcodes when a shared secret is set; the static client token
// guards client opcodes and (preferentially) admin opcodes when a client
// secret is set. Anything unconfigured is open. A node token is accepted
// for the local id or any alive peer id: senders mint with their own id,
// and verifying only self would deny every cross-node send.

func (s *Server) checkAuth(op protocol.Opcode, token []byte) bool {
	s.authMu.RLock()
	shared, clientSecret := s.sharedSecret, s.clientSharedSecret
	s.authMu.RUnlock()
	if shared == "" && clientSecret == "" {
		return true
	}
	now := s.clock.Now()
	switch classifyOpcode(op) {
	case classReplication:
		if shared == "" {
			return true
		}
		return len(token) == protocol.AuthTokenLen && s.verifyNodeTokenAny(shared, token, now)
	case classAdmin:
		if clientSecret != "" {
			return auth.VerifyClientToken(clientSecret, token)
		}
		if shared != "" {
			return len(token) == protocol.AuthTokenLen && s.verifyNodeTokenAny(shared, token, now)
		}
		return true
	default: // classClient
		if clientSecret != "" {
			return auth.VerifyClientToken(clientSecret, token)
		}
		return true
	}
}

// verifyNodeTokenAny accepts a node token minted by self or any alive peer.

func (s *Server) verifyNodeTokenAny(shared string, token []byte, now time.Time) bool {
	if auth.VerifyNodeToken(shared, s.nodeID, token, now) {
		return true
	}
	s.ringMu.RLock()
	table := s.table
	s.ringMu.RUnlock()
	if table == nil {
		return false
	}
	for _, m := range table.Snapshot() {
		if m.ID == s.nodeID || m.State != cluster.Alive {
			continue
		}
		if auth.VerifyNodeToken(shared, m.ID, token, now) {
			return true
		}
	}
	return false
}

// StatusOf maps store/replication errors to wire statuses, shared with
// the gRPC gateway's local-write paths (MultiSet).

func (s *Server) DeleteNoRedirect(key string) protocol.Response {
	if m, rf, mode := s.replication(); m != nil && rf > 1 {
		return writeResult(m.Delete(key, s.replicaNodes(key), mode, 0, 0))
	}
	if err := s.store.RemoveVersioned(key, s.store.MintVersion(), 0); err != nil {
		return protocol.Response{Status: statusOf(err)}
	}
	return protocol.Response{Status: protocol.StatusOK}
}

// ownerOf resolves the ring owner, or self when standalone.

func (s *Server) ownerOf(key string) string {
	s.ringMu.RLock()
	defer s.ringMu.RUnlock()
	if s.ring == nil {
		return s.nodeID
	}
	if owner := s.ring.GetNode(key); owner != "" {
		return owner
	}
	return s.nodeID
}

// redirectTo builds the "moved to" value, appending @host:port when a
// reachable address is known, mirroring redirectValue.

func (s *Server) redirectTo(owner string) string {
	var value strings.Builder
	value.WriteString("moved to ")
	value.WriteString(owner)
	s.ringMu.RLock()
	table := s.table
	s.ringMu.RUnlock()
	if table != nil {
		if info, ok := table.Get(owner); ok && info.Host != "" && info.Port != 0 {
			// JoinHostPort brackets v6 literals; the client parses both
			// bare and bracketed forms.
			value.WriteString("@")
			value.WriteString(net.JoinHostPort(info.Host, info.Port.String()))
		}
	}
	return value.String()
}

func (s *Server) notReady(owner string) protocol.Response {
	s.met.Redirects.Add(1)
	return protocol.Response{Status: protocol.StatusNotReady, HasValue: true, Value: []byte(s.redirectTo(owner))}
}

// mintTrace assigns entry-point trace context, mirroring the C++
// connection: one atomic add mints both IDs, trace takes the first slot.

func (s *Server) mintTrace(traceID, spanID uint64) (uint64, uint64) {
	if traceID == 0 || spanID == 0 {
		base := s.trace.Add(2) - 1
		if base == 0 {
			base = s.trace.Add(2) - 1
		}
		if traceID == 0 {
			traceID = base
		}
		if spanID == 0 {
			spanID = base + 1
		}
	}
	return traceID, spanID
}

func (s *Server) handle(c net.Conn) {
	defer s.done(c)
	// One buffered reader per connection: the old code did two syscalls
	// per frame (header, then payload) via io.ReadFull on the raw socket.
	// Small frames now usually complete without a second read syscall;
	// wire behavior is unchanged (same bytes, same limits, same closes).
	// Default 4 KiB buffer: it batches the header plus small payloads
	// without pinning 1 MiB per connection (the cap allows 10k conns).
	br := bufio.NewReader(c)
	header := make([]byte, protocol.FrameHeaderSize)
	for {
		c.SetDeadline(time.Now().Add(idleTimeout))
		if _, err := io.ReadFull(br, header); err != nil {
			return
		}
		if header[0] != protocol.Magic || header[1] != protocol.Version {
			return // bad protocol header: close, like C++
		}
		payloadLen := binary.BigEndian.Uint32(header[3:7])
		if payloadLen > protocol.MaxPayloadSize || uint64(payloadLen)+protocol.FrameHeaderSize > connBufferSize {
			return // oversized payload: close, like C++
		}
		payload := make([]byte, payloadLen)
		if _, err := io.ReadFull(br, payload); err != nil {
			return
		}
		frame := make([]byte, protocol.FrameHeaderSize+len(payload))
		copy(frame, header)
		copy(frame[protocol.FrameHeaderSize:], payload)
		dec, err := protocol.DecodeRequest(frame)
		var res protocol.Response
		if err != nil {
			// Decode failures answer InvalidArgument without trace echo.
			s.met.DecodeFailures.Add(1)
			if e := s.log().Debug(); e != nil {
				e.Int("bytes", len(frame)).Msg("decode failure")
			}
			res = protocol.Response{Status: protocol.StatusInvalidArgument}
		} else {
			dec.Req.TraceID, dec.Req.SpanID = s.mintTrace(dec.Req.TraceID, dec.Req.SpanID)
			res = s.dispatch(dec.Req, dec.AuthToken)
			res.TraceID = dec.Req.TraceID
			res.SpanID = dec.Req.SpanID
		}
		out, err := protocol.EncodeResponse(res)
		if err != nil {
			s.met.WriteFailures.Add(1)
			return
		}
		if err := writeAll(c, out); err != nil {
			s.met.WriteFailures.Add(1)
			return
		}
	}
}

func writeAll(c net.Conn, b []byte) error {
	for len(b) > 0 {
		n, err := c.Write(b)
		if err != nil {
			return err
		}
		b = b[n:]
	}
	return nil
}

// dispatch routes one decoded request: auth gate first, then ownership,
// replication, admin by opcode. Every decoded request bumps its opcode
// counter and latency, mirroring C++ recordOpcodeMetric (denials count
// too: "one increment per request").

func (s *Server) dispatch(req protocol.Request, authToken []byte) protocol.Response {
	start := s.clock.Now()
	// Request-scoped logger: the wire IDs ride every line, and fan-out
	// forwards the IDs, so replica-side lines correlate by trace_id.
	if e := s.log().Debug(); e != nil {
		log.Fields(e, req.TraceID, req.SpanID).
			Str("opcode", metrics.OpName(req.Opcode)).
			Str("key", string(req.Key)).
			Msg("request")
	}
	res := s.dispatchInner(req, authToken)
	s.met.Record(req.Opcode, uint64(s.clock.Now().Sub(start).Nanoseconds()))
	return res
}

func (s *Server) dispatchInner(req protocol.Request, authToken []byte) protocol.Response {
	if !s.checkAuth(req.Opcode, authToken) {
		// Denials carry no value (so a Get can never surface fake data)
		// and echo the trace for correlation, like C++ denyRequest.
		// Warn, opcode only: keys are user data, debug-only.
		if e := s.log().Warn(); e != nil {
			log.Fields(e, req.TraceID, req.SpanID).
				Str("opcode", metrics.OpName(req.Opcode)).
				Msg("permission denied")
		}
		return protocol.Response{Status: protocol.StatusPermissionDenied}
	}
	switch req.Opcode {
	case protocol.OpPing:
		return protocol.Response{Status: protocol.StatusOK}
	case protocol.OpGet:
		return s.handleGet(req)
	case protocol.OpGetVersioned:
		return s.handleGetVersioned(req)
	case protocol.OpSet:
		return s.handleSet(req)
	case protocol.OpDel:
		return s.handleDelete(req)
	case protocol.OpReplicate, protocol.OpHint:
		return s.applyReplicated(req)
	case protocol.OpGossip:
		return s.handleGossip(req)
	case protocol.OpAntiEntropyDigest, protocol.OpAntiEntropySync:
		return s.handleAntiEntropy(req)
	case protocol.OpCounterAdjust:
		return s.handleCounterAdjust(req)
	case protocol.OpCounterMerge:
		return s.handleCounterMerge(req)
	case protocol.OpCounterGet:
		return s.handleCounterGet(req)
	case protocol.OpCounterAntiEntropy:
		return s.handleCounterAntiEntropy(req)
	default:
		return s.admin(req)
	}
}

// handleCounterAdjust applies a client increment/decrement as the local
// node id, then best-effort replicates full state. Ownership-gated like
// other writes; zero deltas are no-ops that still answer OK.
