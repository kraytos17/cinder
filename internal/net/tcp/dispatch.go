package tcp

import (
	"bufio"
	"encoding/binary"
	"errors"
	"io"
	"net"
	"strings"
	"time"

	"github.com/kraytos17/cinder/internal/auth"
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

// opcodeClass categorizes requests so authentication and ownership routing
// can depend on whether traffic is client, replication, or admin traffic.
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
		// CounterMerge is full-state replication traffic: it requires the
		// node token and bypasses ownership redirects, so each replica can
		// accept the merge.
		return classReplication
	case protocol.OpAdminInfo, protocol.OpAdminCluster, protocol.OpAdminRing,
		protocol.OpAdminCompact, protocol.OpAdminConfigReload, protocol.OpAdminShutdown:
		return classAdmin
	default:
		return classClient
	}
}

// checkAuth validates credentials according to opcode class. Internal
// replication requires a node token when node auth is configured; client
// operations require a client token when client auth is configured. Admin
// operations prefer client auth, then node auth. Unconfigured auth is open.
// Node tokens are accepted for this server or any alive member because a
// sender mints its token using its own node ID.
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
// Membership iteration is snapshot-free (see MatchAliveID): no per-request
// allocation on inter-node traffic.
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
	return table.MatchAliveID(s.nodeID, func(id string) bool {
		return auth.VerifyNodeToken(shared, id, token, now)
	})
}

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

// redirectTo builds a "moved to" response value and appends @host:port
// when membership has a reachable address for the owner.
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

// mintTrace preserves supplied trace IDs and fills missing IDs from a
// unique pair allocated with one atomic increment.
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
	// Fixed-size header on the stack: no per-request alloc.
	var header [protocol.FrameHeaderSize]byte
	// Reused payload staging. Decoders copy every variable-length field
	// out, so no decoded value aliases this buffer and reuse is safe.
	// Oversized slabs are dropped rather than pinned per connection.
	var payloadBuf []byte
	// Reused encode buffer, same retention argument as payloadBuf above.
	var encodeBuf []byte
	for {
		// Read-idle budget only: a slow sender that never completes a
		// frame trips this. Writes get their own shorter budget below.
		if err := c.SetReadDeadline(time.Now().Add(idleTimeout)); err != nil {
			return
		}
		if _, err := io.ReadFull(br, header[:]); err != nil {
			return
		}
		if header[0] != protocol.Magic || header[1] != protocol.Version {
			return // bad protocol header: close the connection
		}

		payloadLen := binary.BigEndian.Uint32(header[3:7])
		if payloadLen > protocol.MaxPayloadSize || uint64(payloadLen)+protocol.FrameHeaderSize > connBufferSize {
			return // oversized payload: close the connection
		}
		if cap(payloadBuf) > maxReusePayload {
			payloadBuf = nil
		}

		payload := payloadBuf
		if uint64(cap(payload)) < uint64(payloadLen) {
			payload = make([]byte, payloadLen)
			if payloadLen <= maxReusePayload {
				payloadBuf = payload
			}
		} else {
			payload = payload[:payloadLen]
		}

		if _, err := io.ReadFull(br, payload); err != nil {
			return
		}

		dec, err := protocol.DecodeRequest(header[:], payload)
		var res protocol.Response
		if err != nil {
			// Decode failures answer InvalidArgument without trace echo.
			s.met.DecodeFailures.Add(1)
			if e := s.log().Debug(); e != nil {
				e.Int("bytes", protocol.FrameHeaderSize+len(payload)).Msg("decode failure")
			}
			res = protocol.Response{Status: protocol.StatusInvalidArgument}
		} else {
			dec.Req.TraceID, dec.Req.SpanID = s.mintTrace(dec.Req.TraceID, dec.Req.SpanID)
			res = s.dispatch(dec.Req, dec.AuthToken)
			res.TraceID = dec.Req.TraceID
			res.SpanID = dec.Req.SpanID
		}

		out, err := protocol.EncodeResponseInto(res, encodeBuf)
		if err != nil {
			s.met.WriteFailures.Add(1)
			return
		}
		if cap(out) > maxReusePayload {
			encodeBuf = nil
		} else {
			encodeBuf = out[:0]
		}

		if err := c.SetWriteDeadline(time.Now().Add(s.currentWriteTimeout())); err != nil {
			s.met.WriteFailures.Add(1)
			return
		}
		if err := writeAll(c, out); err != nil {
			// A write deadline means the peer stopped reading: count a
			// slow-consumer drop, not a generic write failure. Other
			// write errors (resets, closed sockets) stay failures.
			if ne, ok := errors.AsType[net.Error](err); ok && ne.Timeout() {
				s.met.SlowConsumerDrops.Add(1)
				if e := s.log().Debug(); e != nil {
					e.Int("bytes", len(out)).Msg("slow consumer dropped")
				}
				return
			}

			s.met.WriteFailures.Add(1)
			return
		}
		if dec.Req.Opcode == protocol.OpAdminShutdown {
			// Shut down only after the OK is on the wire: Shutdown
			// closes this connection via closeAll, so firing it from
			// dispatch raced the response and surfaced as EOF.
			// (Decode failures can never match: opcode zero is invalid.)
			go s.Shutdown()
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
// counter and latency once per decoded request, including requests denied
// by the authentication gate.
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
		// Denials carry no value (so a Get cannot surface data) and retain
		// trace IDs for correlation.
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
