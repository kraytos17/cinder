// Package client is the Cinder v6 TCP client (Go port).
//
// M3: single-connection client with per-RPC deadlines. Requests on one
// connection are strictly sequential (the server answers in order), so Do
// serializes under a mutex. Redirect following, pooling, and quorum fan-out
// arrive with multi-node support in M4; NotReady surfaces to the caller.
package client

import (
	"bufio"
	"crypto/tls"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"sync"
	"time"

	"github.com/kraytos17/cinder/internal/auth"
	"github.com/kraytos17/cinder/internal/net/protocol"
)

// maxReusePayloadBytes bounds the retained per-connection staging buffer:
// payloads above it still work, but allocate fresh each time instead of
// pinning a ~1 MiB slab per connection after one large value.
const maxReusePayloadBytes = 64 << 10

// Client is a single v6 connection. It is safe for concurrent use; a
// transport error closes the connection and all subsequent calls fail
// until the caller redials (M4 adds transparent reconnect).
type Client struct {
	mu      sync.Mutex
	conn    net.Conn
	br      *bufio.Reader
	timeout time.Duration
	// payloadBuf and encodeBuf are reused staging buffers (see the server
	// read loop for the retention argument; Do is mutex-serialized).
	payloadBuf []byte
	encodeBuf  []byte
	secrets    Secrets
}

// Secrets carries the credentials a client presents: the static client
// token when set, else a time-windowed node token when a shared secret
// and node id are set. Empty means open (no token attached).
type Secrets struct {
	NodeID string
	Shared string
	Client string
}

// tokenFor mirrors the C++ pool precedence: the client token wins when a
// client secret is set, otherwise the node token when a shared secret and
// node id are set, otherwise nothing.
func tokenFor(s Secrets, now time.Time) []byte {
	if s.Client != "" {
		return auth.ClientToken(s.Client)
	}
	if s.Shared != "" && s.NodeID != "" {
		return auth.NodeToken(s.Shared, s.NodeID, now)
	}
	return nil
}

// SetSecrets installs the credentials attached to every subsequent RPC.
// Tokens are minted per call (node tokens are time-windowed), so updating
// secrets takes effect immediately on this connection.
func (c *Client) SetSecrets(s Secrets) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.secrets = s
}

// Dial opens addr with a dial timeout; timeout also bounds every RPC.
func Dial(addr string, timeout time.Duration) (*Client, error) {
	c, err := net.DialTimeout("tcp", addr, timeout) //nolint:noctx // no caller context to propagate; timeout is the deadline mechanism
	if err != nil {
		return nil, err
	}

	setNoDelay(c)
	return &Client{conn: c, br: bufio.NewReader(c), timeout: timeout}, nil
}

// DialTLS opens addr over TLS with a dial timeout; timeout also bounds
// every RPC. cfg must come from tlscfg.Client (chain-only verification
// like the C++ client) or be nil for an unverified connection.
func DialTLS(addr string, timeout time.Duration, cfg *tls.Config) (*Client, error) {
	if cfg == nil {
		cfg = &tls.Config{InsecureSkipVerify: true} //nolint:gosec // test-only path
	}

	raw, err := net.DialTimeout("tcp", addr, timeout) //nolint:noctx // no caller context to propagate; timeout is the deadline mechanism
	if err != nil {
		return nil, err
	}

	setNoDelay(raw)
	c := tls.Client(raw, cfg)
	// Bound the handshake by the RPC timeout: the old DialWithDialer
	// path left it unbounded, hanging the caller on a blackholed peer.
	if err := c.SetDeadline(time.Now().Add(timeout)); err != nil {
		raw.Close()
		return nil, err
	}
	if err := c.Handshake(); err != nil { //nolint:noctx // no caller context to propagate; handshake bounded by the SetDeadline above
		raw.Close()
		return nil, err
	}
	if err := c.SetDeadline(time.Time{}); err != nil {
		raw.Close()
		return nil, err
	}
	return &Client{conn: c, br: bufio.NewReader(c), timeout: timeout}, nil
}

// retainEncode keeps a reusable encode buffer, dropping oversized slabs
// rather than pinning ~1 MiB per connection after one large frame.
// Encoding never retains the returned slice past the write loop, and
// decoding copies variable-length fields out, so reuse is safe.
func retainEncodeBuf(buf []byte) []byte {
	if cap(buf) > maxReusePayloadBytes {
		return nil
	}
	return buf[:0]
}

// setNoDelay disables Nagle's algorithm on TCP connections. Request +
// response are small and strictly sequential, the exact pattern Nagle
// delays (up to ~200ms on real networks); loopback benches don't show
// it, but deployments do. Best-effort: a socket that rejects the
// option still works, just with Nagle enabled.
func setNoDelay(c net.Conn) {
	if tc, ok := c.(*net.TCPConn); ok {
		tc.SetNoDelay(true)
	}
}

// Close closes the connection.
func (c *Client) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.conn == nil {
		return nil
	}
	err := c.conn.Close()
	c.conn = nil
	c.br = nil
	return err
}

func (c *Client) fail(err error) error {
	if c.conn != nil {
		c.conn.Close()
		c.conn = nil
		c.br = nil
	}
	return err
}

// Do sends one request and returns the response frame.
func (c *Client) Do(req protocol.Request) (protocol.Response, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.conn == nil {
		return protocol.Response{}, fmt.Errorf("client: connection closed")
	}
	frame, err := protocol.EncodeRequestInto(req, tokenFor(c.secrets, time.Now()), c.encodeBuf)
	if err != nil {
		return protocol.Response{}, err
	}
	c.encodeBuf = retainEncodeBuf(frame)
	if err := c.conn.SetDeadline(time.Now().Add(c.timeout)); err != nil {
		return protocol.Response{}, c.fail(err)
	}
	for len(frame) > 0 {
		n, err := c.conn.Write(frame)
		if err != nil {
			return protocol.Response{}, c.fail(err)
		}
		frame = frame[n:]
	}
	// Fixed-size header on the stack: no per-RPC alloc (see the server
	// read loop for the same pattern).
	var header [protocol.FrameHeaderSize]byte
	if _, err := io.ReadFull(c.br, header[:]); err != nil {
		return protocol.Response{}, c.fail(err)
	}
	if header[0] != protocol.Magic || header[1] != protocol.Version {
		return protocol.Response{}, c.fail(fmt.Errorf("client: bad response header"))
	}
	if header[2] != 0 {
		return protocol.Response{}, c.fail(fmt.Errorf("client: response opcode %d", header[2]))
	}
	n := binary.BigEndian.Uint32(header[3:7])
	if n > protocol.MaxPayloadSize {
		return protocol.Response{}, c.fail(fmt.Errorf("client: response too large: %d", n))
	}
	// Reused staging buffer: decoders copy every variable-length field
	// out, so nothing retains this memory and reuse is safe. Oversized
	// slabs are dropped rather than pinned per connection.
	if cap(c.payloadBuf) > maxReusePayloadBytes {
		c.payloadBuf = nil
	}
	payload := c.payloadBuf
	if uint64(cap(payload)) < uint64(n) {
		payload = make([]byte, n)
		if n <= maxReusePayloadBytes {
			c.payloadBuf = payload
		}
	} else {
		payload = payload[:n]
	}
	if _, err := io.ReadFull(c.br, payload); err != nil {
		return protocol.Response{}, c.fail(err)
	}
	res, err := protocol.DecodeResponse(payload)
	if err != nil {
		return protocol.Response{}, c.fail(err)
	}
	return res, nil
}

// Raw sends pre-encoded bytes and returns the raw response frame. It
// exists for negative tests (unknown opcodes, bad lengths) that the typed
// API cannot express.
func (c *Client) Raw(frame []byte) ([]byte, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.conn == nil {
		return nil, fmt.Errorf("client: connection closed")
	}
	if err := c.conn.SetDeadline(time.Now().Add(c.timeout)); err != nil {
		return nil, c.fail(err)
	}
	for len(frame) > 0 {
		n, err := c.conn.Write(frame)
		if err != nil {
			return nil, c.fail(err)
		}
		frame = frame[n:]
	}
	header := make([]byte, protocol.FrameHeaderSize)
	if _, err := io.ReadFull(c.br, header); err != nil {
		return nil, c.fail(err)
	}
	n := binary.BigEndian.Uint32(header[3:7])
	if n > protocol.MaxPayloadSize {
		return nil, c.fail(fmt.Errorf("client: response too large: %d", n))
	}
	payload := make([]byte, n)
	if _, err := io.ReadFull(c.br, payload); err != nil {
		return nil, c.fail(err)
	}
	frame = make([]byte, protocol.FrameHeaderSize+len(payload))
	copy(frame, header)
	copy(frame[protocol.FrameHeaderSize:], payload)
	return frame, nil
}

// Ping checks liveness.
func (c *Client) Ping() (protocol.Response, error) {
	return c.Do(protocol.Request{Opcode: protocol.OpPing})
}

// Set stores value under key.
func (c *Client) Set(key string, value []byte) (protocol.Response, error) {
	return c.Do(protocol.Request{Opcode: protocol.OpSet, Key: []byte(key), Value: value})
}

// SetWithTTL stores value with a relative TTL.
func (c *Client) SetWithTTL(key string, value []byte, ttl time.Duration) (protocol.Response, error) {
	return c.Do(protocol.Request{Opcode: protocol.OpSet, Key: []byte(key), Value: value, TTL: ttl})
}

// Get fetches a live value.
func (c *Client) Get(key string) (protocol.Response, error) {
	return c.Do(protocol.Request{Opcode: protocol.OpGet, Key: []byte(key)})
}

// Del deletes key via a versioned tombstone (always OK, like C++).
func (c *Client) Del(key string) (protocol.Response, error) {
	return c.Do(protocol.Request{Opcode: protocol.OpDel, Key: []byte(key)})
}

// GetVersioned fetches the entry with LWW metadata.
func (c *Client) GetVersioned(key string) (protocol.Response, error) {
	return c.Do(protocol.Request{Opcode: protocol.OpGetVersioned, Key: []byte(key)})
}

// CounterAdjust increments (decrement=false) or decrements key's
// PN-counter by delta as the server's node id. Zero delta is a no-op OK.
func (c *Client) CounterAdjust(key string, delta uint64, decrement bool) (protocol.Response, error) {
	return c.Do(protocol.Request{Opcode: protocol.OpCounterAdjust, Key: []byte(key), CounterDelta: delta, CounterDecrement: decrement})
}

// CounterGet fetches key's serialized PN-counter state (decode with
// internal/counter.DecodePNCounter); NotFound when absent.
func (c *Client) CounterGet(key string) (protocol.Response, error) {
	return c.Do(protocol.Request{Opcode: protocol.OpCounterGet, Key: []byte(key)})
}

// Admin sends an admin opcode (11-16) with an empty key.
func (c *Client) Admin(op protocol.Opcode) (protocol.Response, error) {
	return c.Do(protocol.Request{Opcode: op})
}
