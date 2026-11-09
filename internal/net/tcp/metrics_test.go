package tcp_test

import (
	"fmt"
	"io"
	"net"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/kraytos17/cinder/internal/metrics"
	"github.com/kraytos17/cinder/internal/net/protocol"
	"github.com/kraytos17/cinder/internal/net/tcp"
	"github.com/kraytos17/cinder/internal/store"
)

// Dispatch bumps per-opcode counters and latency through the public path.
func TestMetricsDispatchCounts(t *testing.T) {
	srv := tcp.NewServer("n1", 0, store.NewLRUStore(1<<20, nil), nil)
	m := srv.Metrics()
	if m == nil {
		t.Fatal("nil collector")
	}
	srv.Dispatch(protocol.Request{Opcode: protocol.OpPing})
	srv.Dispatch(protocol.Request{Opcode: protocol.OpSet, Key: []byte("k"), Value: []byte("v")})
	srv.Dispatch(protocol.Request{Opcode: protocol.OpGet, Key: []byte("k")})
	srv.Dispatch(protocol.Request{Opcode: protocol.OpGet, Key: []byte("missing")})
	if got := m.OpCount(protocol.OpPing); got != 1 {
		t.Fatalf("ping = %d", got)
	}
	if got := m.OpCount(protocol.OpGet); got != 2 {
		t.Fatalf("get = %d", got)
	}
	if got := m.OpCount(protocol.OpSet); got != 1 {
		t.Fatalf("set = %d", got)
	}
	// Store hooks fire through dispatch too.
	if got := m.Writes.Load(); got != 1 {
		t.Fatalf("writes = %d, want 1", got)
	}
	if got := m.Hits.Load(); got != 1 {
		t.Fatalf("hits = %d, want 1", got)
	}
	if got := m.Misses.Load(); got != 1 {
		t.Fatalf("misses = %d, want 1", got)
	}
	// Denied requests count as well (one increment per request): with a
	// node secret set, an untokened replication opcode is denied.
	if err := srv.ConfigureSecrets("s3cret", ""); err != nil {
		t.Fatal(err)
	}
	denied := srv.Dispatch(protocol.Request{Opcode: protocol.OpGossip})
	if denied.Status != protocol.StatusPermissionDenied {
		t.Fatalf("gossip = %v, want PermissionDenied", denied.Status)
	}
	if got := m.OpCount(protocol.OpGossip); got != 1 {
		t.Fatalf("gossip count = %d, want 1", got)
	}
}

// Live connections and framing failures surface in connection counters,
// and the Prometheus handler renders the collector with live gauges.
func TestMetricsConnectionsAndHandler(t *testing.T) {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	srv := tcp.NewServer("n1", l.Addr().(*net.TCPAddr).Port, store.NewLRUStore(1<<20, nil), nil)
	go srv.Serve(l)
	defer srv.Shutdown()
	m := srv.Metrics()

	c, err := net.Dial("tcp", l.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	frame, err := protocol.EncodeRequest(protocol.Request{Opcode: protocol.OpPing}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := c.Write(frame); err != nil {
		t.Fatal(err)
	}
	header := make([]byte, protocol.FrameHeaderSize)
	if _, err := io.ReadFull(c, header); err != nil {
		t.Fatal(err)
	}
	// Malformed frame on a second connection: decode failure, no crash.
	c2, err := net.Dial("tcp", l.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	_, _ = c2.Write([]byte{protocol.Magic, protocol.Version, 0x7F, 0, 0, 0, 1, 0x00})
	buf := make([]byte, 64)
	_, _ = c2.Read(buf)
	c.Close()
	c2.Close()
	// handle() runs per-connection; shutdown drains them, which bumps
	// the close counter. Poll briefly for the async accounting.
	for i := 0; i < 200 && m.ConnsClosed.Load() < 2; i++ {
		time.Sleep(time.Millisecond)
	}
	if got := m.ConnsOpened.Load(); got < 2 {
		t.Fatalf("opened = %d, want >= 2", got)
	}
	if got := m.DecodeFailures.Load(); got < 1 {
		t.Fatalf("decode failures = %d, want >= 1", got)
	}
	h := metrics.NewHandler(m, srv.MetricGauges)
	rec := httptest.NewRecorder()
	req := httptest.NewRequest("GET", "/metrics", nil)
	h.ServeHTTP(rec, req)
	body := rec.Body.String()
	for _, want := range []string{
		"cinder_operations_total{opcode=\"ping\"} 1\n",
		"cinder_connections_opened_total",
		"cinder_store_capacity_bytes",
		"quantile=\"0.5\"",
	} {
		if !strings.Contains(body, want) {
			t.Fatalf("missing %q", want)
		}
	}
}

// Ownership redirects bump the redirect counter.
func TestMetricsRedirects(t *testing.T) {
	c := newTestCluster(t, []string{"n1", "n2"}, 2, 0)
	defer func() {
		for _, n := range c.nodes {
			n.srv.Shutdown()
		}
	}()
	n1 := c.nodes["n1"].srv
	before := n1.Metrics().Redirects.Load()
	var key string
	for i := range 1000 {
		cand := fmt.Sprintf("redir-%d", i)
		if res := n1.Dispatch(protocol.Request{Opcode: protocol.OpCounterAdjust, Key: []byte(cand), CounterDelta: 1}); res.Status == protocol.StatusNotReady {
			key = cand
			break
		}
	}
	if key == "" {
		t.Skip("no non-owned key found")
	}
	if got := n1.Metrics().Redirects.Load(); got != before+1 {
		t.Fatalf("redirects = %d, want %d", got, before+1)
	}
}
