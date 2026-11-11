package repl

import (
	"errors"
	"testing"
	"time"

	"github.com/kraytos17/cinder/internal/metrics"
	"github.com/kraytos17/cinder/internal/net/protocol"
	"github.com/kraytos17/cinder/internal/store"
	"github.com/kraytos17/cinder/internal/testkit"
)

// Replication paths bump the wired collector: async/quorum outcomes,
// hints enqueued, replayed, and the drop sync.
func TestReplMetrics(t *testing.T) {
	ft := testkit.NewStores("n2", "n3")
	ft.Fail["n3"] = true
	m := NewManager(Config{Local: store.NewLRUStore(1<<20, nil), Self: "n1", Clock: nil, Transport: ft})
	c := metrics.NewCollector()
	m.SetMetrics(c)
	if err := m.Write("k", []byte("v"), 0, []string{"n2", "n3"}, Async, 0, 0); err != nil {
		t.Fatal(err)
	}
	waitFor(t, "hint", func() bool { return m.HintCount() == 1 })
	if got := c.AsyncWrites.Load(); got != 1 {
		t.Fatalf("async writes = %d, want 1", got)
	}
	if got := c.HintsEnqueued.Load(); got != 1 {
		t.Fatalf("enqueued = %d, want 1", got)
	}
	if got := c.ReplicaUnreach.Load(); got != 1 {
		t.Fatalf("unreachable = %d, want 1", got)
	}
	if got := c.HintsDropped.Load(); got != 0 {
		t.Fatalf("dropped = %d, want 0", got)
	}
	// Heal the replica and replay: HintsReplayed bumps.
	waitFor(t, "initial async sends", func() bool {
		return ft.SendCount("n2") == 1 && ft.SendCount("n3") == 1
	})
	ft.Fail["n3"] = false
	if n := m.ReplayHints(); n != 1 {
		t.Fatalf("replayed = %d, want 1", n)
	}
	if got := c.HintsReplayed.Load(); got != 1 {
		t.Fatalf("replay counter = %d, want 1", got)
	}
	// Quorum write against a down replica fails closed and counts it.
	ft.Fail["n2"], ft.Fail["n3"] = true, true
	if err := m.Write("q", []byte("v"), 0, []string{"n2", "n3"}, Quorum, 0, 0); !errors.Is(err, ErrQuorum) {
		t.Fatalf("err = %v, want ErrQuorum", err)
	}
	if got := c.QuorumWritesErr.Load(); got != 1 {
		t.Fatalf("quorum failed = %d, want 1", got)
	}
	// Nil collector: same paths, no panic.
	m.SetMetrics(nil)
	if err := m.Write("k2", []byte("v"), 0, []string{"n2"}, Async, 0, 0); err != nil {
		t.Fatal(err)
	}
}

// Quorum decisions don't wait for stragglers, but their late failures
// still enqueue hints and count separately (QuorumLateFail) from
// in-window failures. RF=5 needs 2 fast remote acks; the 2 slow
// failures (50ms vs microsecond decision) land deterministically late.
func TestQuorumLateFailures(t *testing.T) {
	ft := testkit.NewStores("n2", "n3", "n4", "n5")
	st := &slowFailTransport{inner: ft, slow: map[string]bool{"n4": true, "n5": true}, d: 50 * time.Millisecond}
	m := NewManager(Config{Local: store.NewLRUStore(1<<20, nil), Self: "n1", Clock: nil, Transport: st})
	c := metrics.NewCollector()
	m.SetMetrics(c)
	if err := m.Write("k", []byte("v"), 0, []string{"n2", "n3", "n4", "n5"}, Quorum, 0, 0); err != nil {
		t.Fatal(err)
	}
	waitFor(t, "late failures", func() bool { return c.QuorumLateFail.Load() == 2 })
	if got := c.HintsEnqueued.Load(); got != 2 {
		t.Fatalf("enqueued = %d, want 2", got)
	}
	if got := c.QuorumWritesOK.Load(); got != 1 {
		t.Fatalf("quorum ok = %d, want 1", got)
	}
	if got := c.QuorumWritesErr.Load(); got != 0 {
		t.Fatalf("quorum err = %d, want 0", got)
	}
}

// slowFailTransport fails listed nodes after a delay; the rest pass
// through to the wrapped stores.
type slowFailTransport struct {
	inner *testkit.Stores
	slow  map[string]bool
	d     time.Duration
}

func (s *slowFailTransport) Send(node string, req protocol.Request) (protocol.Response, error) {
	if s.slow[node] {
		time.Sleep(s.d)
		return protocol.Response{}, errors.New("slow down " + node)
	}
	return s.inner.Send(node, req)
}
