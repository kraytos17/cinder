package repl

import (
	"errors"
	"testing"

	"github.com/kraytos17/cinder/internal/metrics"
	"github.com/kraytos17/cinder/internal/store"
	"github.com/kraytos17/cinder/internal/testkit"
)

// Replication paths bump the wired collector: async/quorum outcomes,
// hints enqueued, replayed, and the drop sync.
func TestReplMetrics(t *testing.T) {
	ft := testkit.NewStores("n2", "n3")
	ft.Fail["n3"] = true
	m := NewManager(store.NewLRUStore(1<<20, nil), "n1", nil, ft)
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
