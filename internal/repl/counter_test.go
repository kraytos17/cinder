package repl

import (
	"testing"
	"time"

	"github.com/kraytos17/cinder/internal/store"
)

// ReplicateCounter fans out OpCounterMerge to every replica except self,
// best-effort: failures queue no hints (counter merges are not LWW).
func TestReplicateCounterFansOut(t *testing.T) {
	ft := newFake("n1", "n2")
	local := store.NewLRUStore(1<<20, nil)
	m := NewManager(local, "n1", nil, ft)
	m.ReplicateCounter("k", []byte("state"), []string{"n1", "n2"}, 0, 0)
	time.Sleep(200 * time.Millisecond)
	ft.mu.Lock()
	n2sends := ft.sends["n2"]
	n1sends := ft.sends["n1"]
	ft.mu.Unlock()
	if n2sends != 1 {
		t.Fatalf("sends to n2 = %d, want 1", n2sends)
	}
	if n1sends != 0 {
		t.Fatalf("sends to self = %d, want 0", n1sends)
	}
	if got := m.HintCount(); got != 0 {
		t.Fatalf("hints = %d, want 0", got)
	}
}

// Failed counter sends still queue no hints; AE repairs the drop.
func TestReplicateCounterFailureNoHints(t *testing.T) {
	ft := newFake("n1", "n2")
	ft.fail["n2"] = true
	local := store.NewLRUStore(1<<20, nil)
	m := NewManager(local, "n1", nil, ft)
	m.ReplicateCounter("k", []byte("state"), []string{"n2"}, 0, 0)
	time.Sleep(200 * time.Millisecond)
	if got := m.HintCount(); got != 0 {
		t.Fatalf("hints = %d, want 0", got)
	}
	if got := m.HintsDropped(); got != 0 {
		t.Fatalf("dropped = %d, want 0", got)
	}
}

// Empty state sends nothing.
func TestReplicateCounterEmptyNoSend(t *testing.T) {
	ft := newFake("n1", "n2")
	local := store.NewLRUStore(1<<20, nil)
	m := NewManager(local, "n1", nil, ft)
	m.ReplicateCounter("k", nil, []string{"n2"}, 0, 0)
	time.Sleep(100 * time.Millisecond)
	ft.mu.Lock()
	defer ft.mu.Unlock()
	if ft.sends["n2"] != 0 {
		t.Fatal("empty state sent")
	}
}
