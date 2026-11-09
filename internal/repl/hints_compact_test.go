package repl

import (
	"sync"
	"testing"
	"time"

	"github.com/kraytos17/cinder/internal/store"
)

// stubJournal implements HintJournal with a tiny compaction threshold so
// the auto-compact path is exercisable without 4k appends.
type stubJournal struct {
	mu        sync.Mutex
	adds      int
	compacted int
	lastLive  int
}

func (j *stubJournal) AppendAdd(h Hint) {
	j.mu.Lock()
	defer j.mu.Unlock()
	j.adds++
}

func (j *stubJournal) AppendDel(_, _ string, _ uint64) {}

func (j *stubJournal) NeedsCompaction() bool {
	j.mu.Lock()
	defer j.mu.Unlock()
	return j.adds >= 4
}

func (j *stubJournal) Compact(q *HintQueue) error {
	live := q.Snapshot(time.Now(), 1<<20)
	j.mu.Lock()
	defer j.mu.Unlock()
	j.compacted++
	j.lastLive = len(live)
	return nil
}

func TestEnqueueHintAutoCompacts(t *testing.T) {
	ft := newFake("n2")
	ft.fail["n2"] = true
	m := NewManager(store.NewLRUStore(1<<20, nil), "n1", nil, ft)
	stub := &stubJournal{}
	m.SetHintJournal(stub)
	for i := 0; i < 10; i++ {
		if err := m.Write("k", []byte("v"), 0, false, []string{"n2"}, Async, 0, 0); err != nil {
			t.Fatal(err)
		}
	}
	waitFor(t, "appends", func() bool {
		stub.mu.Lock()
		defer stub.mu.Unlock()
		return stub.adds == 10
	})
	// Each enqueue is synchronous past the fan-out handoff, so every
	// threshold crossing has already compacted by now.
	stub.mu.Lock()
	live := stub.lastLive
	compacted := stub.compacted
	stub.mu.Unlock()
	if compacted < 2 {
		t.Fatalf("compacted = %d, want >= 2 for 10 appends at threshold 4", compacted)
	}
	if live != 1 {
		t.Fatalf("compacted live = %d, want 1 coalesced hint", live)
	}
	// Hints are intact: compaction only rewrites, never drops.
	if got := m.HintCount(); got != 1 {
		t.Fatalf("queue = %d, want 1", got)
	}
}
