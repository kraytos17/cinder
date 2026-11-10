package repl

import (
	"sync"
	"testing"
	"time"

	"github.com/kraytos17/cinder/internal/store"
	"github.com/kraytos17/cinder/internal/testkit"
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
	ft := testkit.NewStores("n2")
	ft.Fail["n2"] = true
	m := NewManager(Config{Local: store.NewLRUStore(1<<20, nil), Self: "n1", Clock: nil, Transport: ft})
	stub := &stubJournal{}
	m.SetHintJournal(stub)
	for range 10 {
		if err := m.Write("k", []byte("v"), 0, []string{"n2"}, Async, 0, 0); err != nil {
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
