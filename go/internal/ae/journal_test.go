package ae

import (
	"errors"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/kraytos17/cinder/go/internal/net/protocol"
	"github.com/kraytos17/cinder/go/internal/repl"
	"github.com/kraytos17/cinder/go/internal/store"
)

// fakeTransport fails named nodes; successes apply Replicate to per-node stores.
type fakeTransport struct {
	mu     sync.Mutex
	fail   map[string]bool
	stores map[string]*store.LRUStore
}

func newFakeTransport(nodes ...string) *fakeTransport {
	ft := &fakeTransport{fail: map[string]bool{}, stores: map[string]*store.LRUStore{}}
	for _, n := range nodes {
		ft.stores[n] = store.NewLRUStore(1<<20, nil)
	}
	return ft
}

func (f *fakeTransport) Send(node string, req protocol.Request) (protocol.Response, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.fail[node] {
		return protocol.Response{}, errors.New("unreachable: " + node)
	}
	st, ok := f.stores[node]
	if !ok {
		return protocol.Response{}, errors.New("unknown node: " + node)
	}
	switch req.Opcode {
	case protocol.OpReplicate, protocol.OpHint:
		e := store.Entry{Value: append([]byte(nil), req.Value...), Version: req.Version, WriterNodeHash: req.WriterNodeHash, Tombstone: req.Tombstone}
		if req.HasExpiresAt {
			e.HasTTL = true
			e.ExpiresAt = req.ExpiresAt
		}
		if req.Tombstone && !req.HasExpiresAt {
			if err := st.RemoveVersioned(string(req.Key), req.Version, req.WriterNodeHash); err != nil {
				return protocol.Response{Status: protocol.StatusInternalError}, nil
			}
			return protocol.Response{Status: protocol.StatusOK}, nil
		}
		if err := st.PutVersioned(string(req.Key), e); err != nil {
			return protocol.Response{Status: protocol.StatusCapacityExceeded}, nil
		}
		return protocol.Response{Status: protocol.StatusOK}, nil
	case protocol.OpGetVersioned:
		e, ok := st.GetVersioned(string(req.Key))
		if !ok {
			return protocol.Response{Status: protocol.StatusNotFound}, nil
		}
		res := protocol.Response{Status: protocol.StatusOK, Version: e.Version, WriterNodeHash: e.WriterNodeHash, Tombstone: e.Tombstone}
		if !e.Tombstone {
			res.Value = append([]byte(nil), e.Value...)
			res.HasValue = true
		}
		if e.HasTTL {
			res.HasExpiresAt = true
			res.ExpiresAt = e.ExpiresAt
		}
		return res, nil
	default:
		return protocol.Response{Status: protocol.StatusNotSupported}, nil
	}
}

func TestJournalReplicatesThroughManager(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "hints.log")
	ft := newFakeTransport("n2")
	m := repl.NewManager(store.NewLRUStore(1<<20, nil), "n1", nil, ft)
	j, _, err := OpenJournal(path, m.HintQueue())
	if err != nil {
		t.Fatal(err)
	}
	m.SetHintJournal(j)
	ft.fail["n2"] = true
	if err := m.Write("k", []byte("v"), 0, false, []string{"n2"}, repl.Async, 0, 0); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(5 * time.Second)
	for m.HintCount() < 1 {
		if time.Now().After(deadline) {
			t.Fatal("no hint queued")
		}
		time.Sleep(5 * time.Millisecond)
	}
	j.Sync()
	j.Close()
	// Restart recovery: the hint survives.
	q2 := repl.NewHintQueue()
	j2, n, err := OpenJournal(path, q2)
	if err != nil || n != 1 {
		t.Fatalf("recovered %d, err %v", n, err)
	}
	defer j2.Close()
	if q2.Len() != 1 {
		t.Fatalf("queue = %d", q2.Len())
	}
}

func testHint(target, key, val string, ver, writer uint64) repl.Hint {
	return repl.Hint{Target: target, Req: protocol.Request{
		Opcode: protocol.OpReplicate, Key: []byte(key), Value: []byte(val),
		Version: ver, WriterNodeHash: writer,
	}}
}

func TestJournalRoundTrip(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "hints.log")
	q := repl.NewHintQueue()
	j, n, err := OpenJournal(path, q)
	if err != nil || n != 0 {
		t.Fatalf("open: %d %v", n, err)
	}
	j.AppendAdd(testHint("n2", "a", "1", 5, 1))
	j.AppendAdd(testHint("n2", "b", "2", 6, 1))
	if err := j.Sync(); err != nil {
		t.Fatal(err)
	}
	if err := j.Close(); err != nil {
		t.Fatal(err)
	}
	q2 := repl.NewHintQueue()
	j2, n, err := OpenJournal(path, q2)
	if err != nil || n != 2 {
		t.Fatalf("recovered %d, err %v", n, err)
	}
	defer j2.Close()
	if q2.Len() != 2 {
		t.Fatalf("queue = %d", q2.Len())
	}
	// DEL removes on replay.
	j2.AppendDel("n2", "a", 5)
	if err := j2.Close(); err != nil {
		t.Fatal(err)
	}
	q3 := repl.NewHintQueue()
	if _, n, err := OpenJournal(path, q3); err != nil || n != 1 {
		t.Fatalf("after del: n=%d err=%v", n, err)
	} else if q3.Len() != 1 {
		t.Fatalf("queue = %d", q3.Len())
	}
}

func TestJournalTornTail(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "hints.log")
	q := repl.NewHintQueue()
	j, _, err := OpenJournal(path, q)
	if err != nil {
		t.Fatal(err)
	}
	j.AppendAdd(testHint("n2", "a", "1", 5, 1))
	j.AppendAdd(testHint("n2", "b", "2", 6, 2))
	j.Close()
	raw, _ := os.ReadFile(path)
	cut := raw[:len(raw)-3]
	if err := os.WriteFile(path, cut, 0644); err != nil {
		t.Fatal(err)
	}
	q2 := repl.NewHintQueue()
	if _, n, err := OpenJournal(path, q2); err != nil {
		t.Fatal(err)
	} else if n != 1 || q2.Len() != 1 {
		t.Fatalf("torn tail: n=%d len=%d", n, q2.Len())
	}
	// Appends after the torn tail land on valid history.
	q3 := repl.NewHintQueue()
	j3, _, err := OpenJournal(path, q3)
	if err != nil {
		t.Fatal(err)
	}
	defer j3.Close()
	if q3.Len() != 1 {
		t.Fatalf("len = %d", q3.Len())
	}
}

func TestJournalExpiryAndLegacy(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "hints.log")
	q := repl.NewHintQueue()
	j, _, err := OpenJournal(path, q)
	if err != nil {
		t.Fatal(err)
	}
	j.AppendAdd(testHint("n2", "a", "1", 5, 1))
	j.Close()
	// Corrupt the checksum: recovery stops there (treated as torn tail).
	raw, _ := os.ReadFile(path)
	raw[len(raw)-1] ^= 0xFF
	os.WriteFile(path, raw, 0644)
	q2 := repl.NewHintQueue()
	if _, n, err := OpenJournal(path, q2); err != nil || n != 0 || q2.Len() != 0 {
		t.Fatalf("corrupt tail: n=%d len=%d err=%v", n, q2.Len(), err)
	}
}
