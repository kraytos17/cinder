package ae

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/kraytos17/cinder/internal/expiry"
	"github.com/kraytos17/cinder/internal/net/protocol"
	"github.com/kraytos17/cinder/internal/repl"
	"github.com/kraytos17/cinder/internal/store"
	"github.com/kraytos17/cinder/internal/testkit"
)

func TestJournalReplicatesThroughManager(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "hints.log")
	ft := testkit.NewStores("n2")
	m := repl.NewManager(repl.Config{Local: store.NewLRUStore(1<<20, nil), Self: "n1", Clock: nil, Transport: ft})
	j, _, err := OpenJournal(path, m.HintQueue())
	if err != nil {
		t.Fatal(err)
	}
	m.SetHintJournal(j)
	ft.Fail["n2"] = true
	if err := m.Write("k", []byte("v"), 0, []string{"n2"}, repl.Async, 0, 0); err != nil {
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
	if err := os.WriteFile(path, cut, 0o644); err != nil {
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
	os.WriteFile(path, raw, 0o644)
	q2 := repl.NewHintQueue()
	if _, n, err := OpenJournal(path, q2); err != nil || n != 0 || q2.Len() != 0 {
		t.Fatalf("corrupt tail: n=%d len=%d err=%v", n, q2.Len(), err)
	}
}

func TestJournalDropsExpiredEntryTTLOnRecovery(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "hints.log")
	q := repl.NewHintQueue()
	j, _, err := OpenJournal(path, q)
	if err != nil {
		t.Fatal(err)
	}
	defer j.Close()
	h := testHint("n2", "expired", "v", 5, 1)
	h.Req.ExpiresAt = expiry.At(time.UnixMilli(1))
	// Keep the journal-level expiry in the future so this specifically tests
	// the entry TTL, not the hint retry TTL.
	j.mu.Lock()
	_, err = j.f.Write(encodeJournalAdd("n2", h.Req, uint64(time.Now().Add(time.Hour).UnixMilli())))
	if err == nil {
		j.records++
	}
	j.mu.Unlock()
	if err != nil {
		t.Fatal(err)
	}
	if err := j.Close(); err != nil {
		t.Fatal(err)
	}

	recovered := repl.NewHintQueue()
	j2, n, err := OpenJournal(path, recovered)
	if err != nil || n != 0 || recovered.Len() != 0 {
		t.Fatalf("recovered expired hint: queued=%d err=%v", n, err)
	}
	j2.Close()
}

func TestJournalCompactDropsDels(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "hints.log")
	q := repl.NewHintQueue()
	j, _, err := OpenJournal(path, q)
	if err != nil {
		t.Fatal(err)
	}
	defer j.Close()
	mkLive := func(key string) repl.Hint {
		h := testHint("n2", key, "v", 5, 1)
		h.ExpiresAt = expiry.At(time.Now().Add(time.Hour))
		return h
	}
	for _, k := range []string{"a", "b", "c", "d", "e"} {
		h := mkLive(k)
		j.AppendAdd(h)
		q.Push(h)
	}
	before, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	j.AppendDel("n2", "b", 5)
	j.AppendDel("n2", "d", 5)
	q.RemoveMatch("n2", "b", 5)
	q.RemoveMatch("n2", "d", 5)
	if err := j.Compact(q); err != nil {
		t.Fatal(err)
	}
	if got := j.Records(); got != 3 {
		t.Fatalf("records = %d, want 3 live", got)
	}
	after, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if after.Size() >= before.Size() {
		t.Fatalf("compacted file did not shrink: %d -> %d", before.Size(), after.Size())
	}
	// Reopen: only live hints come back, and the file stays valid.
	if err := j.Close(); err != nil {
		t.Fatal(err)
	}
	q2 := repl.NewHintQueue()
	j2, n, err := OpenJournal(path, q2)
	if err != nil || n != 3 {
		t.Fatalf("recovered %d, err %v", n, err)
	}
	defer j2.Close()
	if q2.Len() != 3 {
		t.Fatalf("queue = %d", q2.Len())
	}
}

func TestJournalAutoCompactsPastThreshold(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "hints.log")
	q := repl.NewHintQueue()
	j, _, err := OpenJournal(path, q)
	if err != nil {
		t.Fatal(err)
	}
	defer j.Close()
	// Same key with rising versions: the queue coalesces to one live
	// hint, but every enqueue appends a record, so the threshold trips.
	const total = JournalCompactRecords + 100
	for i := range total {
		h := testHint("n2", "k", "v", uint64(i+1), 1)
		h.ExpiresAt = expiry.At(time.Now().Add(time.Hour))
		q.Push(h)
		j.AppendAdd(h)
		if j.NeedsCompaction() {
			if err := j.Compact(q); err != nil {
				t.Fatal(err)
			}
		}
	}
	// The file must hold only post-compact appends: 1 live record at the
	// compact plus the 100 appends after it — not all 4k+ records.
	if got := j.Records(); got != 101 {
		t.Fatalf("records = %d, want 101 (1 live + 100 post-compact)", got)
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if len(raw) > 16384 {
		t.Fatalf("journal did not stay bounded: %d bytes", len(raw))
	}
	// Reopen: the single live hint survives at the latest version.
	if err := j.Close(); err != nil {
		t.Fatal(err)
	}
	q2 := repl.NewHintQueue()
	j2, n, err := OpenJournal(path, q2)
	if err != nil || n != 1 {
		t.Fatalf("recovered %d, err %v", n, err)
	}
	defer j2.Close()
	found := false
	for _, h := range q2.Snapshot(time.Now(), 16) {
		if string(h.Req.Key) == "k" && h.Req.Version == total {
			found = true
		}
	}
	if !found {
		t.Fatal("latest version missing after compact + reopen")
	}
}
