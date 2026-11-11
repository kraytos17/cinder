package ae

import (
	"fmt"
	"math/rand"
	"sync"
	"testing"
	"time"

	"github.com/kraytos17/cinder/internal/metrics"
	"github.com/kraytos17/cinder/internal/store"
)

// digestEqual compares two digests bucket-wise.
func digestEqual(a, b Digest) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// TestIncrementalMatchesFullScan drives random put/overwrite/remove,
// expiry and capacity-eviction traffic through the sink and asserts the
// incremental digest equals a full scan after every batch.
func TestIncrementalMatchesFullScan(t *testing.T) {
	clock := store.NewManualClock(time.Now())
	st := store.NewLRUStore(4096, clock)
	m := NewManager(Config{Store: st, Buckets: 16, RehashInterval: -1})
	defer m.DetachSink()

	m.SetMetrics(metrics.NewCollector())
	rng := rand.New(rand.NewSource(1))
	keys := []string{"a", "b", "c", "d", "e", "f", "g", "h"}

	var ver uint64 = 1
	for round := range 200 {
		op := rng.Intn(100)
		k := keys[rng.Intn(len(keys))]
		switch {
		case op < 55:
			ver++
			if err := st.PutVersioned(k, store.Entry{Value: fmt.Appendf(nil, "v%d", ver), Version: ver, WriterNodeHash: 1}); err != nil {
				t.Fatal(err)
			}
		case op < 70:
			ver++
			if err := st.RemoveVersioned(k, ver, 1); err != nil {
				t.Fatal(err)
			}
		case op < 80:
			st.Remove(k)
		case op < 90:
			ver++
			if err := st.PutWithTTL(k, []byte("t"), time.Millisecond); err != nil {
				t.Fatal(err)
			}
			clock.Advance(2 * time.Millisecond)
			st.EvictExpired()
		default:
			// Capacity pressure: big value forces eviction through the
			// sink-emitting removeNode path.
			ver++
			_ = st.Put(fmt.Sprintf("big-%d-%d", round, rng.Intn(4)), make([]byte, 3000))
		}
		if round%5 == 0 {
			got, rehashed := m.digestForRound()
			if round == 0 && !rehashed {
				t.Fatal("first digest must rehash (cold start)")
			}
			if round > 0 && rehashed {
				t.Fatal("backstop disabled: no rehash expected after cold start")
			}
			if want := m.ComputeDigest(); !digestEqual(got, want) {
				t.Fatalf("round %d: incremental != scan", round)
			}
		}
	}
	if got, _ := m.digestForRound(); !digestEqual(got, m.ComputeDigest()) {
		t.Fatal("final incremental != scan")
	}
}

// TestIncrementalColdStart verifies the first digest triggers a rehash
// even when the accumulator happens to match (empty store).
func TestIncrementalColdStart(t *testing.T) {
	st := store.NewLRUStore(1<<20, nil)
	m := NewManager(Config{Store: st, Buckets: 8, RehashInterval: -1})
	defer m.DetachSink()
	c := metrics.NewCollector()
	m.SetMetrics(c)

	if m.accReady.Load() {
		t.Fatal("accumulator must start cold")
	}
	if _, rehashed := m.digestForRound(); !rehashed {
		t.Fatal("first digest must rehash")
	}
	if got := c.AEFullRehashes.Load(); got != 1 {
		t.Fatalf("rehashes = %d, want 1", got)
	}
	if _, rehashed := m.digestForRound(); rehashed {
		t.Fatal("second digest must be incremental")
	}
	if got := c.AEFullRehashes.Load(); got != 1 {
		t.Fatalf("rehashes = %d, want 1", got)
	}
}

// TestIncrementalBackstopMismatch corrupts one accumulator bucket and
// asserts the next due round rehashes and counts a mismatch.
func TestIncrementalBackstopMismatch(t *testing.T) {
	now := time.Now()
	cur := now
	st := store.NewLRUStore(1<<20, nil)
	m := NewManager(Config{
		Store: st, Buckets: 8, RehashInterval: time.Minute,
		Clock: func() time.Time { return cur },
	})
	defer m.DetachSink()
	c := metrics.NewCollector()
	m.SetMetrics(c)

	if err := st.Put("k", []byte("v")); err != nil {
		t.Fatal(err)
	}
	if _, rehashed := m.digestForRound(); !rehashed {
		t.Fatal("first digest must rehash (cold)")
	}
	base := c.AERehashMismatch.Load()

	// Corrupt bucket state behind the manager's back.
	m.digestMu.Lock()
	m.acc[BucketFor("k", 8)].Store(0xdeadbeef)
	m.digestMu.Unlock()

	cur = now.Add(2 * time.Minute) // backstop due
	got, rehashed := m.digestForRound()
	if !rehashed {
		t.Fatal("backstop-due digest must rehash")
	}
	if !digestEqual(got, m.ComputeDigest()) {
		t.Fatal("post-rehash digest must equal scan")
	}
	if c.AERehashMismatch.Load() != base+1 {
		t.Fatal("mismatch not counted")
	}
}

// TestIncrementalConcurrentSmoke hammers the sink from writers while
// rounds copy the accumulator; the end state must converge to a scan
// after writers stop (torn cross-bucket reads self-heal via rehash).
func TestIncrementalConcurrentSmoke(t *testing.T) {
	st := store.NewLRUStore(1<<22, nil)
	m := NewManager(Config{Store: st, Buckets: 256, RehashInterval: -1})
	defer m.DetachSink()

	var wg sync.WaitGroup
	for w := range 4 {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := range 200 {
				_ = st.Put(fmt.Sprintf("w%d-k%d", w, i), []byte("v"))
			}
		}(w)
	}
	for range 50 {
		_, _ = m.digestForRound()
	}
	wg.Wait()
	// One cold rehash is already done; force a fresh one to publish the
	// exact post-quiesce state, then incremental must match.
	m.accReady.Store(false)
	if got, _ := m.digestForRound(); !digestEqual(got, m.ComputeDigest()) {
		t.Fatal("post-quiesce incremental != scan")
	}
}
