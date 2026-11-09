package ae_test

import (
	"strconv"
	"sync"
	"testing"

	"github.com/kraytos17/cinder/internal/ae"
	"github.com/kraytos17/cinder/internal/hashring"
	"github.com/kraytos17/cinder/internal/net/protocol"
	"github.com/kraytos17/cinder/internal/store"
)

func itoa(i int) string { return strconv.Itoa(i) }

// directTransport routes requests to in-process managers by node id,
// honoring AE opcodes like the TCP server dispatch does.
type directTransport struct {
	mu    sync.Mutex
	peers map[string]*ae.Manager
	sent  map[string]int
}

func newDirectTransport() *directTransport {
	return &directTransport{peers: map[string]*ae.Manager{}, sent: map[string]int{}}
}

func (d *directTransport) add(id string, m *ae.Manager) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.peers[id] = m
}

func (d *directTransport) Send(node string, req protocol.Request) (protocol.Response, error) {
	var m *ae.Manager
	d.mu.Lock()
	m = d.peers[node]
	d.sends(node)
	d.mu.Unlock()
	if m == nil {
		return protocol.Response{}, errUnknown(node)
	}
	switch req.Opcode {
	case protocol.OpAntiEntropyDigest:
		if string(req.Key) == ae.RefinementKey {
			st, val := m.OnBucketRefinement(req.Value)
			return protocol.Response{Status: st, Value: val, HasValue: val != nil}, nil
		}
		st, val := m.OnDigestRequest(req.Value)
		return protocol.Response{Status: st, Value: val, HasValue: val != nil}, nil
	case protocol.OpAntiEntropySync:
		st, val := m.OnSyncRequest(req.Value)
		return protocol.Response{Status: st, Value: val, HasValue: val != nil}, nil
	default:
		return protocol.Response{Status: protocol.StatusNotSupported}, nil
	}
}

func (d *directTransport) sends(node string) { d.sent[node]++ }

type unknownErr string

func (e unknownErr) Error() string { return "unknown node: " + string(e) }
func errUnknown(node string) error { return unknownErr(node) }

func testRing(t *testing.T, ids ...string) *hashring.Ring {
	t.Helper()
	r, err := hashring.New(hashring.DefaultVNodes, 0)
	if err != nil {
		t.Fatal(err)
	}
	if err := r.SetNodes(ids); err != nil {
		t.Fatal(err)
	}
	return r
}

func testManager(st *store.LRUStore, ring *hashring.Ring, self string, t *directTransport) *ae.Manager {
	return ae.NewManager(st, ring, self, t, 256, 1<<20)
}

func putAE(t *testing.T, st *store.LRUStore, key, val string, ver, writer uint64) {
	t.Helper()
	e := store.Entry{Value: []byte(val), Version: ver, WriterNodeHash: writer}
	if err := st.PutVersioned(key, e); err != nil {
		t.Fatal(err)
	}
}

func TestRoundConvergesDivergentStores(t *testing.T) {
	dt := newDirectTransport()
	ring := testRing(t, "a", "b")
	sa, sb := store.NewLRUStore(1<<20, nil), store.NewLRUStore(1<<20, nil)
	ma, mb := testManager(sa, ring, "a", dt), testManager(sb, ring, "b", dt)
	dt.add("a", ma)
	dt.add("b", mb)
	// Disjoint writes plus a conflicting key and a tombstone.
	putAE(t, sa, "only-a", "va", 1, 1)
	putAE(t, sb, "only-b", "vb", 1, 2)
	putAE(t, sa, "conflict", "old", 1, 1)
	putAE(t, sb, "conflict", "new", 9, 2)
	putAE(t, sa, "gone", "x", 3, 1)
	if err := sb.RemoveVersioned("gone", 5, 2); err != nil {
		t.Fatal(err)
	}
	if n := ma.RunRound(2); n == 0 {
		t.Log("a repaired nothing on first round (peer may push)")
	}
	if n := mb.RunRound(2); n == 0 {
		t.Log("b repaired nothing")
	}
	// Both directions again for full convergence.
	ma.RunRound(2)
	mb.RunRound(2)
	for _, key := range []string{"only-a", "only-b", "conflict", "gone"} {
		ea, oka := sa.GetVersioned(key)
		eb, okb := sb.GetVersioned(key)
		if !oka || !okb {
			t.Fatalf("%q missing: a=%v b=%v", key, oka, okb)
		}
		if string(ea.Value) != string(eb.Value) || ea.Version != eb.Version || ea.Tombstone != eb.Tombstone {
			t.Fatalf("%q diverged: %+v vs %+v", key, ea, eb)
		}
	}
	if e, _ := sa.GetVersioned("conflict"); string(e.Value) != "new" {
		t.Fatalf("LWW wrong: %+v", e)
	}
	if _, ok := sa.Get("gone"); ok {
		t.Fatal("tombstone reads live")
	}
	// Converged rounds repair nothing.
	if n := ma.RunRound(2); n != 0 {
		t.Fatalf("converged round repaired %d", n)
	}
	if n := mb.RunRound(2); n != 0 {
		t.Fatalf("converged round repaired %d", n)
	}
}

func TestRoundIBLTPath(t *testing.T) {
	dt := newDirectTransport()
	ring := testRing(t, "a", "b")
	sa, sb := store.NewLRUStore(16<<20, nil), store.NewLRUStore(16<<20, nil)
	ma, mb := testManager(sa, ring, "a", dt), testManager(sb, ring, "b", dt)
	dt.add("a", ma)
	dt.add("b", mb)
	// 40 keys engineered into one bucket forces the IBLT sketch path
	// (>= 32 rows): with per-row metadata the test would use plain rows.
	target := ae.BucketFor("iblt-seed", 256)
	var keys []string
	for i := 0; len(keys) < 40 && i < 200000; i++ {
		key := "ibltkey-" + itoa(i)
		if ae.BucketFor(key, 256) == target {
			keys = append(keys, key)
		}
	}
	if len(keys) < 40 {
		t.Fatal("cannot fill a bucket")
	}
	for i, k := range keys {
		putAE(t, sa, k, "v", uint64(i+1), 1)
	}
	ma.RunRound(2)
	mb.RunRound(2)
	ma.RunRound(2)
	missing := 0
	sb.ForEach(func(key string, _ store.Entry) { missing++ })
	if missing != len(keys) {
		t.Fatalf("b has %d/%d keys after AE", missing, len(keys))
	}
}
