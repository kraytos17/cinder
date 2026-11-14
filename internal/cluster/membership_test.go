package cluster

import (
	"testing"
	"time"
)

func TestNodeStateStringWireTokens(t *testing.T) {
	for state, want := range map[NodeState]string{
		Alive:   "alive",
		Suspect: "suspect",
		Dead:    "dead",
	} {
		if got := state.String(); got != want {
			t.Errorf("NodeState(%d).String() = %q, want %q", state, got, want)
		}
	}
}

func testClock() (func() time.Time, *time.Time) {
	now := time.Now()
	return func() time.Time { return now }, &now
}

func TestSeedSkipsSelf(t *testing.T) {
	clk, _ := testClock()
	tb := NewMembershipTable("n1", clk)
	tb.Seed([]NodeInfo{{ID: "n1"}, {ID: "n2", Host: "h", Port: 1}})
	if tb.ExpectedSize() != 2 {
		t.Fatalf("size = %d", tb.ExpectedSize())
	}
	if _, ok := tb.Get("n2"); !ok {
		t.Fatal("peer missing")
	}
}

func TestApplyRumorAdoptsAlive(t *testing.T) {
	clk, _ := testClock()
	tb := NewMembershipTable("n1", clk)
	tb.ApplyRumor(NodeInfo{ID: "n2", Host: "h", Port: 2, State: Alive, Incarnation: 0})
	n, ok := tb.Get("n2")
	if !ok || n.Host != "h" || n.Port != 2 {
		t.Fatalf("got %+v %v", n, ok)
	}
}

func TestApplyRumorIgnoresNonAliveUnknown(t *testing.T) {
	clk, _ := testClock()
	tb := NewMembershipTable("n1", clk)
	tb.ApplyRumor(NodeInfo{ID: "ghost", State: Suspect, Incarnation: 5})
	if _, ok := tb.Get("ghost"); ok {
		t.Fatal("suspect rumor must not invent a node")
	}
}

func TestApplyRumorStaleIgnored(t *testing.T) {
	clk, _ := testClock()
	tb := NewMembershipTable("n1", clk)
	tb.ApplyRumor(NodeInfo{ID: "n2", State: Alive, Incarnation: 3})
	tb.ApplyRumor(NodeInfo{ID: "n2", State: Dead, Incarnation: 2})
	if n, _ := tb.Get("n2"); n.State != Alive || n.Incarnation != 3 {
		t.Fatalf("stale rumor applied: %+v", n)
	}
}

func TestApplyRumorNewerIncarnationUpdatesAddress(t *testing.T) {
	clk, _ := testClock()
	tb := NewMembershipTable("n1", clk)
	tb.Seed([]NodeInfo{{ID: "n2", Host: "10.0.0.2", Port: 7002}})
	changes := 0
	tb.OnChange(func() { changes++ })

	// Stale/equal incarnation rumors cannot rewrite the address.
	tb.ApplyRumor(NodeInfo{ID: "n2", Host: "10.0.0.20", Port: 7020, State: Alive, Incarnation: 0})
	if got, _ := tb.Get("n2"); got.Host != "10.0.0.2" || got.Port != 7002 {
		t.Fatalf("equal-incarnation rumor changed address: %+v", got)
	}
	if changes != 0 {
		t.Fatalf("equal-incarnation rumor fired %d changes", changes)
	}

	tb.ApplyRumor(NodeInfo{ID: "n2", Host: "10.0.0.20", Port: 7020, State: Alive, Incarnation: 1})
	if got, _ := tb.Get("n2"); got.Host != "10.0.0.20" || got.Port != 7020 || got.Incarnation != 1 {
		t.Fatalf("newer-incarnation address not applied: %+v", got)
	}
	if changes != 1 {
		t.Fatalf("newer-incarnation address change fired %d callbacks, want 1", changes)
	}
}

func TestSelfIncarnationInitializationAndRefutation(t *testing.T) {
	now := time.Unix(1234, 5678)
	tb := NewMembershipTable("n1", func() time.Time { return now })
	tb.InitializeSelfIncarnation()
	self, _ := tb.Self()
	if self.Incarnation != uint64(now.UnixNano()) {
		t.Fatalf("startup incarnation = %d, want %d", self.Incarnation, now.UnixNano())
	}
	address := NodeInfo{ID: "n1", Host: "new-host", Port: 7100, State: Alive, Incarnation: self.Incarnation}
	tb.SetSelfAddress(address.Host, address.Port)

	// A peer can have seen an incarnation larger than the startup clock
	// floor. Refute it locally while preserving this process's address.
	tb.ApplyRumor(NodeInfo{ID: "n1", Host: "old-host", Port: 7000, State: Alive, Incarnation: self.Incarnation + 100})
	got, _ := tb.Self()
	if got.State != Alive || got.Incarnation != self.Incarnation+101 {
		t.Fatalf("self did not advance past observed incarnation: %+v", got)
	}
	if got.Host != "new-host" || got.Port != 7100 {
		t.Fatalf("peer overwrote self address: %+v", got)
	}
}

func TestInitializeSelfIncarnationPreservesHigherExistingValue(t *testing.T) {
	now := time.Unix(1234, 5678)
	tb := NewMembershipTable("n1", func() time.Time { return now })
	tb.MarkAlive("n1", uint64(now.UnixNano())+100)
	tb.InitializeSelfIncarnation()
	got, _ := tb.Self()
	if want := uint64(now.UnixNano()) + 100; got.Incarnation != want {
		t.Fatalf("startup reset existing incarnation: got %d, want %d", got.Incarnation, want)
	}
}

func TestApplyRumorEqualStampNoChange(t *testing.T) {
	clk, _ := testClock()
	tb := NewMembershipTable("n1", clk)
	changes := 0
	tb.OnChange(func() { changes++ })
	tb.ApplyRumor(NodeInfo{ID: "n2", State: Alive, Incarnation: 0})
	if changes != 1 {
		t.Fatalf("changes = %d", changes)
	}
	tb.ApplyRumor(NodeInfo{ID: "n2", State: Alive, Incarnation: 0})
	if changes != 1 {
		t.Fatal("equal stamp must not fire callbacks")
	}
}

func TestApplyRumorsPreservesOrderAndCoalescesCallbacks(t *testing.T) {
	clk, _ := testClock()
	newTable := func() *MembershipTable {
		tb := NewMembershipTable("n1", clk)
		tb.Seed([]NodeInfo{
			{ID: "n2", Host: "old-host", Port: 2},
			{ID: "n3", Host: "host-3", Port: 3},
		})
		return tb
	}

	rumors := []NodeInfo{
		// Newer address update, followed by a stale rumor for the same ID.
		{ID: "n2", Host: "new-host", Port: 22, State: Alive, Incarnation: 2},
		{ID: "n2", Host: "stale-host", Port: 222, State: Dead, Incarnation: 1},
		// Unknown non-alive rows are ignored; a later Alive row adopts it,
		// and the next rumor updates the adopted member in this same batch.
		{ID: "ghost", State: Suspect, Incarnation: 8},
		{ID: "ghost", Host: "ghost-host", Port: 8, State: Alive, Incarnation: 1},
		{ID: "ghost", State: Suspect, Incarnation: 2},
		// Self accusations refute in sequence, so the later higher rumor
		// must advance the local incarnation again.
		{ID: "n1", State: Dead, Incarnation: 10},
		{ID: "n1", State: Suspect, Incarnation: 20},
	}

	sequential := newTable()
	sequentialChanges := 0
	sequential.OnChange(func() { sequentialChanges++ })
	for _, rumor := range rumors {
		sequential.ApplyRumor(rumor)
	}

	batched := newTable()
	batchedChanges := 0
	callbackSawFinalState := false
	batched.OnChange(func() {
		batchedChanges++
		got, ok := batched.Get("n1")
		callbackSawFinalState = ok && got.State == Alive && got.Incarnation == 21
	})
	batched.ApplyRumors(rumors)

	if sequential.ExpectedSize() != batched.ExpectedSize() {
		t.Fatalf("sizes differ: sequential=%d batched=%d", sequential.ExpectedSize(), batched.ExpectedSize())
	}
	for _, want := range sequential.Snapshot() {
		got, ok := batched.Get(want.ID)
		if !ok || got != want {
			t.Errorf("member %q differs: sequential=%+v batched=%+v (present=%v)", want.ID, want, got, ok)
		}
	}
	if sequentialChanges != 5 {
		t.Fatalf("sequential callbacks = %d, want 5 changed rumors", sequentialChanges)
	}
	if batchedChanges != 1 {
		t.Fatalf("batched callbacks = %d, want one callback for changed view", batchedChanges)
	}
	if !callbackSawFinalState {
		t.Fatal("callback did not observe the complete final batch state")
	}

	batched.ApplyRumors([]NodeInfo{
		{ID: "n2", State: Alive, Incarnation: 2},     // equal stamp: no change
		{ID: "ghost", State: Alive, Incarnation: 1},  // stale: no change
		{ID: "unknown", State: Dead, Incarnation: 1}, // unknown non-alive: no adoption
	})
	if batchedChanges != 1 {
		t.Fatalf("no-op batch callbacks = %d, want still 1", batchedChanges)
	}
}

func TestSelfRumorRefuted(t *testing.T) {
	clk, _ := testClock()
	tb := NewMembershipTable("n1", clk)
	tb.ApplyRumor(NodeInfo{ID: "n1", State: Suspect, Incarnation: 0})
	n, _ := tb.Get("n1")
	if n.State != Alive || n.Incarnation != 1 {
		t.Fatalf("self rumor not refuted: %+v", n)
	}
	// A racing higher incarnation still loses to the refutation bump.
	tb.ApplyRumor(NodeInfo{ID: "n1", State: Dead, Incarnation: 5})
	n, _ = tb.Get("n1")
	if n.State != Alive || n.Incarnation != 6 {
		t.Fatalf("got %+v", n)
	}
}

func TestMarkTransitions(t *testing.T) {
	clk, _ := testClock()
	tb := NewMembershipTable("n1", clk)
	tb.Seed([]NodeInfo{{ID: "n2"}})
	changes := 0
	tb.OnChange(func() { changes++ })
	tb.MarkSuspect("n2")
	tb.MarkSuspect("n2") // idempotent
	if n, _ := tb.Get("n2"); n.State != Suspect {
		t.Fatal("not suspect")
	}
	tb.MarkDead("n2")
	tb.MarkDead("n2")    // idempotent
	tb.MarkSuspect("n2") // dead stays dead
	if n, _ := tb.Get("n2"); n.State != Dead {
		t.Fatal("not dead")
	}
	tb.MarkAlive("n2", 4)
	if n, _ := tb.Get("n2"); n.State != Alive || n.Incarnation != 4 {
		t.Fatalf("not revived: %+v", n)
	}
	// Alive at >= incarnation is a no-op.
	tb.MarkAlive("n2", 3)
	if n, _ := tb.Get("n2"); n.Incarnation != 4 {
		t.Fatal("incarnation regressed")
	}
	if changes != 3 {
		t.Fatalf("changes = %d, want 3", changes)
	}
}

func TestIsDegradedSingleNode(t *testing.T) {
	clk, _ := testClock()
	tb := NewMembershipTable("n1", clk)
	if tb.IsDegraded() {
		t.Fatal("lone node is its own majority")
	}
}

func TestDegradedMajority(t *testing.T) {
	clk, _ := testClock()
	tb := NewMembershipTable("n1", clk)
	tb.Seed([]NodeInfo{{ID: "n2"}, {ID: "n3"}})
	if tb.IsDegraded() {
		t.Fatal("3/3 alive must not be degraded")
	}
	tb.MarkSuspect("n2")
	if tb.IsDegraded() {
		t.Fatal("2/3 alive must not be degraded")
	}
	tb.MarkDead("n3")
	if !tb.IsDegraded() {
		t.Fatal("1/3 alive must be degraded")
	}
}

func TestQuarantine(t *testing.T) {
	now := time.Now()
	clk := func() time.Time { return now }
	tb := NewMembershipTable("n1", clk)
	tb.Seed([]NodeInfo{{ID: "n2"}})
	if !tb.IsQuarantined("n2", now.Add(time.Second), 10*time.Second) {
		t.Fatal("fresh join must be quarantined")
	}
	if tb.IsQuarantined("n2", now.Add(11*time.Second), 10*time.Second) {
		t.Fatal("old join must not be quarantined")
	}
	if tb.IsQuarantined("n2", now, 0) {
		t.Fatal("zero quarantine disables the check")
	}
	tb.MarkDead("n2")
	if tb.IsQuarantined("n2", now, 10*time.Second) {
		t.Fatal("dead node is never quarantined")
	}
	if tb.IsQuarantined("ghost", now, 10*time.Second) {
		t.Fatal("unknown node is never quarantined")
	}
}

func TestParseNodeInfoVectors(t *testing.T) {
	info, ok := ParseNodeInfo("node1@127.0.0.1:7000:alive:3")
	if !ok {
		t.Fatal("valid entry rejected")
	}
	if info.ID != "node1" || info.Host != "127.0.0.1" || info.Port != 7000 || info.State != Alive || info.Incarnation != 3 {
		t.Fatalf("got %+v", info)
	}
	if info, ok := ParseNodeInfo("n2@10.0.0.1:8080:suspect:42"); !ok || info.State != Suspect || info.Incarnation != 42 {
		t.Fatalf("suspect: %+v", info)
	}
	if info, ok := ParseNodeInfo("n3@192.168.1.1:9090:dead:0"); !ok || info.State != Dead {
		t.Fatalf("dead: %+v", info)
	}
	for _, bad := range []string{
		"", "node1:7000:alive:1", "node1@127.0.0.1", "node1@127.0.0.1:7000",
		"node1@127.0.0.1:7000:alive", "node1@127.0.0.1:abc:alive:1",
		"node1@127.0.0.1:99999:alive:1", "node1@127.0.0.1:7000:zombie:1",
		"node1@127.0.0.1:7000:alive:x",
	} {
		if _, ok := ParseNodeInfo(bad); ok {
			t.Fatalf("%q accepted", bad)
		}
	}
}

func TestEncodeDecodeRoundTrip(t *testing.T) {
	view := []NodeInfo{
		{ID: "n1", Host: "h1", Port: 1, State: Alive, Incarnation: 0},
		{ID: "n2", Host: "", Port: 0, State: Suspect, Incarnation: 9},
		{ID: "n3", Host: "h3", Port: 7000, State: Dead, Incarnation: 4},
	}
	enc := EncodeView(view)
	if enc != "n1@h1:1:alive:0;n2@:0:suspect:9;n3@h3:7000:dead:4" {
		t.Fatalf("encoded = %q", enc)
	}
	dec := DecodeView(enc + ";garbage;")
	if len(dec) != 3 {
		t.Fatalf("decoded = %+v", dec)
	}
	for i := range view {
		a, b := view[i], dec[i]
		if a.ID != b.ID || a.Host != b.Host || a.Port != b.Port || a.State != b.State || a.Incarnation != b.Incarnation {
			t.Fatalf("row %d: %+v vs %+v", i, a, b)
		}
	}
}

func TestMatchAliveIDSkipsSelfAndNonAlive(t *testing.T) {
	clk, _ := testClock()
	tb := NewMembershipTable("n1", clk)
	tb.Seed([]NodeInfo{{ID: "n2"}, {ID: "n3"}, {ID: "n4"}})
	tb.MarkSuspect("n3")
	tb.MarkDead("n4")
	if !tb.MatchAliveID("n1", func(id string) bool { return id == "n2" }) {
		t.Fatal("alive non-self member must match")
	}
	if tb.MatchAliveID("n1", func(id string) bool { return id == "n1" }) {
		t.Fatal("self must never match")
	}
	if tb.MatchAliveID("n1", func(id string) bool { return id == "n3" || id == "n4" }) {
		t.Fatal("suspect/dead members must not match")
	}
	if tb.MatchAliveID("n1", func(id string) bool { return id == "ghost" }) {
		t.Fatal("unknown member must not match")
	}
}
