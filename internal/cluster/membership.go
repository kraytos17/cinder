// Package cluster implements SWIM-style membership (Go port).
//
// Behavior mirrors include/cinder/cluster (membership table, gossip
// dissemination, failure detector) case-for-case, including the text wire
// format, so Go and C++ nodes converge on one view of the cluster.
package cluster

import (
	"sync"
	"sync/atomic"
	"time"

	"github.com/kraytos17/cinder/internal/metrics"
)

// NodeState mirrors the C++ NodeState.
type NodeState int

const (
	Alive NodeState = iota
	Suspect
	Dead
)

// String renders the wire state token.
func (s NodeState) String() string {
	switch s {
	case Alive:
		return "alive"
	case Suspect:
		return "suspect"
	default:
		return "dead"
	}
}

// NodeInfo is one member's advertised identity.
type NodeInfo struct {
	ID          string
	Host        string
	Port        uint16
	State       NodeState
	Incarnation uint64
	JoinedAt    time.Time
}

// MembershipTable is the local view of cluster membership. Rumor
// application is incarnation-guarded; a node hearing a non-alive rumor
// about itself refutes it by bumping its incarnation and forcing Alive.
type MembershipTable struct {
	mu    sync.Mutex
	self  string
	nodes map[string]*NodeInfo
	cbs   []func()
	clock func() time.Time
	met   atomic.Pointer[metrics.Collector]
}

// SetMetrics wires the observability collector (nil disables). Marks and
// refutations bump only on actual state changes, mirroring C++.
func (t *MembershipTable) SetMetrics(m *metrics.Collector) {
	t.met.Store(m)
}

// NewMembershipTable returns a table containing only self (alive, inc 0).
func NewMembershipTable(self string, clock func() time.Time) *MembershipTable {
	if clock == nil {
		clock = time.Now
	}
	t := &MembershipTable{
		self:  self,
		nodes: map[string]*NodeInfo{},
		clock: clock,
	}
	t.nodes[self] = &NodeInfo{ID: self, State: Alive}
	return t
}

// Seed inserts static peers as Alive/0, skipping self.
func (t *MembershipTable) Seed(peers []NodeInfo) {
	t.mu.Lock()
	for _, p := range peers {
		if p.ID == t.self {
			continue
		}
		cp := p
		cp.State = Alive
		cp.Incarnation = 0
		cp.JoinedAt = t.clock()
		t.nodes[p.ID] = &cp
	}
	t.mu.Unlock()
}

// SetSelfAddress records this node's reachable address so gossip
// advertises it for peers to adopt and migrate keys to.
func (t *MembershipTable) SetSelfAddress(host string, port uint16) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if n, ok := t.nodes[t.self]; ok {
		n.Host = host
		n.Port = port
	}
}

// ApplyRumor merges one rumor, ignoring stale incarnations. New ids are
// adopted only from Alive rumors; equal-stamp no-ops fire no callbacks.
func (t *MembershipTable) ApplyRumor(rumor NodeInfo) {
	t.mu.Lock()
	local, ok := t.nodes[rumor.ID]
	if !ok {
		if rumor.State != Alive {
			t.mu.Unlock()
			return
		}
		fresh := rumor
		fresh.JoinedAt = t.clock()
		t.nodes[rumor.ID] = &fresh
		t.mu.Unlock()
		t.fire()
		return
	}
	if rumor.Incarnation < local.Incarnation {
		t.mu.Unlock()
		return
	}
	if rumor.Incarnation == local.Incarnation && rumor.State == local.State {
		t.mu.Unlock()
		return
	}
	var changed bool
	if rumor.ID == t.self && rumor.State != Alive {
		changed = t.refuteLocked(rumor)
	} else {
		changed = local.State != rumor.State || local.Incarnation != rumor.Incarnation
		local.State = rumor.State
		local.Incarnation = rumor.Incarnation
		if local.Host == "" && rumor.Host != "" {
			local.Host = rumor.Host
		}
		if local.Port == 0 && rumor.Port != 0 {
			local.Port = rumor.Port
		}
	}
	t.mu.Unlock()
	if changed {
		t.fire()
	}
}

// refuteLocked answers a non-alive rumor about self. Caller holds mu.
func (t *MembershipTable) refuteLocked(rumor NodeInfo) bool {
	self, ok := t.nodes[t.self]
	if !ok {
		return false
	}
	changed := false
	if self.Incarnation <= rumor.Incarnation {
		if rumor.Incarnation > self.Incarnation {
			self.Incarnation = rumor.Incarnation
		}
		self.Incarnation++
		changed = true
	}
	if self.State != Alive {
		self.State = Alive
		changed = true
	}
	if changed {
		if m := t.met.Load(); m != nil {
			m.RefutedRumors.Add(1)
		}
	}
	return changed
}

// MarkSuspect flags id suspect (never from Dead; idempotent).
func (t *MembershipTable) MarkSuspect(id string) {
	t.mu.Lock()
	n, ok := t.nodes[id]
	if !ok || n.State == Dead || n.State == Suspect {
		t.mu.Unlock()
		return
	}
	n.State = Suspect
	t.mu.Unlock()
	if m := t.met.Load(); m != nil {
		m.SuspectMarked.Add(1)
	}
	t.fire()
}

// MarkDead flags id dead (idempotent).
func (t *MembershipTable) MarkDead(id string) {
	t.mu.Lock()
	n, ok := t.nodes[id]
	if !ok || n.State == Dead {
		t.mu.Unlock()
		return
	}
	n.State = Dead
	t.mu.Unlock()
	if m := t.met.Load(); m != nil {
		m.DeadMarked.Add(1)
	}
	t.fire()
}

// MarkAlive flags id alive at incarnation; recovering nodes restart their
// quarantine window. Already-alive at >= incarnation is a no-op.
func (t *MembershipTable) MarkAlive(id string, incarnation uint64) {
	t.mu.Lock()
	n, ok := t.nodes[id]
	if !ok {
		t.mu.Unlock()
		return
	}
	if n.State == Alive && n.Incarnation >= incarnation {
		t.mu.Unlock()
		return
	}
	if n.State != Alive {
		n.JoinedAt = t.clock()
	}
	n.State = Alive
	n.Incarnation = incarnation
	t.mu.Unlock()
	if m := t.met.Load(); m != nil {
		m.AliveMarked.Add(1)
	}
	t.fire()
}

// Get returns a copy of one member, or false when unknown.
func (t *MembershipTable) Get(id string) (NodeInfo, bool) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if n, ok := t.nodes[id]; ok {
		return *n, true
	}
	return NodeInfo{}, false
}

// Self returns this node's own entry.
func (t *MembershipTable) Self() (NodeInfo, bool) {
	return t.Get(t.self)
}

// SelfID returns this node's id.
func (t *MembershipTable) SelfID() string { return t.self }

// AliveCount counts Alive members.
func (t *MembershipTable) AliveCount() int {
	t.mu.Lock()
	defer t.mu.Unlock()
	n := 0
	for _, m := range t.nodes {
		if m.State == Alive {
			n++
		}
	}
	return n
}

// ExpectedSize returns total known members.
func (t *MembershipTable) ExpectedSize() int {
	t.mu.Lock()
	defer t.mu.Unlock()
	return len(t.nodes)
}

// IsDegraded reports fewer than a majority visible.
func (t *MembershipTable) IsDegraded() bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	total := len(t.nodes)
	if total == 0 {
		return true
	}
	visible := 0
	for _, m := range t.nodes {
		if m.State == Alive {
			visible++
		}
	}
	return visible < total/2+1
}

// Snapshot returns a point-in-time copy of every member.
func (t *MembershipTable) Snapshot() []NodeInfo {
	t.mu.Lock()
	defer t.mu.Unlock()
	out := make([]NodeInfo, 0, len(t.nodes))
	for _, m := range t.nodes {
		out = append(out, *m)
	}
	return out
}

// AliveIDs returns the ids of Alive members in snapshot order.
func (t *MembershipTable) AliveIDs() []string {
	var out []string
	for _, m := range t.Snapshot() {
		if m.State == Alive {
			out = append(out, m.ID)
		}
	}
	return out
}

// IsQuarantined reports whether id joined (or re-joined) within the
// quarantine window. Zero quarantine disables the check.
func (t *MembershipTable) IsQuarantined(id string, now time.Time, quarantine time.Duration) bool {
	if quarantine <= 0 {
		return false
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	n, ok := t.nodes[id]
	if !ok || n.State != Alive {
		return false
	}
	return now.Sub(n.JoinedAt) < quarantine
}

// OnChange registers a callback fired (without the table lock) on every
// membership change, so observers may safely re-enter the table.
func (t *MembershipTable) OnChange(cb func()) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.cbs = append(t.cbs, cb)
}

func (t *MembershipTable) fire() {
	t.mu.Lock()
	cbs := append([]func(){}, t.cbs...)
	t.mu.Unlock()
	for _, cb := range cbs {
		cb()
	}
}
