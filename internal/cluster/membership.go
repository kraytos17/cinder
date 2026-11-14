// Package cluster implements SWIM-style membership, rumor dissemination,
// and failure detection. Membership updates are incarnation-guarded and
// nodes converge by exchanging the encoded membership view.
package cluster

import (
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/rs/zerolog"

	"github.com/kraytos17/cinder/internal/log"
	"github.com/kraytos17/cinder/internal/metrics"
	"github.com/kraytos17/cinder/internal/netport"
	"github.com/kraytos17/cinder/internal/obs"
)

// NodeState is the liveness state advertised for a cluster member.
type NodeState int

const (
	// Alive marks a reachable member; the table's default assumption.
	Alive NodeState = iota
	Suspect
	Dead
)

// String renders the wire state token; values other than Alive and Suspect
// render as "dead".
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
	Port        netport.Port
	State       NodeState
	Incarnation uint64
	JoinedAt    time.Time
}

var nopLogger = log.Nop()

// MembershipTable is the local view of cluster membership. Rumor
// application is incarnation-guarded; a node hearing a non-alive rumor
// about itself refutes it by bumping its incarnation and forcing Alive.
type MembershipTable struct {
	mu     sync.Mutex
	self   string
	nodes  map[string]*NodeInfo
	cbs    []func()
	clock  func() time.Time
	met    atomic.Pointer[metrics.Collector]
	logger atomic.Pointer[zerolog.Logger]
}

// SetMetrics installs the observability collector; nil disables metrics.
// State counters are incremented only when the state actually changes.
func (t *MembershipTable) SetMetrics(m *metrics.Collector) {
	t.met.Store(m)
}

// SetLogger installs the operational logger (nil selects Nop).
func (t *MembershipTable) SetLogger(l *zerolog.Logger) {
	if l == nil {
		nop := log.Nop()
		l = &nop
	}
	t.logger.Store(l)
}

func (t *MembershipTable) log() *zerolog.Logger {
	if l := t.logger.Load(); l != nil {
		return l
	}
	return &nopLogger
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
func (t *MembershipTable) SetSelfAddress(host string, port netport.Port) {
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
	changed := t.applyRumorLocked(rumor)
	t.mu.Unlock()
	if changed {
		t.fire()
	}
}

// ApplyRumors merges rumors in order under one table lock, preserving the
// same per-rumor rules as ApplyRumor. Observers are notified once after the
// batch if any rumor changed the table, so a single gossip view causes one
// observer rebuild rather than one rebuild per changed member.
func (t *MembershipTable) ApplyRumors(rumors []NodeInfo) {
	if len(rumors) == 0 {
		return
	}

	t.mu.Lock()
	changed := false
	for _, rumor := range rumors {
		changed = t.applyRumorLocked(rumor) || changed
	}

	t.mu.Unlock()
	if changed {
		t.fire()
	}
}

// applyRumorLocked merges one rumor. Caller holds t.mu. It deliberately
// processes exactly one rumor at a time so repeated IDs in a batch observe
// every earlier update in the same message.
func (t *MembershipTable) applyRumorLocked(rumor NodeInfo) bool {
	local, ok := t.nodes[rumor.ID]
	if !ok {
		if rumor.State != Alive {
			return false
		}

		fresh := rumor
		// Detach stored identity from the decoded view buffer: DecodeView
		// substrings share the whole message string, so storing them
		// directly would pin it for the member's lifetime. Steady-state
		// rumors never take this path.
		fresh.ID = strings.Clone(fresh.ID)
		fresh.Host = strings.Clone(fresh.Host)
		fresh.JoinedAt = t.clock()
		t.nodes[fresh.ID] = &fresh
		return true
	}
	if rumor.Incarnation < local.Incarnation {
		return false
	}
	if rumor.Incarnation == local.Incarnation && rumor.State == local.State {
		return false
	}

	var changed bool
	if rumor.ID == t.self {
		if rumor.State != Alive {
			changed = t.refuteLocked(rumor)
		} else if rumor.Incarnation > local.Incarnation {
			// A peer may still know an incarnation newer than this process
			// learned at startup. Refute it by moving strictly past that
			// incarnation; never copy the peer's potentially stale address.
			inc := nextIncarnation(rumor.Incarnation)
			if local.Incarnation != inc || local.State != Alive {
				local.Incarnation = inc
				local.State = Alive
				changed = true
			}
		}
	} else {
		newerIncarnation := rumor.Incarnation > local.Incarnation
		changed = local.State != rumor.State || local.Incarnation != rumor.Incarnation
		local.State = rumor.State
		local.Incarnation = rumor.Incarnation
		if newerIncarnation || local.Host == "" {
			// Address ownership follows the incarnation: a restart's new
			// address supersedes the old one only with a newer stamp.
			if rumor.Host != "" && local.Host != rumor.Host {
				local.Host = strings.Clone(rumor.Host)
				changed = true
			}
			if rumor.Port != 0 && local.Port != rumor.Port {
				local.Port = rumor.Port
				changed = true
			}
		}
	}
	return changed
}

func nextIncarnation(inc uint64) uint64 {
	if inc == ^uint64(0) {
		return inc
	}
	return inc + 1
}

// InitializeSelfIncarnation gives a newly started process a timestamp-sized
// incarnation before it begins gossip. If a peer has already observed an even
// newer incarnation, ApplyRumor advances self past that value before
// advertising again. The timestamp is an initial floor, not the sole
// correctness mechanism.
func (t *MembershipTable) InitializeSelfIncarnation() {
	t.mu.Lock()
	if self, ok := t.nodes[t.self]; ok {
		inc := uint64(t.clock().UnixNano())
		if inc > self.Incarnation {
			self.Incarnation = inc
		}
	}
	t.mu.Unlock()
}

// refuteLocked answers a non-alive rumor about self. Caller holds mu.
func (t *MembershipTable) refuteLocked(rumor NodeInfo) bool {
	self, ok := t.nodes[t.self]
	if !ok {
		return false
	}

	changed := false
	if self.Incarnation <= rumor.Incarnation {
		inc := nextIncarnation(rumor.Incarnation)
		if self.Incarnation != inc {
			self.Incarnation = inc
			changed = true
		}
	}
	if self.State != Alive {
		self.State = Alive
		changed = true
	}
	if changed {
		obs.IfPresent(&t.met, func(m *metrics.Collector) { m.RefutedRumors.Add(1) })
		if e := t.log().Debug(); e != nil {
			e.Str("rumor_state", rumor.State.String()).Uint64("incarnation", rumor.Incarnation).Msg("refuted self rumor")
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
	obs.IfPresent(&t.met, func(m *metrics.Collector) { m.SuspectMarked.Add(1) })
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
	obs.IfPresent(&t.met, func(m *metrics.Collector) { m.DeadMarked.Add(1) })
	// A node death is ops-relevant at info; id only, no user data.
	t.log().Info().Str("node", id).Msg("marked dead")
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
	obs.IfPresent(&t.met, func(m *metrics.Collector) { m.AliveMarked.Add(1) })
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

// MatchAliveID reports whether any alive member other than self satisfies
// match, without building a snapshot. The auth path uses it to verify
// node tokens with zero allocation; the HMAC loop runs under the table
// lock like every other accessor, so membership updates wait
// microseconds, not milliseconds.
func (t *MembershipTable) MatchAliveID(self string, match func(id string) bool) bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	for id, n := range t.nodes {
		if id == self || n.State != Alive {
			continue
		}
		if match(id) {
			return true
		}
	}
	return false
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

// OnChange registers a callback fired (without the table lock) after a
// mutation operation changes membership, so observers may safely re-enter
// the table. ApplyRumors coalesces all changes in its batch into one callback.
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
