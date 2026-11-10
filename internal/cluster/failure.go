package cluster

import (
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"github.com/rs/zerolog"

	"github.com/kraytos17/cinder/internal/log"
	"github.com/kraytos17/cinder/internal/metrics"
	"github.com/kraytos17/cinder/internal/net/protocol"
	"github.com/kraytos17/cinder/internal/obs"
)

// FailureDetector probes peers round-robin with Ping: consecutive failed
// probes (or a reply missing past the timeout) mark Suspect, and a Suspect
// persisting past the timeout escalates to Dead. A single transient
// failure never flaps the ring: suspicion needs SuspectThreshold
// consecutive failures, and success resets the streak.
type FailureDetector struct {
	clock     func() time.Time
	transport Sender
	table     *MembershipTable
	self      string
	timeout   atomic.Int64 // nanoseconds; SetSuspectTimeout is race-safe

	mu     sync.Mutex
	peers  []string
	next   int
	state  map[string]*peerState
	met    atomic.Pointer[metrics.Collector]
	logger atomic.Pointer[zerolog.Logger]
}

type peerState struct {
	pending   bool
	sentAt    time.Time
	suspected bool
	since     time.Time
	failures  int
}

// SuspectThreshold is the consecutive-failure streak that marks Suspect.
const SuspectThreshold = 2

// NewFailureDetector builds a detector over table.
func NewFailureDetector(clock func() time.Time, t Sender, table *MembershipTable, self string, suspectTimeout time.Duration) *FailureDetector {
	if clock == nil {
		clock = time.Now
	}

	fd := &FailureDetector{clock: clock, transport: t, table: table, self: self, state: map[string]*peerState{}}
	fd.timeout.Store(int64(suspectTimeout))
	return fd
}

// Start loads peers and rebuilds them on every membership change.
func (f *FailureDetector) Start() {
	f.mu.Lock()
	f.rebuildLocked()
	f.mu.Unlock()
	f.table.OnChange(func() {
		f.mu.Lock()
		defer f.mu.Unlock()
		f.rebuildLocked()
	})
}

// SetSuspectTimeout changes the timeout at runtime.
func (f *FailureDetector) SetSuspectTimeout(d time.Duration) { f.timeout.Store(int64(d)) }

// SetMetrics wires the observability collector (nil disables).
func (f *FailureDetector) SetMetrics(m *metrics.Collector) {
	f.met.Store(m)
}

// SetLogger installs the operational logger (nil selects Nop).
func (f *FailureDetector) SetLogger(l *zerolog.Logger) {
	if l == nil {
		nop := log.Nop()
		l = &nop
	}
	f.logger.Store(l)
}

func (f *FailureDetector) log() *zerolog.Logger {
	if l := f.logger.Load(); l != nil {
		return l
	}
	return &nopLogger
}

func (f *FailureDetector) suspectTimeout() time.Duration {
	return time.Duration(f.timeout.Load())
}

// Tick sweeps timed-out probes, escalates stale suspects, then probes the
// next eligible peer (not self, no pending probe, not dead).
func (f *FailureDetector) Tick() {
	now := f.clock()

	var timedOut, dead []string
	var target string
	f.mu.Lock()
	for peer, st := range f.state {
		if st.pending && now.Sub(st.sentAt) > f.suspectTimeout() {
			st.pending = false
			timedOut = append(timedOut, peer)
		}
	}

	dead = f.escalateLocked(now)
	if len(f.peers) > 0 {
		for range f.peers {
			peer := f.peers[f.next%len(f.peers)]
			f.next = (f.next + 1) % len(f.peers)
			if peer == f.self {
				continue
			}
			if st, ok := f.state[peer]; ok && st.pending {
				continue
			}
			if info, ok := f.table.Get(peer); ok && info.State == Dead {
				continue
			}

			st := f.state[peer]
			if st == nil {
				st = &peerState{}
				f.state[peer] = st
			}

			st.pending = true
			st.sentAt = now
			target = peer
			break
		}
	}

	f.mu.Unlock()
	for _, peer := range timedOut {
		f.table.MarkSuspect(peer)
	}
	for _, peer := range dead {
		f.table.MarkDead(peer)
	}
	if target == "" {
		return
	}

	obs.IfPresent(&f.met, func(m *metrics.Collector) { m.ProbeSent.Add(1) })
	if e := f.log().Debug(); e != nil {
		e.Str("target", target).Msg("probing")
	}

	res, err := f.transport.Send(target, protocol.Request{Opcode: protocol.OpPing})
	f.onProbeResult(target, err == nil && res.Status == protocol.StatusOK)
}

func (f *FailureDetector) onProbeResult(peer string, acked bool) {
	f.mu.Lock()
	st, ok := f.state[peer]
	if !ok {
		f.mu.Unlock()
		return
	}

	st.pending = false
	if acked {
		st.suspected = false
		st.failures = 0
	}

	var failures int
	if !acked {
		st.failures++
		failures = st.failures
		if !st.suspected {
			st.suspected = true
			st.since = f.clock()
		}
	}

	f.mu.Unlock()
	if acked {
		info, _ := f.table.Get(peer)
		f.table.MarkAlive(peer, info.Incarnation)
		obs.IfPresent(&f.met, func(m *metrics.Collector) { m.ProbeReceived.Add(1) })
		return
	}
	// Sustained unreachable: suspect once the streak crosses the threshold;
	// a lone transient failure is tolerated without flapping the ring.
	if failures >= SuspectThreshold {
		f.table.MarkSuspect(peer)
	}
}

// escalateLocked returns suspects older than the timeout for Dead marking
// and clears their local streak. Caller holds mu; table mutations happen
// outside the lock.
func (f *FailureDetector) escalateLocked(now time.Time) []string {
	var dead []string
	for peer, st := range f.state {
		if !st.suspected {
			continue
		}

		info, ok := f.table.Get(peer)
		if !ok || info.State != Suspect {
			continue
		}
		if now.Sub(st.since) > f.suspectTimeout() {
			dead = append(dead, peer)
		}
	}
	for _, peer := range dead {
		if st, ok := f.state[peer]; ok {
			st.suspected = false
			st.failures = 0
		}
	}

	slices.Sort(dead)
	return dead
}

func (f *FailureDetector) rebuildLocked() {
	f.peers = f.peers[:0]
	for _, m := range f.table.Snapshot() {
		if m.ID != f.self {
			f.peers = append(f.peers, m.ID)
		}
	}
	slices.Sort(f.peers)
}
