package sim

import (
	"cmp"
	"errors"
	"fmt"
	"math/rand/v2"
	"slices"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/kraytos17/cinder/internal/net/protocol"
)

// event is one transport outcome. T is the virtual timestamp of the
// outcome: deliverAt for deliveries and delivery-time drops, send time
// for immediate failures. Out is one of:
//
//	ok:<status>      delivered; <status> is the numeric response status
//	drop:loss        dropped by the loss draw at delivery time
//	drop:partition   cut at send time, or partitioned mid-flight
//	drop:down        destination down at send time, or down mid-flight
//	err:unknown-node destination never existed in this cluster
type event struct {
	T    time.Duration
	From string
	To   string
	Op   protocol.Opcode
	Key  string
	Ver  uint64
	Out  string
}

// downstream delivers one message to its target node and reports whether
// the target is known. It is installed by the cluster after construction.
type downstream func(to string, req protocol.Request) (protocol.Response, bool)

// cut drops traffic between two node sets in both directions.
type cut struct {
	a map[string]bool
	b map[string]bool
}

func (c cut) blocks(from, to string) bool {
	return (c.a[from] && c.b[to]) || (c.a[to] && c.b[from])
}

// SimTransport is one node's view of the shared virtual network. All
// endpoints share one mutex, one virtual clock, one seeded RNG and one
// pending queue, so the whole network is deterministic given the script.
//
// Fault model (all off by default; enable per test):
//
//	down:         sends to a down node fail immediately, like a refused
//	             dial; in-flight events to a node that goes down are
//	             dropped at delivery.
//	partition:    Partition cuts both directions; sends fail immediately
//	             while a cut covers the pair, in-flight ones drop at
//	             delivery.
//	loss:         drops decided by one RNG draw per delivery.
//	jitter:      uniform [0, d] added to the base delay per send.
//	reorder:     adjacent swap of the due list at delivery (order-only;
//	             the delivered multiset is unchanged).
//
// Determinism contract: with loss, jitter and reorder disabled, every
// outcome is a pure function of the call sequence, and manager fan-out
// always issues the same set of sends per operation — so identical
// scripts produce identical logs up to arrival order, which the
// acceptance test removes by sorting. Enable faults only in tests that
// assert converged state, never log bytes.
type SimTransport struct {
	net  *net
	from string
}

// net is the shared virtual network behind all endpoints of a cluster.
type net struct {
	mu       sync.Mutex
	now      func() time.Duration
	rng      *rand.Rand
	known    map[string]bool
	down     map[string]bool
	cuts     []cut
	delay    time.Duration
	jitter   time.Duration
	lossProb float64
	reorder  bool

	pending   []pendingSend
	inflight  atomic.Int64
	seq       uint64
	delivered uint64
	dropped   uint64
	log       []event
	deliver   downstream
}

// pendingSend is a queued message plus its completion channel. The
// channel is buffered so delivery never blocks holding the network lock.
type pendingSend struct {
	at   time.Duration
	seq  uint64
	from string
	to   string
	req  protocol.Request
	done chan faultResult
}

type faultResult struct {
	res protocol.Response
	err error
}

func newNetwork(now func() time.Duration, seed uint64, deliver downstream) *net {
	return &net{
		now:     now,
		rng:     rand.New(rand.NewPCG(seed, 0x9E3779B97F4A7C15)),
		known:   make(map[string]bool),
		down:    make(map[string]bool),
		deliver: deliver,
	}
}

// Send implements transport.Sender. Unknown targets, down nodes and
// partitioned pairs fail immediately (failed dial); everything else is
// queued for virtual-time delivery and the caller parks until then.
func (t *SimTransport) Send(to string, req protocol.Request) (protocol.Response, error) {
	n := t.net
	n.mu.Lock()
	if !n.known[to] {
		n.logLocked(n.now(), t.from, to, req, "err:unknown-node")
		n.mu.Unlock()
		return protocol.Response{}, fmt.Errorf("sim: unknown node %s", to)
	}
	if n.down[to] {
		n.logLocked(n.now(), t.from, to, req, "drop:down")
		n.mu.Unlock()
		return protocol.Response{}, fmt.Errorf("sim: node down %s", to)
	}
	if n.partitionedLocked(t.from, to) {
		n.logLocked(n.now(), t.from, to, req, "drop:partition")
		n.mu.Unlock()
		return protocol.Response{}, fmt.Errorf("sim: partitioned %s", to)
	}

	d := n.delay
	if n.jitter > 0 {
		d += time.Duration(n.rng.Int64N(int64(n.jitter) + 1))
	}

	n.seq++
	ev := pendingSend{at: n.now() + d, seq: n.seq, from: t.from, to: to, req: req, done: make(chan faultResult, 1)}
	n.pending = append(n.pending, ev)
	n.inflight.Add(1)
	n.mu.Unlock()

	r := <-ev.done
	n.inflight.Add(-1)
	return r.res, r.err
}

// partitionedLocked reports whether any cut covers the pair, regardless
// of direction. Caller holds n.mu.
func (n *net) partitionedLocked(from, to string) bool {
	for _, c := range n.cuts {
		if c.blocks(from, to) {
			return true
		}
	}
	return false
}

// Partition cuts all traffic between sets a and b in both directions.
func (t *SimTransport) Partition(a, b []string) {
	n := t.net
	n.mu.Lock()
	defer n.mu.Unlock()
	c := cut{a: make(map[string]bool, len(a)), b: make(map[string]bool, len(b))}
	for _, id := range a {
		c.a[id] = true
	}
	for _, id := range b {
		c.b[id] = true
	}
	n.cuts = append(n.cuts, c)
}

// HealAll removes every partition cut. Down marks are independent and
// survive healing.
func (t *SimTransport) HealAll() {
	n := t.net
	n.mu.Lock()
	defer n.mu.Unlock()
	n.cuts = nil
}

// SetDelay sets the base per-message latency applied to every send.
func (t *SimTransport) SetDelay(d time.Duration) {
	n := t.net
	n.mu.Lock()
	defer n.mu.Unlock()
	n.delay = d
}

// SetJitter adds a uniform [0, d] sample to every send. Jitter consumes
// shared RNG draws: enable it only in verdict-asserting tests, never in
// the identical-log determinism check.
func (t *SimTransport) SetJitter(d time.Duration) {
	n := t.net
	n.mu.Lock()
	defer n.mu.Unlock()
	n.jitter = d
}

// SetLoss sets the per-delivery drop probability, drawn at delivery time.
// Like jitter, loss breaks log identity; assert converged state instead.
func (t *SimTransport) SetLoss(p float64) {
	n := t.net
	n.mu.Lock()
	defer n.mu.Unlock()
	n.lossProb = p
}

// SetReorder swaps consecutive due deliveries, exercising version and
// tie-break paths. Order-only: the delivered multiset is unchanged, so
// sorted-log comparison is unaffected.
func (t *SimTransport) SetReorder(b bool) {
	n := t.net
	n.mu.Lock()
	defer n.mu.Unlock()
	n.reorder = b
}

// Delivered returns the lifetime delivered count.
func (t *SimTransport) Delivered() uint64 {
	n := t.net
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.delivered
}

// Dropped returns the lifetime loss-drop count (fault draws only; failed
// dials never enter the queue).
func (t *SimTransport) Dropped() uint64 {
	n := t.net
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.dropped
}

// Events returns a copy of the event log in insertion order.
func (t *SimTransport) Events() []event {
	n := t.net
	n.mu.Lock()
	defer n.mu.Unlock()
	return append([]event(nil), n.log...)
}

// deliverLocked completes every event due at the current virtual time,
// oldest first with insertion order breaking ties (like the testkit bus).
// Mid-flight outages drop the event with a partition log line, matching
// a connection that broke before its bytes arrived.
func (n *net) deliverLocked() {
	var due []pendingSend
	later := make([]pendingSend, 0, len(n.pending))
	for _, ev := range n.pending {
		if ev.at <= n.now() {
			due = append(due, ev)
		} else {
			later = append(later, ev)
		}
	}

	slices.SortStableFunc(due, func(a, b pendingSend) int {
		return cmp.Compare(a.at, b.at)
	})
	if n.reorder {
		for i := 0; i+1 < len(due); i += 2 {
			due[i], due[i+1] = due[i+1], due[i]
		}
	}

	n.pending = later
	for _, ev := range due {
		if n.partitionedLocked(ev.from, ev.to) || n.down[ev.from] || n.down[ev.to] {
			kind := "drop:partition"
			if n.down[ev.to] || n.down[ev.from] {
				kind = "drop:down"
			}

			n.logLocked(ev.at, ev.from, ev.to, ev.req, kind)
			ev.done <- faultResult{err: errors.New("sim: unreachable " + ev.to)}
			continue
		}
		if n.lossProb > 0 && n.rng.Float64() < n.lossProb {
			n.dropped++
			n.logLocked(ev.at, ev.from, ev.to, ev.req, "drop:loss")
			ev.done <- faultResult{err: errors.New("sim: lost message to " + ev.to)}
			continue
		}

		res, ok := n.deliver(ev.to, ev.req)
		n.delivered++
		if !ok {
			n.logLocked(ev.at, ev.from, ev.to, ev.req, "err:unknown-node")
			ev.done <- faultResult{err: errors.New("sim: unknown node " + ev.to)}
			continue
		}

		n.logLocked(ev.at, ev.from, ev.to, ev.req, "ok:"+strconv.Itoa(int(res.Status)))
		ev.done <- faultResult{res: res}
	}
}

func (n *net) logLocked(at time.Duration, from, to string, req protocol.Request, out string) {
	n.log = append(n.log, event{
		T: at, From: from, To: to, Op: req.Opcode,
		Key: string(req.Key), Ver: req.Version, Out: out,
	})
}
