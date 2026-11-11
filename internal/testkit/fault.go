package testkit

import (
	"errors"
	"math/rand/v2"
	"sync"
	"time"

	"github.com/kraytos17/cinder/internal/net/protocol"
)

// FaultTransport is a deterministic in-process message bus: the SimBus
// counterpart for Go tests. Sends queue until the test advances virtual
// time, so delay, loss, reordering, and node-down behavior are fully
// reproducible from the seed. Same seed, same calls, same schedule.
//
// Model (mirrors SimBus): down targets reject at route time (Send returns
// immediately); loss applies at delivery time (a queued message is silently
// dropped); delay shifts deliver_at; reorder swaps consecutive deliveries.
// Delivery routes the request to the wrapped Stores.
type FaultTransport struct {
	mu        sync.Mutex
	inner     *Stores
	rng       *rand.Rand
	now       time.Duration
	delay     time.Duration
	jitter    time.Duration
	nodeDelay map[string]time.Duration
	loss      float64 // [0,1]
	reorder   bool
	down      map[string]bool
	queue     []faultEvent
	delivered uint64
	dropped   uint64
}

type faultEvent struct {
	deliverAt time.Duration
	to        string
	req       protocol.Request
	done      chan faultResult
}

type faultResult struct {
	res protocol.Response
	err error
}

// NewFaultTransport wraps inner with a deterministic bus seeded by seed.
func NewFaultTransport(inner *Stores, seed uint64) *FaultTransport {
	return &FaultTransport{
		inner:     inner,
		rng:       rand.New(rand.NewPCG(seed, 0x9E3779B97F4A7C15)),
		down:      make(map[string]bool),
		nodeDelay: make(map[string]time.Duration),
	}
}

// SetLossRate sets the per-delivery drop probability in [0,1].
func (f *FaultTransport) SetLossRate(p float64) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.loss = p
}

// SetDelay sets the base delivery delay.
func (f *FaultTransport) SetDelay(d time.Duration) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.delay = d
}

// SetJitter adds a uniform [0, d) sample to every delivery delay.
func (f *FaultTransport) SetJitter(d time.Duration) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.jitter = d
}

// SetDelayFor overrides the delivery delay for one node (straggler
// tests). Falls back to the global delay when unset.
func (f *FaultTransport) SetDelayFor(node string, d time.Duration) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.nodeDelay[node] = d
}

// SetReorder swaps consecutive deliveries, exercising version/tie-break
// paths under arrival inversion.
func (f *FaultTransport) SetReorder(b bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.reorder = b
}

// SetNodeDown makes sends to id fail immediately; SetNodeUp re-admits.
func (f *FaultTransport) SetNodeDown(id string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.down[id] = true
}

// SetNodeUp re-admits id.
func (f *FaultTransport) SetNodeUp(id string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	delete(f.down, id)
}

// Pending returns the queued event count.
func (f *FaultTransport) Pending() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.queue)
}

// Delivered returns the lifetime delivered count.
func (f *FaultTransport) Delivered() uint64 {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.delivered
}

// Dropped returns the lifetime loss-drop count.
func (f *FaultTransport) Dropped() uint64 {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.dropped
}

// Send queues req for delivery at now+delay unless the target is down.
// It blocks until the test delivers the event.
func (f *FaultTransport) Send(node string, req protocol.Request) (protocol.Response, error) {
	f.mu.Lock()
	if f.down[node] {
		f.mu.Unlock()
		return protocol.Response{}, errors.New("fault: node down: " + node)
	}

	d, ok := f.nodeDelay[node]
	if !ok {
		d = f.delay
		if f.jitter > 0 {
			d += time.Duration(f.rng.Int64N(int64(f.jitter) + 1))
		}
	}

	ev := faultEvent{
		deliverAt: f.now + d,
		to:        node,
		req:       req,
		done:      make(chan faultResult, 1),
	}

	f.queue = append(f.queue, ev)
	f.mu.Unlock()
	r := <-ev.done
	return r.res, r.err
}

// Advance moves virtual time forward and delivers everything due.
func (f *FaultTransport) Advance(d time.Duration) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.now += d
	f.deliverLocked()
}

// Deliver releases every event due at the current virtual time.
func (f *FaultTransport) Deliver() {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.deliverLocked()
}

// deliverLocked completes due events in deliver_at order. Loss drops are
// decided here from the seeded stream, so the schedule is reproducible.
func (f *FaultTransport) deliverLocked() {
	var due, later []faultEvent
	for _, ev := range f.queue {
		if ev.deliverAt <= f.now {
			due = append(due, ev)
		} else {
			later = append(later, ev)
		}
	}
	// Stable order by deliver_at (insertion order breaks ties, like the
	// SimBus priority queue).
	for i := 1; i < len(due); i++ {
		j := i
		for j > 0 && due[j].deliverAt < due[j-1].deliverAt {
			due[j], due[j-1] = due[j-1], due[j]
			j--
		}
	}
	if f.reorder {
		for i := 0; i+1 < len(due); i += 2 {
			due[i], due[i+1] = due[i+1], due[i]
		}
	}

	f.queue = later
	for _, ev := range due {
		if f.loss > 0 && f.rng.Float64() < f.loss {
			f.dropped++
			ev.done <- faultResult{err: errors.New("fault: lost message to " + ev.to)}
			continue
		}

		res, err := f.inner.Send(ev.to, ev.req)
		f.delivered++
		ev.done <- faultResult{res: res, err: err}
	}
}
