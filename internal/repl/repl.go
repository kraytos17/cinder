package repl

import (
	"hash/fnv"
	"sync/atomic"

	"github.com/kraytos17/cinder/internal/log"
	"github.com/kraytos17/cinder/internal/metrics"
	"github.com/kraytos17/cinder/internal/store"

	"github.com/rs/zerolog"
	"golang.org/x/sync/singleflight"
)

// Manager coordinates writes and reads across a node's replica set. The
// local store supplies write versions; replicas apply versioned updates
// idempotently.
type Manager struct {
	local     *store.Store
	self      string
	writer    uint64
	clock     store.Clock
	transport Transport
	hints     *HintQueue
	journal   HintJournal
	replaying atomic.Bool
	flights   singleflight.Group
	met       atomic.Pointer[metrics.Collector]
	logger    atomic.Pointer[zerolog.Logger]
}

var nopLogger = log.Nop()

// HintJournal persists hints across restarts. AppendAdd records a queued
// hint; AppendDel records a successful replay so recovery drops it.
// NeedsCompaction/Compact bound journal size by rewriting it from the
// current live queue after its record threshold is reached.
type HintJournal interface {
	AppendAdd(h Hint)
	AppendDel(target, key string, version uint64)
	NeedsCompaction() bool
	Compact(q *HintQueue) error
}

type closableHintJournal interface {
	Close() error
}

// SetHintJournal enables durable hints (nil disables).
func (m *Manager) SetHintJournal(j HintJournal) {
	m.journal = j
}

// CloseHintJournal syncs and closes the optional durable hint journal. It is
// safe to call repeatedly and is a no-op when journaling is disabled.
func (m *Manager) CloseHintJournal() error {
	if j, ok := m.journal.(closableHintJournal); ok {
		return j.Close()
	}
	return nil
}

// SetMetrics wires the observability collector (nil disables).
func (m *Manager) SetMetrics(c *metrics.Collector) {
	m.met.Store(c)
}

// SetLogger installs the operational logger (nil selects Nop).
func (m *Manager) SetLogger(l *zerolog.Logger) {
	if l == nil {
		nop := log.Nop()
		l = &nop
	}
	m.logger.Store(l)
}

// log returns the installed logger (Nop when unset, so direct
// constructions stay silent without a SetLogger call).
func (m *Manager) log() *zerolog.Logger {
	if l := m.logger.Load(); l != nil {
		return l
	}
	return &nopLogger
}

// Config defines the local store, node identity, clock, and inter-node
// transport used by a replication manager. Transport may be nil only when
// every operation is given an empty replica list; a nil Clock selects the
// system clock.
type Config struct {
	Local     *store.Store
	Self      string
	Clock     store.Clock
	Transport Transport
}

// NewManager returns a replication manager configured by cfg.
func NewManager(cfg Config) *Manager {
	clock := cfg.Clock
	if clock == nil {
		clock = store.SystemClock{}
	}

	h := fnv.New64a()
	h.Write([]byte(cfg.Self))
	return &Manager{
		local: cfg.Local, self: cfg.Self, writer: h.Sum64(), clock: clock,
		transport: cfg.Transport, hints: NewHintQueue(),
	}
}

// HintCount returns the queued hint count.
func (m *Manager) HintCount() int { return m.hints.Len() }

// HintBytes returns the live estimated hint byte depth.
func (m *Manager) HintBytes() uint64 { return m.hints.Bytes() }

// HintsDropped returns the lifetime drop count.
func (m *Manager) HintsDropped() uint64 { return m.hints.Dropped() }

// SetMaxHintBytes caps hint queue estimated bytes.
func (m *Manager) SetMaxHintBytes(n uint64) { m.hints.SetMaxBytes(n) }

// Local exposes the underlying store for serving and tests.
func (m *Manager) Local() *store.Store { return m.local }

// HintQueue exposes the hint queue for journal recovery wiring.
func (m *Manager) HintQueue() *HintQueue { return m.hints }

// WriterHash is the stable per-node LWW tiebreaker, computed as FNV-1a of
// the node ID.
func (m *Manager) WriterHash() uint64 { return m.writer }
