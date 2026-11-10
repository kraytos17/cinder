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

// Manager coordinates writes to the primary plus replicas. The store is
// the single version authority; every replica apply is idempotent LWW.
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
// NeedsCompaction/Compact bound the file: past the record budget the
// journal is rewritten from the live queue (mirrors the C++ 4,096 rule).
type HintJournal interface {
	AppendAdd(h Hint)
	AppendDel(target, key string, version uint64)
	NeedsCompaction() bool
	Compact(q *HintQueue) error
}

// SetHintJournal enables durable hints (nil disables).
func (m *Manager) SetHintJournal(j HintJournal) {
	m.journal = j
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

// NewManager returns a replication manager. Transport may be nil only when
// every call passes an empty replica list.
func NewManager(local *store.Store, self string, clock store.Clock, t Transport) *Manager {
	if clock == nil {
		clock = store.SystemClock{}
	}
	h := fnv.New64a()
	h.Write([]byte(self))
	return &Manager{local: local, self: self, writer: h.Sum64(), clock: clock, transport: t, hints: NewHintQueue()}
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

// WriterHash is the stable per-node LWW tiebreaker (FNV-1a of the node id).
func (m *Manager) WriterHash() uint64 { return m.writer }

// Write stores value locally, then fans out Replicate to replicas.
// Async returns after the local commit; quorum requires W acknowledgements
// including the local write and fails closed with ErrQuorum.
