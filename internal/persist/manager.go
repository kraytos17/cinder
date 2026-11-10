package persist

import (
	"sync"
	"sync/atomic"
	"time"

	"github.com/kraytos17/cinder/internal/log"
	"github.com/kraytos17/cinder/internal/store"

	"github.com/rs/zerolog"
)

const (
	walFileName     = "wal.log"
	walSegmentPre   = "wal_"
	walSegmentSuf   = ".log"
	snapshotPre     = "snapshot_"
	snapshotSuf     = ".dat"
	currentFile     = "CURRENT"
	snapshotsToKeep = 2
)

var nopLogger = log.Nop()

// Manager owns one store's durability: WAL appends, snapshot + WAL replay
// on startup, and compaction. It mirrors PersistenceManager's file layout
// (wal.log, wal_<seq>.log, snapshot_<nanos>.dat, CURRENT) so files are
// interchangeable with the C++ tree.

type Manager struct {
	dir   string
	store *store.Store
	clock func() time.Time

	mu     sync.Mutex
	logger atomic.Pointer[zerolog.Logger]
	wal    *Writer
	seq    uint64 // next sealed-segment sequence

	snapshotInterval time.Duration
	maxWALBytes      uint64
	maxWALEntries    uint64
	lastCompact      time.Time
}

// DefaultSnapshotInterval, DefaultMaxWALBytes, and DefaultMaxWALEntries
// are the auto-compact triggers (C++ defaults: 60s snapshots, 64MiB WAL,
// 10,000 WAL entries): periodic snapshots bound recovery replay, the
// budgets bound the file. Zero disables that trigger.

const (
	DefaultSnapshotInterval = 60 * time.Second
	DefaultMaxWALBytes      = 64 << 20
	DefaultMaxWALEntries    = 10000
)

// NewManager returns a manager over dir (created on demand) and store.

func NewManager(dir string, st *store.Store, clock func() time.Time) *Manager {
	if clock == nil {
		clock = time.Now
	}
	m := &Manager{
		dir:              dir,
		store:            st,
		clock:            clock,
		snapshotInterval: DefaultSnapshotInterval,
		maxWALBytes:      DefaultMaxWALBytes,
		maxWALEntries:    DefaultMaxWALEntries,
	}
	m.lastCompact = m.clock()
	return m
}

// SetSnapshotInterval changes the periodic auto-compact cadence (0 disables
// the time trigger). The countdown starts at construction/recovery.

func (m *Manager) SetSnapshotInterval(d time.Duration) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.snapshotInterval = d
}

// SetMaxWALBytes caps the active WAL past which MaybeCompact fires (0
// disables the size trigger).

func (m *Manager) SetMaxWALBytes(n uint64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.maxWALBytes = n
}

// SetMaxWALEntries caps the active WAL record count past which
// MaybeCompact fires (0 disables the count trigger, mirroring C++
// max_wal_entries).

func (m *Manager) SetMaxWALEntries(n uint64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.maxWALEntries = n
}

// SetLogger installs the operational logger (nil selects Nop).

func (m *Manager) SetLogger(l *zerolog.Logger) {
	if l == nil {
		nop := log.Nop()
		l = &nop
	}
	m.logger.Store(l)
}

// log returns the installed logger (Nop when unset).

func (m *Manager) log() *zerolog.Logger {
	if l := m.logger.Load(); l != nil {
		return l
	}
	return &nopLogger
}

// MaybeCompact compacts once when a trigger fires — the snapshot interval
// elapsed since the last compact (or startup), the active WAL passed the
// byte budget, or the record count passed the entry budget — and reports
// whether it did. The server tick calls this every second; tests drive it
// directly with a fake clock.

func (m *Manager) OnWrite(key string, e store.Entry, hardDelete bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.wal == nil {
		return
	}
	rec := Entry{Key: []byte(key)}
	if hardDelete {
		rec.Op = OpDel
		rec.Version = e.Version
		rec.Writer = e.WriterNodeHash
	} else {
		rec.Op = OpSet
		rec.Value = append([]byte(nil), e.Value...)
		rec.Version = e.Version
		rec.Writer = e.WriterNodeHash
		rec.Tombstone = e.Tombstone
		rec.Expiry = e.Expiry
	}
	_ = m.wal.Append(rec)
}

// Sync flushes kernel buffers to disk.

func (m *Manager) Sync() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.wal == nil {
		return nil
	}
	return m.wal.Sync()
}

// Close syncs and closes the active WAL.

func (m *Manager) Close() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.wal == nil {
		return nil
	}
	err := m.wal.Sync()
	if cerr := m.wal.Close(); err == nil {
		err = cerr
	}
	m.wal = nil
	return err
}

// entryFrom converts a decoded record, skipping already-expired ones.
