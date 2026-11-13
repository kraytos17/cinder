package persist

import (
	"fmt"
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

// Manager owns persistence for one store: it appends mutations to the WAL,
// replays snapshots and WAL records during recovery, and compacts the log.
// Its directory contains wal.log, sealed wal_<seq>.log segments,
// snapshot_<nanos>.dat files, and a CURRENT snapshot pointer.
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
	fsyncMode        FsyncMode
	fsyncInterval    time.Duration
	dirty            bool
	syncWAL          func() error
}

// FsyncMode selects when WAL appends are synced to stable storage.
type FsyncMode uint8

const (
	// FsyncNone preserves the historical behavior: append writes are not
	// synced until explicit Sync, Compact, or Close.
	FsyncNone FsyncMode = iota
	// FsyncAlways syncs each successful append before acknowledging it.
	FsyncAlways
	// FsyncInterval syncs periodically, independent of append activity.
	FsyncInterval
)

// DefaultSnapshotInterval, DefaultMaxWALBytes, and DefaultMaxWALEntries
// control automatic compaction. Periodic snapshots bound recovery replay;
// byte and record limits bound the active WAL. Zero disables a trigger.
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
// MaybeCompact fires (0 disables the count trigger).
func (m *Manager) SetMaxWALEntries(n uint64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.maxWALEntries = n
}

// SetFsyncPolicy configures WAL syncing. Interval mode relies on the server's
// periodic sync loop; Close always syncs.
func (m *Manager) SetFsyncPolicy(mode FsyncMode, interval time.Duration) error {
	if mode > FsyncInterval {
		return fmt.Errorf("persist: invalid fsync mode %d", mode)
	}
	if mode == FsyncInterval && interval <= 0 {
		return fmt.Errorf("persist: fsync interval must be positive")
	}
	if mode != FsyncInterval && interval != 0 {
		return fmt.Errorf("persist: fsync interval is only valid in interval mode")
	}

	m.mu.Lock()
	defer m.mu.Unlock()
	m.fsyncMode = mode
	m.fsyncInterval = interval
	return nil
}

// FsyncInterval returns the configured interval, or zero when periodic sync
// is disabled.
func (m *Manager) FsyncInterval() time.Duration {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.fsyncMode != FsyncInterval {
		return 0
	}
	return m.fsyncInterval
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

func (m *Manager) OnWrite(key string, e store.Entry, hardDelete bool) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.wal == nil {
		return nil
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

	if err := m.wal.Append(rec); err != nil {
		m.log().Error().Err(err).Str("key", key).Msg("WAL append failed after in-memory mutation")
		return err
	}
	switch m.fsyncMode {
	case FsyncAlways:
		if err := m.syncActiveWAL(); err != nil {
			m.log().Error().Err(err).Str("key", key).Msg("WAL sync failed after in-memory mutation")
			return err
		}
	case FsyncInterval:
		m.dirty = true
	case FsyncNone:
		// Append without syncing; explicit Sync, Compact, or Close flushes.
	}
	return nil
}

// Sync flushes kernel buffers to disk.
func (m *Manager) Sync() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.wal == nil {
		return nil
	}
	if err := m.syncActiveWAL(); err != nil {
		return err
	}

	m.dirty = false
	return nil
}

// SyncPeriodic flushes the active WAL. Interval mode syncs periodically even
// while idle, so its durability window has an explicit upper bound.
func (m *Manager) SyncPeriodic() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.wal == nil {
		return nil
	}
	if err := m.syncActiveWAL(); err != nil {
		return err
	}

	m.dirty = false
	return nil
}

func (m *Manager) syncActiveWAL() error {
	if m.syncWAL != nil {
		return m.syncWAL()
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

	err := m.syncActiveWAL()
	if err == nil {
		m.dirty = false
	}
	if cerr := m.wal.Close(); err == nil {
		err = cerr
	}

	m.wal = nil
	return err
}
