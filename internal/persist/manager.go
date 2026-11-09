package persist

import (
	"cmp"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/rs/zerolog"

	"github.com/kraytos17/cinder/internal/log"
	"github.com/kraytos17/cinder/internal/store"
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
func (m *Manager) MaybeCompact() bool {
	m.mu.Lock()
	interval, maxBytes, maxEntries, last := m.snapshotInterval, m.maxWALBytes, m.maxWALEntries, m.lastCompact
	var size int64 = -1
	var entries uint64
	if m.wal != nil {
		size = m.wal.Size()
		entries = m.wal.Entries()
	}
	now := m.clock()
	m.mu.Unlock()

	due := interval > 0 && !now.Before(last.Add(interval))
	if !due && maxBytes > 0 && size >= 0 && uint64(size) >= maxBytes {
		due = true
	}
	if !due && maxEntries > 0 && entries >= maxEntries {
		due = true
	}
	if !due {
		return false
	}
	if err := m.Compact(); err != nil {
		return false
	}
	m.mu.Lock()
	m.lastCompact = m.clock()
	m.mu.Unlock()
	return true
}

// OnWrite appends one accepted mutation. It is the store persistence hook:
// synchronous, leaf-only, never calls back into the store. hardDelete
// replays as a hard remove (migration cleanup must not resurrect markers).
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
		if e.HasTTL {
			rec.HasTTL = true
			rec.ExpiresMs = uint64(e.ExpiresAt.UnixMilli())
		}
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
func entryFromWal(e Entry, now time.Time) (store.Entry, bool) {
	out := store.Entry{
		Value: append([]byte(nil), e.Value...), Version: e.Version,
		WriterNodeHash: e.Writer, HasTTL: e.HasTTL, Tombstone: e.Tombstone,
	}
	if e.HasTTL {
		out.ExpiresAt = time.UnixMilli(int64(e.ExpiresMs)).UTC()
		if !out.Expired(now) {
			return out, true
		}
		return store.Entry{}, false
	}
	return out, true
}

func entryFromSnap(e SnapEntry, now time.Time) (store.Entry, bool) {
	out := store.Entry{
		Value: append([]byte(nil), e.Value...), Version: e.Version,
		WriterNodeHash: e.Writer, HasTTL: e.HasTTL, Tombstone: e.Tombstone,
	}
	if e.HasTTL {
		out.ExpiresAt = time.UnixMilli(int64(e.ExpiresMs)).UTC()
		if !out.Expired(now) {
			return out, true
		}
		return store.Entry{}, false
	}
	return out, true
}

// Recover loads the latest snapshot then replays sealed segments plus the
// active log, truncating torn tails. It opens the active WAL in append
// mode for serving; the caller wires the store hook after Recover returns.
// Recovering twice closes the previous writer first.
func (m *Manager) Recover() error {
	if err := os.MkdirAll(m.dir, 0o755); err != nil {
		return err
	}
	m.mu.Lock()
	if m.wal != nil {
		_ = m.wal.Sync()
		_ = m.wal.Close()
		m.wal = nil
	}
	m.mu.Unlock()
	now := m.clock()
	if snap := m.latestSnapshot(); snap != "" {
		r := NewSnapReader(snap)
		next, err := r.ReadStream(func(e SnapEntry) bool {
			ve, ok := entryFromSnap(e, now)
			if !ok {
				return true // skip expired, don't resurrect
			}
			_ = m.store.PutVersioned(string(e.Key), ve)
			m.store.RestoreFreq(string(e.Key), e.Freq)
			return true
		})
		if err != nil {
			return err
		}
		m.store.RestoreVersion(next)
	}
	for _, path := range m.walFiles() {
		st, err := os.Stat(path)
		if err != nil {
			continue
		}
		r, err := OpenReader(path)
		if err != nil {
			return err
		}
		for r.HasMore() {
			e, ok := r.Next()
			if !ok {
				break // torn tail: clean crash artifact
			}
			if r.Corrupted() {
				return fmt.Errorf("persist: corrupt WAL %s", path)
			}
			if e.Op == OpDel {
				// Hard-erase, mirroring the live Remove: OpDel records
				// only ever come from migration cleanup, which runs
				// after every new holder acked — so no older live copy
				// can win by absence that a tombstone would need to
				// suppress. Replanting a tombstone here would be
				// actively harmful: it carries the migrated version,
				// ties the migrated copy, and tombstone-wins ties
				// would delete successfully migrated data on the next
				// anti-entropy round.
				m.store.Remove(string(e.Key))
				continue
			}
			if ve, ok := entryFromWal(e, now); ok {
				_ = m.store.PutVersioned(string(e.Key), ve)
			}
		}
		if valid := r.ValidSize(); valid < uint64(st.Size()) {
			if err := os.Truncate(path, int64(valid)); err != nil {
				return err
			}
		}
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	w, err := OpenWriter(filepath.Join(m.dir, walFileName), false)
	if err != nil {
		return err
	}
	m.wal = w
	m.seq = m.maxSealedSeq() + 1
	m.lastCompact = m.clock()
	return nil
}

// Compact snapshots live entries, publishes CURRENT, and rotates to a
// fresh WAL: sealed segments deleted, old snapshots pruned to the newest
// two. Mirrors PersistenceManager::compact (minus metrics).
func (m *Manager) Compact() error {
	live := m.store.LiveEntries()
	var nextVersion uint64
	snaps := make([]SnapEntry, 0, len(live))
	for _, p := range live {
		if p.Entry.Version >= nextVersion {
			nextVersion = p.Entry.Version + 1
		}
		se := SnapEntry{
			Key: []byte(p.Key), Value: append([]byte(nil), p.Entry.Value...),
			Version: p.Entry.Version, Writer: p.Entry.WriterNodeHash, Tombstone: p.Entry.Tombstone,
			Freq: p.Freq,
		}
		if p.Entry.HasTTL {
			se.HasTTL = true
			se.ExpiresMs = uint64(p.Entry.ExpiresAt.UnixMilli())
		}
		snaps = append(snaps, se)
	}
	name := fmt.Sprintf("%s%d%s", snapshotPre, m.clock().UnixNano(), snapshotSuf)
	path := filepath.Join(m.dir, name)
	if err := NewSnapWriter(path).Write(nextVersion, snaps); err != nil {
		return err
	}
	if err := os.WriteFile(filepath.Join(m.dir, currentFile), []byte(name), 0o644); err != nil {
		return err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.wal != nil {
		_ = m.wal.Sync()
		_ = m.wal.Close()
		m.wal = nil
	}
	w, err := OpenWriter(filepath.Join(m.dir, walFileName), true)
	if err != nil {
		return err
	}
	m.wal = w
	m.deleteSealed()
	m.pruneSnapshots(name)
	m.lastCompact = m.clock()
	m.log().Info().Int("entries", len(snaps)).Str("snapshot", name).Msg("compacted")
	return nil
}

func (m *Manager) latestSnapshot() string {
	if raw, err := os.ReadFile(filepath.Join(m.dir, currentFile)); err == nil {
		name := strings.TrimSpace(string(raw))
		if p := filepath.Join(m.dir, name); isSnapshotName(name) && fileExists(p) {
			return p
		}
	}
	var names []string
	entries, err := os.ReadDir(m.dir)
	if err != nil {
		return ""
	}
	for _, e := range entries {
		if isSnapshotName(e.Name()) {
			names = append(names, e.Name())
		}
	}
	if len(names) == 0 {
		return ""
	}
	sort.Strings(names)
	return filepath.Join(m.dir, names[len(names)-1])
}

func isSnapshotName(name string) bool {
	return strings.HasPrefix(name, snapshotPre) && strings.HasSuffix(name, snapshotSuf) && len(name) > len(snapshotPre)+len(snapshotSuf)
}

func fileExists(path string) bool {
	//nolint:gosec // G703: both call sites pass constants or names
	// pre-validated by isSnapshotName; no remote input reaches Stat.
	_, err := os.Stat(path)
	return err == nil
}

func (m *Manager) walFiles() []string {
	type seg struct {
		seq  uint64
		path string
	}
	var segs []seg
	entries, err := os.ReadDir(m.dir)
	if err != nil {
		return nil
	}
	for _, e := range entries {
		name := e.Name()
		if strings.HasPrefix(name, walSegmentPre) && strings.HasSuffix(name, walSegmentSuf) {
			mid := name[len(walSegmentPre) : len(name)-len(walSegmentSuf)]
			if seq, err := strconv.ParseUint(mid, 10, 64); err == nil {
				segs = append(segs, seg{seq, filepath.Join(m.dir, name)})
			}
		}
	}
	slices.SortFunc(segs, func(a, b seg) int { return cmp.Compare(a.seq, b.seq) })
	var out []string
	for _, s := range segs {
		out = append(out, s.path)
	}
	if active := filepath.Join(m.dir, walFileName); fileExists(active) {
		out = append(out, active)
	}
	return out
}

func (m *Manager) maxSealedSeq() uint64 {
	var maxSeq uint64
	entries, err := os.ReadDir(m.dir)
	if err != nil {
		return 0
	}
	for _, e := range entries {
		name := e.Name()
		if strings.HasPrefix(name, walSegmentPre) && strings.HasSuffix(name, walSegmentSuf) {
			mid := name[len(walSegmentPre) : len(name)-len(walSegmentSuf)]
			if seq, err := strconv.ParseUint(mid, 10, 64); err == nil && seq > maxSeq {
				maxSeq = seq
			}
		}
	}
	return maxSeq
}

func (m *Manager) deleteSealed() {
	entries, err := os.ReadDir(m.dir)
	if err != nil {
		return
	}
	for _, e := range entries {
		name := e.Name()
		if strings.HasPrefix(name, walSegmentPre) && strings.HasSuffix(name, walSegmentSuf) {
			_ = os.Remove(filepath.Join(m.dir, name))
		}
	}
}

func (m *Manager) pruneSnapshots(keep string) {
	var names []string
	entries, err := os.ReadDir(m.dir)
	if err != nil {
		return
	}
	for _, e := range entries {
		if isSnapshotName(e.Name()) {
			names = append(names, e.Name())
		}
	}
	sort.Strings(names)
	for len(names) > snapshotsToKeep {
		oldest := names[0]
		names = names[1:]
		if oldest == keep {
			continue
		}
		_ = os.Remove(filepath.Join(m.dir, oldest))
	}
}
