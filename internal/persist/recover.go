package persist

// Recovery: WAL + snapshot replay on startup, with entry
// conversion and corruption accounting.

import (
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/kraytos17/cinder/internal/store"
)

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
