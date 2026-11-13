package persist

import (
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/kraytos17/cinder/internal/counter"
	"github.com/kraytos17/cinder/internal/store"
)

func (e Entry) toStore(now time.Time) (store.Entry, bool) {
	out := store.Entry{
		Value: append([]byte(nil), e.Value...), Version: e.Version,
		WriterNodeHash: e.Writer, Expiry: e.Expiry, Tombstone: e.Tombstone,
	}
	if !out.Expired(now) {
		return out, true
	}
	return store.Entry{}, false
}

func (e SnapEntry) toStore(now time.Time) (store.Entry, bool) {
	out := store.Entry{
		Value: append([]byte(nil), e.Value...), Version: e.Version,
		WriterNodeHash: e.Writer, Expiry: e.Expiry, Tombstone: e.Tombstone,
	}
	if !out.Expired(now) {
		return out, true
	}
	return store.Entry{}, false
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
		m.wal.Sync()
		m.wal.Close()
		m.wal = nil
	}

	m.mu.Unlock()
	now := m.clock()
	if snap := m.latestSnapshot(); snap != "" {
		r := NewSnapReader(snap)
		next, err := r.ReadStream(func(e SnapEntry) bool {
			ve, ok := e.toStore(now)
			if !ok {
				return true // skip expired, don't resurrect
			}

			m.store.PutVersioned(string(e.Key), ve)
			m.store.RestoreFreq(string(e.Key), e.Freq)
			return true
		})
		if err != nil {
			return err
		}

		m.store.RestoreVersion(next)
		if m.counters != nil {
			states, err := r.ReadCounters()
			if err != nil {
				return err
			}
			for _, state := range states {
				if err := m.counters.Merge(state.Key, state.Counter); err != nil {
					return fmt.Errorf("persist: recover counter %q from snapshot: %w", state.Key, err)
				}
			}
		}
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
				if r.Corrupted() {
					return fmt.Errorf("persist: corrupt WAL %s", path)
				}
				break // torn tail: clean crash artifact
			}
			if e.Op == OpDel {
				// Hard-erase, matching the live Remove operation: OpDel records
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
			if e.Op == OpCounterRemove {
				if m.counters != nil {
					m.counters.Remove(string(e.Key))
				}
				continue
			}
			if e.Op == OpCounterState {
				if m.counters == nil {
					continue
				}

				state, err := counter.DecodePNCounter(e.Counter)
				if err != nil {
					return fmt.Errorf("persist: invalid counter WAL record for %q: %w", e.Key, err)
				}
				if err := m.counters.Merge(string(e.Key), state); err != nil {
					return fmt.Errorf("persist: recover counter %q from WAL: %w", e.Key, err)
				}
				continue
			}
			if ve, ok := e.toStore(now); ok {
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
