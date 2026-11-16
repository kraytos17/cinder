package persist

import (
	"cmp"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"

	"github.com/kraytos17/cinder/internal/counter"
)

// writeFileSynced writes data to path, syncs the file, and syncs dir so
// the new directory entry (or overwrite) is durable. Used for the CURRENT
// pointer, where a torn write would misdirect the next recovery.
func writeFileSynced(path string, data []byte, dir string) error {
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o644)
	if err != nil {
		return err
	}
	if _, err := f.Write(data); err != nil {
		f.Close()
		return err
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	return fsyncDir(dir)
}

func (m *Manager) MaybeCompact() bool {
	m.mu.Lock()
	interval, maxBytes, maxEntries, last := m.snapshotInterval, m.maxWALBytes, m.maxWALEntries, m.lastCompact
	var size int64 = -1
	var entries uint64
	if m.wal != nil {
		size = m.wal.Size()
		entries = m.wal.Entries()
	}

	counterStore := m.counters
	now := m.clock()
	m.mu.Unlock()
	if counterStore != nil && size >= 0 {
		counterBytes := counterStore.SizeBytes()
		maxInt64 := uint64(math.MaxInt64)
		if counterBytes > maxInt64-uint64(size) {
			size = int64(maxInt64)
		} else {
			size += int64(counterBytes)
		}
	}

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

// Compact writes a snapshot, publishes its name in CURRENT, starts a fresh
// active WAL, deletes sealed segments, and keeps the newest two snapshots.
func (m *Manager) Compact() error {
	var counterStore *counter.Store
	if err := m.store.WithPersistenceBarrier(func() error {
		m.mu.Lock()
		counterStore = m.counters
		m.mu.Unlock()
		if counterStore != nil {
			return counterStore.WithPersistenceBarrier(m.compactLocked)
		}
		return m.compactLocked()
	}); err != nil {
		return err
	}
	return nil
}

func (m *Manager) compactLocked() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	live := m.store.LiveEntries()
	counterStore := m.counters

	var nextVersion uint64
	snaps := make([]SnapEntry, 0, len(live))
	for _, p := range live {
		if p.Entry.Version >= nextVersion {
			nextVersion = p.Entry.Version + 1
		}

		// Ownership move: LiveEntries already returns Clones, so a second
		// value copy here would double memcpy on every compacted entry.
		// Key still converts string->[]byte (one alloc, required).
		se := SnapEntry{
			Key: []byte(p.Key), Value: p.Entry.Value,
			Version: p.Entry.Version, Writer: p.Entry.WriterNodeHash, Tombstone: p.Entry.Tombstone,
			Freq: p.Freq,
		}

		se.Expiry = p.Entry.Expiry
		snaps = append(snaps, se)
	}

	name := fmt.Sprintf("%s%d%s", snapshotPre, m.clock().UnixNano(), snapshotSuf)
	path := filepath.Join(m.dir, name)
	var counters []counter.BundleEntry
	if counterStore != nil {
		counters = counterStore.SortedLiveCounters()
	}

	writer := NewSnapWriter(path)
	var err error
	if counterStore != nil {
		err = writer.WriteWithCounters(nextVersion, snaps, counters)
	} else {
		err = writer.Write(nextVersion, snaps)
	}

	if err != nil {
		return err
	}
	// Durable publication: the snapshot file is already synced before
	// rename by SnapWriter.stream; sync the CURRENT pointer file and the
	// directory so a crash cannot leave a renamed snapshot unreferenced or
	// a CURRENT pointing at a missing file.
	if err := writeFileSynced(filepath.Join(m.dir, currentFile), []byte(name), m.dir); err != nil {
		return err
	}
	if m.wal != nil {
		// CURRENT now points at a durable snapshot containing all mutations
		// through the persistence barrier above. Do not rotate/delete WALs
		// unless the old active writer has both synced and closed cleanly:
		// on failure, leave it attached so subsequent writes still attempt
		// WAL append instead of silently taking the nil-WAL fast path.
		if err := m.syncActiveWAL(); err != nil {
			return fmt.Errorf("persist: sync active WAL before rotation: %w", err)
		}
		if err := m.closeActiveWAL(); err != nil {
			return fmt.Errorf("persist: close active WAL before rotation: %w", err)
		}
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

	slices.Sort(names)
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
	out := make([]string, 0, len(segs)+1)
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
			os.Remove(filepath.Join(m.dir, name))
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

	slices.Sort(names)
	for len(names) > snapshotsToKeep {
		oldest := names[0]
		names = names[1:]
		if oldest == keep {
			continue
		}
		os.Remove(filepath.Join(m.dir, oldest))
	}
}
