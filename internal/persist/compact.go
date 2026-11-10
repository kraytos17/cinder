package persist

// Compaction: trigger evaluation, snapshot + WAL rewrite, and
// sealed-segment / snapshot pruning.

import (
	"cmp"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
)

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
	slices.Sort(names)
	for len(names) > snapshotsToKeep {
		oldest := names[0]
		names = names[1:]
		if oldest == keep {
			continue
		}
		_ = os.Remove(filepath.Join(m.dir, oldest))
	}
}
