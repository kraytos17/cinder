package store

// Stats returns the authoritative byte and entry counters.
func (s *Store) Stats() (bytes, entries uint64) {
	return s.totalSize.Load(), s.totalNum.Load()
}

// Capacity returns the configured global capacity in bytes.
func (s *Store) Capacity() uint64 {
	return s.capacity.Load()
}
