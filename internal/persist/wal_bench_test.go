package persist

import (
	"fmt"
	"path/filepath"
	"testing"
)

// BenchmarkWalAppendSequential measures writing N 256-byte records to a
// fresh WAL writer on each benchmark iteration.
func BenchmarkWalAppendSequential(b *testing.B) {
	for _, n := range []int{8, 64, 512} {
		b.Run(fmt.Sprintf("n=%d", n), func(b *testing.B) {
			dir := b.TempDir()
			entries := make([]Entry, n)
			for i := range entries {
				entries[i] = Entry{Op: OpSet, Key: fmt.Appendf(nil, "bench-key-%d", i), Value: make([]byte, 256), Version: uint64(i + 1), Writer: 42}
			}
			b.SetBytes(int64(n * 256))
			b.ResetTimer()
			for i := range b.N {
				w, err := OpenWriter(filepath.Join(dir, fmt.Sprintf("seq_%d.wal", i)), true)
				if err != nil {
					b.Fatal(err)
				}
				for _, e := range entries {
					if err := w.Append(e); err != nil {
						b.Fatal(err)
					}
				}
				w.Close()
			}
		})
	}
}

// BenchmarkWalAppendBatch measures encoding and appending a batch of WAL
// records with one batch operation.
func BenchmarkWalAppendBatch(b *testing.B) {
	for _, n := range []int{8, 64, 512} {
		b.Run(fmt.Sprintf("n=%d", n), func(b *testing.B) {
			dir := b.TempDir()
			entries := make([]Entry, n)
			for i := range entries {
				entries[i] = Entry{Op: OpSet, Key: fmt.Appendf(nil, "bench-key-%d", i), Value: make([]byte, 256), Version: uint64(i + 1), Writer: 42}
			}
			b.SetBytes(int64(n * 256))
			b.ResetTimer()
			for i := range b.N {
				w, err := OpenWriter(filepath.Join(dir, fmt.Sprintf("batch_%d.wal", i)), true)
				if err != nil {
					b.Fatal(err)
				}
				if _, err := w.AppendBatch(entries); err != nil {
					b.Fatal(err)
				}
				w.Close()
			}
		})
	}
}
