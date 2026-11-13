package codec

import (
	"encoding/binary"
	"testing"
)

// benchAppend runs the generic-vs-writer-vs-stdlib comparison over vals.
// The U32/U64 benchmarks differ only in width, so they share this helper
// instead of duplicating the three sub-benchmark bodies.
func benchAppend[V Uint](b *testing.B, vals []V, generic func([]byte, V) []byte, viaWriter func(*Writer, V), direct func([]byte, V) []byte) {
	b.Run("generic", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			var out []byte
			for _, v := range vals {
				out = generic(out, v)
			}
			blackhole = out
		}
	})
	b.Run("writer", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			w := NewWriter(binary.LittleEndian, nil)
			for _, v := range vals {
				viaWriter(&w, v)
			}
			blackhole = w.Out()
		}
	})
	b.Run("stdlib", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			var out []byte
			for _, v := range vals {
				out = direct(out, v)
			}
			blackhole = out
		}
	})
}

// BenchmarkAppendU32 compares the generic Append (any-boxing) against the
// concrete writer and stdlib paths for 4-byte widths.
func BenchmarkAppendU32(b *testing.B) {
	vals := make([]uint32, 100)
	for i := range vals {
		vals[i] = uint32(i)
	}
	benchAppend(b, vals,
		func(dst []byte, v uint32) []byte { return Append(dst, binary.LittleEndian, v) },
		func(w *Writer, v uint32) { w.U32(v) },
		binary.LittleEndian.AppendUint32)
}

// BenchmarkAppendU64 is the 8-byte variant (versions, hashes, incarnations).
func BenchmarkAppendU64(b *testing.B) {
	vals := make([]uint64, 100)
	for i := range vals {
		vals[i] = uint64(i)
	}
	benchAppend(b, vals,
		func(dst []byte, v uint64) []byte { return Append(dst, binary.LittleEndian, v) },
		func(w *Writer, v uint64) { w.U64(v) },
		binary.LittleEndian.AppendUint64)
}

// BenchmarkReaderU32 compares Reader.U32 (interface dispatch on order)
// against a manual little-endian shift decode.
func BenchmarkReaderU32(b *testing.B) {
	var buf []byte
	for i := range uint32(100) {
		buf = binary.LittleEndian.AppendUint32(buf, i)
	}
	b.Run("reader", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			r := NewReader(binary.LittleEndian, buf)
			var sum uint32
			for range 100 {
				v, ok := r.U32()
				if !ok {
					b.Fatal("short")
				}
				sum += v
			}
			blackholeSum += uint64(sum)
		}
	})
	b.Run("manual", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			var sum uint32
			off := 0
			for range 100 {
				sum += uint32(buf[off]) | uint32(buf[off+1])<<8 | uint32(buf[off+2])<<16 | uint32(buf[off+3])<<24
				off += 4
			}
			blackholeSum += uint64(sum)
		}
	})
}

// BenchmarkReaderU64 is the 8-byte decode variant.
func BenchmarkReaderU64(b *testing.B) {
	var buf []byte
	for i := range uint64(100) {
		buf = binary.LittleEndian.AppendUint64(buf, i)
	}
	b.Run("reader", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			r := NewReader(binary.LittleEndian, buf)
			var sum uint64
			for range 100 {
				v, ok := r.U64()
				if !ok {
					b.Fatal("short")
				}
				sum += v
			}
			blackholeSum += sum
		}
	})
	b.Run("manual", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			var sum uint64
			off := 0
			for range 100 {
				sum += uint64(buf[off]) | uint64(buf[off+1])<<8 | uint64(buf[off+2])<<16 | uint64(buf[off+3])<<24 |
					uint64(buf[off+4])<<32 | uint64(buf[off+5])<<40 | uint64(buf[off+6])<<48 | uint64(buf[off+7])<<56
				off += 8
			}
			blackholeSum += sum
		}
	})
}

var (
	blackhole    []byte
	blackholeSum uint64
)
