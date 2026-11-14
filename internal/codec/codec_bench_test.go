package codec

import (
	"encoding/binary"
	"fmt"
	"testing"
)

var (
	codecBenchBytes []byte
	codecBenchU64   uint64
)

type benchAppendCase struct {
	name        string
	order       binary.ByteOrder
	appendOrder binary.AppendByteOrder
}

func benchmarkAppendWidth(b *testing.B, order benchAppendCase, width int) {
	capacity := width * 100
	buf := make([]byte, 0, capacity)
	b.ReportAllocs()
	for _, impl := range []string{"generic", "concrete", "byteorder-interface", "writer", "stdlib"} {
		b.Run(impl, func(b *testing.B) {
			vals := [100]uint64{}
			for i := range vals {
				vals[i] = uint64(i)*0x01020304050607 + 17
			}
			b.ReportAllocs()
			b.ResetTimer()
			for b.Loop() {
				out := buf[:0]
				var w Writer
				if impl == "writer" {
					w = NewWriter(order.order, out)
				}
				for _, value := range vals {
					switch width {
					case 4:
						v := uint32(value)
						switch impl {
						case "generic":
							out = Append(out, order.order, v)
						case "concrete":
							out = AppendU32(out, order.order, v)
						case "byteorder-interface":
							out = order.appendOrder.AppendUint32(out, v)
						case "writer":
							w.U32(v)
						case "stdlib":
							if order.name == "little" {
								out = binary.LittleEndian.AppendUint32(out, v)
							} else {
								out = binary.BigEndian.AppendUint32(out, v)
							}
						}
					case 8:
						switch impl {
						case "generic":
							out = Append(out, order.order, value)
						case "concrete":
							out = AppendU64(out, order.order, value)
						case "byteorder-interface":
							out = order.appendOrder.AppendUint64(out, value)
						case "writer":
							w.U64(value)
						case "stdlib":
							if order.name == "little" {
								out = binary.LittleEndian.AppendUint64(out, value)
							} else {
								out = binary.BigEndian.AppendUint64(out, value)
							}
						}
					}
				}
				if impl == "writer" {
					out = w.Out()
				}
				codecBenchBytes = out
			}
		})
	}
}

// BenchmarkAppendU32 compares generic and concrete append paths to stdlib,
// all into capacity-reused 100-field frames in both byte orders.
func BenchmarkAppendU32(b *testing.B) {
	for _, order := range []benchAppendCase{
		{"little", binary.LittleEndian, binary.LittleEndian},
		{"big", binary.BigEndian, binary.BigEndian},
	} {
		b.Run(order.name, func(b *testing.B) { benchmarkAppendWidth(b, order, 4) })
	}
}

// BenchmarkAppendU64 is the 8-byte version of BenchmarkAppendU32.
func BenchmarkAppendU64(b *testing.B) {
	for _, order := range []benchAppendCase{
		{"little", binary.LittleEndian, binary.LittleEndian},
		{"big", binary.BigEndian, binary.BigEndian},
	} {
		b.Run(order.name, func(b *testing.B) { benchmarkAppendWidth(b, order, 8) })
	}
}

// BenchmarkAppendWidth covers scalar append costs without amortizing call
// overhead over 100 values. This reveals the per-field cost relevant to
// small headers and record encoders.
func BenchmarkAppendWidth(b *testing.B) {
	for _, order := range []struct {
		name  string
		order binary.ByteOrder
	}{
		{"little", binary.LittleEndian},
		{"big", binary.BigEndian},
	} {
		b.Run(order.name, func(b *testing.B) {
			b.Run("u8", func(b *testing.B) {
				buf := make([]byte, 0, 1)
				b.ReportAllocs()
				for b.Loop() {
					codecBenchBytes = AppendU8(buf[:0], 0xab)
				}
			})
			b.Run("u16", func(b *testing.B) {
				buf := make([]byte, 0, 2)
				b.ReportAllocs()
				for b.Loop() {
					codecBenchBytes = AppendU16(buf[:0], order.order, 0xabcd)
				}
			})
			b.Run("u32", func(b *testing.B) {
				buf := make([]byte, 0, 4)
				b.ReportAllocs()
				for b.Loop() {
					codecBenchBytes = AppendU32(buf[:0], order.order, 0x12345678)
				}
			})
			b.Run("u64", func(b *testing.B) {
				buf := make([]byte, 0, 8)
				b.ReportAllocs()
				for b.Loop() {
					codecBenchBytes = AppendU64(buf[:0], order.order, 0x1122334455667788)
				}
			})
		})
	}
}

// BenchmarkReaderScalar compares checked Reader reads against a checked
// stdlib baseline with identical length validation and cursor advancement.
func BenchmarkReaderScalar(b *testing.B) {
	for _, order := range []struct {
		name  string
		order binary.ByteOrder
	}{
		{"little", binary.LittleEndian},
		{"big", binary.BigEndian},
	} {
		for _, width := range []string{"u8", "u32", "u64"} {
			b.Run(fmt.Sprintf("%s/%s/reader", order.name, width), func(b *testing.B) {
				buf := make([]byte, 0, 512)
				for range 100 {
					switch width {
					case "u8":
						buf = AppendU8(buf, 0x88)
					case "u32":
						buf = AppendU32(buf, order.order, 0x11223344)
					case "u64":
						buf = AppendU64(buf, order.order, 0x1122334455667788)
					}
				}
				b.ReportAllocs()
				for b.Loop() {
					r := NewReader(order.order, buf)
					var sum uint64
					for range 100 {
						switch width {
						case "u8":
							v, ok := r.U8()
							if !ok {
								b.Fatal("short u8")
							}
							sum += uint64(v)
						case "u32":
							v, ok := r.U32()
							if !ok {
								b.Fatal("short u32")
							}
							sum += uint64(v)
						case "u64":
							v, ok := r.U64()
							if !ok {
								b.Fatal("short u64")
							}
							sum += v
						}
					}
					codecBenchU64 = sum
				}
			})
			b.Run(fmt.Sprintf("%s/%s/stdlib-checked", order.name, width), func(b *testing.B) {
				widthBytes := map[string]int{"u8": 1, "u32": 4, "u64": 8}[width]
				buf := make([]byte, 0, 800)
				for range 100 {
					switch width {
					case "u8":
						buf = AppendU8(buf, 0x88)
					case "u32":
						buf = AppendU32(buf, order.order, 0x11223344)
					case "u64":
						buf = AppendU64(buf, order.order, 0x1122334455667788)
					}
				}
				b.ReportAllocs()
				for b.Loop() {
					off := 0
					var sum uint64
					for range 100 {
						if len(buf)-off < widthBytes {
							b.Fatal("short input")
						}
						switch width {
						case "u8":
							sum += uint64(buf[off])
						case "u32":
							sum += uint64(order.order.Uint32(buf[off:]))
						case "u64":
							sum += order.order.Uint64(buf[off:])
						}
						off += widthBytes
					}
					codecBenchU64 = sum
				}
			})
		}
	}
}

// BenchmarkReaderU32/64 retain the earlier hand-shift comparison, but both
// paths use the same bounds checks and offsets. The result determines
// whether manual decoding merits production complexity.
func BenchmarkReaderManual(b *testing.B) {
	for _, width := range []int{4, 8} {
		b.Run(fmt.Sprintf("width=%d/reader", width), func(b *testing.B) {
			buf := make([]byte, 0, width*100)
			for i := range 100 {
				if width == 4 {
					buf = binary.LittleEndian.AppendUint32(buf, uint32(i))
				} else {
					buf = binary.LittleEndian.AppendUint64(buf, uint64(i))
				}
			}
			b.ReportAllocs()
			for b.Loop() {
				r := NewReader(binary.LittleEndian, buf)
				var sum uint64
				for range 100 {
					if width == 4 {
						v, ok := r.U32()
						if !ok {
							b.Fatal("short")
						}
						sum += uint64(v)
					} else {
						v, ok := r.U64()
						if !ok {
							b.Fatal("short")
						}
						sum += v
					}
				}
				codecBenchU64 = sum
			}
		})
		b.Run(fmt.Sprintf("width=%d/manual-checked", width), func(b *testing.B) {
			buf := make([]byte, 0, width*100)
			for i := range 100 {
				if width == 4 {
					buf = binary.LittleEndian.AppendUint32(buf, uint32(i))
				} else {
					buf = binary.LittleEndian.AppendUint64(buf, uint64(i))
				}
			}
			b.ReportAllocs()
			for b.Loop() {
				off := 0
				var sum uint64
				for range 100 {
					if len(buf)-off < width {
						b.Fatal("short")
					}
					if width == 4 {
						sum += uint64(uint32(buf[off]) | uint32(buf[off+1])<<8 | uint32(buf[off+2])<<16 | uint32(buf[off+3])<<24)
					} else {
						sum += uint64(buf[off]) | uint64(buf[off+1])<<8 | uint64(buf[off+2])<<16 | uint64(buf[off+3])<<24 |
							uint64(buf[off+4])<<32 | uint64(buf[off+5])<<40 | uint64(buf[off+6])<<48 | uint64(buf[off+7])<<56
					}
					off += width
				}
				codecBenchU64 = sum
			}
		})
	}
}

// BenchmarkGetU32/U64 measure the generic single-value API, which is not
// used by production callers today, against a checked stdlib read. These
// prevent assumptions about the generic type switch leaking into API
// design decisions.
func BenchmarkGetU32(b *testing.B) {
	for _, order := range []struct {
		name  string
		order binary.ByteOrder
	}{
		{"little", binary.LittleEndian},
		{"big", binary.BigEndian},
	} {
		b.Run(order.name+"/generic", func(b *testing.B) {
			buf := make([]byte, 4)
			order.order.PutUint32(buf, 0x12345678)
			b.ReportAllocs()
			for b.Loop() {
				v, ok := Get[uint32](order.order, buf)
				if !ok {
					b.Fatal("short")
				}
				codecBenchU64 += uint64(v)
			}
		})
		b.Run(order.name+"/stdlib-checked", func(b *testing.B) {
			buf := make([]byte, 4)
			order.order.PutUint32(buf, 0x12345678)
			b.ReportAllocs()
			for b.Loop() {
				if len(buf) < 4 {
					b.Fatal("short")
				}
				codecBenchU64 += uint64(order.order.Uint32(buf))
			}
		})
	}
}

func BenchmarkGetU64(b *testing.B) {
	for _, order := range []struct {
		name  string
		order binary.ByteOrder
	}{
		{"little", binary.LittleEndian},
		{"big", binary.BigEndian},
	} {
		b.Run(order.name+"/generic", func(b *testing.B) {
			buf := make([]byte, 8)
			order.order.PutUint64(buf, 0x1122334455667788)
			b.ReportAllocs()
			for b.Loop() {
				v, ok := Get[uint64](order.order, buf)
				if !ok {
					b.Fatal("short")
				}
				codecBenchU64 += v
			}
		})
		b.Run(order.name+"/stdlib-checked", func(b *testing.B) {
			buf := make([]byte, 8)
			order.order.PutUint64(buf, 0x1122334455667788)
			b.ReportAllocs()
			for b.Loop() {
				if len(buf) < 8 {
					b.Fatal("short")
				}
				codecBenchU64 += order.order.Uint64(buf)
			}
		})
	}
}

// BenchmarkAppendBlob covers the framing helper at realistic payload sizes.
func BenchmarkAppendBlob(b *testing.B) {
	for _, size := range []int{16, 256, 4096} {
		payload := make([]byte, size)
		buf := make([]byte, 0, 4+size)
		b.Run(fmt.Sprintf("bytes=%d", size), func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				codecBenchBytes = AppendBlob(buf[:0], binary.LittleEndian, payload)
			}
		})
	}
}

// BenchmarkWriterResetFrame models reuse of a protocol-sized staging buffer.
func BenchmarkWriterResetFrame(b *testing.B) {
	buf := make([]byte, 0, 512)
	w := NewWriter(binary.BigEndian, buf)
	key := []byte("bench-key-0001")
	value := make([]byte, 256)
	b.ReportAllocs()
	for b.Loop() {
		w.Reset(buf[:0])
		w.U8(1)
		w.U32(256)
		w.Blob(key)
		w.Blob(value)
		w.U64(42)
		codecBenchBytes = w.Out()
	}
}
