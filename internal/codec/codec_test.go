package codec

import (
	"encoding/binary"
	"testing"
)

// TestAppendGetRoundTrip covers every width in both endiannesses,
// including short-input rejection on Get.
func TestAppendGetRoundTrip(t *testing.T) {
	for _, order := range []binary.ByteOrder{binary.BigEndian, binary.LittleEndian} {
		var b []byte
		b = Append(b, order, uint8(0xAB))
		b = Append(b, order, uint16(0xCDEF))
		b = Append(b, order, uint32(0x12345678))
		b = Append(b, order, uint64(0x1122334455667788))
		if len(b) != 1+2+4+8 {
			t.Fatalf("order %v: len %d", order, len(b))
		}
		if v, ok := Get[uint8](order, b[0:]); !ok || v != 0xAB {
			t.Fatalf("get8: %x %v", v, ok)
		}
		if v, ok := Get[uint16](order, b[1:]); !ok || v != 0xCDEF {
			t.Fatalf("get16: %x %v", v, ok)
		}
		if v, ok := Get[uint32](order, b[3:]); !ok || v != 0x12345678 {
			t.Fatalf("get32: %x %v", v, ok)
		}
		if v, ok := Get[uint64](order, b[7:]); !ok || v != 0x1122334455667788 {
			t.Fatalf("get64: %x %v", v, ok)
		}
		for _, short := range [][]byte{nil, {}, {1}, b[:2], b[:6], b[:7]} {
			if _, ok := Get[uint64](order, short); ok {
				t.Fatalf("order %v: short %d accepted", order, len(short))
			}
		}
		// Reader cursor over the same layout.
		r := NewReader(order, b)
		if v, ok := r.U8(); !ok || v != 0xAB {
			t.Fatalf("u8: %x %v", v, ok)
		}
		bb, ok := r.Bytes(2)
		if !ok || order.Uint16(bb) != 0xCDEF {
			t.Fatalf("bytes2: %x %v", bb, ok)
		}
		if v, ok := r.U32(); !ok || v != 0x12345678 {
			t.Fatalf("u32: %x %v", v, ok)
		}
		if v, ok := r.U64(); !ok || v != 0x1122334455667788 {
			t.Fatalf("u64: %x %v", v, ok)
		}
		if r.Remaining() != 0 {
			t.Fatalf("order %v: %d trailing", order, r.Remaining())
		}
	}
}

// TestBlobRoundTrip covers length-prefixed blobs, including truncation.
func TestBlobRoundTrip(t *testing.T) {
	for _, order := range []binary.ByteOrder{binary.BigEndian, binary.LittleEndian} {
		w := NewWriter(order, nil)
		w.Blob([]byte("hello"))
		w.Blob(nil)
		w.Blob([]byte{})
		r := NewReader(order, w.Out())
		for _, want := range []string{"hello", "", ""} {
			got, ok := r.Blob()
			if !ok || string(got) != want {
				t.Fatalf("order %v: got %q %v", order, got, ok)
			}
		}
		if r.Remaining() != 0 {
			t.Fatalf("order %v: %d trailing", order, r.Remaining())
		}
		trunc := w.Out()[:len(w.Out())-1]
		rr := NewReader(order, trunc)
		if _, ok := rr.Blob(); !ok {
			t.Fatal("first blob must parse")
		}
		if _, ok := rr.Blob(); !ok {
			t.Fatal("empty blob must parse")
		}
		if _, ok := rr.Blob(); ok {
			t.Fatal("truncated blob must fail")
		}
	}
}

// TestWriterReaderEncoding checks the cursor against the specified pattern
// (u32 length + raw bytes).
func TestWriterReaderEncoding(t *testing.T) {
	order := binary.LittleEndian
	var manual []byte
	manual = order.AppendUint32(manual, uint32(len("target")))
	manual = append(manual, "target"...)
	manual = order.AppendUint64(manual, 0xDEAD)
	w := NewWriter(order, nil)
	w.Blob([]byte("target"))
	w.U64(0xDEAD)
	if string(w.Out()) != string(manual) {
		t.Fatalf("encoding differs: %x vs %x", w.Out(), manual)
	}
	r := NewReader(order, w.Out())
	b, ok := r.Blob()
	if !ok || string(b) != "target" {
		t.Fatalf("blob: %q %v", b, ok)
	}
	if v, ok := r.U64(); !ok || v != 0xDEAD {
		t.Fatalf("u64: %x %v", v, ok)
	}
}
