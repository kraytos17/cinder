package ae

import (
	"bytes"
	"testing"
	"time"

	"github.com/kraytos17/cinder/internal/expiry"
	"github.com/kraytos17/cinder/internal/net/protocol"
	"github.com/kraytos17/cinder/internal/repl"
)

// fuzzJournalNowMs anchors expiry decisions so decoding is a pure function
// of (image, nowMs). Seeds use sys/entry expiries comfortably on either
// side of it.
const fuzzJournalNowMs = uint64(1_700_000_000_000)

func journalImageSeeds() [][]byte {
	addPayload := func(target, key, val string, ver, writer uint64, tomb bool, sysExp, entryExp uint64) []byte {
		t := repl.Hint{Target: target, Req: protocol.Request{
			Opcode: protocol.OpReplicate, Key: []byte(key), Value: []byte(val),
			Version: ver, WriterNodeHash: writer, Tombstone: tomb,
		}}
		if entryExp != 0 {
			t.Req.ExpiresAt = expiry.At(time.UnixMilli(int64(entryExp)))
		}
		return frameJournalRecord(encodeJournalAddPayload(target, t.Req, sysExp))
	}
	live := fuzzJournalNowMs + 3_600_000
	out := make([][]byte, 0, 16)
	out = append(out, []byte{})
	out = append(out, []byte{0x01, 0x02, 0x03}) // torn header
	out = append(out, addPayload("n2", "a", "1", 5, 1, false, live, 0))
	out = append(out, addPayload("n2", "t", "v", 6, 2, true, live, 0))
	out = append(out, addPayload("n2", "gone", "old", 7, 3, false, fuzzJournalNowMs-1_000, 0))   // expired sys: discard, not corrupt
	out = append(out, addPayload("n2", "stale", "v", 8, 4, false, live, fuzzJournalNowMs-1_000)) // expired entry: discard
	out = append(out, encodeJournalDel("n2", "a", 5))
	// Legacy ADD without the tombstone tail.
	legacy := encodeJournalAddPayload("n3", protocol.Request{Opcode: protocol.OpReplicate, Key: []byte("k"), Value: []byte("v"), Version: 1, WriterNodeHash: 1}, live)
	out = append(out, frameJournalRecord(legacy[:len(legacy)-9]))
	// Two framed records back to back.
	two := append([]byte{}, out[2]...)
	out = append(out, append(two, out[5]...))
	// Torn tail: valid frame cut mid-payload, and a bad checksum.
	out = append(out, out[2][:len(out[2])-3])
	bad := append([]byte(nil), out[2]...)
	bad[len(bad)-1] ^= 0xff
	out = append(out, bad)
	// Unknown op and truncated DEL payload.
	out = append(out, frameJournalRecord([]byte{0x7F, 0, 0, 0, 0}))
	del := encodeJournalDel("n2", "b", 9)
	out = append(out, del[:len(del)-2])
	out = append(out, bytes.Repeat([]byte{0xFF}, 64))
	return out
}

// FuzzJournalImage feeds arbitrary bytes to the journal image decoder. It
// must never panic; every image must decode to a valid prefix whose
// re-decode is identical (truncation fixed point). Expiry uses the fixed
// fuzz clock so results are deterministic.
func FuzzJournalImage(f *testing.F) {
	for _, s := range journalImageSeeds() {
		f.Add(s)
	}
	f.Fuzz(func(t *testing.T, data []byte) {
		ops, valid := decodeJournalImage(data, fuzzJournalNowMs)
		if valid < 0 || valid > len(data) {
			t.Fatalf("valid prefix %d outside image of %d bytes", valid, len(data))
		}
		again, validAgain := decodeJournalImage(data[:valid], fuzzJournalNowMs)
		if validAgain != valid {
			t.Fatalf("re-decode valid prefix moved: %d -> %d", valid, validAgain)
		}
		if !journalOpsEqual(ops, again) {
			t.Fatalf("ops changed on re-decode of valid prefix")
		}
	})
}

func journalOpsEqual(a, b []decodedJournalRecord) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i].discard != b[i].discard {
			return false
		}
		ad, bd := a[i].del, b[i].del
		if (ad == nil) != (bd == nil) {
			return false
		}
		if ad != nil && (*ad != *bd) {
			return false
		}
		ah, bh := a[i].hint, b[i].hint
		// Hint.ExpiresAt is recovery-stamped from the pass clock; with a
		// fixed nowMs both decodes agree, so compare it too.
		if ah.Target != bh.Target || !bytes.Equal(ah.Req.Key, bh.Req.Key) ||
			!bytes.Equal(ah.Req.Value, bh.Req.Value) || ah.Req.Version != bh.Req.Version ||
			ah.Req.WriterNodeHash != bh.Req.WriterNodeHash || ah.Req.Tombstone != bh.Req.Tombstone ||
			ah.Req.ExpiresAt.UnixMilli() != bh.Req.ExpiresAt.UnixMilli() ||
			ah.ExpiresAt.UnixMilli() != bh.ExpiresAt.UnixMilli() {
			return false
		}
	}
	return true
}
