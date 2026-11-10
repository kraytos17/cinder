package ae

import (
	"encoding/binary"
	"io"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/kraytos17/cinder/internal/codec"

	"github.com/zeebo/xxh3"

	"github.com/kraytos17/cinder/internal/expiry"
	"github.com/kraytos17/cinder/internal/net/protocol"
	"github.com/kraytos17/cinder/internal/repl"
)

// Journal is the durable hint queue (add/del records, torn-tail truncation
// on open). Record framing: len u32 + XXH3 u64 + payload. ADD payload: op
// u8=1, sys_expiry u64, target, key, value (+lengths), version, writer,
// entry_expiry u64, tombstone u8. DEL payload: op u8=2, target, key,
// version. Recovered ADD records replay as Replicate requests.
type Journal struct {
	mu      sync.Mutex
	f       *os.File
	path    string
	records uint64
}

const (
	journalOpAdd = 1
	journalOpDel = 2
	// hintTTL is the lifetime assigned to recovered hints.
	hintTTL = 30 * time.Second
)

// JournalCompactRecords bounds the journal: after this many records it is
// rewritten from the live queue, so a
// long outage with persistence on cannot grow the file without bound.
const JournalCompactRecords = 4096

// NeedsCompaction reports whether the record count passed the rewrite
// bound. Checked by the replication manager after every journaled enqueue.
func (j *Journal) NeedsCompaction() bool {
	j.mu.Lock()
	defer j.mu.Unlock()
	return j.f != nil && j.records >= JournalCompactRecords
}

// snapshotAllHints exceeds any queue depth (queues are 1024-slot bounded),
// so the compact snapshot cannot truncate live hints.
const snapshotAllHints = 1 << 30

// Compact rewrites the journal from the queue's live snapshot, dropping
// replayed (DEL) and expired hints. Atomic via tmp file + rename, like the
// snapshot writer; concurrent appends block on the journal lock for the
// duration. On any error the old journal is left intact.
func (j *Journal) Compact(q *repl.HintQueue) error {
	j.mu.Lock()
	defer j.mu.Unlock()
	if j.f == nil {
		return nil
	}

	live := q.Snapshot(time.Now(), snapshotAllHints)
	tmpPath := j.path + ".tmp"
	tmp, err := os.OpenFile(tmpPath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o644)
	if err != nil {
		return err
	}
	for _, h := range live {
		exp, ok := h.ExpiresAt.Time()
		if !ok {
			exp = time.Now().Add(hintTTL)
		}

		rec := frameJournalRecord(encodeJournalAddPayload(h.Target, h.Req, uint64(exp.UnixMilli())))
		if _, err := tmp.Write(rec); err != nil {
			tmp.Close()
			os.Remove(tmpPath)
			return err
		}
	}
	if err := tmp.Sync(); err != nil {
		tmp.Close()
		os.Remove(tmpPath)
		return err
	}
	if err := tmp.Close(); err != nil {
		os.Remove(tmpPath)
		return err
	}
	if err := os.Rename(tmpPath, j.path); err != nil {
		return err
	}

	f, err := os.OpenFile(j.path, os.O_RDWR|os.O_CREATE, 0o644)
	if err != nil {
		return err
	}
	if _, err := f.Seek(0, io.SeekEnd); err != nil {
		f.Close()
		return err
	}

	j.f.Close()
	j.f = f
	j.records = uint64(len(live))
	return nil
}

// OpenJournal opens (creating) the journal and replays live hints into q.
// A torn tail truncates. Returns the recovered count.
func OpenJournal(path string, q *repl.HintQueue) (*Journal, int, error) {
	if dir := filepath.Dir(path); dir != "" {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return nil, 0, err
		}
	}

	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0o644)
	if err != nil {
		return nil, 0, err
	}

	j := &Journal{f: f, path: path}
	n, err := j.recover(q)
	if err != nil {
		f.Close()
		return nil, 0, err
	}
	return j, n, nil
}

func (j *Journal) recover(q *repl.HintQueue) (int, error) {
	data, err := os.ReadFile(j.path)
	if err != nil {
		return 0, err
	}

	nowMs := uint64(time.Now().UnixMilli())
	off := 0
	decoded := 0
	for off < len(data) {
		if len(data)-off < 12 {
			break // torn length: truncate
		}

		ln := int(binary.LittleEndian.Uint32(data[off:]))
		sum := binary.LittleEndian.Uint64(data[off+4:])
		off += 12
		if ln < 0 || len(data)-off < ln {
			break // torn payload / corrupt length: truncate
		}

		payload := data[off : off+ln]
		if xxh3.Hash(payload) != sum {
			break // checksum mismatch: truncate (torn tail)
		}

		off += ln
		if h, ok, del := decodeJournalRecord(payload, nowMs); ok {
			decoded++
			if del == nil {
				q.Push(h)
			} else {
				q.RemoveMatch(del.target, del.key, del.version)
			}
		} else {
			break // structurally invalid: truncate
		}
	}
	if off < len(data) {
		if err := os.Truncate(j.path, int64(off)); err != nil {
			return 0, err
		}
	}

	j.records = uint64(decoded)
	// Append at the end: recovery read via a separate descriptor.
	if _, err := j.f.Seek(0, io.SeekEnd); err != nil {
		return 0, err
	}
	return q.Len(), nil
}

type journalDel struct {
	target  string
	key     string
	version uint64
}

func decodeJournalRecord(payload []byte, nowMs uint64) (repl.Hint, bool, *journalDel) {
	c := codec.NewReader(binary.LittleEndian, payload)
	op, ok := c.U8()
	if !ok {
		return repl.Hint{}, false, nil
	}
	if op == journalOpAdd {
		exp, ok := c.U64()
		if !ok {
			return repl.Hint{}, false, nil
		}

		tlen, ok := c.U32()
		if !ok {
			return repl.Hint{}, false, nil
		}

		tb, ok := c.Bytes(int(tlen))
		if !ok {
			return repl.Hint{}, false, nil
		}
		klen, ok := c.U32()
		if !ok {
			return repl.Hint{}, false, nil
		}

		kb, ok := c.Bytes(int(klen))
		if !ok {
			return repl.Hint{}, false, nil
		}

		vlen, ok := c.U32()
		if !ok {
			return repl.Hint{}, false, nil
		}

		vb, ok := c.Bytes(int(vlen))
		if !ok {
			return repl.Hint{}, false, nil
		}

		ver, ok := c.U64()
		if !ok {
			return repl.Hint{}, false, nil
		}

		writer, ok := c.U64()
		if !ok {
			return repl.Hint{}, false, nil
		}

		var entryExp uint64
		var tomb uint8
		if c.Remaining() > 0 {
			if entryExp, ok = c.U64(); !ok {
				return repl.Hint{}, false, nil
			}
			if tomb, ok = c.U8(); !ok || tomb > 1 || c.Remaining() != 0 {
				return repl.Hint{}, false, nil
			}
		} else {
			// Legacy records (pre-tombstone): live writes.
			tomb = 0
		}
		if exp <= nowMs {
			return repl.Hint{}, true, nil // expired: skip, not corrupt
		}

		req := protocol.Request{Opcode: protocol.OpReplicate, Key: append([]byte(nil), kb...), Value: append([]byte(nil), vb...), Version: ver, WriterNodeHash: writer, Tombstone: tomb != 0}
		req.ExpiresAt = expiry.FromUnixMilli(int64(entryExp))
		return repl.Hint{Target: string(tb), Req: req, ExpiresAt: expiry.At(time.Now().Add(hintTTL))}, true, nil
	}
	if op == journalOpDel {
		tlen, ok := c.U32()
		if !ok {
			return repl.Hint{}, false, nil
		}

		tb, ok := c.Bytes(int(tlen))
		if !ok {
			return repl.Hint{}, false, nil
		}

		klen, ok := c.U32()
		if !ok {
			return repl.Hint{}, false, nil
		}

		kb, ok := c.Bytes(int(klen))
		if !ok {
			return repl.Hint{}, false, nil
		}

		ver, ok := c.U64()
		if !ok || c.Remaining() != 0 {
			return repl.Hint{}, false, nil
		}
		return repl.Hint{}, true, &journalDel{target: string(tb), key: string(kb), version: ver}
	}
	return repl.Hint{}, false, nil
}

func encodeJournalAddPayload(target string, req protocol.Request, sysExpiryMs uint64) []byte {
	entryExp := uint64(req.ExpiresAt.UnixMilli())

	var tomb uint8
	if req.Tombstone {
		tomb = 1
	}

	payload := make([]byte, 0, 1+8+4+len(target)+4+len(req.Key)+4+len(req.Value)+8+8+8+1)
	payload = append(payload, journalOpAdd)
	payload = codec.Append(payload, binary.LittleEndian, sysExpiryMs)
	payload = codec.AppendBlob(payload, binary.LittleEndian, target)
	payload = codec.AppendBlob(payload, binary.LittleEndian, req.Key)
	payload = codec.AppendBlob(payload, binary.LittleEndian, req.Value)
	payload = codec.Append(payload, binary.LittleEndian, req.Version)
	payload = codec.Append(payload, binary.LittleEndian, req.WriterNodeHash)
	payload = codec.Append(payload, binary.LittleEndian, entryExp)
	payload = append(payload, tomb)
	return payload
}

func encodeJournalAdd(target string, req protocol.Request, sysExpiryMs uint64) []byte {
	return frameJournalRecord(encodeJournalAddPayload(target, req, sysExpiryMs))
}

func encodeJournalDel(target, key string, version uint64) []byte {
	payload := make([]byte, 0, 1+4+len(target)+4+len(key)+8)
	payload = append(payload, journalOpDel)
	payload = codec.AppendBlob(payload, binary.LittleEndian, target)
	payload = codec.AppendBlob(payload, binary.LittleEndian, key)
	payload = codec.Append(payload, binary.LittleEndian, version)
	return frameJournalRecord(payload)
}

func frameJournalRecord(payload []byte) []byte {
	out := make([]byte, 0, 12+len(payload))
	out = codec.AppendLen(out, binary.LittleEndian, len(payload))
	out = codec.Append(out, binary.LittleEndian, xxh3.Hash(payload))
	return append(out, payload...)
}

// AppendAdd journals one queued hint. No-op on write error (in-memory
// queue remains authoritative).
func (j *Journal) AppendAdd(h repl.Hint) {
	j.mu.Lock()
	defer j.mu.Unlock()
	if j.f == nil {
		return
	}

	sysExp := uint64(time.Now().Add(hintTTL).UnixMilli())
	rec := encodeJournalAdd(h.Target, h.Req, sysExp)
	if _, err := j.f.Write(rec); err == nil {
		j.records++
	}
}

// AppendDel journals a successful replay so recovery drops the hint.
func (j *Journal) AppendDel(target, key string, version uint64) {
	j.mu.Lock()
	defer j.mu.Unlock()
	if j.f == nil {
		return
	}
	if _, err := j.f.Write(encodeJournalDel(target, key, version)); err == nil {
		j.records++
	}
}

// Sync flushes journal buffers to disk.
func (j *Journal) Sync() error {
	j.mu.Lock()
	defer j.mu.Unlock()
	if j.f == nil {
		return nil
	}
	return j.f.Sync()
}

// Close syncs and closes.
func (j *Journal) Close() error {
	j.mu.Lock()
	defer j.mu.Unlock()
	if j.f == nil {
		return nil
	}

	err := j.f.Sync()
	if cerr := j.f.Close(); err == nil {
		err = cerr
	}

	j.f = nil
	return err
}

// Records returns the lifetime record count (appends, including dels).
func (j *Journal) Records() uint64 {
	j.mu.Lock()
	defer j.mu.Unlock()
	return j.records
}
