package tcp_test

import (
	"bytes"
	"encoding/json"
	"testing"

	"github.com/rs/zerolog"

	"github.com/kraytos17/cinder/internal/log"
	"github.com/kraytos17/cinder/internal/net/protocol"
	"github.com/kraytos17/cinder/internal/net/tcp"
	"github.com/kraytos17/cinder/internal/store"
)

// Denied requests log a warn line carrying the request's trace IDs;
// debug request lines appear only at debug and carry the opcode.
func TestDispatchLogsCarryTraceIDs(t *testing.T) {
	var buf bytes.Buffer
	l := log.New(&buf, zerolog.DebugLevel)
	srv := tcp.NewServer("n1", 0, store.NewLRUStore(1<<20, nil), nil)
	srv.SetLogger(&l)
	if err := srv.ConfigureSecrets("s3cret", ""); err != nil {
		t.Fatal(err)
	}
	denied := srv.Dispatch(protocol.Request{Opcode: protocol.OpGossip, TraceID: 7, SpanID: 9})
	if denied.Status != protocol.StatusPermissionDenied {
		t.Fatalf("status = %v", denied.Status)
	}
	var lines []map[string]any
	for line := range bytes.SplitSeq(buf.Bytes(), []byte("\n")) {
		if len(bytes.TrimSpace(line)) == 0 {
			continue
		}
		var m map[string]any
		if err := json.Unmarshal(line, &m); err != nil {
			t.Fatalf("not JSON: %v\n%s", err, line)
		}
		lines = append(lines, m)
	}
	found := false
	for _, m := range lines {
		if m["message"] == "permission denied" {
			found = true
			if m["trace_id"] != float64(7) || m["span_id"] != float64(9) {
				t.Fatalf("denied line missing trace IDs: %v", m)
			}
			if m["opcode"] != "gossip" {
				t.Fatalf("denied line missing opcode: %v", m)
			}
		}
	}
	if !found {
		t.Fatalf("no denied line in %d lines", len(lines))
	}
	// Debug request line present at debug level.
	srv.Dispatch(protocol.Request{Opcode: protocol.OpPing, TraceID: 1, SpanID: 2})
	if !bytes.Contains(buf.Bytes(), []byte(`"message":"request"`)) {
		t.Fatal("no debug request line")
	}
}

// Default (Nop) logger: dispatch works and stays silent.
func TestDispatchNopLoggerSilent(t *testing.T) {
	srv := tcp.NewServer("n1", 0, store.NewLRUStore(1<<20, nil), nil)
	if res := srv.Dispatch(protocol.Request{Opcode: protocol.OpPing}); res.Status != protocol.StatusOK {
		t.Fatalf("ping = %v", res.Status)
	}
}
