package testkit

import (
	"errors"
	"testing"

	"github.com/kraytos17/cinder/internal/net/protocol"
)

// TestScriptReplaysInOrder covers queue consumption and the OK default.
func TestScriptReplaysInOrder(t *testing.T) {
	s := NewScript()
	s.Fail("n2", errors.New("x"), errors.New("y"))
	if _, err := s.Send("n2", protocol.Request{}); err == nil || err.Error() != "x" {
		t.Fatalf("got %v", err)
	}
	if _, err := s.Send("n2", protocol.Request{}); err == nil || err.Error() != "y" {
		t.Fatalf("got %v", err)
	}
	if res, err := s.Send("n2", protocol.Request{}); err != nil || res.Status != protocol.StatusOK {
		t.Fatalf("unscripted must answer OK: %+v %v", res, err)
	}
	if got := s.Count("n2"); got != 3 {
		t.Fatalf("count = %d", got)
	}
}

// TestStoresTombstoneFallback covers the replicate-then-remove path the
// replication tests depend on.
func TestStoresTombstoneFallback(t *testing.T) {
	f := NewStores("n2")
	req := protocol.Request{Opcode: protocol.OpReplicate, Key: []byte("k"), Value: []byte("v"), Version: 3, WriterNodeHash: 7}
	if res, err := f.Send("n2", req); err != nil || res.Status != protocol.StatusOK {
		t.Fatalf("put: %+v %v", res, err)
	}
	req.Tombstone = true
	if res, err := f.Send("n2", req); err != nil || res.Status != protocol.StatusOK {
		t.Fatalf("tombstone: %+v %v", res, err)
	}
	e, ok := f.Stores["n2"].GetVersioned("k")
	if !ok || !e.Tombstone {
		t.Fatalf("want tombstone, got %+v %v", e, ok)
	}
	f.Fail["n2"] = true
	if _, err := f.Send("n2", req); err == nil {
		t.Fatal("failed node must error")
	}
	if _, err := f.Send("ghost", req); err == nil {
		t.Fatal("unknown node must error")
	}
}
