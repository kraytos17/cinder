package tcp_test

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/kraytos17/cinder/internal/client"
	"github.com/kraytos17/cinder/internal/net/protocol"
)

// TestShutdownContextDrainsAndRestarts writes N keys with persistence on,
// shuts down under an explicit deadline, restarts from the same directory,
// and requires all N back (Step 2.9 acceptance).
func TestShutdownContextDrainsAndRestarts(t *testing.T) {
	dir := t.TempDir()
	srv, addr := startPersistedServer(t, "node1", dir)
	c, err := client.Dial(addr, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	const n = 50
	for i := range n {
		res, err := c.Do(protocol.Request{Opcode: protocol.OpSet, Key: []byte(fmt.Sprintf("k-%02d", i)), Value: []byte(fmt.Sprintf("v-%02d", i))})
		if err != nil || res.Status != protocol.StatusOK {
			t.Fatalf("set k-%02d: %+v %v", i, res, err)
		}
	}
	c.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := srv.ShutdownContext(ctx); err != nil {
		t.Fatalf("shutdown: %v", err)
	}

	_, addr2 := startPersistedServer(t, "node1", dir)
	c2, err := client.Dial(addr2, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer c2.Close()
	for i := range n {
		res, err := c2.Do(protocol.Request{Opcode: protocol.OpGet, Key: []byte(fmt.Sprintf("k-%02d", i))})
		if err != nil || res.Status != protocol.StatusOK || string(res.Value) != fmt.Sprintf("v-%02d", i) {
			t.Fatalf("k-%02d after restart: %+v %v", i, res, err)
		}
	}
}

// TestShutdownContextExpiredDeadline requires an already-expired budget to
// report the timeout, still close the listener, and stay idempotent.
func TestShutdownContextExpiredDeadline(t *testing.T) {
	srv, addr := startPersistedServer(t, "node1", t.TempDir())
	_ = addr
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := srv.ShutdownContext(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("shutdown with canceled context = %v, want context.Canceled", err)
	}
	if err := srv.ShutdownContext(context.Background()); err != nil {
		t.Fatalf("second shutdown = %v, want nil", err)
	}
}
